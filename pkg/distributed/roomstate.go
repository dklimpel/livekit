// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package distributed

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/psrpc/pkg/bus"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

const publishTimeout = 3 * time.Second

// roomState is the distributed part of a room on this node. It mirrors the
// participants of the other nodes into the local rtc.Room and publishes the
// local participants to the other nodes.
type roomState struct {
	m      *Manager
	room   *rtc.Room
	logger logger.Logger

	ctx    context.Context
	cancel context.CancelFunc

	lock         sync.Mutex
	remote       map[livekit.ParticipantIdentity]*RemoteParticipant
	conns        map[livekit.NodeID]*relayConn
	lastMetadata string
	lastSpeakers map[livekit.ParticipantID]speaker

	closingFromRemote atomic.Bool
	closeOnce         sync.Once
}

var _ rtc.DistributedRoomListener = (*roomState)(nil)

func newRoomState(m *Manager, room *rtc.Room) *roomState {
	rs := &roomState{
		m:            m,
		room:         room,
		logger:       room.Logger().WithComponent("distributed"),
		remote:       make(map[livekit.ParticipantIdentity]*RemoteParticipant),
		conns:        make(map[livekit.NodeID]*relayConn),
		lastMetadata: room.ToProto().Metadata,
	}
	rs.ctx, rs.cancel = context.WithCancel(m.ctx)
	return rs
}

func (rs *roomState) nodeID() livekit.NodeID {
	return rs.m.currentNode.NodeID()
}

func (rs *roomState) start() {
	sub, err := subscribe(rs.ctx, rs.m.bus, roomChannel(rs.room.Name()))
	if err != nil {
		rs.logger.Errorw("could not subscribe to distributed room channel", err)
		return
	}
	rs.room.SetDistributedListener(rs)

	go rs.readWorker(sub)
	go rs.speakerWorker()
	go rs.nodeCheckWorker()

	// ask the other nodes for their participants
	rs.publish(&message{Type: msgSyncRequest})
	rs.publishAllLocalParticipants()
}

func (rs *roomState) close() {
	rs.closeOnce.Do(func() {
		rs.cancel()

		rs.lock.Lock()
		conns := rs.conns
		rs.conns = make(map[livekit.NodeID]*relayConn)
		remote := rs.remote
		rs.remote = make(map[livekit.ParticipantIdentity]*RemoteParticipant)
		rs.lock.Unlock()

		for _, rp := range remote {
			_ = rp.Close(false, types.ParticipantCloseReasonRoomClosed, false)
		}
		for _, c := range conns {
			c.close()
		}
	})
}

func (rs *roomState) publish(msg *message) {
	rs.publishWithContext(rs.ctx, msg)
}

func (rs *roomState) publishWithContext(ctx context.Context, msg *message) {
	msg.Node = rs.nodeID()
	msg.Room = rs.room.Name()
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := publish(ctx, rs.m.bus, roomChannel(rs.room.Name()), msg); err != nil && rs.ctx.Err() == nil {
		rs.logger.Warnw("could not publish distributed room message", err, "type", msg.Type)
	}
}

// --- local -> remote ---

func (rs *roomState) onLocalRoomClosed(reason types.RoomCloseReason) {
	// publish with a fresh context, the room context is about to be cancelled
	ctx := context.Background()
	if reason == types.RoomCloseReasonAPIDelete && !rs.closingFromRemote.Load() {
		rs.publishWithContext(ctx, &message{Type: msgRoomClose})
	}
	rs.publishWithContext(ctx, &message{Type: msgNodeLeft})
}

func (rs *roomState) onParticipantChanged(p types.Participant) {
	if rtc.IsRemoteParticipant(p) {
		return
	}
	if p.IsDisconnected() {
		rs.onLocalParticipantLeft(p)
	}
	rs.publishLocalParticipant(p)
}

func (rs *roomState) publishLocalParticipant(p types.Participant) {
	info := p.ToProto()
	if p.IsDisconnected() {
		info = proto.CloneOf(info)
		info.State = livekit.ParticipantInfo_DISCONNECTED
	}
	perm, _ := p.SubscriptionPermission()
	perm = rs.resolvePermissionIdentities(perm)
	rs.publish(&message{
		Type:                   msgParticipantUpdate,
		Participant:            marshalProto(info),
		SubscriptionPermission: marshalProto(perm),
	})
}

// resolvePermissionIdentities fills in identities for permissions given by
// participant ID, other nodes evaluate permissions by identity.
func (rs *roomState) resolvePermissionIdentities(perm *livekit.SubscriptionPermission) *livekit.SubscriptionPermission {
	if perm == nil {
		return nil
	}
	perm = proto.CloneOf(perm)
	for _, tp := range perm.TrackPermissions {
		if tp.ParticipantIdentity == "" && tp.ParticipantSid != "" {
			tp.ParticipantIdentity = string(rs.room.GetParticipantIdentityByID(livekit.ParticipantID(tp.ParticipantSid)))
		}
	}
	return perm
}

// OnLocalSubscriptionPermissionChanged implements rtc.DistributedRoomListener.
func (rs *roomState) OnLocalSubscriptionPermissionChanged(p types.LocalParticipant) {
	rs.publishLocalParticipant(p)
}

func (rs *roomState) publishAllLocalParticipants() {
	for _, p := range rs.room.GetParticipants() {
		rs.publishLocalParticipant(p)
	}
}

func (rs *roomState) onLocalParticipantLeft(p types.Participant) {
	// the identity might already have re-joined (e.g. duplicate identity replacement)
	if lp := rs.room.GetParticipant(p.Identity()); lp != nil && lp.ID() != p.ID() {
		return
	}
	if err := rs.m.store.RemoveParticipantNode(rs.ctx, rs.room.Name(), p.Identity(), rs.nodeID()); err != nil && rs.ctx.Err() == nil {
		rs.logger.Warnw("could not remove participant placement", err, "participant", p.Identity())
	}
}

// OnLocalDataPacket implements rtc.DistributedRoomListener.
func (rs *roomState) OnLocalDataPacket(_ types.LocalParticipant, kind livekit.DataPacket_Kind, dp *livekit.DataPacket) {
	if !rs.hasRemoteNodes() {
		return
	}
	rs.publish(&message{
		Type:       msgDataPacket,
		DataKind:   kind,
		DataPacket: marshalProto(dp),
	})
}

func (rs *roomState) onRoomUpdated() {
	metadata := rs.room.ToProto().Metadata
	rs.lock.Lock()
	changed := metadata != rs.lastMetadata
	rs.lastMetadata = metadata
	rs.lock.Unlock()
	if changed {
		rs.publish(&message{Type: msgRoomMetadata, Metadata: metadata})
	}
}

func (rs *roomState) hasRemoteNodes() bool {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return len(rs.remote) != 0
}

// speakerWorker publishes the audio levels of local speakers, so that remote
// participants take part in the active speaker detection of the other nodes.
func (rs *roomState) speakerWorker() {
	interval := time.Duration(rs.m.fullConf.Audio.UpdateInterval) * time.Millisecond
	if interval <= 0 {
		interval = 400 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-rs.ctx.Done():
			return
		case <-ticker.C:
		}

		active := rs.room.GetLocalActiveSpeakers()
		next := make(map[livekit.ParticipantID]speaker, len(active))
		var speakers []speaker
		for _, s := range active {
			sp := speaker{ParticipantID: livekit.ParticipantID(s.Sid), Level: s.Level, Active: true}
			next[sp.ParticipantID] = sp
			speakers = append(speakers, sp)
		}
		rs.lock.Lock()
		for pID := range rs.lastSpeakers {
			if _, ok := next[pID]; !ok {
				speakers = append(speakers, speaker{ParticipantID: pID})
			}
		}
		rs.lastSpeakers = next
		rs.lock.Unlock()

		// active speakers are refreshed periodically, remote levels time out otherwise
		if len(speakers) != 0 && rs.hasRemoteNodes() {
			rs.publish(&message{Type: msgSpeakers, Speakers: speakers})
		}
	}
}

// --- remote -> local ---

func (rs *roomState) readWorker(sub bus.Subscription[*wrapperspb.BytesValue]) {
	defer func() { _ = sub.Close() }()
	for {
		select {
		case <-rs.ctx.Done():
			return
		case v, ok := <-sub.Channel():
			if !ok {
				return
			}
			msg, err := decode(v)
			if err != nil {
				rs.logger.Warnw("could not decode distributed room message", err)
				continue
			}
			if msg.Node == rs.nodeID() {
				continue
			}
			rs.handleMessage(msg)
		}
	}
}

func (rs *roomState) handleMessage(msg *message) {
	switch msg.Type {
	case msgParticipantUpdate:
		info := &livekit.ParticipantInfo{}
		if err := proto.Unmarshal(msg.Participant, info); err != nil {
			rs.logger.Warnw("could not decode participant", err)
			return
		}
		var perm *livekit.SubscriptionPermission
		if len(msg.SubscriptionPermission) != 0 {
			perm = &livekit.SubscriptionPermission{}
			if err := proto.Unmarshal(msg.SubscriptionPermission, perm); err != nil {
				perm = nil
			}
		}
		rs.handleParticipantUpdate(msg.Node, info, perm)

	case msgSyncRequest:
		rs.publishAllLocalParticipants()

	case msgDataPacket:
		dp := &livekit.DataPacket{}
		if err := proto.Unmarshal(msg.DataPacket, dp); err != nil {
			rs.logger.Warnw("could not decode data packet", err)
			return
		}
		rs.room.DeliverRemoteDataPacket(msg.DataKind, dp)

	case msgSpeakers:
		rs.lock.Lock()
		byID := make(map[livekit.ParticipantID]*RemoteParticipant, len(rs.remote))
		for _, rp := range rs.remote {
			byID[rp.ID()] = rp
		}
		rs.lock.Unlock()
		for _, s := range msg.Speakers {
			if rp := byID[s.ParticipantID]; rp != nil {
				rp.setAudioLevel(float64(s.Level), s.Active)
			}
		}

	case msgRoomMetadata:
		rs.lock.Lock()
		rs.lastMetadata = msg.Metadata
		rs.lock.Unlock()
		if rs.room.ToProto().Metadata != msg.Metadata {
			rs.room.SetMetadata(msg.Metadata)
		}

	case msgRoomClose:
		rs.logger.Infow("room deleted on another node", "remoteNodeID", msg.Node)
		rs.closingFromRemote.Store(true)
		go rs.room.Close(types.RoomCloseReasonAPIDelete)

	case msgNodeLeft:
		rs.removeNode(msg.Node, "node left room")
	}
}

func (rs *roomState) handleParticipantUpdate(nodeID livekit.NodeID, info *livekit.ParticipantInfo, perm *livekit.SubscriptionPermission) {
	identity := livekit.ParticipantIdentity(info.Identity)

	if lp := rs.room.GetParticipant(identity); lp != nil && !lp.IsDisconnected() && info.State != livekit.ParticipantInfo_DISCONNECTED {
		// identity is hosted locally now (e.g. after a failover), the remote session is stale
		rs.logger.Debugw("ignoring remote update for local participant", "participant", identity, "remoteNodeID", nodeID)
		return
	}

	rs.lock.Lock()
	rp := rs.remote[identity]
	rs.lock.Unlock()

	if info.State == livekit.ParticipantInfo_DISCONNECTED {
		if rp != nil && rp.ID() == livekit.ParticipantID(info.Sid) {
			rs.removeRemoteParticipant(rp)
		}
		return
	}

	if rp != nil && (rp.ID() != livekit.ParticipantID(info.Sid) || rp.NodeID() != nodeID) {
		// a new session of the identity replaced the old one
		rs.removeRemoteParticipant(rp)
		rp = nil
	}

	if rp == nil {
		rp = newRemoteParticipant(nodeID, info, rs.logger)
		rp.update(info, perm)
		rs.lock.Lock()
		rs.remote[identity] = rp
		rs.lock.Unlock()

		rs.logger.Infow("remote participant joined", "participant", identity, "pID", info.Sid, "remoteNodeID", nodeID)
		rs.room.AddRemoteParticipant(rp)
		rs.syncTracks(rp, info)
		return
	}

	if !rp.update(info, perm) {
		return
	}
	rs.syncTracks(rp, info)
	rs.room.UpdateRemoteParticipant(rp)
}

func (rs *roomState) syncTracks(rp *RemoteParticipant, info *livekit.ParticipantInfo) {
	seen := make(map[livekit.TrackID]struct{}, len(info.Tracks))
	for _, ti := range info.Tracks {
		trackID := livekit.TrackID(ti.Sid)
		seen[trackID] = struct{}{}

		if t := rp.getTrack(trackID); t != nil {
			t.UpdateTrackInfo(relayTrackInfo(ti))
			if t.IsMuted() != ti.Muted {
				t.SetMuted(ti.Muted)
			}
			continue
		}

		conn := rs.getOrCreateConn(rp.NodeID())
		if conn == nil {
			return
		}
		t := newRemoteTrack(rs, conn, rp, relayTrackInfo(ti))
		rp.setTrack(t)
		rs.room.AddRemoteTrack(rp, t)
	}

	for _, t := range rp.remoteTracks() {
		if _, ok := seen[t.ID()]; ok {
			continue
		}
		rp.deleteTrack(t.ID())
		rs.room.RemoveRemoteTrack(rp, t)
		t.Close(false)
	}
}

func (rs *roomState) removeRemoteParticipant(rp *RemoteParticipant) {
	rs.lock.Lock()
	if rs.remote[rp.Identity()] == rp {
		delete(rs.remote, rp.Identity())
	}
	rs.lock.Unlock()

	rs.logger.Infow("remote participant left", "participant", rp.Identity(), "pID", rp.ID(), "remoteNodeID", rp.NodeID())
	rp.markDisconnected()
	rs.room.RemoveRemoteParticipant(rp)
	for _, t := range rp.remoteTracks() {
		rp.deleteTrack(t.ID())
		t.Close(false)
	}
}

// removeNode drops all participants and the relay connection of a node.
func (rs *roomState) removeNode(nodeID livekit.NodeID, reason string) []*RemoteParticipant {
	rs.lock.Lock()
	var participants []*RemoteParticipant
	for _, rp := range rs.remote {
		if rp.NodeID() == nodeID {
			participants = append(participants, rp)
		}
	}
	conn := rs.conns[nodeID]
	delete(rs.conns, nodeID)
	rs.lock.Unlock()

	if len(participants) != 0 || conn != nil {
		rs.logger.Infow("removing remote node from room", "remoteNodeID", nodeID, "reason", reason, "numParticipants", len(participants))
	}
	for _, rp := range participants {
		rs.removeRemoteParticipant(rp)
	}
	if conn != nil {
		conn.close()
	}
	return participants
}

func (rs *roomState) getOrCreateConn(nodeID livekit.NodeID) *relayConn {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if rs.ctx.Err() != nil {
		return nil
	}
	c := rs.conns[nodeID]
	if c == nil {
		c = newRelayConn(rs, nodeID)
		rs.conns[nodeID] = c
	}
	return c
}

func (rs *roomState) getConn(nodeID livekit.NodeID) *relayConn {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.conns[nodeID]
}

// handleRelayMessage handles relay control messages of a peer node.
func (rs *roomState) handleRelayMessage(msg *message) {
	conn := rs.getOrCreateConn(msg.Node)
	if conn == nil {
		return
	}
	switch msg.Type {
	case msgRelaySubscribe:
		if s := conn.getOrCreateSender(); s != nil {
			s.addTrack(msg.TrackID)
		}
	case msgRelayUnsubscribe:
		if s := conn.getSender(); s != nil {
			s.removeTrack(msg.TrackID)
		}
	case msgRelayQuality:
		if s := conn.getSender(); s != nil {
			s.handleQualities(msg.TrackID, msg.Qualities)
		}
	case msgRelayAnswer:
		if s := conn.getSender(); s != nil {
			s.handleAnswer(msg.SessionID, msg.SDP)
		}
	case msgRelayOffer:
		conn.handleOffer(msg.SessionID, msg.SDP)
	case msgRelayCandidate:
		if msg.ToSender {
			if s := conn.getSender(); s != nil {
				s.addCandidate(msg.SessionID, msg.Candidate)
			}
		} else {
			conn.addRecvCandidate(msg.SessionID, msg.Candidate)
		}
	}
}
