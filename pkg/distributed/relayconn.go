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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/utils"
)

const (
	relaySubscribeRetryInterval = 3 * time.Second

	// a failed sending relay connection is recreated with exponential backoff
	relayRetryBackoffMin = 3 * time.Second
	relayRetryBackoffMax = time.Minute
)

// relayConn is the relay between this node and a peer node for one room.
//
// Each direction uses its own peer connection, offered by the sending node:
//   - receiving: tracks published on the peer, subscribed by local participants
//   - sending (relaySender): local tracks, subscribed by participants of the peer
type relayConn struct {
	rs     *roomState
	peer   livekit.NodeID
	logger logger.Logger

	// buffers of tracks received from the peer
	bufferFactory *buffer.Factory

	lock              sync.Mutex
	recvPC            *webrtc.PeerConnection
	recvSession       string
	pendingCandidates []webrtc.ICECandidateInit
	requested         map[livekit.TrackID]*remoteTrack
	sender            *relaySender
	senderFailures    int
	senderRetryAt     time.Time

	closed atomic.Bool
}

func newRelayConn(rs *roomState, peer livekit.NodeID) *relayConn {
	return &relayConn{
		rs:            rs,
		peer:          peer,
		logger:        rs.logger.WithValues("remoteNodeID", peer),
		bufferFactory: rs.room.GetBufferFactory(),
		requested:     make(map[livekit.TrackID]*remoteTrack),
	}
}

// pionLogger is the logger for the WebRTC stack of relay connections. It uses
// the transport component, so that logging.pion_level applies like for
// participant connections.
func (c *relayConn) pionLogger() logger.Logger {
	return logger.GetLogger().WithComponent(utils.ComponentTransport).WithValues(
		"room", c.rs.room.Name(),
		"remoteNodeID", c.peer,
		"relay", true,
	)
}

func (c *relayConn) close() {
	if c.closed.Swap(true) {
		return
	}
	c.lock.Lock()
	pc := c.recvPC
	c.recvPC = nil
	sender := c.sender
	c.sender = nil
	c.lock.Unlock()

	if pc != nil {
		_ = pc.Close()
	}
	if sender != nil {
		sender.close()
	}
}

func (c *relayConn) sendToPeer(msg *message) {
	msg.Node = c.rs.nodeID()
	msg.Room = c.rs.room.Name()
	ctx, cancel := context.WithTimeout(c.rs.ctx, publishTimeout)
	defer cancel()
	if err := publish(ctx, c.rs.m.bus, nodeChannel(c.peer), msg); err != nil && c.rs.ctx.Err() == nil {
		c.logger.Warnw("could not send relay message", err, "type", msg.Type)
	}
}

// --- receiving side ---

// requestTrack asks the peer to relay a track. The request is repeated until
// media arrives, which covers lost messages and peers still setting up.
func (c *relayConn) requestTrack(t *remoteTrack) {
	c.lock.Lock()
	c.requested[t.ID()] = t
	c.lock.Unlock()

	c.logger.Debugw("requesting relayed track", "trackID", t.ID())
	go func() {
		for {
			c.sendToPeer(&message{Type: msgRelaySubscribe, TrackID: t.ID()})
			select {
			case <-c.rs.ctx.Done():
				return
			case <-time.After(relaySubscribeRetryInterval):
			}
			if c.closed.Load() || !t.relayRequested.Load() || len(t.Receivers()) != 0 {
				return
			}
		}
	}()
}

func (c *relayConn) releaseTrack(t *remoteTrack) {
	c.lock.Lock()
	if c.requested[t.ID()] == t {
		delete(c.requested, t.ID())
	}
	c.lock.Unlock()
	if !c.closed.Load() {
		c.sendToPeer(&message{Type: msgRelayUnsubscribe, TrackID: t.ID()})
	}
}

func (c *relayConn) writeRTCP(pkts []rtcp.Packet) {
	c.lock.Lock()
	pc := c.recvPC
	c.lock.Unlock()
	if pc != nil {
		_ = pc.WriteRTCP(pkts)
	}
}

func (c *relayConn) sendSubscribedQualities(trackID livekit.TrackID, codecs []*livekit.SubscribedCodec) {
	if c.closed.Load() {
		return
	}
	c.sendToPeer(&message{
		Type:      msgRelayQuality,
		TrackID:   trackID,
		Qualities: marshalProto(&livekit.SubscribedQualityUpdate{TrackSid: string(trackID), SubscribedCodecs: codecs}),
	})
}

func (c *relayConn) handleOffer(session string, sdp string) {
	c.lock.Lock()
	pc := c.recvPC
	if pc != nil && c.recvSession != session {
		// the peer restarted its sending side
		c.logger.Infow("relay session changed, recreating receiving connection")
		_ = pc.Close()
		pc = nil
	}
	if pc == nil {
		var err error
		pc, err = c.rs.m.relay.newPeerConnection(c.rs.room.ToProto().EnabledCodecs, c.bufferFactory, true, c.pionLogger())
		if err != nil {
			c.lock.Unlock()
			c.logger.Errorw("could not create receiving relay connection", err)
			return
		}
		c.recvPC = pc
		c.recvSession = session
		c.pendingCandidates = nil
		pc.OnTrack(c.onTrack)
		pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
			if cand == nil {
				return
			}
			data, _ := json.Marshal(cand.ToJSON())
			c.sendToPeer(&message{Type: msgRelayCandidate, Candidate: string(data), ToSender: true, SessionID: session})
		})
		pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			c.logger.Debugw("receiving relay connection state changed", "state", state)
			if state == webrtc.PeerConnectionStateFailed {
				c.logConnectionFailed("receiving")
			}
		})
	}
	c.lock.Unlock()

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
		c.logger.Errorw("could not set relay offer", err)
		return
	}

	c.lock.Lock()
	pending := c.pendingCandidates
	c.pendingCandidates = nil
	c.lock.Unlock()
	for _, cand := range pending {
		_ = pc.AddICECandidate(cand)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		c.logger.Errorw("could not create relay answer", err)
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		c.logger.Errorw("could not set relay answer", err)
		return
	}
	c.sendToPeer(&message{Type: msgRelayAnswer, SDP: answer.SDP, SessionID: session})
}

func (c *relayConn) addRecvCandidate(session string, candidate string) {
	var cand webrtc.ICECandidateInit
	if err := json.Unmarshal([]byte(candidate), &cand); err != nil {
		return
	}
	c.lock.Lock()
	pc := c.recvPC
	if pc == nil || c.recvSession != session || pc.RemoteDescription() == nil {
		c.pendingCandidates = append(c.pendingCandidates, cand)
		c.lock.Unlock()
		return
	}
	c.lock.Unlock()
	_ = pc.AddICECandidate(cand)
}

// relayed streams are identified by "<trackID>|<mime>|<stream index>"
func relayStreamID(trackID livekit.TrackID, mimeType mime.MimeType, idx int) string {
	return fmt.Sprintf("%s|%s|%d", trackID, mimeType, idx)
}

func parseRelayStreamID(id string) (livekit.TrackID, mime.MimeType, int, bool) {
	parts := strings.Split(id, "|")
	if len(parts) != 3 {
		return "", mime.MimeTypeUnknown, 0, false
	}
	idx, err := strconv.Atoi(parts[2])
	if err != nil {
		return "", mime.MimeTypeUnknown, 0, false
	}
	return livekit.TrackID(parts[0]), mime.NormalizeMimeType(parts[1]), idx, true
}

func (c *relayConn) onTrack(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	trackID, mimeType, idx, ok := parseRelayStreamID(track.ID())
	if !ok {
		c.logger.Warnw("unexpected relayed track", nil, "id", track.ID())
		return
	}

	c.lock.Lock()
	t := c.requested[trackID]
	c.lock.Unlock()
	if t == nil {
		c.logger.Infow("relayed track not requested anymore", "trackID", trackID)
		return
	}

	// simulcast streams are mapped back to the RID of their layer
	rid := ""
	if idx > 0 || len(buffer.GetVideoLayersForMimeType(mimeType, t.ToProto())) > 1 {
		for _, l := range buffer.GetVideoLayersForMimeType(mimeType, t.ToProto()) {
			if int(l.SpatialLayer) == idx {
				rid = l.Rid
				break
			}
		}
	}

	mid := ""
	for _, tr := range c.recvPCTransceivers() {
		if tr.Receiver() == receiver {
			mid = tr.Mid()
			break
		}
	}

	c.logger.Debugw("relayed track received", "trackID", trackID, "mime", mimeType, "stream", idx, "rid", rid, "ssrc", track.SSRC())
	if _, ok := t.MediaTrack.AddReceiver(receiver, &relayTrackRemote{TrackRemote: track, rid: rid}, mid); !ok {
		c.logger.Warnw("could not add relayed receiver", nil, "trackID", trackID, "mime", mimeType, "stream", idx)
	}
}

func (c *relayConn) recvPCTransceivers() []*webrtc.RTPTransceiver {
	c.lock.Lock()
	pc := c.recvPC
	c.lock.Unlock()
	if pc == nil {
		return nil
	}
	return pc.GetTransceivers()
}

// --- sending side ---

func (c *relayConn) getOrCreateSender() *relaySender {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.closed.Load() {
		return nil
	}
	if c.sender != nil && c.sender.isFailed() {
		c.sender.close()
		c.sender = nil
		c.senderFailures++
		backoff := relayRetryBackoffMin << min(c.senderFailures-1, 5)
		if backoff > relayRetryBackoffMax {
			backoff = relayRetryBackoffMax
		}
		c.senderRetryAt = time.Now().Add(backoff)
		c.logger.Infow("retrying sending relay connection later", "failures", c.senderFailures, "backoff", backoff)
	}
	if c.sender == nil && time.Now().Before(c.senderRetryAt) {
		// the peer repeats its subscription requests until the relay works
		return nil
	}
	if c.sender == nil {
		s, err := newRelaySender(c)
		if err != nil {
			c.logger.Errorw("could not create sending relay connection", err)
			return nil
		}
		c.sender = s
	}
	return c.sender
}

func (c *relayConn) onSenderConnected() {
	c.lock.Lock()
	failures := c.senderFailures
	c.senderFailures = 0
	c.senderRetryAt = time.Time{}
	c.lock.Unlock()
	if failures > 0 {
		c.logger.Infow("sending relay connection established after failures", "failures", failures)
	}
}

// logConnectionFailed explains the most likely cause of a failed relay connection.
func (c *relayConn) logConnectionFailed(direction string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	remote, _ := c.rs.m.store.GetNodeRelayAddress(ctx, c.peer)
	c.logger.Warnw(
		"relay connection to remote node failed, nodes must reach each other on the relay UDP port range", nil,
		"direction", direction,
		"localRelayAddress", c.rs.m.relay.address(),
		"remoteRelayAddress", remote,
		"hint", "check firewall rules for distributed.relay.port_range_start..port_range_end and distributed.relay.node_ip / "+RelayNodeIPEnv,
	)
}

func (c *relayConn) getSender() *relaySender {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.sender
}

// relayTrackRemote presents a relayed stream with the RID of its simulcast layer.
type relayTrackRemote struct {
	*webrtc.TrackRemote
	rid string
}

var _ sfu.TrackRemote = (*relayTrackRemote)(nil)

func (t *relayTrackRemote) RID() string                   { return t.rid }
func (t *relayTrackRemote) RTCTrack() *webrtc.TrackRemote { return t.TrackRemote }
