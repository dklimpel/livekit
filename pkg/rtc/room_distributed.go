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

package rtc

import (
	"maps"
	"slices"

	"github.com/pion/webrtc/v4"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// Distributed rooms (pkg/distributed) mirror participants hosted on other nodes
// into the local room. This file holds the room side of that integration, so
// that the changes to room.go stay small.

// RemoteNodeParticipant is implemented by participants hosted on another node.
type RemoteNodeParticipant interface {
	types.Participant
	IsRemoteNodeParticipant() bool
}

// IsRemoteParticipant reports whether p is hosted on another node.
func IsRemoteParticipant(p types.Participant) bool {
	rp, ok := p.(RemoteNodeParticipant)
	return ok && rp.IsRemoteNodeParticipant()
}

// DistributedRoomListener receives events of the local room that need to be
// propagated to the other nodes of a distributed room.
type DistributedRoomListener interface {
	// OnLocalDataPacket is called for data packets sent by local participants or
	// by the server API on this node.
	OnLocalDataPacket(source types.LocalParticipant, kind livekit.DataPacket_Kind, dp *livekit.DataPacket)
	// OnLocalSubscriptionPermissionChanged is called when a local participant
	// changed the subscription permissions of its tracks.
	OnLocalSubscriptionPermissionChanged(p types.LocalParticipant)
}

type roomDistributedState struct {
	listener           DistributedRoomListener
	remoteParticipants map[livekit.ParticipantIdentity]RemoteNodeParticipant
}

// SetDistributedListener installs the listener of a distributed room.
func (r *Room) SetDistributedListener(l DistributedRoomListener) {
	r.lock.Lock()
	r.distributed.listener = l
	r.lock.Unlock()
}

// GetRemoteParticipant returns a participant hosted on another node.
func (r *Room) GetRemoteParticipant(identity livekit.ParticipantIdentity) RemoteNodeParticipant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.distributed.remoteParticipants[identity]
}

// GetRemoteParticipants returns the participants hosted on other nodes.
func (r *Room) GetRemoteParticipants() []RemoteNodeParticipant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return slices.Collect(maps.Values(r.distributed.remoteParticipants))
}

// AddRemoteParticipant adds (or replaces) a participant hosted on another node
// and announces it to the local participants.
func (r *Room) AddRemoteParticipant(p RemoteNodeParticipant) {
	r.lock.Lock()
	if r.distributed.remoteParticipants == nil {
		r.distributed.remoteParticipants = make(map[livekit.ParticipantIdentity]RemoteNodeParticipant)
	}
	r.distributed.remoteParticipants[p.Identity()] = p
	r.lock.Unlock()

	r.protoProxy.MarkDirty(false)
	r.UpdateRemoteParticipant(p)
}

// UpdateRemoteParticipant announces a state change of a remote participant.
func (r *Room) UpdateRemoteParticipant(p RemoteNodeParticipant) {
	// subscribers re-evaluate permissions on track changes
	for _, t := range p.GetPublishedTracks() {
		r.trackManager.NotifyTrackChanged(t.ID())
	}
	r.broadcastParticipantState(p, broadcastOptions{skipSource: true, immediate: true})
	if r.onParticipantChanged != nil {
		r.onParticipantChanged(p)
	}
}

// RemoveRemoteParticipant removes a participant hosted on another node.
// p.ToProto() is expected to report the participant as DISCONNECTED.
func (r *Room) RemoveRemoteParticipant(p RemoteNodeParticipant) {
	r.lock.Lock()
	if r.distributed.remoteParticipants[p.Identity()] != p {
		r.lock.Unlock()
		return
	}
	delete(r.distributed.remoteParticipants, p.Identity())
	r.lock.Unlock()

	for _, t := range p.GetPublishedTracks() {
		r.trackManager.RemoveTrack(t)
	}
	r.protoProxy.MarkDirty(false)
	r.broadcastParticipantState(p, broadcastOptions{skipSource: true, immediate: true})
	if r.onParticipantChanged != nil {
		r.onParticipantChanged(p)
	}
}

// AddRemoteTrack publishes a track of a remote participant in the local room,
// auto-subscribing local participants.
func (r *Room) AddRemoteTrack(p RemoteNodeParticipant, track types.MediaTrack) {
	r.onTrackPublished(p, track)
}

// RemoveRemoteTrack unpublishes a track of a remote participant.
func (r *Room) RemoveRemoteTrack(p RemoteNodeParticipant, track types.MediaTrack) {
	r.onTrackUnpublished(p, track)
}

// DeliverRemoteDataPacket delivers a data packet received from another node to
// the local participants.
func (r *Room) DeliverRemoteDataPacket(kind livekit.DataPacket_Kind, dp *livekit.DataPacket) {
	BroadcastDataPacketForRoom(r, nil, kind, dp, r.logger)
}

// SendRemoteSpeakerChanges forwards active speaker changes of remote participants.
func (r *Room) SendRemoteSpeakerChanges(speakers []*livekit.SpeakerInfo) {
	if len(speakers) != 0 {
		r.sendSpeakerChanges(speakers)
	}
}

// GetLocalActiveSpeakers returns the active speakers among the local participants.
func (r *Room) GetLocalActiveSpeakers() []*livekit.SpeakerInfo {
	return r.getActiveSpeakers(toParticipants(r.GetParticipants()))
}

// --- helpers used by room.go ---

func (r *Room) notifyDistributedLocalData(source types.LocalParticipant, kind livekit.DataPacket_Kind, dp *livekit.DataPacket) {
	r.lock.RLock()
	l := r.distributed.listener
	r.lock.RUnlock()
	if l != nil {
		l.OnLocalDataPacket(source, kind, dp)
	}
}

func (r *Room) notifyDistributedPermissionChanged(p types.LocalParticipant) {
	r.lock.RLock()
	l := r.distributed.listener
	r.lock.RUnlock()
	if l != nil {
		l.OnLocalSubscriptionPermissionChanged(p)
	}
}

// GetParticipantIdentityByID resolves a participant ID of a local or remote participant.
func (r *Room) GetParticipantIdentityByID(participantID livekit.ParticipantID) livekit.ParticipantIdentity {
	if p := r.getParticipantByIDIncludingRemote(participantID); p != nil {
		return p.Identity()
	}
	return ""
}

// appendRemoteParticipantsLocked, r.lock must be held
func (r *Room) appendRemoteParticipantsLocked(participants []types.Participant) []types.Participant {
	for _, p := range r.distributed.remoteParticipants {
		participants = append(participants, p)
	}
	return participants
}

// numRemoteParticipantsLocked, r.lock must be held
func (r *Room) numRemoteParticipantsLocked() uint32 {
	n := uint32(0)
	for _, p := range r.distributed.remoteParticipants {
		if !p.IsDependent() {
			n++
		}
	}
	return n
}

func (r *Room) getParticipantByIDIncludingRemote(participantID livekit.ParticipantID) types.Participant {
	if p := r.GetParticipantByID(participantID); p != nil {
		return p
	}
	r.lock.RLock()
	defer r.lock.RUnlock()
	for _, p := range r.distributed.remoteParticipants {
		if p.ID() == participantID {
			return p
		}
	}
	return nil
}

func (r *Room) getParticipantsIncludingRemote() []types.Participant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.appendRemoteParticipantsLocked(toParticipants(slices.Collect(maps.Values(r.participants))))
}

func (r *Room) addRemoteParticipantCounts(room *livekit.Room) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	for _, p := range r.distributed.remoteParticipants {
		if !p.IsDependent() {
			room.NumParticipants++
		}
		if p.IsPublisher() {
			room.NumPublishers++
		}
	}
}

// NewRelayMediaEngine creates the media engine of node-to-node relay
// connections of distributed rooms. It registers the same codecs and header
// extensions as publisher connections, so every published stream can be relayed.
func NewRelayMediaEngine(codecs []*livekit.Codec, conf *WebRTCConfig) (*webrtc.MediaEngine, error) {
	dc := conf.Publisher
	dc.FlexFEC = FlexFECDirectionConfig{}
	return createMediaEngine(codecs, dc, false)
}
