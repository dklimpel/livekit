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
	"maps"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/datatrack"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

const remoteSpeakerTimeout = 3 * time.Second

// RemoteParticipant mirrors a participant hosted on another node of a
// distributed room. Its state is driven by participant_update messages of the
// hosting node, its tracks are relayed on demand.
type RemoteParticipant struct {
	nodeID livekit.NodeID
	logger logger.Logger

	lock        sync.RWMutex
	info        *livekit.ParticipantInfo
	permission  *livekit.SubscriptionPermission
	tracks      map[livekit.TrackID]*remoteTrack
	closed      bool
	audioLevel  float64
	audioActive bool
	audioAt     time.Time
}

var _ rtc.RemoteNodeParticipant = (*RemoteParticipant)(nil)

func newRemoteParticipant(nodeID livekit.NodeID, info *livekit.ParticipantInfo, l logger.Logger) *RemoteParticipant {
	return &RemoteParticipant{
		nodeID: nodeID,
		info:   info,
		tracks: make(map[livekit.TrackID]*remoteTrack),
		logger: l.WithValues("participant", info.Identity, "pID", info.Sid, "remoteNodeID", nodeID),
	}
}

func (p *RemoteParticipant) IsRemoteNodeParticipant() bool { return true }
func (p *RemoteParticipant) NodeID() livekit.NodeID        { return p.nodeID }

func (p *RemoteParticipant) getInfo() *livekit.ParticipantInfo {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.info
}

// update applies a new state, returns false for outdated updates.
func (p *RemoteParticipant) update(info *livekit.ParticipantInfo, perm *livekit.SubscriptionPermission) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	if info.Version < p.info.Version {
		return false
	}
	p.info = info
	p.permission = perm
	return true
}

// lastActiveInfo returns the participant info as reported by its node.
func (p *RemoteParticipant) lastActiveInfo() *livekit.ParticipantInfo {
	return p.getInfo()
}

func (p *RemoteParticipant) markDisconnected() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.closed = true
	info := proto.CloneOf(p.info)
	info.State = livekit.ParticipantInfo_DISCONNECTED
	info.Version++
	p.info = info
}

func (p *RemoteParticipant) setAudioLevel(level float64, active bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.audioLevel, p.audioActive, p.audioAt = level, active, time.Now()
}

func (p *RemoteParticipant) getTrack(trackID livekit.TrackID) *remoteTrack {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.tracks[trackID]
}

func (p *RemoteParticipant) setTrack(t *remoteTrack) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.tracks[t.ID()] = t
}

func (p *RemoteParticipant) deleteTrack(trackID livekit.TrackID) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.tracks, trackID)
}

func (p *RemoteParticipant) remoteTracks() []*remoteTrack {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return slices.Collect(maps.Values(p.tracks))
}

// --- types.Participant ---

func (p *RemoteParticipant) ID() livekit.ParticipantID {
	return livekit.ParticipantID(p.getInfo().Sid)
}

func (p *RemoteParticipant) Identity() livekit.ParticipantIdentity {
	return livekit.ParticipantIdentity(p.getInfo().Identity)
}

func (p *RemoteParticipant) State() livekit.ParticipantInfo_State {
	return p.getInfo().State
}

func (p *RemoteParticipant) ConnectedAt() time.Time {
	info := p.getInfo()
	if info.JoinedAtMs != 0 {
		return time.UnixMilli(info.JoinedAtMs)
	}
	return time.Unix(info.JoinedAt, 0)
}

func (p *RemoteParticipant) CloseReason() types.ParticipantCloseReason {
	return types.ParticipantCloseReasonNone
}

func (p *RemoteParticipant) Kind() livekit.ParticipantInfo_Kind {
	return p.getInfo().Kind
}

func (p *RemoteParticipant) KindDetails() []livekit.ParticipantInfo_KindDetail {
	return p.getInfo().KindDetails
}

func (p *RemoteParticipant) IsRecorder() bool {
	info := p.getInfo()
	return info.Kind == livekit.ParticipantInfo_EGRESS || info.Permission.GetRecorder()
}

func (p *RemoteParticipant) IsAgent() bool {
	info := p.getInfo()
	return info.Kind == livekit.ParticipantInfo_AGENT || info.Permission.GetAgent()
}

func (p *RemoteParticipant) IsDependent() bool {
	return p.IsRecorder() || p.IsAgent()
}

func (p *RemoteParticipant) GetLogger() logger.Logger { return p.logger }

func (p *RemoteParticipant) CanSkipBroadcast() bool { return false }

func (p *RemoteParticipant) Version() utils.TimedVersion {
	return utils.TimedVersion(0)
}

func (p *RemoteParticipant) ToProto() *livekit.ParticipantInfo {
	return p.getInfo()
}

func (p *RemoteParticipant) ToProtoWithVersion() (*livekit.ParticipantInfo, utils.TimedVersion) {
	return p.getInfo(), p.Version()
}

func (p *RemoteParticipant) IsPublisher() bool {
	return p.getInfo().IsPublisher
}

func (p *RemoteParticipant) GetPublishedTrack(trackID livekit.TrackID) types.MediaTrack {
	if t := p.getTrack(trackID); t != nil {
		return t
	}
	return nil
}

func (p *RemoteParticipant) GetPublishedTracks() []types.MediaTrack {
	p.lock.RLock()
	defer p.lock.RUnlock()
	tracks := make([]types.MediaTrack, 0, len(p.tracks))
	for _, t := range p.tracks {
		tracks = append(tracks, t)
	}
	return tracks
}

func (p *RemoteParticipant) RemovePublishedTrack(track types.MediaTrack, isExpectedToResume bool) {
	p.deleteTrack(track.ID())
}

func (p *RemoteParticipant) GetPublishedDataTracks() []types.DataTrack           { return nil }
func (p *RemoteParticipant) GetPublishedDataTrack(handle uint16) types.DataTrack { return nil }
func (p *RemoteParticipant) RemovePublishedDataTrack(track types.DataTrack)      {}

func (p *RemoteParticipant) GetAudioLevel() (float64, bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if !p.audioActive || time.Since(p.audioAt) > remoteSpeakerTimeout {
		return 0, false
	}
	return p.audioLevel, true
}

func (p *RemoteParticipant) HasPermission(trackID livekit.TrackID, subIdentity livekit.ParticipantIdentity) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	perm := p.permission
	if perm == nil || perm.AllParticipants {
		return true
	}
	for _, tp := range perm.TrackPermissions {
		if livekit.ParticipantIdentity(tp.ParticipantIdentity) != subIdentity {
			continue
		}
		if tp.AllTracks {
			return true
		}
		for _, sid := range tp.TrackSids {
			if livekit.TrackID(sid) == trackID {
				return true
			}
		}
		return false
	}
	return false
}

func (p *RemoteParticipant) Hidden() bool {
	return p.getInfo().Permission.GetHidden()
}

func (p *RemoteParticipant) MigrateState() types.MigrateState { return types.MigrateStateComplete }

func (p *RemoteParticipant) Close(sendLeave bool, reason types.ParticipantCloseReason, isExpectedToResume bool) error {
	p.markDisconnected()
	for _, t := range p.remoteTracks() {
		t.Close(false)
	}
	return nil
}

func (p *RemoteParticipant) IsClosed() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.closed
}

func (p *RemoteParticipant) IsDisconnected() bool {
	return p.IsClosed() || p.State() == livekit.ParticipantInfo_DISCONNECTED
}

func (p *RemoteParticipant) SubscriptionPermission() (*livekit.SubscriptionPermission, utils.TimedVersion) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.permission, utils.TimedVersion(0)
}

func (p *RemoteParticipant) UpdateSubscriptionPermission(
	_ *livekit.SubscriptionPermission,
	_ utils.TimedVersion,
	_ func(participantID livekit.ParticipantID) types.LocalParticipant,
) error {
	// permissions of remote participants are owned by their hosting node
	return nil
}

func (p *RemoteParticipant) DebugInfo() map[string]any {
	info := p.getInfo()
	return map[string]any{
		"ID":       info.Sid,
		"Identity": info.Identity,
		"State":    info.State.String(),
		"NodeID":   p.nodeID,
		"Remote":   true,
	}
}

func (p *RemoteParticipant) HandleReceivedDataTrackMessage([]byte, *datatrack.Packet, int64) {}

func (p *RemoteParticipant) GetParticipantListener() types.ParticipantListener {
	return &types.NullParticipantListener{}
}

func (p *RemoteParticipant) AddDataBlob(*livekit.DataBlob)                      {}
func (p *RemoteParticipant) GetDataBlob(*livekit.DataBlobKey) *livekit.DataBlob { return nil }
