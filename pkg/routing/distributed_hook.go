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

package routing

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/livekit/protocol/livekit"
)

// ParticipantPlacer places participants of distributed rooms on nodes.
// It is implemented by pkg/distributed and installed on the RedisRouter when
// distributed rooms are enabled. Without a placer the router behaves like upstream.
type ParticipantPlacer interface {
	// NodeForParticipant selects the node hosting the participant's session.
	NodeForParticipant(ctx context.Context, roomName livekit.RoomName, pi ParticipantInit) (livekit.NodeID, string, error)
	// NodeForRoom returns the node that would host the next participant of the room.
	NodeForRoom(ctx context.Context, roomName livekit.RoomName) (*livekit.Node, error)
}

// SetParticipantPlacer installs a placer, must be called before the router serves requests.
func (r *RedisRouter) SetParticipantPlacer(p ParticipantPlacer) {
	r.placer = p
}

// RedisClient exposes the Redis client used by the router.
func (r *RedisRouter) RedisClient() redis.UniversalClient {
	return r.rc
}

func (r *RedisRouter) startParticipantSignalWithPlacer(ctx context.Context, roomName livekit.RoomName, pi ParticipantInit) (StartParticipantSignalResults, error) {
	nodeID, reason, err := r.placer.NodeForParticipant(ctx, roomName, pi)
	if err != nil {
		return StartParticipantSignalResults{}, err
	}
	res, err := r.StartParticipantSignalWithNodeID(ctx, roomName, pi, nodeID)
	if err == nil {
		res.NodeSelectionReason = reason
	}
	return res, err
}

// HasParticipantPlacer reports whether router places participants of a room on
// multiple nodes. In that case a node must accept sessions for rooms that are
// (primarily) hosted on another node.
func HasParticipantPlacer(router Router) bool {
	rr, ok := router.(*RedisRouter)
	return ok && rr.placer != nil
}
