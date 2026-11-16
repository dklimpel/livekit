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

package service

import (
	"context"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/rtc"
)

// removeLocalRoom forgets a closed room on this node without deleting the
// cluster wide room state, which is still used by other nodes of a distributed room.
func (r *RoomManager) removeLocalRoom(room *rtc.Room) {
	r.lock.Lock()
	if r.rooms[room.Name()] == room {
		delete(r.rooms, room.Name())
	}
	r.lock.Unlock()
}

// cleanupFailedNodeParticipant removes a participant of a failed node from the
// room store, unless the identity has rejoined with a new session meanwhile.
func (r *RoomManager) cleanupFailedNodeParticipant(ctx context.Context, roomName livekit.RoomName, pi *livekit.ParticipantInfo) {
	identity := livekit.ParticipantIdentity(pi.Identity)
	if store, ok := r.roomStore.(OSSServiceStore); ok {
		stored, err := store.LoadParticipant(ctx, roomName, identity)
		if err != nil || stored.Sid != pi.Sid {
			return
		}
	}
	if err := r.roomStore.DeleteParticipant(ctx, roomName, identity); err != nil {
		logger.Warnw("could not delete participant of failed node", err, "room", roomName, "participant", identity)
	}
}
