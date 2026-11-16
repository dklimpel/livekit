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
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"
)

const (
	// hash: node_id => unix time the node started hosting the room
	roomNodesKeyPrefix = "dist:room_nodes:"
	// hash: participant identity => node_id hosting the participant
	participantNodeKeyPrefix = "dist:participant_node:"
	// hash: misc. per room state (e.g. which node dispatched room agents)
	roomMetaKeyPrefix = "dist:room_meta:"
	// string: short lived placement lock
	placementLockKeyPrefix = "dist:placement_lock:"
	// hash: node_id => relay address (ip:port), for operations and tests
	nodeRelayKey = "dist:node_relay"

	roomMetaAgentsNode = "agents_node"

	placementLockTTL = 3 * time.Second
)

var ErrPlacementLockTimeout = errors.New("could not acquire placement lock")

// removes a hash field only if it still holds the expected value
var hdelIfEqualScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], ARGV[1]) == ARGV[2] then
	return redis.call("HDEL", KEYS[1], ARGV[1])
end
return 0
`)

// adds a node to the room and returns the number of nodes
var addRoomNodeScript = redis.NewScript(`
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
return redis.call("HLEN", KEYS[1])
`)

// removes a node from the room and returns the number of remaining nodes,
// or -1 when the node was not registered (anymore)
var removeRoomNodeScript = redis.NewScript(`
if redis.call("HDEL", KEYS[1], ARGV[1]) == 0 then
	return -1
end
return redis.call("HLEN", KEYS[1])
`)

// deletes the lock only if it is still owned by the caller
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// Store keeps the cluster wide state of distributed rooms in Redis.
type Store struct {
	rc redis.UniversalClient
}

func NewStore(rc redis.UniversalClient) *Store {
	return &Store{rc: rc}
}

func roomNodesKey(roomName livekit.RoomName) string {
	return roomNodesKeyPrefix + string(roomName)
}

func participantNodeKey(roomName livekit.RoomName) string {
	return participantNodeKeyPrefix + string(roomName)
}

func roomMetaKey(roomName livekit.RoomName) string {
	return roomMetaKeyPrefix + string(roomName)
}

// AddRoomNode registers nodeID as a host of the room and returns the number of nodes hosting the room.
func (s *Store) AddRoomNode(ctx context.Context, roomName livekit.RoomName, nodeID livekit.NodeID) (int, error) {
	return addRoomNodeScript.Run(ctx, s.rc, []string{roomNodesKey(roomName)}, string(nodeID), time.Now().Unix()).Int()
}

// RemoveRoomNode unregisters nodeID and returns the number of nodes still
// hosting the room. lastNode is true for exactly one caller: the one that
// removed the last node of the room.
func (s *Store) RemoveRoomNode(ctx context.Context, roomName livekit.RoomName, nodeID livekit.NodeID) (remaining int, lastNode bool, err error) {
	n, err := removeRoomNodeScript.Run(ctx, s.rc, []string{roomNodesKey(roomName)}, string(nodeID)).Int()
	if err != nil {
		return 0, false, err
	}
	if n < 0 {
		return 0, false, nil
	}
	return n, n == 0, nil
}

// GetRoomNodes returns the nodes hosting the room.
func (s *Store) GetRoomNodes(ctx context.Context, roomName livekit.RoomName) ([]livekit.NodeID, error) {
	keys, err := s.rc.HKeys(ctx, roomNodesKey(roomName)).Result()
	if err != nil {
		return nil, err
	}
	return livekit.StringsAsIDs[livekit.NodeID](keys), nil
}

// SetParticipantNode records the node a participant is (or will be) hosted on.
func (s *Store) SetParticipantNode(ctx context.Context, roomName livekit.RoomName, identity livekit.ParticipantIdentity, nodeID livekit.NodeID) error {
	return s.rc.HSet(ctx, participantNodeKey(roomName), string(identity), string(nodeID)).Err()
}

// GetParticipantNode returns the node hosting the participant, "" if unknown.
func (s *Store) GetParticipantNode(ctx context.Context, roomName livekit.RoomName, identity livekit.ParticipantIdentity) (livekit.NodeID, error) {
	nodeID, err := s.rc.HGet(ctx, participantNodeKey(roomName), string(identity)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return livekit.NodeID(nodeID), err
}

// RemoveParticipantNode removes the participant mapping if it still points to nodeID.
func (s *Store) RemoveParticipantNode(ctx context.Context, roomName livekit.RoomName, identity livekit.ParticipantIdentity, nodeID livekit.NodeID) error {
	return hdelIfEqualScript.Run(ctx, s.rc, []string{participantNodeKey(roomName)}, string(identity), string(nodeID)).Err()
}

// GetParticipantNodes returns identity => node for all participants of the room.
func (s *Store) GetParticipantNodes(ctx context.Context, roomName livekit.RoomName) (map[livekit.ParticipantIdentity]livekit.NodeID, error) {
	all, err := s.rc.HGetAll(ctx, participantNodeKey(roomName)).Result()
	if err != nil {
		return nil, err
	}
	res := make(map[livekit.ParticipantIdentity]livekit.NodeID, len(all))
	for identity, nodeID := range all {
		res[livekit.ParticipantIdentity(identity)] = livekit.NodeID(nodeID)
	}
	return res, nil
}

// RemoveNode removes all state of a (dead) node from the room. lastNode is
// true when it was the last node of the room.
func (s *Store) RemoveNode(ctx context.Context, roomName livekit.RoomName, nodeID livekit.NodeID) (lastNode bool, err error) {
	participants, err := s.GetParticipantNodes(ctx, roomName)
	if err != nil {
		return false, err
	}
	for identity, pNodeID := range participants {
		if pNodeID == nodeID {
			if err := s.RemoveParticipantNode(ctx, roomName, identity, nodeID); err != nil {
				return false, err
			}
		}
	}
	_, lastNode, err = s.RemoveRoomNode(ctx, roomName, nodeID)
	return lastNode, err
}

// ClaimRoomAgents returns true for exactly one node of the room, which is then
// responsible for dispatching room level agents.
func (s *Store) ClaimRoomAgents(ctx context.Context, roomName livekit.RoomName, nodeID livekit.NodeID) (bool, error) {
	ok, err := s.rc.HSetNX(ctx, roomMetaKey(roomName), roomMetaAgentsNode, string(nodeID)).Result()
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	owner, err := s.rc.HGet(ctx, roomMetaKey(roomName), roomMetaAgentsNode).Result()
	if err != nil {
		return false, err
	}
	return owner == string(nodeID), nil
}

// SetNodeRelayAddress publishes the relay address of a node.
func (s *Store) SetNodeRelayAddress(ctx context.Context, nodeID livekit.NodeID, addr string) error {
	return s.rc.HSet(ctx, nodeRelayKey, string(nodeID), addr).Err()
}

// GetNodeRelayAddress returns the relay address of a node, "" if unknown.
func (s *Store) GetNodeRelayAddress(ctx context.Context, nodeID livekit.NodeID) (string, error) {
	addr, err := s.rc.HGet(ctx, nodeRelayKey, string(nodeID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return addr, err
}

// PruneNodeRelayAddresses removes the relay addresses of all nodes not in keep.
func (s *Store) PruneNodeRelayAddresses(ctx context.Context, keep map[livekit.NodeID]bool) ([]livekit.NodeID, error) {
	nodeIDs, err := s.rc.HKeys(ctx, nodeRelayKey).Result()
	if err != nil {
		return nil, err
	}
	var removed []livekit.NodeID
	for _, id := range nodeIDs {
		if keep[livekit.NodeID(id)] {
			continue
		}
		if err := s.rc.HDel(ctx, nodeRelayKey, id).Err(); err != nil {
			return removed, err
		}
		removed = append(removed, livekit.NodeID(id))
	}
	return removed, nil
}

// RemoveNodeRelayAddress removes the relay address of a node.
func (s *Store) RemoveNodeRelayAddress(ctx context.Context, nodeID livekit.NodeID) error {
	return s.rc.HDel(ctx, nodeRelayKey, string(nodeID)).Err()
}

// DeleteRoom removes all distributed state of a room.
func (s *Store) DeleteRoom(ctx context.Context, roomName livekit.RoomName) error {
	return s.rc.Del(ctx, roomNodesKey(roomName), participantNodeKey(roomName), roomMetaKey(roomName)).Err()
}

// LockPlacement serializes participant placement for a room across the cluster.
func (s *Store) LockPlacement(ctx context.Context, roomName livekit.RoomName) (func(), error) {
	key := placementLockKeyPrefix + string(roomName)
	token := utils.RandomSecret()
	deadline := time.Now().Add(placementLockTTL)
	for {
		ok, err := s.rc.SetNX(ctx, key, token, placementLockTTL).Result()
		if err != nil {
			return nil, err
		}
		if ok {
			return func() {
				_ = unlockScript.Run(context.Background(), s.rc, []string{key}, token).Err()
			}, nil
		}
		if time.Now().After(deadline) {
			return nil, ErrPlacementLockTimeout
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
