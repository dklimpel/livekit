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
	"slices"

	"github.com/redis/go-redis/v9"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/selector"
)

type NodeLister interface {
	ListNodes() ([]*livekit.Node, error)
}

// Placer decides which node hosts a participant of a distributed room.
//
// Strategy "fill, then overflow": a participant is placed on a node that
// already hosts the room, preferring the fullest node that is still below its
// limits. Only when all nodes of the room are full, another node of the
// cluster is added to the room.
type Placer struct {
	conf     config.DistributedConfig
	limits   config.LimitConfig
	store    *Store
	rc       redis.UniversalClient
	nodes    NodeLister
	selector selector.NodeSelector
	logger   logger.Logger
}

var _ routing.ParticipantPlacer = (*Placer)(nil)

func NewPlacer(conf *config.Config, store *Store, rc redis.UniversalClient, nodes NodeLister) (*Placer, error) {
	sel, err := selector.CreateNodeSelector(conf)
	if err != nil {
		return nil, err
	}
	return &Placer{
		conf:     conf.Distributed,
		limits:   conf.Limit,
		store:    store,
		rc:       rc,
		nodes:    nodes,
		selector: sel,
		logger:   logger.GetLogger().WithComponent("distributed"),
	}, nil
}

type placementState struct {
	available  map[livekit.NodeID]*livekit.Node
	roomNodes  []livekit.NodeID
	counts     map[livekit.NodeID]int
	assignment map[livekit.ParticipantIdentity]livekit.NodeID
}

func (p *Placer) loadState(ctx context.Context, roomName livekit.RoomName) (*placementState, error) {
	nodes, err := p.nodes.ListNodes()
	if err != nil {
		return nil, err
	}
	st := &placementState{
		available: make(map[livekit.NodeID]*livekit.Node),
		counts:    make(map[livekit.NodeID]int),
	}
	for _, n := range selector.GetAvailableNodes(nodes) {
		st.available[livekit.NodeID(n.Id)] = n
	}

	st.assignment, err = p.store.GetParticipantNodes(ctx, roomName)
	if err != nil {
		return nil, err
	}
	for _, nodeID := range st.assignment {
		st.counts[nodeID]++
	}

	roomNodes, err := p.store.GetRoomNodes(ctx, roomName)
	if err != nil {
		return nil, err
	}
	// nodes with assigned participants, which have not created the room yet, are room nodes as well
	for nodeID := range st.counts {
		if !slices.Contains(roomNodes, nodeID) {
			roomNodes = append(roomNodes, nodeID)
		}
	}
	// node allocated by the standard room allocator (e.g. RoomService.CreateRoom)
	if primary, err := p.rc.HGet(ctx, routing.NodeRoomKey, string(roomName)).Result(); err == nil {
		if !slices.Contains(roomNodes, livekit.NodeID(primary)) {
			roomNodes = append(roomNodes, livekit.NodeID(primary))
		}
	} else if !errors.Is(err, redis.Nil) {
		return nil, err
	}
	slices.Sort(roomNodes)
	st.roomNodes = roomNodes
	return st, nil
}

// isFull reports whether node can not take another participant of the room.
func (p *Placer) isFull(node *livekit.Node, roomParticipants int) bool {
	if p.conf.MaxParticipantsPerNode > 0 && roomParticipants >= p.conf.MaxParticipantsPerNode {
		return true
	}
	if selector.LimitsReached(p.limits, node.Stats) {
		return true
	}
	if p.conf.CPULoadLimit > 0 && node.Stats != nil && node.Stats.CpuLoad > p.conf.CPULoadLimit {
		return true
	}
	return false
}

// pick returns the node for the next participant of the room.
func (p *Placer) pick(st *placementState) (*livekit.Node, string, error) {
	// fill: fullest room node below its limits
	var best *livekit.Node
	bestCount := -1
	for _, nodeID := range st.roomNodes {
		node := st.available[nodeID]
		if node == nil {
			continue
		}
		count := st.counts[nodeID]
		if p.isFull(node, count) {
			continue
		}
		if count > bestCount {
			best, bestCount = node, count
		}
	}
	if best != nil {
		return best, "fill", nil
	}

	// overflow: add another node to the room
	var candidates []*livekit.Node
	for nodeID, node := range st.available {
		if slices.Contains(st.roomNodes, nodeID) {
			continue
		}
		if p.isFull(node, 0) {
			continue
		}
		candidates = append(candidates, node)
	}
	if len(candidates) == 0 {
		return nil, "", routing.ErrNodeLimitReached
	}
	node, err := p.selector.SelectNode(candidates)
	if err != nil {
		return nil, "", err
	}
	if len(st.roomNodes) == 0 {
		return node, "new", nil
	}
	return node, "overflow", nil
}

// NodeForParticipant selects (and records) the node for a participant.
// A participant that is already assigned to a live node stays there, so
// resumes and identity replacements are handled by the node holding the
// participant's session.
func (p *Placer) NodeForParticipant(ctx context.Context, roomName livekit.RoomName, pi routing.ParticipantInit) (livekit.NodeID, string, error) {
	unlock, err := p.store.LockPlacement(ctx, roomName)
	if err != nil {
		return "", "", err
	}
	defer unlock()

	st, err := p.loadState(ctx, roomName)
	if err != nil {
		return "", "", err
	}

	if nodeID := st.assignment[pi.Identity]; nodeID != "" && st.available[nodeID] != nil {
		return nodeID, "assigned", nil
	}

	node, reason, err := p.pick(st)
	if err != nil {
		return "", "", err
	}
	nodeID := livekit.NodeID(node.Id)
	if pi.Identity != "" {
		if err := p.store.SetParticipantNode(ctx, roomName, pi.Identity, nodeID); err != nil {
			return "", "", err
		}
	}
	// keep upstream routing state pointing to a live node of the room
	if primary := firstAvailable(st.roomNodes, st.available); primary == "" {
		if err := p.rc.HSet(ctx, routing.NodeRoomKey, string(roomName), string(nodeID)).Err(); err != nil {
			return "", "", err
		}
	}

	p.logger.Infow(
		"placed participant",
		"room", roomName,
		"participant", pi.Identity,
		"nodeID", nodeID,
		"reason", reason,
		"roomNodes", st.roomNodes,
		"roomParticipantsOnNode", st.counts[nodeID]+1,
	)
	return nodeID, reason, nil
}

// NodeForRoom returns the node that would host the next participant of the room.
// It is used where upstream asks for "the" node of a room (room creation, WHIP,
// limit checks).
func (p *Placer) NodeForRoom(ctx context.Context, roomName livekit.RoomName) (*livekit.Node, error) {
	st, err := p.loadState(ctx, roomName)
	if err != nil {
		return nil, err
	}
	if len(st.roomNodes) == 0 {
		return nil, routing.ErrNotFound
	}
	node, _, err := p.pick(st)
	if err != nil {
		// everything is full, report a room node so that callers apply their limit handling
		if nodeID := firstAvailable(st.roomNodes, st.available); nodeID != "" {
			return st.available[nodeID], nil
		}
		return nil, routing.ErrNotFound
	}
	return node, nil
}

func firstAvailable(nodeIDs []livekit.NodeID, available map[livekit.NodeID]*livekit.Node) livekit.NodeID {
	for _, nodeID := range nodeIDs {
		if available[nodeID] != nil {
			return nodeID
		}
	}
	return ""
}
