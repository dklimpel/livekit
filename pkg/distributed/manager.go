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

// Package distributed hosts a single LiveKit room on multiple nodes.
//
// Every node hosting a part of a room runs a regular rtc.Room with its local
// participants. Participants on other nodes are mirrored into the local room
// as remote participants, their tracks are pulled on demand over a WebRTC
// relay connection between the nodes. See docs/distributed-rooms.
package distributed

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/bus"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/selector"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

var ErrRedisRequired = errors.New("distributed rooms require redis")

const (
	defaultNodeCheckInterval = 2 * time.Second
	detachTimeout            = 5 * time.Second
	relayAddressInterval     = 30 * time.Second
)

// Manager is the entry point of the distributed room implementation on a node.
// All methods are safe to call on a nil Manager, which is what callers get
// when distributed rooms are disabled.
type Manager struct {
	conf        config.DistributedConfig
	fullConf    *config.Config
	rtcConf     *rtc.WebRTCConfig
	currentNode routing.LocalNode
	router      routing.Router
	store       *Store
	placer      *Placer
	bus         psrpc.MessageBus
	relay       *relayTransport
	logger      logger.Logger

	lock  sync.Mutex
	rooms map[livekit.RoomName]*roomState

	onFailedNodeParticipant func(ctx context.Context, roomName livekit.RoomName, pi *livekit.ParticipantInfo)

	ctx    context.Context
	cancel context.CancelFunc
}

// NewManager returns nil when distributed rooms are disabled.
func NewManager(
	conf *config.Config,
	rtcConf *rtc.WebRTCConfig,
	currentNode routing.LocalNode,
	router routing.Router,
	bus psrpc.MessageBus,
) (*Manager, error) {
	if !conf.Distributed.Enabled {
		return nil, nil
	}
	rr, ok := router.(*routing.RedisRouter)
	if !ok || rr.RedisClient() == nil {
		return nil, ErrRedisRequired
	}

	dc := conf.Distributed
	if dc.NodeCheckInterval <= 0 {
		dc.NodeCheckInterval = defaultNodeCheckInterval
	}

	store := NewStore(rr.RedisClient())
	placer, err := NewPlacer(conf, store, rr.RedisClient(), router)
	if err != nil {
		return nil, err
	}
	rr.SetParticipantPlacer(placer)

	m := &Manager{
		conf:        dc,
		fullConf:    conf,
		rtcConf:     rtcConf,
		currentNode: currentNode,
		router:      router,
		store:       store,
		placer:      placer,
		bus:         bus,
		logger:      logger.GetLogger().WithComponent("distributed").WithValues("nodeID", currentNode.NodeID()),
		rooms:       make(map[livekit.RoomName]*roomState),
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())

	relayIP, relayIPSource, err := relayNodeIP(dc.Relay.NodeIP, os.Getenv(RelayNodeIPEnv), currentNode.NodeIP())
	if err != nil {
		m.cancel()
		return nil, err
	}
	m.logger.Infow("relay node ip", "ip", relayIP, "source", relayIPSource)
	if m.relay, err = newRelayTransport(dc.Relay, relayIP, rtcConf, m.logger); err != nil {
		m.cancel()
		return nil, err
	}

	if err := store.SetNodeRelayAddress(m.ctx, currentNode.NodeID(), m.relay.address()); err != nil {
		m.logger.Warnw("could not register relay address", err)
	}

	sub, err := subscribe(m.ctx, bus, nodeChannel(currentNode.NodeID()))
	if err != nil {
		m.cancel()
		m.relay.close()
		return nil, err
	}
	go m.nodeReadWorker(sub)
	go m.relayAddressWorker()

	m.logger.Infow("distributed rooms enabled",
		"maxParticipantsPerNode", dc.MaxParticipantsPerNode,
		"cpuLoadLimit", dc.CPULoadLimit,
	)
	return m, nil
}

// OnFailedNodeParticipant registers a callback for participants removed because
// their node failed. The callback cleans up state the failed node could not
// clean up itself (e.g. the participant in the room store).
func (m *Manager) OnFailedNodeParticipant(f func(ctx context.Context, roomName livekit.RoomName, pi *livekit.ParticipantInfo)) {
	if m == nil {
		return
	}
	m.onFailedNodeParticipant = f
}

func (m *Manager) Enabled() bool {
	return m != nil
}

func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.cancel()

	m.lock.Lock()
	rooms := make([]*roomState, 0, len(m.rooms))
	for _, rs := range m.rooms {
		rooms = append(rooms, rs)
	}
	m.lock.Unlock()
	for _, rs := range rooms {
		rs.close()
	}
	_ = m.store.RemoveNodeRelayAddress(context.Background(), m.currentNode.NodeID())
	m.relay.close()
}

// relayAddressWorker keeps this node's relay address registered and removes
// the addresses of nodes that are gone (crashed, or replaced by a new pod).
// Live nodes re-register every interval, so a node that is not registered in
// the node registry yet is restored on its next run.
func (m *Manager) relayAddressWorker() {
	ticker := time.NewTicker(relayAddressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}

		if err := m.store.SetNodeRelayAddress(m.ctx, m.currentNode.NodeID(), m.relay.address()); err != nil {
			m.logger.Warnw("could not register relay address", err)
			continue
		}
		nodes, err := m.router.ListNodes()
		if err != nil {
			continue
		}
		available := make(map[livekit.NodeID]bool, len(nodes))
		for _, n := range nodes {
			if selector.IsAvailable(n) {
				available[livekit.NodeID(n.Id)] = true
			}
		}
		available[m.currentNode.NodeID()] = true
		removed, err := m.store.PruneNodeRelayAddresses(m.ctx, available)
		if err != nil {
			m.logger.Warnw("could not prune relay addresses", err)
		} else if len(removed) > 0 {
			m.logger.Debugw("removed relay addresses of unavailable nodes", "nodeIDs", removed)
		}
	}
}

// nodeReadWorker dispatches relay control messages addressed to this node.
func (m *Manager) nodeReadWorker(sub bus.Subscription[*wrapperspb.BytesValue]) {
	defer func() { _ = sub.Close() }()
	for {
		select {
		case <-m.ctx.Done():
			return
		case v, ok := <-sub.Channel():
			if !ok {
				return
			}
			msg, err := decode(v)
			if err != nil {
				m.logger.Warnw("could not decode relay message", err)
				continue
			}
			m.lock.Lock()
			rs := m.rooms[msg.Room]
			m.lock.Unlock()
			if rs == nil {
				m.logger.Debugw("relay message for unknown room", "room", msg.Room, "type", msg.Type)
				continue
			}
			rs.handleRelayMessage(msg)
		}
	}
}

// PrepareRoomInternal is called before a node creates its rtc.Room for a room.
// Room level agents are dispatched by one node of the room only.
func (m *Manager) PrepareRoomInternal(ctx context.Context, roomName livekit.RoomName, internal *livekit.RoomInternal) *livekit.RoomInternal {
	if m == nil || internal == nil || len(internal.AgentDispatches) == 0 {
		return internal
	}
	claimed, err := m.store.ClaimRoomAgents(ctx, roomName, m.currentNode.NodeID())
	if err != nil {
		m.logger.Warnw("could not claim room agents", err, "room", roomName)
	}
	if claimed {
		return internal
	}
	internal = proto.CloneOf(internal)
	internal.AgentDispatches = nil
	return internal
}

// AttachRoom is called after a node created its rtc.Room for a room. It
// returns true when this node is the first node hosting the room (or
// distributed rooms are disabled), i.e. when the room actually started.
func (m *Manager) AttachRoom(ctx context.Context, room *rtc.Room) bool {
	if m == nil {
		return true
	}
	numNodes, err := m.store.AddRoomNode(ctx, room.Name(), m.currentNode.NodeID())
	if err != nil {
		room.Logger().Errorw("could not register room node", err)
		numNodes = 1
	}

	rs := newRoomState(m, room)
	m.lock.Lock()
	if old := m.rooms[room.Name()]; old != nil {
		defer old.close()
	}
	m.rooms[room.Name()] = rs
	m.lock.Unlock()

	rs.start()
	room.Logger().Infow("room attached to distributed room", "numNodes", numNodes)
	return numNodes == 1
}

// DetachRoom is called when the local rtc.Room closed. It returns true when the
// cluster wide room state should be deleted, i.e. this node was the last node
// hosting the room (or distributed rooms are disabled).
func (m *Manager) DetachRoom(_ context.Context, room *rtc.Room, reason types.RoomCloseReason) bool {
	if m == nil {
		return true
	}
	// the context of the request that created the room is long gone when the room closes
	ctx, cancel := context.WithTimeout(context.Background(), detachTimeout)
	defer cancel()

	m.lock.Lock()
	rs := m.rooms[room.Name()]
	if rs != nil && rs.room == room {
		delete(m.rooms, room.Name())
	} else {
		rs = nil
	}
	m.lock.Unlock()

	if rs != nil {
		rs.onLocalRoomClosed(reason)
		rs.close()
	}

	_, lastNode, err := m.store.RemoveRoomNode(ctx, room.Name(), m.currentNode.NodeID())
	if err != nil {
		room.Logger().Errorw("could not unregister room node", err)
		return false
	}
	if !lastNode {
		// nodes that died without unregistering would keep the room alive forever
		remaining, prunedLast, err := m.pruneRoomNodes(ctx, room.Name())
		if err != nil {
			room.Logger().Errorw("could not get room nodes", err)
			return false
		}
		if !prunedLast {
			room.Logger().Infow("room closed on this node, still hosted by other nodes", "remainingNodes", remaining)
			return false
		}
	}
	if err := m.store.DeleteRoom(ctx, room.Name()); err != nil {
		room.Logger().Errorw("could not delete distributed room state", err)
	}
	return true
}

// pruneRoomNodes removes nodes that are no longer available (crashed, or
// restarted with a new node ID) from the room. It returns the remaining
// available nodes and whether this call removed the last node of the room.
func (m *Manager) pruneRoomNodes(ctx context.Context, roomName livekit.RoomName) ([]livekit.NodeID, bool, error) {
	roomNodes, err := m.store.GetRoomNodes(ctx, roomName)
	if err != nil || len(roomNodes) == 0 {
		return nil, false, err
	}
	nodes, err := m.router.ListNodes()
	if err != nil {
		return nil, false, err
	}
	available := make(map[livekit.NodeID]bool, len(nodes))
	for _, n := range nodes {
		available[livekit.NodeID(n.Id)] = selector.IsAvailable(n)
	}
	var live []livekit.NodeID
	removedLast := false
	for _, nodeID := range roomNodes {
		if available[nodeID] {
			live = append(live, nodeID)
			continue
		}
		m.logger.Infow("removing unavailable node from room", "room", roomName, "remoteNodeID", nodeID)
		last, err := m.store.RemoveNode(ctx, roomName, nodeID)
		if err != nil {
			return nil, false, err
		}
		removedLast = removedLast || last
	}
	return live, removedLast, nil
}

// OnParticipantChanged is called for every participant change of a local room.
func (m *Manager) OnParticipantChanged(room *rtc.Room, p types.Participant) {
	if m == nil {
		return
	}
	if rs := m.getRoomState(room); rs != nil {
		rs.onParticipantChanged(p)
	}
}

func (m *Manager) getRoomState(room *rtc.Room) *roomState {
	m.lock.Lock()
	defer m.lock.Unlock()
	rs := m.rooms[room.Name()]
	if rs == nil || rs.room != room {
		return nil
	}
	return rs
}

// OnRoomUpdated is called when the room proto of a local room changed.
func (m *Manager) OnRoomUpdated(room *rtc.Room) {
	if m == nil {
		return
	}
	if rs := m.getRoomState(room); rs != nil {
		rs.onRoomUpdated()
	}
}
