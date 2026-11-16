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
	"time"

	"github.com/livekit/protocol/livekit"

	"github.com/livekit/livekit-server/pkg/routing/selector"
)

// nodeCheckWorker detects nodes of the room that died without announcing it
// (crash, network partition). Their participants are removed from the local
// room; the affected clients reconnect and are placed on a live node, because
// the placement ignores participant assignments to dead nodes.
func (rs *roomState) nodeCheckWorker() {
	ticker := time.NewTicker(rs.m.conf.NodeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rs.ctx.Done():
			return
		case <-ticker.C:
		}

		nodes, err := rs.m.router.ListNodes()
		if err != nil {
			rs.logger.Warnw("could not list nodes", err)
			continue
		}
		alive := make(map[livekit.NodeID]bool, len(nodes))
		for _, n := range nodes {
			alive[livekit.NodeID(n.Id)] = selector.IsAvailable(n)
		}

		for _, nodeID := range rs.remoteNodeIDs() {
			if alive[nodeID] {
				continue
			}
			rs.logger.Warnw("remote node of room is unavailable", nil, "remoteNodeID", nodeID)
			removed := rs.removeNode(nodeID, "node unavailable")
			if f := rs.m.onFailedNodeParticipant; f != nil {
				for _, rp := range removed {
					f(rs.ctx, rs.room.Name(), rp.lastActiveInfo())
				}
			}
			if _, err := rs.m.store.RemoveNode(rs.ctx, rs.room.Name(), nodeID); err != nil && rs.ctx.Err() == nil {
				rs.logger.Warnw("could not remove dead node from room", err, "remoteNodeID", nodeID)
			}
		}
	}
}

func (rs *roomState) remoteNodeIDs() []livekit.NodeID {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	seen := make(map[livekit.NodeID]struct{})
	var ids []livekit.NodeID
	add := func(id livekit.NodeID) {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	for _, rp := range rs.remote {
		add(rp.NodeID())
	}
	for id := range rs.conns {
		add(id)
	}
	return ids
}
