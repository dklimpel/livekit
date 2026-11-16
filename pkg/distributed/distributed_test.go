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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/selector"
	"github.com/livekit/livekit-server/pkg/rtc"
)

func testNode(id string, cpu float32) *livekit.Node {
	return &livekit.Node{
		Id:    id,
		State: livekit.NodeState_SERVING,
		Stats: &livekit.NodeStats{UpdatedAt: time.Now().Unix(), CpuLoad: cpu, NumCpus: 4},
	}
}

func testPlacer(t *testing.T, maxPerNode int) *Placer {
	conf := &config.Config{Distributed: config.DistributedConfig{Enabled: true, MaxParticipantsPerNode: maxPerNode}}
	conf.NodeSelector.SortBy = "random"
	conf.NodeSelector.Algorithm = "lowest"
	sel, err := selector.CreateNodeSelector(conf)
	require.NoError(t, err)
	return &Placer{conf: conf.Distributed, limits: conf.Limit, selector: sel, logger: logger.GetLogger()}
}

func testState(nodes []*livekit.Node, roomNodes []livekit.NodeID, counts map[livekit.NodeID]int) *placementState {
	st := &placementState{
		available: make(map[livekit.NodeID]*livekit.Node),
		roomNodes: roomNodes,
		counts:    counts,
	}
	for _, n := range nodes {
		st.available[livekit.NodeID(n.Id)] = n
	}
	return st
}

func TestPlacementFillThenOverflow(t *testing.T) {
	p := testPlacer(t, 2)
	nodes := []*livekit.Node{testNode("A", 0.1), testNode("B", 0.1), testNode("C", 0.1)}

	t.Run("new room picks any node", func(t *testing.T) {
		node, reason, err := p.pick(testState(nodes, nil, map[livekit.NodeID]int{}))
		require.NoError(t, err)
		require.NotNil(t, node)
		require.Equal(t, "new", reason)
	})

	t.Run("fills the room node below its limit", func(t *testing.T) {
		node, reason, err := p.pick(testState(nodes, []livekit.NodeID{"A"}, map[livekit.NodeID]int{"A": 1}))
		require.NoError(t, err)
		require.Equal(t, "A", node.Id)
		require.Equal(t, "fill", reason)
	})

	t.Run("prefers the fullest room node", func(t *testing.T) {
		p := testPlacer(t, 10)
		node, _, err := p.pick(testState(nodes, []livekit.NodeID{"A", "B"}, map[livekit.NodeID]int{"A": 3, "B": 7}))
		require.NoError(t, err)
		require.Equal(t, "B", node.Id)
	})

	t.Run("overflows to a node outside the room", func(t *testing.T) {
		node, reason, err := p.pick(testState(nodes, []livekit.NodeID{"A", "B"}, map[livekit.NodeID]int{"A": 2, "B": 2}))
		require.NoError(t, err)
		require.Equal(t, "C", node.Id)
		require.Equal(t, "overflow", reason)
	})

	t.Run("everything full", func(t *testing.T) {
		_, _, err := p.pick(testState(nodes, []livekit.NodeID{"A", "B", "C"}, map[livekit.NodeID]int{"A": 2, "B": 2, "C": 2}))
		require.ErrorIs(t, err, routing.ErrNodeLimitReached)
	})

	t.Run("dead room node is skipped", func(t *testing.T) {
		// "A" is a room node, but not available (missing from the available nodes)
		node, reason, err := p.pick(testState(nodes[1:], []livekit.NodeID{"A"}, map[livekit.NodeID]int{"A": 1}))
		require.NoError(t, err)
		require.NotEqual(t, "A", node.Id)
		require.Equal(t, "overflow", reason)
	})
}

func TestPlacementCPULimit(t *testing.T) {
	p := testPlacer(t, 0)
	p.conf.CPULoadLimit = 0.8
	nodes := []*livekit.Node{testNode("A", 0.9), testNode("B", 0.2)}

	node, reason, err := p.pick(testState(nodes, []livekit.NodeID{"A"}, map[livekit.NodeID]int{"A": 1}))
	require.NoError(t, err)
	require.Equal(t, "B", node.Id)
	require.Equal(t, "overflow", reason)
}

func TestRelayStreamID(t *testing.T) {
	id := relayStreamID("TR_abc", mime.MimeTypeVP8, 2)
	trackID, mimeType, idx, ok := parseRelayStreamID(id)
	require.True(t, ok)
	require.Equal(t, livekit.TrackID("TR_abc"), trackID)
	require.Equal(t, mime.MimeTypeVP8, mimeType)
	require.Equal(t, 2, idx)

	_, _, _, ok = parseRelayStreamID("garbage")
	require.False(t, ok)
}

func TestMessageRoundTrip(t *testing.T) {
	info := &livekit.ParticipantInfo{Sid: "PA_1", Identity: "alice", Version: 3}
	data, err := encodeForTest(&message{Type: msgParticipantUpdate, Node: "N1", Room: "room", Participant: marshalProto(info)})
	require.NoError(t, err)

	msg, err := decode(data)
	require.NoError(t, err)
	require.Equal(t, msgParticipantUpdate, msg.Type)
	require.Equal(t, livekit.NodeID("N1"), msg.Node)
	require.NotEmpty(t, msg.Participant)
}

func TestRemoteParticipant(t *testing.T) {
	info := &livekit.ParticipantInfo{
		Sid:         "PA_1",
		Identity:    "alice",
		State:       livekit.ParticipantInfo_ACTIVE,
		Version:     5,
		IsPublisher: true,
		Permission:  &livekit.ParticipantPermission{Hidden: true},
	}
	rp := newRemoteParticipant("N1", info, logger.GetLogger())
	require.True(t, rtc.IsRemoteParticipant(rp))
	require.Equal(t, livekit.ParticipantIdentity("alice"), rp.Identity())
	require.True(t, rp.Hidden())
	require.True(t, rp.IsPublisher())

	t.Run("outdated updates are ignored", func(t *testing.T) {
		require.False(t, rp.update(&livekit.ParticipantInfo{Sid: "PA_1", Identity: "alice", Version: 4}, nil))
		require.True(t, rp.update(&livekit.ParticipantInfo{Sid: "PA_1", Identity: "alice", Version: 6, State: livekit.ParticipantInfo_ACTIVE}, nil))
	})

	t.Run("permissions", func(t *testing.T) {
		require.True(t, rp.HasPermission("TR_1", "bob"))
		rp.update(&livekit.ParticipantInfo{Sid: "PA_1", Identity: "alice", Version: 7}, &livekit.SubscriptionPermission{
			TrackPermissions: []*livekit.TrackPermission{
				{ParticipantIdentity: "bob", TrackSids: []string{"TR_1"}},
				{ParticipantIdentity: "carol", AllTracks: true},
			},
		})
		require.True(t, rp.HasPermission("TR_1", "bob"))
		require.False(t, rp.HasPermission("TR_2", "bob"))
		require.True(t, rp.HasPermission("TR_2", "carol"))
		require.False(t, rp.HasPermission("TR_1", "dave"))
	})

	t.Run("audio level times out", func(t *testing.T) {
		rp.setAudioLevel(0.5, true)
		level, active := rp.GetAudioLevel()
		require.True(t, active)
		require.Equal(t, 0.5, level)

		rp.lock.Lock()
		rp.audioAt = time.Now().Add(-2 * remoteSpeakerTimeout)
		rp.lock.Unlock()
		_, active = rp.GetAudioLevel()
		require.False(t, active)
	})

	t.Run("disconnect", func(t *testing.T) {
		before := rp.ToProto().Version
		require.NoError(t, rp.Close(false, 0, false))
		require.True(t, rp.IsDisconnected())
		require.Equal(t, livekit.ParticipantInfo_DISCONNECTED, rp.ToProto().State)
		require.Greater(t, rp.ToProto().Version, before)
	})
}

func encodeForTest(msg *message) (*wrapperspb.BytesValue, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return wrapperspb.Bytes(data), nil
}

func TestRelayNodeIP(t *testing.T) {
	ip, source, err := relayNodeIP("10.0.0.1", "10.0.0.2", "1.2.3.4")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", ip)
	require.Equal(t, "config", source)

	ip, source, err = relayNodeIP("", "10.0.0.2", "1.2.3.4")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2", ip)
	require.Equal(t, RelayNodeIPEnv, source)

	ip, _, err = relayNodeIP("", "", "1.2.3.4")
	require.NoError(t, err)
	require.Equal(t, "1.2.3.4", ip)

	_, _, err = relayNodeIP("", "not-an-ip", "1.2.3.4")
	require.Error(t, err)
	_, _, err = relayNodeIP("", "fd00::1", "1.2.3.4")
	require.Error(t, err)
}

func TestRelayTrackInfo(t *testing.T) {
	ti := &livekit.TrackInfo{
		Sid:    "TR_1",
		Layers: []*livekit.VideoLayer{{Quality: livekit.VideoQuality_LOW, Ssrc: 1, RepairSsrc: 2, Rid: "q"}},
		Codecs: []*livekit.SimulcastCodecInfo{{
			MimeType: "video/VP8",
			Layers:   []*livekit.VideoLayer{{Quality: livekit.VideoQuality_HIGH, Ssrc: 3, Rid: "f"}},
		}},
	}
	rti := relayTrackInfo(ti)
	require.Zero(t, rti.Layers[0].Ssrc)
	require.Zero(t, rti.Layers[0].RepairSsrc)
	require.Equal(t, "q", rti.Layers[0].Rid)
	require.Zero(t, rti.Codecs[0].Layers[0].Ssrc)
	// the original is not modified
	require.Equal(t, uint32(1), ti.Layers[0].Ssrc)
	require.Equal(t, uint32(3), ti.Codecs[0].Layers[0].Ssrc)
}

func TestRelayPortRange(t *testing.T) {
	start, end, err := relayPortRange(config.DistributedRelayConfig{})
	require.NoError(t, err)
	require.Equal(t, uint16(7890), start)
	require.Equal(t, uint16(8889), end)

	start, end, err = relayPortRange(config.DistributedRelayConfig{PortRangeStart: 20000, PortRangeEnd: 20099})
	require.NoError(t, err)
	require.Equal(t, uint16(20000), start)
	require.Equal(t, uint16(20099), end)

	// deprecated udp_port becomes the start of the range
	start, end, err = relayPortRange(config.DistributedRelayConfig{UDPPort: 9000})
	require.NoError(t, err)
	require.Equal(t, uint16(9000), start)
	require.Equal(t, uint16(9999), end)

	_, _, err = relayPortRange(config.DistributedRelayConfig{PortRangeStart: 9000, PortRangeEnd: 8000})
	require.Error(t, err)
}
