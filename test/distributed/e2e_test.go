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

// Package distributed contains end-to-end tests for distributed rooms. They run
// against an already running cluster (see test/distributed/README.md) and are
// skipped unless LIVEKIT_DISTRIBUTED_E2E=1 is set.
package distributed

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twitchtv/twirp"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils/guid"

	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/livekit-server/pkg/testutils"
	testclient "github.com/livekit/livekit-server/test/client"
)

func TestMain(m *testing.M) {
	// the test client uses the server's transport, which records metrics
	prometheus.Init("test", livekit.NodeType_SERVER)
	os.Exit(m.Run())
}

type cluster struct {
	urls      []string
	apiKey    string
	apiSecret string
}

func getCluster(t *testing.T) *cluster {
	if os.Getenv("LIVEKIT_DISTRIBUTED_E2E") != "1" {
		t.Skip("set LIVEKIT_DISTRIBUTED_E2E=1 to run distributed room end-to-end tests against a running cluster")
	}
	c := &cluster{
		urls:      strings.Split(getenv("LIVEKIT_NODE_URLS", "ws://localhost:7880,ws://localhost:7980,ws://localhost:8080"), ","),
		apiKey:    getenv("LIVEKIT_API_KEY", "devkey"),
		apiSecret: getenv("LIVEKIT_API_SECRET", "secretsecretsecretsecretsecretsecret"),
	}
	return c
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// clientOptions returns the test client options. LIVEKIT_E2E_SIGNALLING=v1
// uses the single peer connection signalling (/rtc/v1) of current browser SDKs,
// the default is the classic signalling with two peer connections (/rtc).
func clientOptions() *testclient.Options {
	opts := &testclient.Options{AutoSubscribe: true}
	if os.Getenv("LIVEKIT_E2E_SIGNALLING") == "v1" {
		opts.RTCServicePath = "/rtc/v1"
		opts.UseJoinRequestQueryParam = true
	}
	return opts
}

// join connects a participant through the signal node at urls[idx]. The
// cluster decides on which node the participant is hosted.
func (c *cluster) join(t *testing.T, room string, identity string, idx int) *testclient.RTCClient {
	at := auth.NewAccessToken(c.apiKey, c.apiSecret).SetIdentity(identity).SetName(identity)
	at.SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: room})
	token, err := at.ToJWT()
	require.NoError(t, err)

	opts := clientOptions()
	url := c.urls[idx%len(c.urls)]
	ws, err := testclient.NewWebSocketConn(url, token, opts)
	require.NoError(t, err, "could not connect to %s", url)
	client, err := testclient.NewRTCClient(ws, opts.UseJoinRequestQueryParam, opts)
	require.NoError(t, err)
	go func() { _ = client.Run() }()
	require.NoError(t, client.WaitUntilConnected(10*time.Second), "%s did not connect", identity)
	t.Cleanup(client.Stop)
	return client
}

func identities(infos []*livekit.ParticipantInfo) []string {
	var ids []string
	for _, pi := range infos {
		ids = append(ids, pi.Identity)
	}
	return ids
}

func waitForParticipants(t *testing.T, c *testclient.RTCClient, expected ...string) {
	testutils.WithTimeout(t, func() string {
		got := identities(c.RemoteParticipants())
		for _, e := range expected {
			found := false
			for _, g := range got {
				found = found || g == e
			}
			if !found {
				return fmt.Sprintf("participant %s not visible, got %v", e, got)
			}
		}
		return ""
	}, 15*time.Second)
}

// TestDistributedRoom runs all scenarios in one room with participants spread
// over the nodes (requires distributed.max_participants_per_node: 1).
func TestDistributedRoom(t *testing.T) {
	c := getCluster(t)
	room := "dist-" + guid.New("")

	alice := c.join(t, room, "alice", 0)
	bob := c.join(t, room, "bob", 1)
	carol := c.join(t, room, "carol", 2)

	t.Run("participants see each other across nodes", func(t *testing.T) {
		waitForParticipants(t, alice, "bob", "carol")
		waitForParticipants(t, bob, "alice", "carol")
		waitForParticipants(t, carol, "alice", "bob")
	})

	t.Run("media is relayed across nodes", func(t *testing.T) {
		audio, err := alice.AddStaticTrack(webrtc.MimeTypeOpus, "audio", "alice-audio")
		require.NoError(t, err)
		defer audio.Stop()
		video, err := alice.AddStaticTrack(webrtc.MimeTypeVP8, "video", "alice-video")
		require.NoError(t, err)
		defer video.Stop()

		for _, sub := range []*testclient.RTCClient{bob, carol} {
			testutils.WithTimeout(t, func() string {
				tracks := sub.SubscribedTracks()[alice.ID()]
				if len(tracks) != 2 {
					return fmt.Sprintf("expected 2 subscribed tracks of alice, got %d", len(tracks))
				}
				return ""
			}, 20*time.Second)
		}
		for _, sub := range []*testclient.RTCClient{bob, carol} {
			before := sub.BytesReceived()
			testutils.WithTimeout(t, func() string {
				if received := sub.BytesReceived(); received <= before {
					return fmt.Sprintf("no media received yet (%d bytes)", received)
				}
				return ""
			}, 10*time.Second)
		}
	})

	t.Run("media is relayed in both directions", func(t *testing.T) {
		// node pairs now relay in both directions (alice -> bob and bob -> alice)
		audio, err := bob.AddStaticTrack(webrtc.MimeTypeOpus, "audio", "bob-audio")
		require.NoError(t, err)
		defer audio.Stop()

		for _, sub := range []*testclient.RTCClient{alice, carol} {
			testutils.WithTimeout(t, func() string {
				if n := len(sub.SubscribedTracks()[bob.ID()]); n != 1 {
					return fmt.Sprintf("expected 1 subscribed track of bob, got %d", n)
				}
				return ""
			}, 20*time.Second)
		}
		// alice publishes only, everything she receives comes from bob
		before := alice.BytesReceived()
		testutils.WithTimeout(t, func() string {
			if received := alice.BytesReceived(); received <= before {
				return fmt.Sprintf("alice received no media from bob yet (%d bytes)", received)
			}
			return ""
		}, 10*time.Second)
	})

	t.Run("data packets are relayed across nodes", func(t *testing.T) {
		var lock sync.Mutex
		received := map[string]string{}
		for name, sub := range map[string]*testclient.RTCClient{"alice": alice, "carol": carol} {
			sub.SetOnDataReceived(func(data []byte, sid string) {
				lock.Lock()
				received[name] = string(data)
				lock.Unlock()
			})
		}
		testutils.WithTimeout(t, func() string {
			// data channels might still be opening, keep sending
			_ = bob.PublishData([]byte("hello from bob"), livekit.DataPacket_RELIABLE)
			lock.Lock()
			defer lock.Unlock()
			for _, name := range []string{"alice", "carol"} {
				if received[name] != "hello from bob" {
					return name + " did not receive data"
				}
			}
			return ""
		}, 15*time.Second)
	})

	t.Run("leaving participant is removed on all nodes", func(t *testing.T) {
		carol.Stop()
		for _, c := range []*testclient.RTCClient{alice, bob} {
			testutils.WithTimeout(t, func() string {
				for _, id := range identities(c.RemoteParticipants()) {
					if id == "carol" {
						return "carol still visible"
					}
				}
				return ""
			}, 15*time.Second)
		}
	})
}

// TestNodeFailure stops (kills) the node hosting a participant and verifies
// that the room continues on the other nodes and the participant can rejoin.
//
// Requires
//   - LIVEKIT_REDIS_ADDR: Redis of the cluster
//   - LIVEKIT_NODE_RELAY_ADDRS: relay address (ip:port) of the nodes, in the order of LIVEKIT_NODE_URLS
//   - LIVEKIT_STOP_NODE_CMD: shell command, {node} is replaced by the relay address of the node to stop
func TestNodeFailure(t *testing.T) {
	c := getCluster(t)
	stopCmd := os.Getenv("LIVEKIT_STOP_NODE_CMD")
	relayAddrs := strings.Split(os.Getenv("LIVEKIT_NODE_RELAY_ADDRS"), ",")
	if stopCmd == "" || len(relayAddrs) != len(c.urls) {
		t.Skip("set LIVEKIT_STOP_NODE_CMD and LIVEKIT_NODE_RELAY_ADDRS to run the node failure test")
	}
	rc := redis.NewClient(&redis.Options{Addr: getenv("LIVEKIT_REDIS_ADDR", "localhost:6379")})
	defer rc.Close()
	ctx := context.Background()

	room := "dist-failover-" + guid.New("")
	names := []string{"alice", "bob", "carol"}
	clients := map[string]*testclient.RTCClient{}
	signalIdx := map[string]int{}
	for i, name := range names {
		clients[name] = c.join(t, room, name, i)
		signalIdx[name] = i
	}
	waitForParticipants(t, clients["alice"], "bob", "carol")

	// which node (index of LIVEKIT_NODE_URLS) hosts which participant
	nodeIdx := map[string]int{}
	for _, name := range names {
		nodeID, err := rc.HGet(ctx, "dist:participant_node:"+room, name).Result()
		require.NoError(t, err)
		addr, err := rc.HGet(ctx, "dist:node_relay", nodeID).Result()
		require.NoError(t, err)
		idx := slices.Index(relayAddrs, addr)
		require.GreaterOrEqual(t, idx, 0, "unknown relay address %s", addr)
		nodeIdx[name] = idx
	}
	victim := "carol"
	failed := nodeIdx[victim]
	for _, name := range names {
		if name != victim {
			require.NotEqual(t, failed, nodeIdx[name], "%s shares the node of %s, check max_participants_per_node: 1", name, victim)
		}
	}

	// the observer's session survives: neither its signal nor its RTC node fails
	var observer, other string
	for _, name := range names {
		if name == victim {
			continue
		}
		if observer == "" && signalIdx[name] != failed {
			observer = name
		} else {
			other = name
		}
	}
	require.NotEmpty(t, observer)
	t.Logf("victim %s on node %d, observer %s, other %s", victim, failed, observer, other)

	audio, err := clients[observer].AddStaticTrack(webrtc.MimeTypeOpus, "audio", observer+"-audio")
	require.NoError(t, err)
	defer audio.Stop()

	cmd := strings.ReplaceAll(stopCmd, "{node}", relayAddrs[failed])
	t.Logf("stopping node %d: %s", failed, cmd)
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	require.NoError(t, err, string(out))
	clients[victim].Stop()

	t.Run("participants of the failed node are removed", func(t *testing.T) {
		testutils.WithTimeout(t, func() string {
			for _, id := range identities(clients[observer].RemoteParticipants()) {
				if id == victim {
					return victim + " still visible to " + observer
				}
			}
			return ""
		}, 30*time.Second)

		testutils.WithTimeout(t, func() string {
			ids, err := c.listParticipants(room, signalIdx[observer])
			if err != nil {
				return err.Error()
			}
			if slices.Contains(ids, victim) {
				return fmt.Sprintf("%s still listed by RoomService: %v", victim, ids)
			}
			return ""
		}, 30*time.Second)
	})

	t.Run("participant rejoins on a live node", func(t *testing.T) {
		// free a slot on a live node (max_participants_per_node: 1)
		clients[other].Stop()

		var rejoined *testclient.RTCClient
		testutils.WithTimeout(t, func() string {
			cl, ok := c.tryJoin(t, room, victim, signalIdx[observer])
			if !ok {
				return victim + " could not rejoin yet"
			}
			rejoined = cl
			return ""
		}, 40*time.Second)

		waitForParticipants(t, rejoined, observer)
		waitForParticipants(t, clients[observer], victim)
		testutils.WithTimeout(t, func() string {
			if len(rejoined.SubscribedTracks()[clients[observer].ID()]) != 1 {
				return victim + " is not subscribed to the audio of " + observer
			}
			return ""
		}, 20*time.Second)

		nodeID, err := rc.HGet(ctx, "dist:participant_node:"+room, victim).Result()
		require.NoError(t, err)
		addr, err := rc.HGet(ctx, "dist:node_relay", nodeID).Result()
		require.NoError(t, err)
		require.NotEqual(t, relayAddrs[failed], addr)
	})
}

func (c *cluster) listParticipants(room string, idx int) ([]string, error) {
	at := auth.NewAccessToken(c.apiKey, c.apiSecret)
	at.SetVideoGrant(&auth.VideoGrant{RoomAdmin: true, Room: room})
	token, err := at.ToJWT()
	if err != nil {
		return nil, err
	}
	header := make(http.Header)
	testclient.SetAuthorizationToken(header, token)
	ctx, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
	if err != nil {
		return nil, err
	}
	httpURL := strings.Replace(c.urls[idx], "ws", "http", 1)
	res, err := livekit.NewRoomServiceProtobufClient(httpURL, &http.Client{Timeout: 5 * time.Second}).
		ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: room})
	if err != nil {
		return nil, err
	}
	return identities(res.Participants), nil
}

// tryJoin is like join, but returns false when the node at urls[idx] is not reachable.
func (c *cluster) tryJoin(t *testing.T, room string, identity string, idx int) (*testclient.RTCClient, bool) {
	at := auth.NewAccessToken(c.apiKey, c.apiSecret).SetIdentity(identity).SetName(identity)
	at.SetVideoGrant(&auth.VideoGrant{RoomJoin: true, Room: room})
	token, err := at.ToJWT()
	require.NoError(t, err)

	opts := clientOptions()
	ws, err := testclient.NewWebSocketConn(c.urls[idx], token, opts)
	if err != nil {
		return nil, false
	}
	client, err := testclient.NewRTCClient(ws, opts.UseJoinRequestQueryParam, opts)
	if err != nil {
		return nil, false
	}
	go func() { _ = client.Run() }()
	if err := client.WaitUntilConnected(10 * time.Second); err != nil {
		client.Stop()
		return nil, false
	}
	t.Cleanup(client.Stop)
	return client, true
}
