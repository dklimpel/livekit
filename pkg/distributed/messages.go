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
	"encoding/json"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/bus"
)

// Node-to-node messages are JSON envelopes inside a wrapperspb.BytesValue, so
// that no changes to github.com/livekit/protocol are needed. LiveKit protos
// inside the envelope are carried in their binary encoding.

type messageType string

const (
	// room channel
	msgParticipantUpdate messageType = "participant_update"
	msgSyncRequest       messageType = "sync_request"
	msgDataPacket        messageType = "data_packet"
	msgSpeakers          messageType = "speakers"
	msgRoomMetadata      messageType = "room_metadata"
	msgRoomClose         messageType = "room_close"
	msgNodeLeft          messageType = "node_left"

	// node channel (relay control, see relay*.go)
	msgRelaySubscribe   messageType = "relay_subscribe"
	msgRelayUnsubscribe messageType = "relay_unsubscribe"
	msgRelayOffer       messageType = "relay_offer"
	msgRelayAnswer      messageType = "relay_answer"
	msgRelayCandidate   messageType = "relay_candidate"
	msgRelayQuality     messageType = "relay_quality"
)

type message struct {
	Type messageType      `json:"t"`
	Node livekit.NodeID   `json:"n"`
	Room livekit.RoomName `json:"r"`

	// msgParticipantUpdate
	Participant            []byte `json:"p,omitempty"`  // livekit.ParticipantInfo
	SubscriptionPermission []byte `json:"sp,omitempty"` // livekit.SubscriptionPermission

	// msgDataPacket
	DataKind   livekit.DataPacket_Kind `json:"dk,omitempty"`
	DataPacket []byte                  `json:"dp,omitempty"` // livekit.DataPacket

	// msgSpeakers
	Speakers []speaker `json:"s,omitempty"`

	// msgRoomMetadata
	Metadata string `json:"m,omitempty"`

	// relay messages
	TrackID   livekit.TrackID `json:"tr,omitempty"`
	SDP       string          `json:"sdp,omitempty"`
	Candidate string          `json:"c,omitempty"`
	Qualities []byte          `json:"q,omitempty"` // livekit.SubscribedQualityUpdate
	SessionID string          `json:"sid,omitempty"`
	ToSender  bool            `json:"ts,omitempty"` // relay candidate for the sending side
}

type speaker struct {
	ParticipantID livekit.ParticipantID `json:"p"`
	Level         float32               `json:"l"`
	Active        bool                  `json:"a"`
}

func roomChannel(roomName livekit.RoomName) bus.Channel {
	name := "dist|room|" + string(roomName)
	return bus.Channel{Legacy: name, Server: name}
}

func nodeChannel(nodeID livekit.NodeID) bus.Channel {
	name := "dist|node|" + string(nodeID)
	return bus.Channel{Legacy: name, Server: name}
}

func publish(ctx context.Context, b psrpc.MessageBus, ch bus.Channel, msg *message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return b.Publish(ctx, ch, wrapperspb.Bytes(data))
}

func subscribe(ctx context.Context, b psrpc.MessageBus, ch bus.Channel) (bus.Subscription[*wrapperspb.BytesValue], error) {
	return bus.Subscribe[*wrapperspb.BytesValue](ctx, b, ch, 1000)
}

func decode(v *wrapperspb.BytesValue) (*message, error) {
	msg := &message{}
	if err := json.Unmarshal(v.GetValue(), msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func marshalProto(m proto.Message) []byte {
	if m == nil {
		return nil
	}
	b, _ := proto.Marshal(m)
	return b
}
