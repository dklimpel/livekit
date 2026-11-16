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
	"github.com/pion/rtcp"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/observability/roomobs"

	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
)

// remoteTrack is a track published on another node. It is a regular
// rtc.MediaTrack, whose receivers are fed by the relay connection to the
// publisher's node. Media is only relayed while local subscribers exist.
type remoteTrack struct {
	*rtc.MediaTrack

	publisher *RemoteParticipant
	conn      *relayConn

	relayRequested atomic.Bool
}

var _ types.MediaTrack = (*remoteTrack)(nil)

func newRemoteTrack(rs *roomState, conn *relayConn, publisher *RemoteParticipant, ti *livekit.TrackInfo) *remoteTrack {
	t := &remoteTrack{
		publisher: publisher,
		conn:      conn,
	}
	info := publisher.getInfo()
	t.MediaTrack = rtc.NewMediaTrack(rtc.MediaTrackParams{
		ParticipantID:          publisher.ID,
		ParticipantIdentity:    publisher.Identity(),
		ParticipantVersion:     info.Version,
		ParticipantKind:        info.Kind,
		ParticipantKindDetails: info.KindDetails,
		BufferFactory:          conn.bufferFactory,
		ReceiverConfig:         rs.m.rtcConf.Receiver,
		SubscriberConfig:       rs.m.rtcConf.Subscriber,
		AudioConfig:            rs.m.fullConf.Audio,
		VideoConfig:            rs.m.fullConf.Video,
		PLIThrottleConfig:      rs.m.fullConf.RTC.PLIThrottle,
		TelemetryListener: func() types.ParticipantTelemetryListener {
			return &types.NullParticipantTelemetryListener{}
		},
		Logger:   rtc.LoggerWithTrack(publisher.GetLogger(), livekit.TrackID(ti.Sid), true),
		Reporter: roomobs.NewNoopTrackReporter(),
		OnRTCP: func(pkts []rtcp.Packet) {
			conn.writeRTCP(pkts)
		},
		ShouldRegressCodec:              func() bool { return false },
		EnableRTPStreamRestartDetection: rs.m.fullConf.RTC.EnableRTPStreamRestartDetection,
		OnSubscribedMaxQualityChange: func(
			trackID livekit.TrackID,
			_ *livekit.TrackInfo,
			subscribedQualities []*livekit.SubscribedCodec,
			_ []types.SubscribedCodecQuality,
		) error {
			conn.sendSubscribedQualities(trackID, subscribedQualities)
			return nil
		},
		OnSubscribedAudioCodecChange: func(livekit.TrackID, []*livekit.SubscribedAudioCodec) error {
			return nil
		},
	}, ti)
	return t
}

// AddSubscriber starts relaying the track from the publisher's node on first use.
// Until the relayed media arrives, the subscription manager retries on ErrNoReceiver.
func (t *remoteTrack) AddSubscriber(sub types.LocalParticipant) (types.SubscribedTrack, error) {
	if !t.relayRequested.Swap(true) {
		t.conn.requestTrack(t)
	}
	return t.MediaTrack.AddSubscriber(sub)
}

func (t *remoteTrack) Close(isExpectedToResume bool) {
	if t.relayRequested.Swap(false) {
		t.conn.releaseTrack(t)
	}
	t.MediaTrack.Close(isExpectedToResume)
}

// relayTrackInfo returns the track info for the receiving side of the relay.
// Relayed streams use their own SSRCs, the publisher's SSRCs in the layers
// would prevent the receiver from registering them ("not overriding ssrc").
func relayTrackInfo(ti *livekit.TrackInfo) *livekit.TrackInfo {
	ti = proto.CloneOf(ti)
	clear := func(layers []*livekit.VideoLayer) {
		for _, l := range layers {
			l.Ssrc = 0
			l.RepairSsrc = 0
		}
	}
	clear(ti.Layers)
	for _, c := range ti.Codecs {
		clear(c.Layers)
	}
	return ti
}
