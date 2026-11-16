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
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/guid"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	sfuutils "github.com/livekit/livekit-server/pkg/sfu/utils"
)

const relayNegotiationTimeout = 10 * time.Second

// relaySender relays local tracks to one peer node. Every relayed stream
// (codec x simulcast layer) is a separate sendonly transceiver, so the
// receiving node gets all layers and does layer selection for its own
// subscribers.
type relaySender struct {
	conn    *relayConn
	pc      *webrtc.PeerConnection
	session string
	logger  logger.Logger

	lock              sync.Mutex
	tracks            map[livekit.TrackID]*relayedTrack
	negotiating       bool
	negotiationNeeded bool
	negotiationTimer  *time.Timer
	pendingCandidates []webrtc.ICECandidateInit

	failed atomic.Bool
	closed atomic.Bool
}

type relayedTrack struct {
	track      types.MediaTrack
	downTracks []*relayDownTrack
}

func newRelaySender(c *relayConn) (*relaySender, error) {
	pc, err := c.rs.m.relay.newPeerConnection(c.rs.room.ToProto().EnabledCodecs, nil, false, c.pionLogger())
	if err != nil {
		return nil, err
	}
	s := &relaySender{
		conn:    c,
		pc:      pc,
		session: guid.New("RS_"),
		logger:  c.logger.WithValues("relaySession", ""),
		tracks:  make(map[livekit.TrackID]*relayedTrack),
	}
	s.logger = c.logger.WithValues("relaySession", s.session)

	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand == nil {
			return
		}
		data, _ := json.Marshal(cand.ToJSON())
		c.sendToPeer(&message{Type: msgRelayCandidate, Candidate: string(data), SessionID: s.session})
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.logger.Debugw("sending relay connection state changed", "state", state)
		switch state {
		case webrtc.PeerConnectionStateConnected:
			c.onSenderConnected()
		case webrtc.PeerConnectionStateFailed:
			s.failed.Store(true)
			c.logConnectionFailed("sending")
		}
	})
	return s, nil
}

func (s *relaySender) isFailed() bool {
	return s.failed.Load() || s.closed.Load()
}

func (s *relaySender) close() {
	if s.closed.Swap(true) {
		return
	}
	s.lock.Lock()
	tracks := s.tracks
	s.tracks = make(map[livekit.TrackID]*relayedTrack)
	if s.negotiationTimer != nil {
		s.negotiationTimer.Stop()
	}
	s.lock.Unlock()

	for _, rt := range tracks {
		s.detach(rt)
	}
	_ = s.pc.Close()
}

func (s *relaySender) findLocalTrack(trackID livekit.TrackID) types.MediaTrack {
	for _, p := range s.conn.rs.room.GetParticipants() {
		if t := p.GetPublishedTrack(trackID); t != nil {
			return t
		}
	}
	return nil
}

// addTrack starts relaying a local track to the peer.
func (s *relaySender) addTrack(trackID livekit.TrackID) {
	s.lock.Lock()
	if s.tracks[trackID] != nil {
		s.lock.Unlock()
		return
	}
	s.lock.Unlock()

	track := s.findLocalTrack(trackID)
	if track == nil {
		s.logger.Debugw("relay requested for unknown track", "trackID", trackID)
		return
	}
	receivers := track.Receivers()
	if len(receivers) == 0 {
		// not receiving media yet, the peer repeats its request
		s.logger.Debugw("relay requested for track without receivers", "trackID", trackID)
		return
	}

	rt := &relayedTrack{track: track}
	for _, r := range receivers {
		dt, err := s.addReceiver(trackID, track, r)
		if err != nil {
			s.logger.Warnw("could not relay receiver", err, "trackID", trackID, "mime", r.Mime())
			continue
		}
		rt.downTracks = append(rt.downTracks, dt)
	}
	if len(rt.downTracks) == 0 {
		return
	}

	s.lock.Lock()
	if s.closed.Load() || s.tracks[trackID] != nil {
		s.lock.Unlock()
		s.detach(rt)
		return
	}
	s.tracks[trackID] = rt
	s.lock.Unlock()

	track.AddOnClose(func(bool) {
		s.removeTrack(trackID)
	})

	// make sure the publisher keeps sending, even without local subscribers,
	// until the peer reports its own subscribed qualities
	if lt, ok := track.(types.LocalMediaTrack); ok {
		var qualities []types.SubscribedCodecQuality
		for _, r := range receivers {
			qualities = append(qualities, types.SubscribedCodecQuality{CodecMime: r.Mime(), Quality: livekit.VideoQuality_HIGH})
		}
		lt.NotifySubscriberNodeMaxQuality(s.conn.peer, qualities)
	}

	s.logger.Infow("relaying track to remote node", "trackID", trackID, "numCodecs", len(rt.downTracks))
	s.negotiate()
}

func (s *relaySender) addReceiver(trackID livekit.TrackID, track types.MediaTrack, r sfu.TrackReceiver) (*relayDownTrack, error) {
	mimeType := r.Mime()
	numStreams := 1
	if mime.IsMimeTypeVideo(mimeType) && sfuutils.IsSimulcastMode(r.VideoLayerMode()) {
		if n := len(buffer.GetVideoLayersForMimeType(mimeType, track.ToProto())); n > 1 {
			numStreams = n
		}
	}

	dt := &relayDownTrack{
		sender:       s,
		receiver:     r,
		trackID:      trackID,
		subscriberID: livekit.ParticipantID("RELAY_" + string(s.conn.peer) + "_" + s.session),
		simulcast:    numStreams > 1,
	}
	for i := 0; i < numStreams; i++ {
		local, err := webrtc.NewTrackLocalStaticRTP(r.Codec().RTPCodecCapability, relayStreamID(trackID, mimeType, i), string(trackID))
		if err != nil {
			return nil, err
		}
		tr, err := s.pc.AddTransceiverFromTrack(local, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
		if err != nil {
			return nil, err
		}
		st := &relayStream{
			downTrack: dt,
			layer:     int32(i),
			local:     local,
			sender:    tr.Sender(),
		}
		dt.streams = append(dt.streams, st)
		go st.readRTCP()
	}

	if err := r.AddDownTrack(dt); err != nil {
		for _, st := range dt.streams {
			_ = s.pc.RemoveTrack(st.sender)
		}
		return nil, err
	}
	return dt, nil
}

func (s *relaySender) removeTrack(trackID livekit.TrackID) {
	s.lock.Lock()
	rt := s.tracks[trackID]
	delete(s.tracks, trackID)
	s.lock.Unlock()
	if rt == nil {
		return
	}

	s.logger.Infow("stopped relaying track to remote node", "trackID", trackID)
	s.detach(rt)
	if lt, ok := rt.track.(types.LocalMediaTrack); ok {
		lt.NotifySubscriberNodeMaxQuality(s.conn.peer, nil)
	}
	if !s.closed.Load() {
		s.negotiate()
	}
}

func (s *relaySender) detach(rt *relayedTrack) {
	for _, dt := range rt.downTracks {
		dt.Close()
		dt.receiver.DeleteDownTrack(dt.subscriberID)
		if !s.closed.Load() {
			for _, st := range dt.streams {
				_ = s.pc.RemoveTrack(st.sender)
			}
		}
	}
}

// handleQualities forwards the qualities subscribed on the peer to the
// publisher's dynacast, so that unused simulcast layers are paused.
func (s *relaySender) handleQualities(trackID livekit.TrackID, data []byte) {
	s.lock.Lock()
	rt := s.tracks[trackID]
	s.lock.Unlock()
	if rt == nil {
		return
	}
	lt, ok := rt.track.(types.LocalMediaTrack)
	if !ok {
		return
	}
	update := &livekit.SubscribedQualityUpdate{}
	if err := proto.Unmarshal(data, update); err != nil {
		return
	}
	qualities := make([]types.SubscribedCodecQuality, 0, len(update.SubscribedCodecs))
	for _, sc := range update.SubscribedCodecs {
		maxQuality := livekit.VideoQuality_OFF
		for _, q := range sc.Qualities {
			if q.Enabled && (maxQuality == livekit.VideoQuality_OFF || q.Quality > maxQuality) {
				maxQuality = q.Quality
			}
		}
		qualities = append(qualities, types.SubscribedCodecQuality{CodecMime: mime.NormalizeMimeType(sc.Codec), Quality: maxQuality})
	}
	lt.NotifySubscriberNodeMaxQuality(s.conn.peer, qualities)
}

// --- negotiation, the sending side is always the offerer ---

func (s *relaySender) negotiate() {
	s.lock.Lock()
	if s.negotiating {
		s.negotiationNeeded = true
		s.lock.Unlock()
		return
	}
	s.negotiating = true
	s.negotiationNeeded = false
	s.lock.Unlock()

	offer, err := s.pc.CreateOffer(nil)
	if err == nil {
		err = s.pc.SetLocalDescription(offer)
	}
	if err != nil {
		s.logger.Errorw("could not create relay offer", err)
		s.lock.Lock()
		s.negotiating = false
		s.lock.Unlock()
		return
	}

	s.lock.Lock()
	if s.negotiationTimer != nil {
		s.negotiationTimer.Stop()
	}
	// an answer might get lost, offer again after a timeout
	s.negotiationTimer = time.AfterFunc(relayNegotiationTimeout, func() {
		s.lock.Lock()
		stuck := s.negotiating && !s.closed.Load()
		s.lock.Unlock()
		if stuck {
			s.logger.Infow("relay negotiation timed out, retrying")
			_ = s.pc.SetLocalDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeRollback})
			s.lock.Lock()
			s.negotiating = false
			s.lock.Unlock()
			s.negotiate()
		}
	})
	s.lock.Unlock()

	s.conn.sendToPeer(&message{Type: msgRelayOffer, SDP: offer.SDP, SessionID: s.session})
}

func (s *relaySender) handleAnswer(session string, sdp string) {
	if session != s.session {
		return
	}
	if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}); err != nil {
		s.logger.Errorw("could not set relay answer", err)
		return
	}

	s.lock.Lock()
	if s.negotiationTimer != nil {
		s.negotiationTimer.Stop()
	}
	s.negotiating = false
	again := s.negotiationNeeded
	pending := s.pendingCandidates
	s.pendingCandidates = nil
	var downTracks []*relayDownTrack
	for _, rt := range s.tracks {
		downTracks = append(downTracks, rt.downTracks...)
	}
	s.lock.Unlock()

	for _, cand := range pending {
		_ = s.pc.AddICECandidate(cand)
	}
	for _, dt := range downTracks {
		dt.updateExtensionMaps()
	}
	if again {
		s.negotiate()
	}
}

func (s *relaySender) addCandidate(session string, candidate string) {
	if session != s.session {
		return
	}
	var cand webrtc.ICECandidateInit
	if err := json.Unmarshal([]byte(candidate), &cand); err != nil {
		return
	}
	s.lock.Lock()
	if s.pc.RemoteDescription() == nil {
		s.pendingCandidates = append(s.pendingCandidates, cand)
		s.lock.Unlock()
		return
	}
	s.lock.Unlock()
	_ = s.pc.AddICECandidate(cand)
}

// --- relayDownTrack, the sfu.TrackSender attached to a local receiver ---

type relayDownTrack struct {
	sender       *relaySender
	receiver     sfu.TrackReceiver
	trackID      livekit.TrackID
	subscriberID livekit.ParticipantID
	simulcast    bool
	streams      []*relayStream
	closed       atomic.Bool
}

var _ sfu.TrackSender = (*relayDownTrack)(nil)

type relayStream struct {
	downTrack *relayDownTrack
	layer     int32
	local     *webrtc.TrackLocalStaticRTP
	sender    *webrtc.RTPSender
	// source header extension id => negotiated relay header extension id
	extMap atomic.Pointer[map[uint8]uint8]
	ssrc   atomic.Uint32
}

func (d *relayDownTrack) updateExtensionMaps() {
	source := d.receiver.HeaderExtensions()
	for _, st := range d.streams {
		params := st.sender.GetParameters()
		m := make(map[uint8]uint8)
		for _, src := range source {
			for _, neg := range params.HeaderExtensions {
				if src.URI == neg.URI {
					m[uint8(src.ID)] = uint8(neg.ID)
				}
			}
		}
		st.extMap.Store(&m)
		if len(params.Encodings) != 0 {
			st.ssrc.Store(uint32(params.Encodings[0].SSRC))
		}
	}
}

func (d *relayDownTrack) streamForLayer(layer int32) *relayStream {
	idx := int32(0)
	if d.simulcast {
		idx = layer
	}
	if idx < 0 || int(idx) >= len(d.streams) {
		return nil
	}
	return d.streams[idx]
}

func (d *relayDownTrack) WriteRTP(p *buffer.ExtPacket, layer int32) int32 {
	if d.closed.Load() || p == nil || p.Packet == nil || len(p.Packet.Payload) == 0 {
		return 0
	}
	st := d.streamForLayer(layer)
	if st == nil {
		return 0
	}
	extMap := st.extMap.Load()
	if extMap == nil {
		// not negotiated yet
		return 0
	}

	// header extension ids differ between the publisher's and the relay connection
	src := &p.Packet.Header
	hdr := *src
	hdr.Padding = false
	hdr.PaddingSize = 0
	hdr.Extension = false
	hdr.ExtensionProfile = 0
	hdr.Extensions = nil
	for _, id := range src.GetExtensionIDs() {
		if relayID, ok := (*extMap)[id]; ok {
			_ = hdr.SetExtension(relayID, src.GetExtension(id))
		}
	}

	if err := st.local.WriteRTP(&rtp.Packet{Header: hdr, Payload: p.Packet.Payload}); err != nil {
		return 0
	}
	return int32(len(p.Packet.Payload))
}

// HandleRTCPSenderReportData forwards the publisher's sender reports, which
// keeps audio/video synchronization intact across the relay.
func (d *relayDownTrack) HandleRTCPSenderReportData(_ webrtc.PayloadType, layer int32, sr *livekit.RTCPSenderReportState) error {
	st := d.streamForLayer(layer)
	if st == nil || sr == nil {
		return nil
	}
	ssrc := st.ssrc.Load()
	if ssrc == 0 {
		return nil
	}
	return d.sender.pc.WriteRTCP([]rtcp.Packet{&rtcp.SenderReport{
		SSRC:        ssrc,
		NTPTime:     sr.NtpTimestamp,
		RTPTime:     sr.RtpTimestamp,
		PacketCount: sr.Packets,
		OctetCount:  uint32(sr.Octets),
	}})
}

func (d *relayDownTrack) UpTrackLayersChange()                       {}
func (d *relayDownTrack) UpTrackBitrateAvailabilityChange()          {}
func (d *relayDownTrack) UpTrackMaxPublishedLayerChange(int32)       {}
func (d *relayDownTrack) UpTrackMaxTemporalLayerSeenChange(int32)    {}
func (d *relayDownTrack) UpTrackBitrateReport([]int32, sfu.Bitrates) {}
func (d *relayDownTrack) Close()                                     { d.closed.Store(true) }
func (d *relayDownTrack) IsClosed() bool                             { return d.closed.Load() }
func (d *relayDownTrack) ID() string                                 { return string(d.trackID) }
func (d *relayDownTrack) SubscriberID() livekit.ParticipantID        { return d.subscriberID }
func (d *relayDownTrack) Resync()                                    {}
func (d *relayDownTrack) SetReceiver(sfu.TrackReceiver)              {}
func (d *relayDownTrack) ReceiverRestart(sfu.TrackReceiver)          {}

// readRTCP forwards key frame requests of the peer to the publisher.
func (st *relayStream) readRTCP() {
	for {
		pkts, _, err := st.sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, pkt := range pkts {
			switch pkt.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				st.downTrack.receiver.SendPLI(st.layer, false)
			}
		}
	}
}
