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
	"fmt"
	"net"
	"strconv"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/webrtc/v4"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/logger/pionlogger"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
)

const (
	defaultRelayPortRangeStart = 7890
	defaultRelayPortRangeSize  = 1000
)

// RelayNodeIPEnv sets the relay IP of a node when distributed.relay.node_ip is
// not configured. In Kubernetes it is typically filled with the pod IP via the
// downward API, so that all pods can share one configuration.
const RelayNodeIPEnv = "LIVEKIT_RELAY_NODE_IP"

// relayNodeIP selects the IP other nodes use to reach this node's relay port:
// configuration, then environment, then the node IP.
func relayNodeIP(configured, env, nodeIP string) (ip string, source string, err error) {
	switch {
	case configured != "":
		ip, source = configured, "config"
	case env != "":
		ip, source = env, RelayNodeIPEnv
	default:
		ip, source = nodeIP, "node_ip"
	}
	if parsed := net.ParseIP(ip); parsed == nil || parsed.To4() == nil {
		return "", source, fmt.Errorf("invalid relay node ip %q (from %s), an IPv4 address is required", ip, source)
	}
	return ip, source, nil
}

// relayTransport holds the node wide settings of relay peer connections.
//
// Every relay peer connection binds its own UDP port from the port range. A
// shared, multiplexed port does not work: the connections of both directions
// (and of several rooms) between the same two nodes would use the same
// address pair and could not be told apart.
type relayTransport struct {
	portMin uint16
	portMax uint16
	ip      string
	// local interface IP equal to ip, restricts candidates to that interface
	localIP net.IP
	rtcConf *rtc.WebRTCConfig
	logger  logger.Logger
}

func relayPortRange(conf config.DistributedRelayConfig) (uint16, uint16, error) {
	start, end := conf.PortRangeStart, conf.PortRangeEnd
	if start == 0 && conf.UDPPort != 0 {
		start = uint16(conf.UDPPort)
	}
	if start == 0 {
		start = defaultRelayPortRangeStart
	}
	if end == 0 {
		end = start + defaultRelayPortRangeSize - 1
	}
	if end < start {
		return 0, 0, fmt.Errorf("invalid relay port range %d-%d", start, end)
	}
	return start, end, nil
}

func newRelayTransport(conf config.DistributedRelayConfig, ip string, rtcConf *rtc.WebRTCConfig, l logger.Logger) (*relayTransport, error) {
	portMin, portMax, err := relayPortRange(conf)
	if err != nil {
		return nil, err
	}
	if conf.UDPPort != 0 {
		l.Warnw("distributed.relay.udp_port is deprecated, use port_range_start/port_range_end", nil, "udpPort", conf.UDPPort)
	}
	t := &relayTransport{
		portMin: portMin,
		portMax: portMax,
		ip:      ip,
		rtcConf: rtcConf,
		logger:  l,
	}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		relayIP := net.ParseIP(ip)
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.Equal(relayIP) {
				t.localIP = relayIP
			}
		}
	}
	l.Infow("relay transport", "portRange", fmt.Sprintf("%d-%d", portMin, portMax), "ip", ip, "localInterface", t.localIP != nil)
	return t, nil
}

// address identifies the relay endpoint of the node (ip and first port).
func (t *relayTransport) address() string {
	return net.JoinHostPort(t.ip, strconv.Itoa(int(t.portMin)))
}

func (t *relayTransport) close() {}

// newPeerConnection creates a relay peer connection. A receiving connection
// writes into LiveKit buffers of bufferFactory (like publisher connections do),
// a sending connection answers NACKs of the receiving node.
func (t *relayTransport) newPeerConnection(codecs []*livekit.Codec, bufferFactory *buffer.Factory, receiving bool, l logger.Logger) (*webrtc.PeerConnection, error) {
	me, err := rtc.NewRelayMediaEngine(codecs, t.rtcConf)
	if err != nil {
		return nil, err
	}

	se := webrtc.SettingEngine{}
	if err := se.SetEphemeralUDPPortRange(t.portMin, t.portMax); err != nil {
		return nil, err
	}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	if t.localIP != nil {
		// the relay IP is a local address (e.g. the pod IP), gather only on it
		localIP := t.localIP
		se.SetIPFilter(func(ip net.IP) bool { return ip.Equal(localIP) })
		if localIP.IsLoopback() {
			se.SetIncludeLoopbackCandidate(true)
		}
	} else {
		// the relay IP is mapped to this node (NAT), announce it instead of local addresses
		se.SetNAT1To1IPs([]string{t.ip}, webrtc.ICECandidateTypeHost)
	}
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	se.DisableSRTPReplayProtection(true)
	se.DisableSRTCPReplayProtection(true)
	se.SetLite(false)
	se.LoggerFactory = pionlogger.NewLoggerFactory(l)
	if receiving {
		se.BufferFactory = bufferFactory.GetOrNew
	}

	ir := &interceptor.Registry{}
	if !receiving {
		responder, err := nack.NewResponderInterceptor()
		if err != nil {
			return nil, err
		}
		ir.Add(responder)
	}

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(me),
		webrtc.WithSettingEngine(se),
		webrtc.WithInterceptorRegistry(ir),
	)
	return api.NewPeerConnection(webrtc.Configuration{})
}
