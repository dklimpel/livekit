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

package config

import "time"

// DistributedConfig enables hosting a single room on multiple nodes.
// See docs/distributed-rooms for details. Requires Redis.
type DistributedConfig struct {
	// Enabled turns on distributed rooms. When false (default) the server
	// behaves exactly like upstream LiveKit: one room is hosted by one node.
	Enabled bool `yaml:"enabled,omitempty"`

	// MaxParticipantsPerNode is the number of participants of a single room a
	// node accepts before further participants of that room overflow to another
	// node ("fill, then overflow"). 0 disables the participant based limit, in
	// that case only node limits (limit.*, cpu_load_limit) trigger an overflow.
	MaxParticipantsPerNode int `yaml:"max_participants_per_node,omitempty"`

	// CPULoadLimit (0..1) marks a node as full for new participants when its
	// reported CPU load exceeds the value. 0 disables the check.
	CPULoadLimit float32 `yaml:"cpu_load_limit,omitempty"`

	// NodeCheckInterval is how often a node checks the liveness of the other
	// nodes of its distributed rooms. Defaults to 2s.
	NodeCheckInterval time.Duration `yaml:"node_check_interval,omitempty"`

	// Relay configures the node-to-node media relay.
	Relay DistributedRelayConfig `yaml:"relay,omitempty"`
}

type DistributedRelayConfig struct {
	// PortRangeStart/PortRangeEnd is the UDP port range of node-to-node relay
	// connections. Every relay peer connection uses its own port: one per
	// direction, room and remote node. Defaults to 7890-8889.
	PortRangeStart uint16 `yaml:"port_range_start,omitempty"`
	PortRangeEnd   uint16 `yaml:"port_range_end,omitempty"`

	// Deprecated: a single shared port mixes up connections between the same
	// two nodes. If set without a port range, it is used as port_range_start.
	UDPPort uint32 `yaml:"udp_port,omitempty"`

	// NodeIP is the IP other nodes use to reach this node's relay port.
	// Defaults to the node IP (rtc.node_ip / auto detected).
	NodeIP string `yaml:"node_ip,omitempty"`
}
