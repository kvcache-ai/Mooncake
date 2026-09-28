// Copyright 2024 KVCache.AI
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

package mooncakestore

// SoftPinAction selects the soft-pin intent of a put operation. A soft pin
// guards an object from eviction until a deadline fixed at write time;
// reads never extend it.
type SoftPinAction int

const (
	// SoftPinPreserve keeps an existing unexpired soft-pin deadline. It is
	// the zero value, so plain puts commit ordinary cache.
	SoftPinPreserve SoftPinAction = iota
	// SoftPinEnable commits a new soft-pin lifetime when the write
	// becomes readable.
	SoftPinEnable
	// SoftPinDisable removes an existing soft pin when the write
	// becomes readable.
	SoftPinDisable
)

// ReplicateConfig controls replica placement for put operations.
type ReplicateConfig struct {
	ReplicaNum    int
	SoftPinAction SoftPinAction
	// SoftPinTTLMs overrides the master's default soft-pin TTL in
	// milliseconds. Only valid with SoftPinEnable.
	SoftPinTTLMs      *uint64
	WithHardPin       bool
	PreferredSegments []string
}

// DefaultReplicateConfig returns the default configuration (1 replica, no pinning).
func DefaultReplicateConfig() ReplicateConfig {
	return ReplicateConfig{
		ReplicaNum:  1,
		WithHardPin: false,
	}
}
