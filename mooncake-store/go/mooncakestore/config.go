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

// ReplicateConfig controls replica placement for put operations.
type ReplicateConfig struct {
	ReplicaNum        int
	WithSoftPin       bool
	WithHardPin       bool
	PreferredSegments []string
}

// DefaultReplicateConfig returns the default configuration (1 replica, no pinning).
func DefaultReplicateConfig() ReplicateConfig {
	return ReplicateConfig{
		ReplicaNum:  1,
		WithSoftPin: false,
		WithHardPin: false,
	}
}
