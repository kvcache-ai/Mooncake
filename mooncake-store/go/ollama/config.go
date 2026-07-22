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

package ollama

import (
	"errors"

	store "github.com/kvcache-ai/Mooncake/mooncake-store/go/mooncakestore"
)

// Role describes what a node is allowed to do against the shared KV cache in a
// prefill/decode-disaggregated deployment.
type Role int

const (
	// RoleMixed both publishes freshly computed KV and reuses cached prefixes.
	// Suitable for a single-pool or aggregated deployment.
	RoleMixed Role = iota
	// RolePrefill is a prefill worker: it reuses cached prefixes and publishes
	// the KV it computes for decode workers (and peers) to consume.
	RolePrefill
	// RoleDecode is a decode worker: it consumes (reads) cached KV but does not
	// publish. Use RoleMixed if decode-side write-back is desired.
	RoleDecode
)

func (r Role) String() string {
	switch r {
	case RoleMixed:
		return "mixed"
	case RolePrefill:
		return "prefill"
	case RoleDecode:
		return "decode"
	default:
		return "unknown"
	}
}

// ReadsEnabled reports whether the role may load cached prefixes. All roles may
// reuse cached prefixes; prefill workers benefit from it just as decode workers
// do.
func (r Role) ReadsEnabled() bool { return true }

// WritesEnabled reports whether the role may publish computed KV pages.
func (r Role) WritesEnabled() bool { return r == RoleMixed || r == RolePrefill }

// PageLayout describes the byte size of one KV cache page. It mirrors the
// runtime's paged-KV geometry so the connector knows how many bytes to move per
// page. The factor of two accounts for the separate K and V tensors.
type PageLayout struct {
	NumLayers  int // transformer layers contributing KV
	NumKVHeads int // key/value heads per layer (after GQA/MQA grouping)
	HeadDim    int // dimension per head
	PageTokens int // tokens per page; must equal prefix.Config.PageSize
	ElemSize   int // bytes per stored element (for example 2 for fp16/bf16)
}

// PageBytes returns the number of bytes occupied by one page across all layers.
func (l PageLayout) PageBytes() int {
	return 2 * l.NumLayers * l.NumKVHeads * l.HeadDim * l.PageTokens * l.ElemSize
}

// Validate checks that every field is positive.
func (l PageLayout) Validate() error {
	if l.NumLayers <= 0 || l.NumKVHeads <= 0 || l.HeadDim <= 0 ||
		l.PageTokens <= 0 || l.ElemSize <= 0 {
		return errors.New("ollama: page layout fields must all be positive")
	}
	return nil
}

// Config configures a Connector: how to reach the Mooncake cluster, what role
// this node plays, and the geometry of the KV cache being cached.
type Config struct {
	// Cluster connection (passed through to mooncakestore.Store.Setup).
	LocalHostname     string
	MetadataServer    string
	Protocol          string // "tcp" or "rdma"
	DeviceName        string // RDMA device; empty for TCP or auto-discovery
	MasterServerAddr  string
	GlobalSegmentSize uint64
	LocalBufferSize   uint64

	// Connector behaviour.
	Role      Role
	Model     string
	Layout    PageLayout
	KeyPrefix string // optional; defaults to prefix.DefaultKeyPrefix

	// Replicate controls replica placement for published pages. A zero
	// ReplicaNum is replaced with the store default (single replica).
	Replicate store.ReplicateConfig
}
