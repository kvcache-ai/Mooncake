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

// Package ollama provides an Ollama-side connector to the Mooncake distributed
// KV cache, enabling Ollama to participate in a prefill/decode-disaggregated
// (PD) or shared-KV deployment.
//
// # Motivation
//
// By default each Ollama instance keeps KV cache only in its own process. This
// connector lets an instance offload and reuse KV cache through Mooncake Store,
// following the same hash-based prefix-caching pattern that the vLLM
// (MooncakeStoreConnector) and SGLang HiCache integrations use: a prefill worker
// publishes the KV it computes, and a decode worker (or any peer) reuses the
// longest cached prefix instead of recomputing it.
//
// # Workflow
//
// For each request the runtime supplies the token ids and one buffer pointer per
// KV page (slices of a registered staging region):
//
//	// decode / reuse path
//	res, _ := c.LoadCachedPrefix(tokens, pageDst)
//	// runtime computes only tokens[res.MatchedTokens:]
//
//	// prefill / publish path
//	c.StoreComputedPrefix(tokens, pageSrc)
//
// Page identity is derived from the tokens by the prefix subpackage, so workers
// agree on keys without coordination and different models never alias.
//
// # Scope
//
// This package is the store-transfer and keying half of the integration. Wiring
// the page buffers to llama.cpp's KV tensors (extraction and injection) lives in
// the Ollama runtime and is tracked separately. Zero-copy RDMA paths require the
// full Mooncake build and RDMA-capable hardware; the TCP transport works for
// functional testing.
package ollama
