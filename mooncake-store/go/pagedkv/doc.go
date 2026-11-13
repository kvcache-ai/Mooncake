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

// Package pagedkv connects an inference runtime that holds its KV cache in
// fixed-size pages to the Mooncake distributed store, so that instances can
// share computed KV instead of each recomputing it.
//
// # Motivation
//
// By default an inference process keeps KV cache to itself. This package lets it
// publish the KV it computes and reuse what a peer already published, following
// the hash-based prefix-caching pattern the vLLM MooncakeStoreConnector uses to
// share KV blocks across instances. That covers a prefill/decode-disaggregated
// deployment, where prefill publishes and decode consumes, and equally a pool of
// peer instances that merely share a prompt prefix.
//
// # Workflow
//
// For each request the runtime supplies the token ids and one buffer pointer per
// KV page (slices of a registered staging region):
//
//	// reuse path
//	res, _ := c.LoadCachedPrefix(tokens, pageDst)
//	// runtime computes only tokens[res.MatchedTokens:]
//
//	// publish path
//	c.StoreComputedPrefix(tokens, pageSrc)
//
// Page identity is derived from the tokens by the prefix subpackage, so workers
// agree on keys without coordination and different models never alias.
//
// # Scope
//
// This package is the store-transfer and keying half only, and is deliberately
// tied to no particular runtime. Moving bytes between these page buffers and the
// runtime's own KV tensors is the runtime's job and is not provided here. A
// runtime can drive this package when it keeps KV in fixed-size token pages,
// can expose each page as a pointer into memory it has registered with
// RegisterKVBuffer, and derives page identity from stable token ids.
//
// Zero-copy RDMA paths require the full Mooncake build and RDMA-capable
// hardware; the TCP transport works for functional testing.
package pagedkv
