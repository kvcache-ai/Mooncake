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
	"github.com/kvcache-ai/Mooncake/mooncake-store/go/ollama/prefix"
)

var (
	// ErrReadNotAllowed is returned by LoadCachedPrefix when the role forbids reads.
	ErrReadNotAllowed = errors.New("ollama: role does not permit loading cached KV")
	// ErrWriteNotAllowed is returned by StoreComputedPrefix when the role forbids writes.
	ErrWriteNotAllowed = errors.New("ollama: role does not permit publishing KV")
	// ErrBufferCount is returned when the supplied per-page buffer slice is too short.
	ErrBufferCount = errors.New("ollama: fewer page buffers than pages")
)

// Connector bridges an Ollama runtime to a Mooncake Store cluster for
// prefill/decode-disaggregated KV cache sharing.
//
// The runtime is expected to keep KV cache in paged form. For each request the
// runtime hands the connector the request's token ids plus one buffer pointer
// per page (a slice of the runtime's registered KV staging region). The
// connector decides page identity from the tokens and moves whole pages to and
// from the store:
//
//   - LoadCachedPrefix reads the longest cached prefix into the page buffers so
//     the runtime can skip recomputing it (decode side, or prefill reuse).
//   - StoreComputedPrefix publishes freshly computed pages (prefill side).
//
// Zero-copy transfers require the page buffers to live inside a region that has
// been passed to RegisterKVBuffer.
type Connector struct {
	store  *store.Store
	cfg    Config
	pcfg   prefix.Config
	repl   store.ReplicateConfig
	nbytes uint64
}

// MatchResult reports how much of a request was served from the store.
type MatchResult struct {
	MatchedPages  int
	MatchedTokens int
}

// New creates a Connector, opens a Store client and connects to the cluster.
// Call Close when done.
func New(cfg Config) (*Connector, error) {
	if err := cfg.Layout.Validate(); err != nil {
		return nil, err
	}
	if cfg.Model == "" {
		return nil, errors.New("ollama: Config.Model must be set")
	}

	repl := cfg.Replicate
	if repl.ReplicaNum <= 0 {
		repl = store.DefaultReplicateConfig()
	}

	s, err := store.New()
	if err != nil {
		return nil, err
	}
	if err := s.Setup(cfg.LocalHostname, cfg.MetadataServer,
		cfg.GlobalSegmentSize, cfg.LocalBufferSize,
		cfg.Protocol, cfg.DeviceName, cfg.MasterServerAddr); err != nil {
		s.Close()
		return nil, err
	}

	return &Connector{
		store: s,
		cfg:   cfg,
		pcfg: prefix.Config{
			PageSize:  cfg.Layout.PageTokens,
			Model:     cfg.Model,
			KeyPrefix: cfg.KeyPrefix,
		},
		repl:   repl,
		nbytes: uint64(cfg.Layout.PageBytes()),
	}, nil
}

// Store exposes the underlying store client for advanced use (buffer
// registration, health checks, direct operations).
func (c *Connector) Store() *store.Store { return c.store }

// Close releases the store client.
func (c *Connector) Close() {
	if c.store != nil {
		c.store.Close()
		c.store = nil
	}
}

// RegisterKVBuffer registers the runtime's KV staging region for zero-copy
// transfer. Page pointers passed to LoadCachedPrefix / StoreComputedPrefix must
// fall inside a registered region. The memory must stay valid and pinned until
// UnregisterKVBuffer is called.
func (c *Connector) RegisterKVBuffer(ptr uintptr, size uint64) error {
	return c.store.RegisterBuffer(ptr, size)
}

// UnregisterKVBuffer unregisters a region previously registered with
// RegisterKVBuffer.
func (c *Connector) UnregisterKVBuffer(ptr uintptr) error {
	return c.store.UnregisterBuffer(ptr)
}

// keysFor returns the page keys for tokens, capped to the number of buffers the
// caller supplied (we can only move as many pages as we have buffers for).
func (c *Connector) keysFor(tokens []int32, nBuffers int) ([]prefix.Page, error) {
	pages, err := c.pcfg.Pages(tokens)
	if err != nil {
		return nil, err
	}
	if len(pages) > nBuffers {
		pages = pages[:nBuffers]
	}
	return pages, nil
}

// LoadCachedPrefix loads the longest cached KV prefix for tokens into the given
// per-page buffers and reports how many leading pages were filled. pageDst[i]
// receives page i; it must point inside a registered region and be at least
// PageBytes long. Pages beyond the returned MatchedPages are left untouched and
// must be computed by the runtime.
func (c *Connector) LoadCachedPrefix(tokens []int32, pageDst []uintptr) (MatchResult, error) {
	if !c.cfg.Role.ReadsEnabled() {
		return MatchResult{}, ErrReadNotAllowed
	}
	pages, err := c.keysFor(tokens, len(pageDst))
	if err != nil {
		return MatchResult{}, err
	}
	if len(pages) == 0 {
		return MatchResult{}, nil
	}

	keys := make([]string, len(pages))
	for i := range pages {
		keys[i] = pages[i].Key
	}

	present, err := c.store.BatchExists(keys)
	if err != nil {
		return MatchResult{}, err
	}
	matched := prefix.LongestPresentRun(present)
	if matched == 0 {
		return MatchResult{}, nil
	}

	sizes := make([]uint64, matched)
	for i := range sizes {
		sizes[i] = c.nbytes
	}
	got, err := c.store.BatchGetInto(keys[:matched], pageDst[:matched], sizes)
	if err != nil {
		return MatchResult{}, err
	}

	// A page may be evicted between the existence probe and the read. KV reuse
	// needs a contiguous prefix, so stop at the first short or failed read.
	ok := 0
	for _, n := range got {
		if n < 0 || uint64(n) != c.nbytes {
			break
		}
		ok++
	}
	return MatchResult{MatchedPages: ok, MatchedTokens: ok * c.cfg.Layout.PageTokens}, nil
}

// StoreComputedPrefix publishes the KV pages for tokens from the given per-page
// buffers and returns how many pages were newly stored. Pages already present
// in the store are skipped, so a peer that computed the same prefix first is not
// overwritten. pageSrc[i] holds page i and must point inside a registered
// region.
func (c *Connector) StoreComputedPrefix(tokens []int32, pageSrc []uintptr) (int, error) {
	if !c.cfg.Role.WritesEnabled() {
		return 0, ErrWriteNotAllowed
	}
	pages, err := c.keysFor(tokens, len(pageSrc))
	if err != nil {
		return 0, err
	}
	if len(pages) == 0 {
		return 0, nil
	}

	keys := make([]string, len(pages))
	for i := range pages {
		keys[i] = pages[i].Key
	}

	present, err := c.store.BatchExists(keys)
	if err != nil {
		return 0, err
	}

	putKeys := make([]string, 0, len(pages))
	putPtrs := make([]uintptr, 0, len(pages))
	putSizes := make([]uint64, 0, len(pages))
	for i := range pages {
		if present[i] {
			continue
		}
		putKeys = append(putKeys, keys[i])
		putPtrs = append(putPtrs, pageSrc[i])
		putSizes = append(putSizes, c.nbytes)
	}
	if len(putKeys) == 0 {
		return 0, nil
	}

	results, err := c.store.BatchPutFrom(putKeys, putPtrs, putSizes, &c.repl)
	if err != nil {
		return 0, err
	}
	stored := 0
	for _, r := range results {
		if r == 0 {
			stored++
		}
	}
	return stored, nil
}
