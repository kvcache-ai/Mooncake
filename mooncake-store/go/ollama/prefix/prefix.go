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

// Package prefix computes content-addressed store keys for KV cache pages.
//
// It is deliberately free of any CGo or Mooncake Store dependency so that the
// keying logic (the part that decides which pages two requests may share) can
// be unit-tested without a running cluster.
//
// Keys form a hash chain over fixed-size token pages:
//
//	seed      = H("mooncake-ollama-kv" || model)
//	hash[i]   = H(hash[i-1] || tokens[page i])
//	key[i]    = "<prefix>/<model>/<hex(hash[i])>"
//
// Two requests that share a token prefix therefore produce identical keys for
// the shared pages (enabling cross-instance reuse), while any divergence in an
// earlier token changes every subsequent key (so a decode worker can never load
// KV that does not match its own tokens). Folding the model name into the seed
// keeps caches for different models disjoint even under a shared store.
package prefix

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	// DefaultKeyPrefix namespaces Ollama keys so they never collide with keys
	// written by the vLLM or SGLang connectors against the same store.
	DefaultKeyPrefix = "mc/ollama"

	// keyHashBytes is how many bytes of the 256-bit chained digest are rendered
	// into the key string. 128 bits is well beyond the collision budget of any
	// realistic KV cache population while keeping keys short.
	keyHashBytes = 16
)

// hashSeedDomain separates this key space from any other SHA-256 usage.
const hashSeedDomain = "mooncake-ollama-kv\x00"

// ErrInvalidPageSize is returned when PageSize is not positive.
var ErrInvalidPageSize = errors.New("prefix: page size must be positive")

// Config describes how a token sequence is split into cacheable pages and how
// the resulting keys are named. The zero value is not usable; PageSize must be
// set. An empty KeyPrefix defaults to DefaultKeyPrefix.
type Config struct {
	// PageSize is the number of tokens per page. It must match the KV paging
	// granularity used by the runtime so that a stored page maps to exactly one
	// contiguous KV block.
	PageSize int

	// Model identifies the model whose KV cache is being stored. It is folded
	// into every page hash, so different models never alias.
	Model string

	// KeyPrefix namespaces the generated keys. Defaults to DefaultKeyPrefix.
	KeyPrefix string
}

func (c Config) keyPrefix() string {
	if c.KeyPrefix == "" {
		return DefaultKeyPrefix
	}
	return c.KeyPrefix
}

// Page describes one cacheable KV page: its position in the sequence, the
// half-open token range it covers, and the store key it is addressed by.
type Page struct {
	Index      int    // zero-based page index within the sequence
	StartToken int    // inclusive
	EndToken   int    // exclusive
	Key        string // Mooncake Store key
}

// NumFullPages returns how many whole pages fit in numTokens. A trailing
// partial page is not cacheable and is not counted.
func NumFullPages(numTokens, pageSize int) int {
	if pageSize <= 0 || numTokens <= 0 {
		return 0
	}
	return numTokens / pageSize
}

// Pages splits tokens into full pages and returns their descriptors in order.
// Only whole pages are produced; a trailing partial page is dropped because it
// is not a stable, reusable unit. The returned keys are deterministic in
// (Model, KeyPrefix, PageSize, tokens).
func (c Config) Pages(tokens []int32) ([]Page, error) {
	if c.PageSize <= 0 {
		return nil, ErrInvalidPageSize
	}

	n := NumFullPages(len(tokens), c.PageSize)
	pages := make([]Page, 0, n)
	prefixStr := c.keyPrefix()

	h := sha256.Sum256([]byte(hashSeedDomain + c.Model))
	for i := 0; i < n; i++ {
		start := i * c.PageSize
		end := start + c.PageSize
		h = chain(h, tokens[start:end])
		key := fmt.Sprintf("%s/%s/%s", prefixStr, c.Model, hex.EncodeToString(h[:keyHashBytes]))
		pages = append(pages, Page{Index: i, StartToken: start, EndToken: end, Key: key})
	}
	return pages, nil
}

// Keys is a convenience wrapper around Pages that returns just the key strings.
func (c Config) Keys(tokens []int32) ([]string, error) {
	pages, err := c.Pages(tokens)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(pages))
	for i := range pages {
		keys[i] = pages[i].Key
	}
	return keys, nil
}

// chain folds one page of tokens into the running digest. Tokens are encoded as
// fixed-width big-endian uint32 (llama.cpp token ids are int32) so the encoding
// is unambiguous and stable across platforms.
func chain(prev [32]byte, tokens []int32) [32]byte {
	hasher := sha256.New()
	hasher.Write(prev[:])
	var b [4]byte
	for _, t := range tokens {
		binary.BigEndian.PutUint32(b[:], uint32(t))
		hasher.Write(b[:])
	}
	var out [32]byte
	copy(out[:], hasher.Sum(nil))
	return out
}

// LongestPresentRun returns the length of the leading run of true values,
// stopping at the first false. KV cache reuse requires a contiguous prefix, so
// a gap terminates the usable run even if later pages happen to be present
// (for example after an eviction in the middle of a chain).
func LongestPresentRun(present []bool) int {
	for i, ok := range present {
		if !ok {
			return i
		}
	}
	return len(present)
}
