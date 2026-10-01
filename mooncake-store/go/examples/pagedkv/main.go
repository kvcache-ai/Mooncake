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

// Command pagedkv demonstrates the paged-KV <-> Mooncake Store connector end to
// end against a running store, using the TCP transport (no RDMA hardware
// required).
//
// It plays both roles in one process: it publishes the KV pages of a prompt
// (prefill), then loads them back for a second request that shares the same
// prefix (decode), and verifies the bytes round-trip exactly.
//
// Prerequisites:
//
//	mooncake_master --enable_http_metadata_server=true \
//	  --http_metadata_server_port=8080
package main

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"unsafe"

	pagedkv "github.com/kvcache-ai/Mooncake/mooncake-store/go/pagedkv"
)

func main() {
	metadataServer := envOrDefault("MC_METADATA_SERVER", "http://localhost:8080/metadata")
	masterAddr := envOrDefault("MC_MASTER_ADDR", "localhost:50051")

	// A tiny layout so the example moves kilobytes, not gigabytes.
	layout := pagedkv.PageLayout{
		NumLayers:  4,
		NumKVHeads: 2,
		HeadDim:    16,
		PageTokens: 8,
		ElemSize:   2,
	}
	pageBytes := layout.PageBytes()

	c, err := pagedkv.New(pagedkv.Config{
		LocalHostname:     "localhost",
		MetadataServer:    metadataServer,
		MasterServerAddr:  masterAddr,
		GlobalSegmentSize: 256 * 1024 * 1024,
		LocalBufferSize:   64 * 1024 * 1024,
		Protocol:          "tcp",
		Role:              pagedkv.RoleMixed,
		Model:             "demo-model",
		Layout:            layout,
	})
	if err != nil {
		log.Fatalf("connector.New: %v", err)
	}
	defer c.Close()
	fmt.Println("Connected to Mooncake Store")

	// Two requests sharing a 3-page prefix; the fourth page diverges.
	const pageTokens = 8
	prompt := makeTokens(0, 4*pageTokens) // 4 pages
	follow := append(cloneTokens(prompt[:3*pageTokens]), makeTokens(9000, pageTokens)...)

	// One contiguous, registered staging region big enough for all pages.
	nPages := 4
	region := C.malloc(C.size_t(nPages * pageBytes))
	if region == nil {
		log.Fatal("malloc failed")
	}
	defer C.free(region)
	base := uintptr(region)
	if err := c.RegisterKVBuffer(base, uint64(nPages*pageBytes)); err != nil {
		log.Fatalf("RegisterKVBuffer: %v", err)
	}
	defer c.UnregisterKVBuffer(base)

	srcPtrs := make([]uintptr, nPages)
	for i := 0; i < nPages; i++ {
		srcPtrs[i] = base + uintptr(i*pageBytes)
		fillPage(region, i*pageBytes, pageBytes, byte(i+1))
	}

	// Prefill: publish the prompt's KV pages.
	stored, err := c.StoreComputedPrefix(prompt, srcPtrs)
	if err != nil {
		log.Fatalf("StoreComputedPrefix: %v", err)
	}
	fmt.Printf("Published %d/%d pages for the prompt\n", stored, nPages)

	// Decode: a second request sharing the first 3 pages should hit exactly
	// those and miss the diverging 4th.
	dstRegion := C.malloc(C.size_t(nPages * pageBytes))
	if dstRegion == nil {
		log.Fatal("malloc failed")
	}
	defer C.free(dstRegion)
	dstBase := uintptr(dstRegion)
	if err := c.RegisterKVBuffer(dstBase, uint64(nPages*pageBytes)); err != nil {
		log.Fatalf("RegisterKVBuffer(dst): %v", err)
	}
	defer c.UnregisterKVBuffer(dstBase)

	dstPtrs := make([]uintptr, nPages)
	for i := 0; i < nPages; i++ {
		dstPtrs[i] = dstBase + uintptr(i*pageBytes)
	}

	res, err := c.LoadCachedPrefix(follow, dstPtrs)
	if err != nil {
		log.Fatalf("LoadCachedPrefix: %v", err)
	}
	fmt.Printf("Cache hit: %d pages (%d tokens)\n", res.MatchedPages, res.MatchedTokens)
	if res.MatchedPages != 3 {
		log.Fatalf("expected 3 shared pages, got %d", res.MatchedPages)
	}

	// Correctness gate: loaded bytes must equal what we published.
	for i := 0; i < res.MatchedPages; i++ {
		if !bytes.Equal(readPage(region, i*pageBytes, pageBytes), readPage(dstRegion, i*pageBytes, pageBytes)) {
			log.Fatalf("page %d round-trip mismatch", i)
		}
	}
	fmt.Println("Round-trip verified: loaded KV matches published KV")

	if _, err := c.Store().RemoveAll(true); err != nil {
		log.Printf("cleanup RemoveAll: %v", err)
	}
	fmt.Println("Done!")
}

// The pages live in a C allocation, so offsets are taken on the region pointer
// itself: rebuilding a pointer from a uintptr would be unsound in general.
func fillPage(region unsafe.Pointer, off, n int, tag byte) {
	C.memset(unsafe.Add(region, off), C.int(tag), C.size_t(n))
}

func readPage(region unsafe.Pointer, off, n int) []byte {
	return C.GoBytes(unsafe.Add(region, off), C.int(n))
}

func makeTokens(base, n int) []int32 {
	t := make([]int32, n)
	for i := range t {
		t[i] = int32(base + i)
	}
	return t
}

func cloneTokens(t []int32) []int32 { return append([]int32(nil), t...) }

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
