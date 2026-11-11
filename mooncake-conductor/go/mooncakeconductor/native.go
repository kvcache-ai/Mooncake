// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
package mooncakeconductor

/*
#include <stdlib.h>
#include <conductor/client/conductor_c.h>
*/
import "C"

import (
	"runtime"
	"strings"
	"unsafe"
)

type nativeClient struct {
	handle      C.mooncake_conductor_t
	initialized bool
}

func newNative() (*nativeClient, error) {
	handle := C.mooncake_conductor_create()
	if handle == nil {
		return nil, &Error{Op: "create", Code: InternalError}
	}
	return &nativeClient{handle: handle}, nil
}

// Pack NUL-terminated inputs into one C allocation. Reject embedded NULs so
// the shared C ABI cannot silently truncate Go strings.
func cStrings(op string, values ...string) ([]*C.char, func(), error) {
	size := 0
	for _, s := range values {
		if strings.IndexByte(s, 0) >= 0 {
			return nil, nil, &Error{Op: op, Code: InvalidParams}
		}
		size += len(s) + 1
	}
	data := C.malloc(C.size_t(size))
	views := make([]*C.char, len(values))
	offset := 0
	for i, s := range values {
		ptr := unsafe.Add(data, offset)
		bytes := unsafe.Slice((*byte)(ptr), len(s)+1)
		copy(bytes, s)
		bytes[len(s)] = 0
		views[i] = (*C.char)(ptr)
		offset += len(s) + 1
	}
	return views, func() { C.free(data) }, nil
}

func (n *nativeClient) close() error {
	C.mooncake_conductor_destroy(n.handle)
	n.handle = nil
	n.initialized = false
	return nil
}

func (n *nativeClient) disconnect() error {
	C.mooncake_conductor_close(n.handle)
	n.initialized = false
	return nil
}

func (n *nativeClient) setup(address string, connect, request int64) error {
	n.initialized = false
	v, free, err := cStrings("setup", address)
	if err != nil {
		n.disconnect()
		return err
	}
	defer free()
	err = nativeError("setup", int32(C.mooncake_conductor_setup(n.handle, v[0], C.int64_t(connect), C.int64_t(request))))
	n.initialized = err == nil
	return err
}

func (n *nativeClient) health() (HealthStatus, error) {
	// Rust's status-only API remembers any prior successful setup. Go retains
	// its existing contract: failed replacement setup means NotInitialized.
	if !n.initialized {
		return NotInitialized, nil
	}
	var status C.int
	rc := C.mooncake_conductor_health_check_ex(n.handle, &status)
	return HealthStatus(status), nativeError("health_check", int32(rc))
}

func publisherKind(value PublisherType) (C.int, error) {
	switch value {
	case "", PublisherVLLM:
		return C.MOONCAKE_CONDUCTOR_PUBLISHER_VLLM, nil
	case PublisherSGLang:
		return C.MOONCAKE_CONDUCTOR_PUBLISHER_SGLANG, nil
	case PublisherMooncake:
		return C.MOONCAKE_CONDUCTOR_PUBLISHER_MOONCAKE, nil
	default:
		return 0, &Error{Op: "register", Code: InvalidParams}
	}
}

func publisherType(kind C.int) PublisherType {
	switch kind {
	case C.MOONCAKE_CONDUCTOR_PUBLISHER_VLLM:
		return PublisherVLLM
	case C.MOONCAKE_CONDUCTOR_PUBLISHER_SGLANG:
		return PublisherSGLang
	case C.MOONCAKE_CONDUCTOR_PUBLISHER_MOONCAKE:
		return PublisherMooncake
	default:
		return ""
	}
}

func (n *nativeClient) register(cfg ServiceConfig) (RegisterResult, error) {
	kind, err := publisherKind(cfg.PublisherType)
	if err != nil {
		return RegisterResult{}, err
	}
	v, free, err := cStrings("register", cfg.InstanceID, cfg.Endpoint, cfg.ReplayEndpoint, cfg.ModelName,
		cfg.LoRAName, cfg.TenantID, cfg.HashProfile.Strategy, cfg.HashProfile.Algorithm, cfg.HashProfile.PythonHashSeed, cfg.HashProfile.IndexProjection)
	if err != nil {
		return RegisterResult{}, err
	}
	defer free()
	c := C.mooncake_conductor_service_config_t{instance_id: v[0], endpoint: v[1], replay_endpoint: v[2], publisher_kind: kind,
		model_name: v[3], lora_name: v[4], tenant_id: v[5], block_size: C.int64_t(cfg.BlockSize), dp_rank: C.int(cfg.DPRank),
		hash_profile: C.mooncake_conductor_hash_profile_config_t{strategy: v[6], algorithm: v[7], python_hash_seed: v[8], index_projection: v[9]}}
	if cfg.CacheGroup != nil {
		c.has_cache_group = 1
		c.cache_group = C.int64_t(*cfg.CacheGroup)
	}
	var isNew C.int
	if err := nativeError("register", int32(C.mooncake_conductor_register(n.handle, &c, &isNew))); err != nil {
		return RegisterResult{}, err
	}
	return RegisterResult{InstanceID: cfg.InstanceID, IsNew: isNew != 0}, nil
}

func (n *nativeClient) unregister(id, tenant string, rank int32) (UnregisterResult, error) {
	v, free, err := cStrings("unregister", id, tenant)
	if err != nil {
		return UnregisterResult{}, err
	}
	defer free()
	var key *C.char
	if err := nativeError("unregister", int32(C.mooncake_conductor_unregister_ex(n.handle, v[0], v[1], C.int(rank), &key))); err != nil {
		return UnregisterResult{}, err
	}
	defer C.mooncake_conductor_string_free(key)
	return UnregisterResult{RemovedKey: C.GoString(key)}, nil
}

func (n *nativeClient) query(req QueryRequest) (QueryResult, error) {
	v, free, err := cStrings("query", req.ModelName, req.LoRAName, req.TenantID, req.CacheSalt, req.InstanceFilter)
	if err != nil {
		return QueryResult{}, err
	}
	defer free()
	c := C.mooncake_conductor_query_request_t{model_name: v[0], lora_name: v[1], tenant_id: v[2], block_size: C.int64_t(req.BlockSize), cache_salt: v[3], instance_filter: v[4], token_count: C.size_t(len(req.TokenIDs))}
	if len(req.TokenIDs) != 0 {
		// The shared ABI embeds the borrowed token pointer in the request.
		var pinned runtime.Pinner
		pinned.Pin(&req.TokenIDs[0])
		defer pinned.Unpin()
		c.token_ids = (*C.int32_t)(unsafe.Pointer(&req.TokenIDs[0]))
	}
	var result C.mooncake_conductor_query_result_t
	rc := C.mooncake_conductor_query(n.handle, &c, &result)
	runtime.KeepAlive(req.TokenIDs)
	if err := nativeError("query", int32(rc)); err != nil {
		return QueryResult{}, err
	}
	defer C.mooncake_conductor_query_result_free(&result)
	out := QueryResult{Instances: make(map[string]CacheHit, int(result.hit_count))}
	for _, hit := range unsafe.Slice(result.hits, int(result.hit_count)) {
		h := CacheHit{TierMatch: TierMatch{Device: int64(hit.device), CPULocal: int64(hit.cpu_local), CPUShare: int64(hit.cpu_share), Disk: int64(hit.disk)}, LongestMatched: int64(hit.longest_matched), DP: make(map[int64]int64, int(hit.dp_count)), RankMatches: make(map[int64]TierMatch, int(hit.rank_match_count))}
		for _, dp := range unsafe.Slice(hit.dp, int(hit.dp_count)) {
			h.DP[int64(dp.rank)] = int64(dp.tokens)
		}
		for _, rank := range unsafe.Slice(hit.rank_matches, int(hit.rank_match_count)) {
			h.RankMatches[int64(rank.rank)] = TierMatch{Device: int64(rank.device), CPULocal: int64(rank.cpu_local), CPUShare: int64(rank.cpu_share), Disk: int64(rank.disk)}
		}
		out.Instances[C.GoString(hit.instance_id)] = h
	}
	return out, nil
}

func hashConfig(p C.mooncake_conductor_hash_profile_t) HashProfileConfig {
	return HashProfileConfig{Strategy: C.GoString(p.strategy), Algorithm: C.GoString(p.algorithm), PythonHashSeed: C.GoString(p.python_hash_seed), IndexProjection: C.GoString(p.index_projection)}
}

func (n *nativeClient) list() ([]RegisteredService, error) {
	var result C.mooncake_conductor_service_list_t
	if err := nativeError("list_services", int32(C.mooncake_conductor_list_services(n.handle, &result))); err != nil {
		return nil, err
	}
	defer C.mooncake_conductor_service_list_free(&result)
	out := make([]RegisteredService, 0, int(result.service_count))
	for _, c := range unsafe.Slice(result.services, int(result.service_count)) {
		g := ServiceConfig{InstanceID: C.GoString(c.instance_id), Endpoint: C.GoString(c.endpoint), ReplayEndpoint: C.GoString(c.replay_endpoint), PublisherType: publisherType(c.publisher_kind), ModelName: C.GoString(c.model_name), LoRAName: C.GoString(c.lora_name), TenantID: C.GoString(c.tenant_id), BlockSize: int64(c.block_size), DPRank: int32(c.dp_rank), HashProfile: hashConfig(c.hash_profile)}
		if c.has_cache_group != 0 {
			group := int64(c.cache_group)
			g.CacheGroup = &group
		}
		out = append(out, RegisteredService{ServiceConfig: g, RootDigest: C.GoString(c.hash_profile.root_digest)})
	}
	return out, nil
}

func (n *nativeClient) global() (GlobalView, error) {
	var result C.mooncake_conductor_global_view_t
	if err := nativeError("get_global_view", int32(C.mooncake_conductor_get_global_view(n.handle, &result))); err != nil {
		return GlobalView{}, err
	}
	defer C.mooncake_conductor_global_view_free(&result)
	out := GlobalView{ContextCount: int32(result.context_count), Contexts: make([]ContextView, 0, int(result.context_count))}
	for _, c := range unsafe.Slice(result.contexts, int(result.context_count)) {
		v := ContextView{ModelName: C.GoString(c.model_name), LoRAName: C.GoString(c.lora_name), TenantID: C.GoString(c.tenant_id), BlockSize: int64(c.block_size), PrefixCount: uint64(c.prefix_count), HashProfile: ResolvedHashProfile{HashProfileConfig: hashConfig(c.hash_profile), RootDigest: C.GoString(c.hash_profile.root_digest)}, Instances: make(map[string][]int64, int(c.instance_count))}
		for _, instance := range unsafe.Slice(c.instances, int(c.instance_count)) {
			ranks := make([]int64, int(instance.rank_count))
			for i, r := range unsafe.Slice(instance.ranks, int(instance.rank_count)) {
				ranks[i] = int64(r)
			}
			v.Instances[C.GoString(instance.instance_id)] = ranks
		}
		out.Contexts = append(out.Contexts, v)
	}
	return out, nil
}
