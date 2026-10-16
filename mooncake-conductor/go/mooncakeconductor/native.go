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
	"unsafe"
)

type nativeClient struct{ handle C.mooncake_conductor_client_t }

func newNative() (*nativeClient, error) {
	if C.mooncake_conductor_abi_version() != C.MOONCAKE_CONDUCTOR_ABI_VERSION {
		return nil, &Error{Op: "abi_version", Code: InternalError}
	}
	n := &nativeClient{}
	if err := nativeError("create", int32(C.mooncake_conductor_create(&n.handle))); err != nil {
		return nil, err
	}
	return n, nil
}

// One C allocation per request keeps cgo crossings independent of field count.
// Every pointer in the returned views references C memory, never a Go string.
func stringViews(values ...string) ([]C.mooncake_conductor_string, func()) {
	size := 0
	for _, s := range values {
		size += len(s)
	}
	var data unsafe.Pointer
	if size != 0 {
		data = C.malloc(C.size_t(size))
	}
	views := make([]C.mooncake_conductor_string, len(values))
	offset := 0
	for i, s := range values {
		if len(s) != 0 {
			ptr := unsafe.Add(data, offset)
			copy(unsafe.Slice((*byte)(ptr), len(s)), s)
			views[i] = C.mooncake_conductor_string{data: (*C.char)(ptr), size: C.size_t(len(s))}
			offset += len(s)
		}
	}
	return views, func() { C.free(data) }
}

func goString(s C.mooncake_conductor_string) string {
	return string(unsafe.Slice((*byte)(unsafe.Pointer(s.data)), int(s.size)))
}

func (n *nativeClient) close() error {
	err := nativeError("close", int32(C.mooncake_conductor_destroy(n.handle)))
	n.handle = nil
	return err
}

func (n *nativeClient) disconnect() error {
	return nativeError("close", int32(C.mooncake_conductor_close(n.handle)))
}

func (n *nativeClient) setup(address string, connect, request int64) error {
	v, free := stringViews(address)
	defer free()
	cfg := C.mooncake_conductor_client_config{address: v[0], connect_timeout_ms: C.int64_t(connect), request_timeout_ms: C.int64_t(request)}
	return nativeError("setup", int32(C.mooncake_conductor_setup(n.handle, &cfg)))
}

func (n *nativeClient) health() (HealthStatus, error) {
	var status C.int32_t
	rc := C.mooncake_conductor_health_check(n.handle, &status)
	return HealthStatus(status), nativeError("health_check", int32(rc))
}

func (n *nativeClient) register(cfg ServiceConfig) (RegisterResult, error) {
	v, free := stringViews(cfg.InstanceID, cfg.Endpoint, cfg.ReplayEndpoint, string(cfg.PublisherType), cfg.ModelName,
		cfg.LoRAName, cfg.TenantID, cfg.HashProfile.Strategy, cfg.HashProfile.Algorithm, cfg.HashProfile.PythonHashSeed, cfg.HashProfile.IndexProjection)
	defer free()
	c := C.mooncake_conductor_service_config{instance_id: v[0], endpoint: v[1], replay_endpoint: v[2], publisher_type: v[3],
		model_name: v[4], lora_name: v[5], tenant_id: v[6], block_size: C.int64_t(cfg.BlockSize), dp_rank: C.int32_t(cfg.DPRank),
		hash_profile: C.mooncake_conductor_hash_config{strategy: v[7], algorithm: v[8], python_hash_seed: v[9], index_projection: v[10]}}
	if cfg.CacheGroup != nil {
		c.has_cache_group = 1
		c.cache_group = C.int64_t(*cfg.CacheGroup)
	}
	var result *C.mooncake_conductor_registration_result
	if err := nativeError("register", int32(C.mooncake_conductor_register(n.handle, &c, &result))); err != nil {
		return RegisterResult{}, err
	}
	defer C.mooncake_conductor_registration_free(result)
	var id C.mooncake_conductor_string
	var isNew C.int32_t
	if err := nativeError("register_view", int32(C.mooncake_conductor_registration_view(result, &id, &isNew))); err != nil {
		return RegisterResult{}, err
	}
	return RegisterResult{InstanceID: goString(id), IsNew: isNew != 0}, nil
}

func (n *nativeClient) unregister(id, tenant string, rank int32) (UnregisterResult, error) {
	v, free := stringViews(id, tenant)
	defer free()
	var result *C.mooncake_conductor_unregistration_result
	if err := nativeError("unregister", int32(C.mooncake_conductor_unregister(n.handle, v[0], v[1], C.int32_t(rank), &result))); err != nil {
		return UnregisterResult{}, err
	}
	defer C.mooncake_conductor_unregistration_free(result)
	var key C.mooncake_conductor_string
	if err := nativeError("unregister_view", int32(C.mooncake_conductor_unregistration_view(result, &key))); err != nil {
		return UnregisterResult{}, err
	}
	return UnregisterResult{RemovedKey: goString(key)}, nil
}

func tierMatch(m C.mooncake_conductor_tier_match) TierMatch {
	return TierMatch{Device: int64(m.device), CPULocal: int64(m.cpu_local), CPUShare: int64(m.cpu_share), Disk: int64(m.disk)}
}

func (n *nativeClient) query(req QueryRequest) (QueryResult, error) {
	v, free := stringViews(req.ModelName, req.LoRAName, req.TenantID, req.CacheSalt, req.InstanceFilter)
	defer free()
	c := C.mooncake_conductor_query_request{model_name: v[0], lora_name: v[1], tenant_id: v[2], block_size: C.int64_t(req.BlockSize), cache_salt: v[3], instance_filter: v[4]}
	var tokens *C.int32_t
	if len(req.TokenIDs) != 0 {
		tokens = (*C.int32_t)(unsafe.Pointer(&req.TokenIDs[0]))
	}
	var result *C.mooncake_conductor_query_result
	rc := C.mooncake_conductor_query(n.handle, &c, tokens, C.size_t(len(req.TokenIDs)), &result)
	runtime.KeepAlive(req.TokenIDs)
	if err := nativeError("query", int32(rc)); err != nil {
		return QueryResult{}, err
	}
	defer C.mooncake_conductor_query_free(result)
	var hits *C.mooncake_conductor_cache_hit
	var count C.size_t
	if err := nativeError("query_view", int32(C.mooncake_conductor_query_view(result, &hits, &count))); err != nil {
		return QueryResult{}, err
	}
	out := QueryResult{Instances: make(map[string]CacheHit, int(count))}
	for _, hit := range unsafe.Slice(hits, int(count)) {
		h := CacheHit{TierMatch: tierMatch(hit.match), LongestMatched: int64(hit.longest_matched), DP: make(map[int64]int64, int(hit.dp_count)), RankMatches: make(map[int64]TierMatch, int(hit.rank_count))}
		for _, dp := range unsafe.Slice(hit.dp, int(hit.dp_count)) {
			h.DP[int64(dp.rank)] = int64(dp.tokens)
		}
		for _, rank := range unsafe.Slice(hit.rank_matches, int(hit.rank_count)) {
			h.RankMatches[int64(rank.rank)] = tierMatch(rank.match)
		}
		out.Instances[goString(hit.instance_id)] = h
	}
	return out, nil
}

func hashConfig(p C.mooncake_conductor_hash_config) HashProfileConfig {
	return HashProfileConfig{Strategy: goString(p.strategy), Algorithm: goString(p.algorithm), PythonHashSeed: goString(p.python_hash_seed), IndexProjection: goString(p.index_projection)}
}

func (n *nativeClient) list() ([]RegisteredService, error) {
	var result *C.mooncake_conductor_services_result
	if err := nativeError("list_services", int32(C.mooncake_conductor_list_services(n.handle, &result))); err != nil {
		return nil, err
	}
	defer C.mooncake_conductor_services_free(result)
	var services *C.mooncake_conductor_registered_service
	var count C.size_t
	if err := nativeError("services_view", int32(C.mooncake_conductor_services_view(result, &services, &count))); err != nil {
		return nil, err
	}
	out := make([]RegisteredService, 0, int(count))
	for _, svc := range unsafe.Slice(services, int(count)) {
		c := svc.config
		g := ServiceConfig{InstanceID: goString(c.instance_id), Endpoint: goString(c.endpoint), ReplayEndpoint: goString(c.replay_endpoint), PublisherType: PublisherType(goString(c.publisher_type)), ModelName: goString(c.model_name), LoRAName: goString(c.lora_name), TenantID: goString(c.tenant_id), BlockSize: int64(c.block_size), DPRank: int32(c.dp_rank), HashProfile: hashConfig(c.hash_profile)}
		if c.has_cache_group != 0 {
			group := int64(c.cache_group)
			g.CacheGroup = &group
		}
		out = append(out, RegisteredService{ServiceConfig: g, RootDigest: goString(svc.root_digest)})
	}
	return out, nil
}

func (n *nativeClient) global() (GlobalView, error) {
	var result *C.mooncake_conductor_global_result
	if err := nativeError("get_global_view", int32(C.mooncake_conductor_get_global_view(n.handle, &result))); err != nil {
		return GlobalView{}, err
	}
	defer C.mooncake_conductor_global_free(result)
	var contexts *C.mooncake_conductor_context_view
	var count C.size_t
	if err := nativeError("global_view", int32(C.mooncake_conductor_global_view(result, &contexts, &count))); err != nil {
		return GlobalView{}, err
	}
	out := GlobalView{ContextCount: int32(count), Contexts: make([]ContextView, 0, int(count))}
	for _, c := range unsafe.Slice(contexts, int(count)) {
		v := ContextView{ModelName: goString(c.model_name), LoRAName: goString(c.lora_name), TenantID: goString(c.tenant_id), BlockSize: int64(c.block_size), PrefixCount: uint64(c.prefix_count), HashProfile: ResolvedHashProfile{HashProfileConfig: hashConfig(c.hash_profile.config), RootDigest: goString(c.hash_profile.root_digest)}, Instances: make(map[string][]int64, int(c.instance_count))}
		for _, instance := range unsafe.Slice(c.instances, int(c.instance_count)) {
			ranks := make([]int64, int(instance.rank_count))
			for i, r := range unsafe.Slice(instance.ranks, int(instance.rank_count)) {
				ranks[i] = int64(r)
			}
			v.Instances[goString(instance.instance_id)] = ranks
		}
		out.Contexts = append(out.Contexts, v)
	}
	return out, nil
}
