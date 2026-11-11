// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
package mooncakeconductor

import "time"

type ClientConfig struct {
	Address        string
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
}

type PublisherType string

const (
	PublisherVLLM     PublisherType = "vLLM"
	PublisherSGLang   PublisherType = "SGLang"
	PublisherMooncake PublisherType = "Mooncake"
)

type HealthStatus int32

const (
	Healthy HealthStatus = iota
	NotInitialized
	Unreachable
)

type HashProfileConfig struct {
	Strategy        string `json:"strategy"`
	Algorithm       string `json:"algorithm"`
	PythonHashSeed  string `json:"python_hash_seed"`
	IndexProjection string `json:"index_projection"`
}

type ResolvedHashProfile struct {
	HashProfileConfig
	RootDigest string `json:"root_digest"`
}

type ServiceConfig struct {
	InstanceID     string            `json:"instance_id"`
	Endpoint       string            `json:"endpoint"`
	ReplayEndpoint string            `json:"replay_endpoint"`
	PublisherType  PublisherType     `json:"publisher_type"`
	ModelName      string            `json:"model_name"`
	LoRAName       string            `json:"lora_name"`
	TenantID       string            `json:"tenant_id"`
	BlockSize      int64             `json:"block_size"`
	DPRank         int32             `json:"dp_rank"`
	CacheGroup     *int64            `json:"cache_group"`
	HashProfile    HashProfileConfig `json:"hash_profile"`
}

type RegisteredService struct {
	ServiceConfig
	RootDigest string `json:"root_digest"`
}

type QueryRequest struct {
	ModelName      string
	LoRAName       string
	TenantID       string
	BlockSize      int64
	TokenIDs       []int32
	CacheSalt      string
	InstanceFilter string
}

// TierMatch holds cumulative continuous prefix lengths, not additive counts.
type TierMatch struct {
	Device   int64 `json:"device"`
	CPULocal int64 `json:"cpu_local"`
	CPUShare int64 `json:"cpu_share"`
	Disk     int64 `json:"disk"`
}

type CacheHit struct {
	TierMatch
	LongestMatched int64               `json:"longest_matched"`
	DP             map[int64]int64     `json:"dp"`
	RankMatches    map[int64]TierMatch `json:"rank_matches"`
}

type QueryResult struct {
	Instances map[string]CacheHit `json:"instances"`
}

type RegisterResult struct {
	InstanceID string
	IsNew      bool
}

type UnregisterResult struct{ RemovedKey string }

type ContextView struct {
	ModelName   string              `json:"model_name"`
	LoRAName    string              `json:"lora_name"`
	TenantID    string              `json:"tenant_id"`
	BlockSize   int64               `json:"block_size"`
	PrefixCount uint64              `json:"prefix_count"`
	HashProfile ResolvedHashProfile `json:"hash_profile"`
	Instances   map[string][]int64  `json:"instances"`
}

type GlobalView struct {
	ContextCount int32         `json:"context_count"`
	Contexts     []ContextView `json:"contexts"`
}
