#pragma once

#include <algorithm>

#include "tenant_quota_sharded.h"

namespace mooncake {

template <size_t NumShards>
TenantQuotaResult ShardedTenantQuotaTable<NumShards>::UpsertTenantPolicy(
    const TenantId& tenant_id, uint64_t requested_quota_bytes,
    uint64_t allocatable_capacity_bytes) {
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    auto& shard = GetShard(tenant_id);
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto result =
            shard.table.UpsertTenantPolicy(tenant_id, requested_quota_bytes);
        if (!result) {
            return result;
        }
    }
    RecomputeEffectiveQuotasLocked(allocatable_capacity_bytes);
    return {};
}

template <size_t NumShards>
TenantQuotaPolicyResult
ShardedTenantQuotaTable<NumShards>::DisableTenantPolicyIfEmpty(
    const TenantId& tenant_id) {
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.DisableTenantPolicyIfEmpty(tenant_id);
}

template <size_t NumShards>
void ShardedTenantQuotaTable<NumShards>::ApplyTenantPolicies(
    const TenantQuotaPolicyMap& policies, uint64_t allocatable_capacity_bytes) {
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    std::array<TenantQuotaPolicyMap, kNumShards> grouped_policies;
    for (const auto& [tenant_id, requested_quota_bytes] : policies) {
        grouped_policies[GetShardIndex(tenant_id)].emplace(
            tenant_id, requested_quota_bytes);
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        auto& shard = shards_[i];
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.table.ApplyTenantPolicies(grouped_policies[i]);
    }
    RecomputeEffectiveQuotasLocked(allocatable_capacity_bytes);
}

template <size_t NumShards>
TenantQuotaPolicyMap ShardedTenantQuotaTable<NumShards>::GetTenantPolicies()
    const {
    TenantQuotaPolicyMap policies;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto shard_policies = shard.table.GetTenantPolicies();
        policies.insert(shard_policies.begin(), shard_policies.end());
    }
    return policies;
}

template <size_t NumShards>
void ShardedTenantQuotaTable<NumShards>::RecomputeEffectiveQuotas(
    uint64_t allocatable_capacity_bytes) {
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    RecomputeEffectiveQuotasLocked(allocatable_capacity_bytes);
}

template <size_t NumShards>
bool ShardedTenantQuotaTable<NumShards>::IsTenantRegistered(
    const TenantId& tenant_id) const {
    const auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.IsTenantRegistered(tenant_id);
}

template <size_t NumShards>
std::optional<TenantQuotaSnapshot>
ShardedTenantQuotaTable<NumShards>::GetTenantSnapshot(
    const TenantId& tenant_id) const {
    const auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.GetTenantSnapshot(tenant_id);
}

template <size_t NumShards>
std::vector<TenantQuotaSnapshot>
ShardedTenantQuotaTable<NumShards>::ListTenantSnapshots() const {
    std::vector<TenantQuotaSnapshot> snapshots;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto shard_snapshots = shard.table.ListTenantSnapshots();
        snapshots.insert(snapshots.end(), shard_snapshots.begin(),
                         shard_snapshots.end());
    }
    std::sort(snapshots.begin(), snapshots.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.tenant_id < rhs.tenant_id;
              });
    return snapshots;
}

template <size_t NumShards>
uint64_t ShardedTenantQuotaTable<NumShards>::ComputeDeficit(
    const TenantId& tenant_id, uint64_t incoming_bytes) const {
    const auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.ComputeDeficit(tenant_id, incoming_bytes);
}

template <size_t NumShards>
TenantQuotaResult ShardedTenantQuotaTable<NumShards>::Reserve(
    const TenantId& tenant_id, uint64_t bytes) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.Reserve(tenant_id, bytes);
}

template <size_t NumShards>
TenantQuotaResult ShardedTenantQuotaTable<NumShards>::Commit(
    const TenantId& tenant_id, uint64_t bytes) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.Commit(tenant_id, bytes);
}

template <size_t NumShards>
TenantQuotaResult ShardedTenantQuotaTable<NumShards>::CommitAdditional(
    const TenantId& tenant_id, uint64_t bytes) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.CommitAdditional(tenant_id, bytes);
}

template <size_t NumShards>
TenantQuotaResult ShardedTenantQuotaTable<NumShards>::Abort(
    const TenantId& tenant_id, uint64_t bytes) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.Abort(tenant_id, bytes);
}

template <size_t NumShards>
TenantQuotaResult ShardedTenantQuotaTable<NumShards>::Release(
    const TenantId& tenant_id, uint64_t bytes) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.Release(tenant_id, bytes);
}

template <size_t NumShards>
TenantQuotaResult ShardedTenantQuotaTable<NumShards>::ReleasePartial(
    const TenantId& tenant_id, uint64_t bytes) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.ReleasePartial(tenant_id, bytes);
}

template <size_t NumShards>
void ShardedTenantQuotaTable<NumShards>::IncrementMetadataObjectCount(
    const TenantId& tenant_id) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    shard.table.IncrementMetadataObjectCount(tenant_id);
}

template <size_t NumShards>
TenantQuotaResult
ShardedTenantQuotaTable<NumShards>::DecrementMetadataObjectCount(
    const TenantId& tenant_id) {
    auto& shard = GetShard(tenant_id);
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.table.DecrementMetadataObjectCount(tenant_id);
}

template <size_t NumShards>
void ShardedTenantQuotaTable<NumShards>::RebuildUsage(
    const TenantQuotaUsageMap& usage, uint64_t allocatable_capacity_bytes) {
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    std::array<TenantQuotaUsageMap, kNumShards> grouped_usage;
    for (const auto& [tenant_id, tenant_usage] : usage) {
        grouped_usage[GetShardIndex(tenant_id)].emplace(tenant_id,
                                                        tenant_usage);
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        auto& shard = shards_[i];
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.table.RebuildUsage(grouped_usage[i]);
    }
    RecomputeEffectiveQuotasLocked(allocatable_capacity_bytes);
}

template <size_t NumShards>
size_t ShardedTenantQuotaTable<NumShards>::GetShardIndex(
    const TenantId& tenant_id) const {
    return TenantIdHash{}(tenant_id) % kNumShards;
}

template <size_t NumShards>
typename ShardedTenantQuotaTable<NumShards>::Shard&
ShardedTenantQuotaTable<NumShards>::GetShard(const TenantId& tenant_id) {
    return shards_[GetShardIndex(tenant_id)];
}

template <size_t NumShards>
const typename ShardedTenantQuotaTable<NumShards>::Shard&
ShardedTenantQuotaTable<NumShards>::GetShard(const TenantId& tenant_id) const {
    return shards_[GetShardIndex(tenant_id)];
}

template <size_t NumShards>
void ShardedTenantQuotaTable<NumShards>::RecomputeEffectiveQuotasLocked(
    uint64_t allocatable_capacity_bytes) {
    std::vector<TenantQuotaSnapshot> snapshots;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto shard_snapshots = shard.table.ListTenantSnapshots();
        snapshots.insert(snapshots.end(), shard_snapshots.begin(),
                         shard_snapshots.end());
    }

    auto effective_quotas = TenantQuotaTable::BuildEffectiveQuotaAssignments(
        snapshots, allocatable_capacity_bytes);
    std::array<std::map<TenantId, uint64_t>, kNumShards> grouped_quotas;
    for (const auto& [tenant_id, effective_quota_bytes] : effective_quotas) {
        grouped_quotas[GetShardIndex(tenant_id)].emplace(tenant_id,
                                                         effective_quota_bytes);
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        auto& shard = shards_[i];
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.table.ApplyEffectiveQuotas(grouped_quotas[i]);
    }
}

}  // namespace mooncake
