#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "tenant_id.h"

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

struct TenantQuotaSnapshot {
    TenantId tenant_id;
    uint64_t requested_quota_bytes = 0;
    uint64_t effective_quota_bytes = 0;
    uint64_t used_bytes = 0;
    uint64_t reserved_bytes = 0;
    uint64_t committed_count = 0;
    uint64_t metadata_object_count = 0;
    bool has_explicit_policy = false;
    bool over_quota = false;
};

struct TenantQuotaUsage {
    uint64_t used_bytes = 0;
    uint64_t committed_count = 0;
    uint64_t metadata_object_count = 0;
};

using TenantQuotaPolicyMap = std::map<TenantId, uint64_t>;
using TenantQuotaUsageMap =
    std::unordered_map<TenantId, TenantQuotaUsage, TenantIdHash>;

enum class TenantQuotaError {
    kQuotaExceeded,
    kInvalidArgument,
    kAccountingMismatch,
    kTenantNotRegistered,
    kTenantNotEmpty,
    kTenantNotFound,
};

using TenantQuotaResult = tl::expected<void, TenantQuotaError>;
using TenantQuotaPolicyResult = tl::expected<uint64_t, TenantQuotaError>;

template <size_t NumShards = 1024>
class ShardedTenantQuotaTable;

// Single-threaded tenant quota state machine. This class owns quota rules and
// accounting invariants, but deliberately contains no locking or sharding.
class TenantQuotaTable {
   public:
    TenantQuotaResult UpsertTenantPolicy(const TenantId& tenant_id,
                                         uint64_t requested_quota_bytes);
    TenantQuotaPolicyResult DisableTenantPolicyIfEmpty(
        const TenantId& tenant_id);
    void ApplyTenantPolicies(const TenantQuotaPolicyMap& policies);
    TenantQuotaPolicyMap GetTenantPolicies() const;

    void RecomputeEffectiveQuotas(uint64_t allocatable_capacity_bytes);

    bool IsTenantRegistered(const TenantId& tenant_id) const;
    std::optional<TenantQuotaSnapshot> GetTenantSnapshot(
        const TenantId& tenant_id) const;
    std::vector<TenantQuotaSnapshot> ListTenantSnapshots() const;
    uint64_t ComputeDeficit(const TenantId& tenant_id,
                            uint64_t incoming_bytes) const;

    TenantQuotaResult Reserve(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult Commit(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult CommitAdditional(const TenantId& tenant_id,
                                       uint64_t bytes);
    TenantQuotaResult Abort(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult Release(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult ReleasePartial(const TenantId& tenant_id, uint64_t bytes);

    void IncrementMetadataObjectCount(const TenantId& tenant_id);
    TenantQuotaResult DecrementMetadataObjectCount(const TenantId& tenant_id);
    void RebuildUsage(const TenantQuotaUsageMap& usage);

   private:
    template <size_t>
    friend class ShardedTenantQuotaTable;

    struct TenantQuotaState {
        uint64_t requested_quota_bytes = 0;
        uint64_t effective_quota_bytes = 0;
        uint64_t used_bytes = 0;
        uint64_t reserved_bytes = 0;
        uint64_t committed_count = 0;
        uint64_t metadata_object_count = 0;
        bool has_explicit_policy = false;
        bool over_quota = false;
    };

    using StateMap = std::map<TenantId, TenantQuotaState>;

    TenantQuotaState& GetOrCreateState(const TenantId& tenant_id);
    TenantQuotaSnapshot MakeSnapshot(const TenantId& tenant_id,
                                     const TenantQuotaState& state) const;
    static bool IsLazyEmptyTenant(const TenantQuotaState& state);
    static void RefreshOverQuota(TenantQuotaState* state);
    static std::map<TenantId, uint64_t> BuildEffectiveQuotaAssignments(
        const std::vector<TenantQuotaSnapshot>& tenants,
        uint64_t allocatable_capacity_bytes);
    void ApplyEffectiveQuotas(
        const std::map<TenantId, uint64_t>& effective_quotas);
    void EraseIfLazyEmpty(StateMap::iterator it);

    StateMap tenants_;
};

// Thread-safe production wrapper around TenantQuotaTable. Per-tenant
// operations lock only one shard; cross-shard policy, usage, and recompute
// operations are serialized by recompute_mutex_.
template <size_t NumShards>
class ShardedTenantQuotaTable {
   public:
    static_assert(NumShards > 0, "tenant quota table needs at least one shard");
    static constexpr size_t kNumShards = NumShards;

    TenantQuotaResult UpsertTenantPolicy(const TenantId& tenant_id,
                                         uint64_t requested_quota_bytes,
                                         uint64_t allocatable_capacity_bytes);
    TenantQuotaPolicyResult DisableTenantPolicyIfEmpty(
        const TenantId& tenant_id);
    void ApplyTenantPolicies(const TenantQuotaPolicyMap& policies,
                             uint64_t allocatable_capacity_bytes);
    TenantQuotaPolicyMap GetTenantPolicies() const;

    void RecomputeEffectiveQuotas(uint64_t allocatable_capacity_bytes);

    bool IsTenantRegistered(const TenantId& tenant_id) const;
    std::optional<TenantQuotaSnapshot> GetTenantSnapshot(
        const TenantId& tenant_id) const;
    std::vector<TenantQuotaSnapshot> ListTenantSnapshots() const;
    uint64_t ComputeDeficit(const TenantId& tenant_id,
                            uint64_t incoming_bytes) const;

    TenantQuotaResult Reserve(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult Commit(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult CommitAdditional(const TenantId& tenant_id,
                                       uint64_t bytes);
    TenantQuotaResult Abort(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult Release(const TenantId& tenant_id, uint64_t bytes);
    TenantQuotaResult ReleasePartial(const TenantId& tenant_id, uint64_t bytes);

    void IncrementMetadataObjectCount(const TenantId& tenant_id);
    TenantQuotaResult DecrementMetadataObjectCount(const TenantId& tenant_id);
    void RebuildUsage(const TenantQuotaUsageMap& usage,
                      uint64_t allocatable_capacity_bytes);

   private:
    struct Shard {
        mutable std::mutex mutex;
        TenantQuotaTable table;
    };

    size_t GetShardIndex(const TenantId& tenant_id) const;
    Shard& GetShard(const TenantId& tenant_id);
    const Shard& GetShard(const TenantId& tenant_id) const;
    void RecomputeEffectiveQuotasLocked(uint64_t allocatable_capacity_bytes);

    std::array<Shard, kNumShards> shards_;
    mutable std::mutex recompute_mutex_;
};

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
