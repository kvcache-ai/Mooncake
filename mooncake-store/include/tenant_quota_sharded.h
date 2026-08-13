#pragma once

#include <array>
#include <cstddef>
#include <mutex>

#include "tenant_quota.h"

namespace mooncake {

// Thread-safe production wrapper around TenantQuotaTable. Per-tenant
// operations lock only one shard; cross-shard policy, usage, and recompute
// operations are serialized by recompute_mutex_.
template <size_t NumShards = 1024>
class ShardedTenantQuotaTable {
   public:
    static_assert(NumShards > 0, "tenant quota table needs at least one shard");
    static constexpr size_t kNumShards = NumShards;

    TenantQuotaResult UpsertTenantPolicy(const TenantId& tenant_id,
                                         uint64_t requested_quota_bytes,
                                         uint64_t allocatable_capacity_bytes);
    TenantQuotaResult DisableTenantPolicyIfEmpty(const TenantId& tenant_id);
    TenantQuotaResult ApplyTenantPolicies(const TenantQuotaPolicyMap& policies,
                                          uint64_t allocatable_capacity_bytes);
    TenantQuotaPolicyMap GetTenantPolicies() const;

    void RecomputeEffectiveQuotas(uint64_t allocatable_capacity_bytes);

    bool IsTenantRegistered(const TenantId& tenant_id) const;
    // May create a stable closed tombstone for a previously unseen tenant.
    TenantQuotaHandle GetOrCreateTenantHandle(const TenantId& tenant_id);
    std::optional<TenantQuotaSnapshot> GetTenantSnapshot(
        const TenantId& tenant_id) const;
    std::vector<TenantQuotaSnapshot> ListTenantSnapshots() const;

    // Rebuild overwrites runtime accounting and must only run while data-plane
    // charge/release operations are quiescent.
    TenantQuotaResult RebuildUsage(const TenantQuotaUsageMap& usage,
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

}  // namespace mooncake

#include "tenant_quota_sharded_impl.h"
