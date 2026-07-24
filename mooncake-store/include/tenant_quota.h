#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
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

template <size_t NumShards>
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

}  // namespace mooncake
