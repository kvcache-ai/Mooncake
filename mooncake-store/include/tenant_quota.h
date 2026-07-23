#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "tenant_id.h"

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

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

struct TenantQuotaSnapshot {
    std::string tenant_id;
    uint64_t requested_quota_bytes = 0;
    uint64_t effective_quota_bytes = 0;
    uint64_t used_bytes = 0;
    uint64_t reserved_bytes = 0;
    uint64_t committed_count = 0;
    uint64_t metadata_object_count = 0;
    bool has_explicit_policy = false;
    bool over_quota = false;
};

struct TenantQuotaAssignment {
    TenantId tenant_id;
    uint64_t effective_quota_bytes = 0;
};

enum class TenantQuotaError {
    kQuotaExceeded,
    kInvalidArgument,
    kAccountingMismatch,
};

using TenantQuotaResult = tl::expected<void, TenantQuotaError>;

std::vector<TenantQuotaAssignment> BuildEffectiveQuotaAssignments(
    const std::map<TenantId, TenantQuotaState>& tenants,
    uint64_t allocatable_capacity_bytes);

class TenantQuotaTable {
   public:
    TenantQuotaResult UpsertTenantPolicy(std::string tenant_id,
                                         uint64_t requested_quota_bytes);
    void EraseTenantPolicy(std::string tenant_id);

    void RecomputeEffectiveQuotas(uint64_t allocatable_capacity_bytes);

    std::optional<TenantQuotaSnapshot> GetTenantSnapshot(
        std::string tenant_id) const;
    std::vector<TenantQuotaSnapshot> ListTenantSnapshots() const;

    TenantQuotaResult Reserve(std::string tenant_id, uint64_t bytes);
    TenantQuotaResult Commit(std::string tenant_id, uint64_t bytes);
    TenantQuotaResult Abort(std::string tenant_id, uint64_t bytes);
    TenantQuotaResult Release(std::string tenant_id, uint64_t bytes);
    TenantQuotaResult ReleasePartial(std::string tenant_id, uint64_t bytes);

   private:
    TenantQuotaState& GetOrCreateState(const TenantId& tenant_id);
    TenantQuotaSnapshot MakeSnapshot(const TenantId& tenant_id,
                                     const TenantQuotaState& state) const;
    void RefreshOverQuota(TenantQuotaState* state) const;

    std::map<TenantId, TenantQuotaState> tenants_;
};

}  // namespace mooncake
