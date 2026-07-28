#include "tenant_quota.h"

#include <algorithm>
#include <limits>

namespace mooncake {
namespace {

struct RemainderShare {
    TenantId tenant_id;
    uint64_t base = 0;
    unsigned __int128 remainder = 0;
};

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

std::map<TenantId, uint64_t> BuildEffectiveQuotaAssignmentsImpl(
    const std::vector<TenantQuotaSnapshot>& tenants,
    uint64_t allocatable_capacity_bytes) {
    unsigned __int128 explicit_requested_sum = 0;
    std::vector<std::pair<TenantId, uint64_t>> explicit_tenants;
    std::map<TenantId, uint64_t> assigned;

    for (const auto& snapshot : tenants) {
        const TenantId& tenant_id = snapshot.tenant_id;
        assigned.emplace(tenant_id, 0);
        if (snapshot.has_explicit_policy) {
            explicit_requested_sum += snapshot.requested_quota_bytes;
            explicit_tenants.emplace_back(tenant_id,
                                          snapshot.requested_quota_bytes);
        }
    }

    if (explicit_requested_sum <= allocatable_capacity_bytes) {
        for (const auto& [tenant_id, requested_quota_bytes] :
             explicit_tenants) {
            assigned[tenant_id] = requested_quota_bytes;
        }
        return assigned;
    }

    std::vector<RemainderShare> shares;
    shares.reserve(explicit_tenants.size());
    uint64_t base_assigned = 0;
    for (const auto& [tenant_id, requested_quota_bytes] : explicit_tenants) {
        const unsigned __int128 product =
            static_cast<unsigned __int128>(allocatable_capacity_bytes) *
            requested_quota_bytes;
        const uint64_t base =
            static_cast<uint64_t>(product / explicit_requested_sum);
        shares.push_back({.tenant_id = tenant_id,
                          .base = base,
                          .remainder = product % explicit_requested_sum});
        base_assigned += base;
    }

    std::sort(shares.begin(), shares.end(),
              [](const RemainderShare& lhs, const RemainderShare& rhs) {
                  if (lhs.remainder != rhs.remainder) {
                      return lhs.remainder > rhs.remainder;
                  }
                  return lhs.tenant_id < rhs.tenant_id;
              });

    uint64_t remaining = allocatable_capacity_bytes - base_assigned;
    for (auto& share : shares) {
        if (remaining > 0) {
            ++share.base;
            --remaining;
        }
        assigned[share.tenant_id] = share.base;
    }
    return assigned;
}

TenantQuotaResult AccountingMismatch() {
    return tl::make_unexpected(TenantQuotaError::kAccountingMismatch);
}

}  // namespace

std::map<TenantId, uint64_t> TenantQuotaTable::BuildEffectiveQuotaAssignments(
    const std::vector<TenantQuotaSnapshot>& tenants,
    uint64_t allocatable_capacity_bytes) {
    return BuildEffectiveQuotaAssignmentsImpl(tenants,
                                              allocatable_capacity_bytes);
}

TenantQuotaResult TenantQuotaTable::UpsertTenantPolicy(
    const TenantId& tenant_id, uint64_t requested_quota_bytes) {
    if (requested_quota_bytes == 0) {
        return tl::make_unexpected(TenantQuotaError::kInvalidArgument);
    }

    auto& state = GetOrCreateState(tenant_id);
    state.requested_quota_bytes = requested_quota_bytes;
    state.has_explicit_policy = true;
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaPolicyResult TenantQuotaTable::DisableTenantPolicyIfEmpty(
    const TenantId& tenant_id) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || !it->second.has_explicit_policy) {
        return tl::make_unexpected(TenantQuotaError::kTenantNotFound);
    }

    auto& state = it->second;
    if (state.used_bytes != 0 || state.reserved_bytes != 0 ||
        state.committed_count != 0 || state.metadata_object_count != 0) {
        return tl::make_unexpected(TenantQuotaError::kTenantNotEmpty);
    }

    const uint64_t requested_quota_bytes = state.requested_quota_bytes;
    state.requested_quota_bytes = 0;
    state.effective_quota_bytes = 0;
    state.has_explicit_policy = false;
    RefreshOverQuota(&state);
    EraseIfLazyEmpty(it);
    return requested_quota_bytes;
}

void TenantQuotaTable::ApplyTenantPolicies(
    const TenantQuotaPolicyMap& policies) {
    for (auto it = tenants_.begin(); it != tenants_.end();) {
        auto policy_it = policies.find(it->first);
        auto& state = it->second;
        if (policy_it != policies.end()) {
            state.requested_quota_bytes = policy_it->second;
            state.has_explicit_policy = true;
            RefreshOverQuota(&state);
            ++it;
            continue;
        }

        state.requested_quota_bytes = 0;
        state.effective_quota_bytes = 0;
        state.has_explicit_policy = false;
        if (IsLazyEmptyTenant(state)) {
            it = tenants_.erase(it);
        } else {
            RefreshOverQuota(&state);
            ++it;
        }
    }

    for (const auto& [tenant_id, requested_quota_bytes] : policies) {
        auto [it, inserted] = tenants_.try_emplace(tenant_id);
        if (!inserted) {
            continue;
        }
        it->second.requested_quota_bytes = requested_quota_bytes;
        it->second.has_explicit_policy = true;
        RefreshOverQuota(&it->second);
    }
}

TenantQuotaPolicyMap TenantQuotaTable::GetTenantPolicies() const {
    TenantQuotaPolicyMap policies;
    for (const auto& [tenant_id, state] : tenants_) {
        if (state.has_explicit_policy) {
            policies.emplace(tenant_id, state.requested_quota_bytes);
        }
    }
    return policies;
}

void TenantQuotaTable::RecomputeEffectiveQuotas(
    uint64_t allocatable_capacity_bytes) {
    ApplyEffectiveQuotas(BuildEffectiveQuotaAssignments(
        ListTenantSnapshots(), allocatable_capacity_bytes));
}

bool TenantQuotaTable::IsTenantRegistered(const TenantId& tenant_id) const {
    auto it = tenants_.find(tenant_id);
    return it != tenants_.end() && it->second.has_explicit_policy;
}

std::optional<TenantQuotaSnapshot> TenantQuotaTable::GetTenantSnapshot(
    const TenantId& tenant_id) const {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end()) {
        return std::nullopt;
    }
    return MakeSnapshot(it->first, it->second);
}

std::vector<TenantQuotaSnapshot> TenantQuotaTable::ListTenantSnapshots() const {
    std::vector<TenantQuotaSnapshot> snapshots;
    snapshots.reserve(tenants_.size());
    for (const auto& [tenant_id, state] : tenants_) {
        if (!IsLazyEmptyTenant(state)) {
            snapshots.push_back(MakeSnapshot(tenant_id, state));
        }
    }
    return snapshots;
}

uint64_t TenantQuotaTable::ComputeDeficit(const TenantId& tenant_id,
                                          uint64_t incoming_bytes) const {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end()) {
        return incoming_bytes;
    }

    const auto& state = it->second;
    const unsigned __int128 demand =
        static_cast<unsigned __int128>(state.used_bytes) +
        state.reserved_bytes + incoming_bytes;
    if (demand <= state.effective_quota_bytes) {
        return 0;
    }

    const unsigned __int128 deficit = demand - state.effective_quota_bytes;
    return deficit > std::numeric_limits<uint64_t>::max()
               ? std::numeric_limits<uint64_t>::max()
               : static_cast<uint64_t>(deficit);
}

TenantQuotaResult TenantQuotaTable::Reserve(const TenantId& tenant_id,
                                            uint64_t bytes) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || !it->second.has_explicit_policy) {
        return tl::make_unexpected(TenantQuotaError::kTenantNotRegistered);
    }
    if (bytes == 0) {
        return {};
    }

    auto& state = it->second;
    if (static_cast<unsigned __int128>(state.used_bytes) +
            state.reserved_bytes + bytes >
        state.effective_quota_bytes) {
        return tl::make_unexpected(TenantQuotaError::kQuotaExceeded);
    }

    state.reserved_bytes += bytes;
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaResult TenantQuotaTable::Commit(const TenantId& tenant_id,
                                           uint64_t bytes) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || it->second.reserved_bytes < bytes) {
        return AccountingMismatch();
    }
    if (bytes == 0) {
        return {};
    }

    auto& state = it->second;
    state.reserved_bytes -= bytes;
    state.used_bytes = SaturatingAdd(state.used_bytes, bytes);
    if (state.committed_count < std::numeric_limits<uint64_t>::max()) {
        ++state.committed_count;
    }
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaResult TenantQuotaTable::CommitAdditional(const TenantId& tenant_id,
                                                     uint64_t bytes) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || it->second.reserved_bytes < bytes) {
        return AccountingMismatch();
    }
    if (bytes == 0) {
        return {};
    }

    auto& state = it->second;
    state.reserved_bytes -= bytes;
    state.used_bytes = SaturatingAdd(state.used_bytes, bytes);
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaResult TenantQuotaTable::Abort(const TenantId& tenant_id,
                                          uint64_t bytes) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || it->second.reserved_bytes < bytes) {
        return AccountingMismatch();
    }
    if (bytes == 0) {
        return {};
    }

    it->second.reserved_bytes -= bytes;
    RefreshOverQuota(&it->second);
    EraseIfLazyEmpty(it);
    return {};
}

TenantQuotaResult TenantQuotaTable::Release(const TenantId& tenant_id,
                                            uint64_t bytes) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || it->second.used_bytes < bytes ||
        (bytes != 0 && it->second.committed_count == 0)) {
        return AccountingMismatch();
    }
    if (bytes == 0) {
        return {};
    }

    auto& state = it->second;
    state.used_bytes -= bytes;
    --state.committed_count;
    RefreshOverQuota(&state);
    EraseIfLazyEmpty(it);
    return {};
}

TenantQuotaResult TenantQuotaTable::ReleasePartial(const TenantId& tenant_id,
                                                   uint64_t bytes) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || it->second.used_bytes < bytes) {
        return AccountingMismatch();
    }
    if (bytes == 0) {
        return {};
    }

    it->second.used_bytes -= bytes;
    RefreshOverQuota(&it->second);
    EraseIfLazyEmpty(it);
    return {};
}

void TenantQuotaTable::IncrementMetadataObjectCount(const TenantId& tenant_id) {
    auto& state = GetOrCreateState(tenant_id);
    if (state.metadata_object_count < std::numeric_limits<uint64_t>::max()) {
        ++state.metadata_object_count;
    }
    RefreshOverQuota(&state);
}

TenantQuotaResult TenantQuotaTable::DecrementMetadataObjectCount(
    const TenantId& tenant_id) {
    auto it = tenants_.find(tenant_id);
    if (it == tenants_.end() || it->second.metadata_object_count == 0) {
        return AccountingMismatch();
    }

    --it->second.metadata_object_count;
    RefreshOverQuota(&it->second);
    EraseIfLazyEmpty(it);
    return {};
}

void TenantQuotaTable::RebuildUsage(const TenantQuotaUsageMap& usage) {
    for (auto& [_, state] : tenants_) {
        state.used_bytes = 0;
        state.reserved_bytes = 0;
        state.committed_count = 0;
        state.metadata_object_count = 0;
    }

    for (const auto& [tenant_id, tenant_usage] : usage) {
        auto& state = GetOrCreateState(tenant_id);
        if (!state.has_explicit_policy) {
            state.requested_quota_bytes = 0;
            state.effective_quota_bytes = 0;
        }
        state.used_bytes = tenant_usage.used_bytes;
        state.committed_count = tenant_usage.committed_count;
        state.metadata_object_count = tenant_usage.metadata_object_count;
        RefreshOverQuota(&state);
    }

    for (auto it = tenants_.begin(); it != tenants_.end();) {
        if (IsLazyEmptyTenant(it->second)) {
            it = tenants_.erase(it);
        } else {
            RefreshOverQuota(&it->second);
            ++it;
        }
    }
}

TenantQuotaTable::TenantQuotaState& TenantQuotaTable::GetOrCreateState(
    const TenantId& tenant_id) {
    return tenants_.try_emplace(tenant_id).first->second;
}

TenantQuotaSnapshot TenantQuotaTable::MakeSnapshot(
    const TenantId& tenant_id, const TenantQuotaState& state) const {
    return TenantQuotaSnapshot{
        .tenant_id = tenant_id,
        .requested_quota_bytes = state.requested_quota_bytes,
        .effective_quota_bytes = state.effective_quota_bytes,
        .used_bytes = state.used_bytes,
        .reserved_bytes = state.reserved_bytes,
        .committed_count = state.committed_count,
        .metadata_object_count = state.metadata_object_count,
        .has_explicit_policy = state.has_explicit_policy,
        .over_quota = state.over_quota,
    };
}

bool TenantQuotaTable::IsLazyEmptyTenant(const TenantQuotaState& state) {
    return !state.has_explicit_policy && state.used_bytes == 0 &&
           state.reserved_bytes == 0 && state.committed_count == 0 &&
           state.metadata_object_count == 0;
}

void TenantQuotaTable::RefreshOverQuota(TenantQuotaState* state) {
    state->over_quota =
        (!state->has_explicit_policy && state->metadata_object_count > 0) ||
        static_cast<unsigned __int128>(state->used_bytes) +
                state->reserved_bytes >
            state->effective_quota_bytes;
}

void TenantQuotaTable::ApplyEffectiveQuotas(
    const std::map<TenantId, uint64_t>& effective_quotas) {
    for (auto& [tenant_id, state] : tenants_) {
        auto it = effective_quotas.find(tenant_id);
        state.effective_quota_bytes =
            it == effective_quotas.end() ? 0 : it->second;
        RefreshOverQuota(&state);
    }
}

void TenantQuotaTable::EraseIfLazyEmpty(StateMap::iterator it) {
    if (IsLazyEmptyTenant(it->second)) {
        tenants_.erase(it);
    }
}

}  // namespace mooncake
