#include "tenant_quota.h"

#include <algorithm>
#include <limits>

#include "types.h"

#include <glog/logging.h>

namespace mooncake {
namespace {

struct RemainderShare {
    std::string tenant_id;
    uint64_t base = 0;
    unsigned __int128 remainder = 0;
};

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

bool IsLazyEmptyTenant(const TenantQuotaState& state) {
    return !state.has_explicit_policy && state.used_bytes == 0 &&
           state.reserved_bytes == 0 && state.committed_count == 0;
}

TenantQuotaResult AccountingMismatch(const char* operation,
                                     const std::string& tenant_id,
                                     uint64_t requested, uint64_t available) {
    LOG(WARNING) << operation << " accounting mismatch for tenant " << tenant_id
                 << ": requested=" << requested << ", available=" << available;
    return tl::make_unexpected(TenantQuotaError::kAccountingMismatch);
}

}  // namespace

void TenantQuotaTable::SetDefaultRequestedQuota(uint64_t bytes) {
    default_requested_quota_bytes_ = bytes;
    for (auto& [_, state] : tenants_) {
        if (!state.has_explicit_policy) {
            state.requested_quota_bytes = default_requested_quota_bytes_;
        }
    }
}

uint64_t TenantQuotaTable::GetDefaultRequestedQuota() const {
    return default_requested_quota_bytes_;
}

TenantQuotaResult TenantQuotaTable::UpsertTenantPolicy(
    std::string tenant_id, uint64_t requested_quota_bytes) {
    if (requested_quota_bytes == 0) {
        return tl::make_unexpected(TenantQuotaError::kInvalidArgument);
    }

    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    auto& state = GetOrCreateState(normalized_tenant_id);
    state.requested_quota_bytes = requested_quota_bytes;
    state.has_explicit_policy = true;
    return {};
}

void TenantQuotaTable::EraseTenantPolicy(std::string tenant_id) {
    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    auto it = tenants_.find(normalized_tenant_id);
    if (it == tenants_.end()) {
        return;
    }
    auto& state = it->second;
    state.requested_quota_bytes = default_requested_quota_bytes_;
    state.has_explicit_policy = false;
}

void TenantQuotaTable::RecomputeEffectiveQuotas(
    uint64_t allocatable_capacity_bytes) {
    unsigned __int128 explicit_requested_sum = 0;
    std::vector<std::string> explicit_tenants;
    std::vector<std::string> default_tenants;

    for (auto& [tenant_id, state] : tenants_) {
        if (state.has_explicit_policy) {
            explicit_tenants.push_back(tenant_id);
            explicit_requested_sum += state.requested_quota_bytes;
        } else {
            state.requested_quota_bytes = default_requested_quota_bytes_;
            if (!IsLazyEmptyTenant(state)) {
                default_tenants.push_back(tenant_id);
            }
        }
        state.effective_quota_bytes = 0;
    }

    auto distribute = [&](const std::vector<std::string>& tenant_ids,
                          uint64_t capacity, bool proportional_to_requested) {
        if (tenant_ids.empty() || capacity == 0) {
            return;
        }

        std::vector<RemainderShare> shares;
        shares.reserve(tenant_ids.size());
        uint64_t assigned = 0;
        unsigned __int128 denominator = proportional_to_requested
                                            ? explicit_requested_sum
                                            : tenant_ids.size();

        for (const auto& tenant_id : tenant_ids) {
            auto& state = tenants_.at(tenant_id);
            unsigned __int128 numerator =
                proportional_to_requested ? state.requested_quota_bytes : 1;
            unsigned __int128 product =
                static_cast<unsigned __int128>(capacity) * numerator;
            uint64_t base = static_cast<uint64_t>(product / denominator);
            unsigned __int128 remainder = product % denominator;
            shares.push_back({tenant_id, base, remainder});
            assigned += base;
        }

        std::sort(shares.begin(), shares.end(),
                  [](const RemainderShare& lhs, const RemainderShare& rhs) {
                      if (lhs.remainder != rhs.remainder) {
                          return lhs.remainder > rhs.remainder;
                      }
                      return lhs.tenant_id < rhs.tenant_id;
                  });

        uint64_t remaining = capacity - assigned;
        for (auto& share : shares) {
            if (remaining > 0) {
                ++share.base;
                --remaining;
            }
            tenants_.at(share.tenant_id).effective_quota_bytes = share.base;
        }
    };

    if (explicit_requested_sum <= allocatable_capacity_bytes) {
        for (const auto& tenant_id : explicit_tenants) {
            auto& state = tenants_.at(tenant_id);
            state.effective_quota_bytes = state.requested_quota_bytes;
        }

        const uint64_t remaining_capacity =
            allocatable_capacity_bytes -
            static_cast<uint64_t>(explicit_requested_sum);
        distribute(default_tenants, remaining_capacity,
                   /*proportional_to_requested=*/false);
    } else {
        distribute(explicit_tenants, allocatable_capacity_bytes,
                   /*proportional_to_requested=*/true);
    }

    for (auto& [_, state] : tenants_) {
        RefreshOverQuota(&state);
    }
}

std::optional<TenantQuotaSnapshot> TenantQuotaTable::GetTenantSnapshot(
    std::string tenant_id) const {
    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    auto it = tenants_.find(normalized_tenant_id);
    if (it == tenants_.end()) {
        return std::nullopt;
    }
    return MakeSnapshot(it->first, it->second);
}

std::vector<TenantQuotaSnapshot> TenantQuotaTable::ListTenantSnapshots() const {
    std::vector<TenantQuotaSnapshot> snapshots;
    for (const auto& [tenant_id, state] : tenants_) {
        if (IsLazyEmptyTenant(state)) {
            continue;
        }
        snapshots.push_back(MakeSnapshot(tenant_id, state));
    }
    return snapshots;
}

TenantQuotaResult TenantQuotaTable::Reserve(std::string tenant_id,
                                            uint64_t bytes) {
    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    if (bytes == 0) {
        GetOrCreateState(normalized_tenant_id);
        return {};
    }

    auto it = tenants_.find(normalized_tenant_id);
    if (it == tenants_.end()) {
        return tl::make_unexpected(TenantQuotaError::kQuotaExceeded);
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

TenantQuotaResult TenantQuotaTable::Commit(std::string tenant_id,
                                           uint64_t bytes) {
    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    auto& state = GetOrCreateState(normalized_tenant_id);
    if (bytes == 0) {
        return {};
    }

    if (state.reserved_bytes < bytes) {
        return AccountingMismatch("Commit", normalized_tenant_id, bytes,
                                  state.reserved_bytes);
    }

    state.reserved_bytes -= bytes;
    state.used_bytes = SaturatingAdd(state.used_bytes, bytes);
    ++state.committed_count;
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaResult TenantQuotaTable::Abort(std::string tenant_id,
                                          uint64_t bytes) {
    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    auto& state = GetOrCreateState(normalized_tenant_id);
    if (bytes == 0) {
        return {};
    }

    if (state.reserved_bytes < bytes) {
        return AccountingMismatch("Abort", normalized_tenant_id, bytes,
                                  state.reserved_bytes);
    }

    state.reserved_bytes -= bytes;
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaResult TenantQuotaTable::Release(std::string tenant_id,
                                            uint64_t bytes) {
    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    auto& state = GetOrCreateState(normalized_tenant_id);
    if (bytes == 0) {
        return {};
    }

    if (state.used_bytes < bytes) {
        return AccountingMismatch("Release", normalized_tenant_id, bytes,
                                  state.used_bytes);
    }

    state.used_bytes -= bytes;
    if (state.committed_count > 0) {
        --state.committed_count;
    } else {
        LOG(WARNING) << "Release found zero committed_count for tenant "
                     << normalized_tenant_id;
    }
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaResult TenantQuotaTable::ReleasePartial(std::string tenant_id,
                                                   uint64_t bytes) {
    auto normalized_tenant_id = NormalizeTenantId(std::move(tenant_id));
    auto& state = GetOrCreateState(normalized_tenant_id);
    if (bytes == 0) {
        return {};
    }

    DCHECK_LE(bytes, state.used_bytes);
    if (state.used_bytes < bytes) {
        return AccountingMismatch("ReleasePartial", normalized_tenant_id, bytes,
                                  state.used_bytes);
    }

    state.used_bytes -= bytes;
    RefreshOverQuota(&state);
    return {};
}

TenantQuotaState& TenantQuotaTable::GetOrCreateState(
    const std::string& tenant_id) {
    auto [it, inserted] = tenants_.try_emplace(tenant_id);
    if (inserted) {
        it->second.requested_quota_bytes = default_requested_quota_bytes_;
    }
    return it->second;
}

TenantQuotaSnapshot TenantQuotaTable::MakeSnapshot(
    const std::string& tenant_id, const TenantQuotaState& state) const {
    return TenantQuotaSnapshot{
        .tenant_id = tenant_id,
        .requested_quota_bytes = state.requested_quota_bytes,
        .effective_quota_bytes = state.effective_quota_bytes,
        .used_bytes = state.used_bytes,
        .reserved_bytes = state.reserved_bytes,
        .committed_count = state.committed_count,
        .has_explicit_policy = state.has_explicit_policy,
        .over_quota = state.over_quota,
    };
}

void TenantQuotaTable::RefreshOverQuota(TenantQuotaState* state) const {
    state->over_quota = static_cast<unsigned __int128>(state->used_bytes) +
                            state->reserved_bytes >
                        state->effective_quota_bytes;
}

}  // namespace mooncake
