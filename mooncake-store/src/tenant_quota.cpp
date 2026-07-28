#include "tenant_quota.h"

#include <algorithm>
#include <cassert>
#include <limits>

namespace mooncake {
namespace {

struct RemainderShare {
    TenantId tenant_id;
    uint64_t base = 0;
    unsigned __int128 remainder = 0;
};

TenantQuotaResult AccountingMismatch() {
    return tl::make_unexpected(TenantQuotaError::kAccountingMismatch);
}

TenantQuotaChargeResult ChargeFailure(TenantQuotaError error,
                                      uint64_t deficit_bytes = 0) {
    return tl::make_unexpected(TenantQuotaChargeFailure{
        .error = error,
        .deficit_bytes = deficit_bytes,
    });
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

}  // namespace

TenantQuotaChargeResult TenantQuotaAccount::TryCharge(uint64_t bytes) {
    if (bytes > kMaxChargedBytes) {
        return ChargeFailure(TenantQuotaError::kInvalidArgument);
    }

    for (;;) {
        const uint64_t sequence_before =
            policy_sequence_.load(std::memory_order_acquire);
        if (sequence_before & 1) {
            continue;
        }

        uint64_t expected = charged_state_.load(std::memory_order_acquire);
        if (expected & kAdmissionClosed) {
            const uint64_t sequence_after =
                policy_sequence_.load(std::memory_order_acquire);
            if (sequence_before == sequence_after && !(sequence_after & 1)) {
                return ChargeFailure(TenantQuotaError::kTenantNotRegistered);
            }
            continue;
        }

        const uint64_t charged_bytes = expected & kChargedBytesMask;
        const uint64_t effective_quota_bytes =
            effective_quota_bytes_.load(std::memory_order_acquire);
        if (bytes != 0 && (charged_bytes > effective_quota_bytes ||
                           bytes > effective_quota_bytes - charged_bytes)) {
            const uint64_t sequence_after =
                policy_sequence_.load(std::memory_order_acquire);
            if (sequence_before == sequence_after && !(sequence_after & 1)) {
                const unsigned __int128 demand =
                    static_cast<unsigned __int128>(charged_bytes) + bytes;
                const unsigned __int128 deficit =
                    demand - effective_quota_bytes;
                return ChargeFailure(
                    TenantQuotaError::kQuotaExceeded,
                    deficit > std::numeric_limits<uint64_t>::max()
                        ? std::numeric_limits<uint64_t>::max()
                        : static_cast<uint64_t>(deficit));
            }
            continue;
        }

        if (bytes != 0) {
            const uint64_t desired = charged_bytes + bytes;
            if (!charged_state_.compare_exchange_weak(
                    expected, desired, std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                continue;
            }
        }

        const uint64_t state_after =
            charged_state_.load(std::memory_order_acquire);
        const uint64_t sequence_after =
            policy_sequence_.load(std::memory_order_acquire);
        if (sequence_before == sequence_after && !(sequence_after & 1) &&
            !(state_after & kAdmissionClosed)) {
            return {};
        }

        if (bytes != 0) {
            auto release_result = Release(bytes);
            if (!release_result) {
                return ChargeFailure(release_result.error());
            }
        }
    }
}

TenantQuotaResult TenantQuotaAccount::Release(uint64_t bytes) {
    if (bytes == 0) {
        return {};
    }

    uint64_t expected = charged_state_.load(std::memory_order_acquire);
    for (;;) {
        const uint64_t charged_bytes = expected & kChargedBytesMask;
        if (bytes > charged_bytes) {
            return AccountingMismatch();
        }

        const uint64_t desired =
            (expected & kAdmissionClosed) | (charged_bytes - bytes);
        if (charged_state_.compare_exchange_weak(expected, desired,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
            return {};
        }
    }
}

uint64_t TenantQuotaAccount::ChargedBytes() const {
    return charged_state_.load(std::memory_order_acquire) & kChargedBytesMask;
}

uint64_t TenantQuotaAccount::EffectiveQuotaBytes() const {
    return effective_quota_bytes_.load(std::memory_order_acquire);
}

bool TenantQuotaAccount::AdmissionClosed() const {
    return charged_state_.load(std::memory_order_acquire) & kAdmissionClosed;
}

void TenantQuotaAccount::BeginPolicyUpdate() {
    const uint64_t previous =
        policy_sequence_.fetch_add(1, std::memory_order_acq_rel);
    assert((previous & 1) == 0);
    (void)previous;
}

void TenantQuotaAccount::EndPolicyUpdate() {
    const uint64_t previous =
        policy_sequence_.fetch_add(1, std::memory_order_release);
    assert((previous & 1) != 0);
    (void)previous;
}

void TenantQuotaAccount::SetAdmissionClosed(bool closed) {
    uint64_t expected = charged_state_.load(std::memory_order_acquire);
    for (;;) {
        const uint64_t desired =
            closed ? expected | kAdmissionClosed : expected & kChargedBytesMask;
        if (charged_state_.compare_exchange_weak(expected, desired,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
            return;
        }
    }
}

void TenantQuotaAccount::ApplyEffectiveQuota(uint64_t effective_quota_bytes) {
    effective_quota_bytes_.store(effective_quota_bytes,
                                 std::memory_order_release);
    SetAdmissionClosed(!has_explicit_policy_);
}

void TenantQuotaAccount::SetChargedBytesForRebuild(uint64_t charged_bytes) {
    assert(charged_bytes <= kMaxChargedBytes);
    charged_state_.store(charged_bytes | kAdmissionClosed,
                         std::memory_order_release);
}

std::map<TenantId, uint64_t> TenantQuotaShard::BuildEffectiveQuotaAssignments(
    const std::vector<TenantQuotaSnapshot>& tenants,
    uint64_t allocatable_capacity_bytes) {
    return BuildEffectiveQuotaAssignmentsImpl(tenants,
                                              allocatable_capacity_bytes);
}

TenantQuotaResult TenantQuotaShard::UpsertTenantPolicy(
    const TenantId& tenant_id, uint64_t requested_quota_bytes) {
    if (requested_quota_bytes == 0 ||
        requested_quota_bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return tl::make_unexpected(TenantQuotaError::kInvalidArgument);
    }

    auto& account = GetOrCreateAccount(tenant_id);
    account.BeginPolicyUpdate();
    account.SetAdmissionClosed(true);
    account.requested_quota_bytes_ = requested_quota_bytes;
    account.has_explicit_policy_ = true;
    account.EndPolicyUpdate();
    return {};
}

TenantQuotaPolicyResult TenantQuotaShard::DisableTenantPolicyIfEmpty(
    const TenantId& tenant_id) {
    auto it = accounts_.find(tenant_id);
    if (it == accounts_.end() || !it->second->has_explicit_policy_) {
        return tl::make_unexpected(TenantQuotaError::kTenantNotFound);
    }

    auto& account = *it->second;
    account.BeginPolicyUpdate();
    account.SetAdmissionClosed(true);
    if (account.ChargedBytes() != 0) {
        account.SetAdmissionClosed(false);
        account.EndPolicyUpdate();
        return tl::make_unexpected(TenantQuotaError::kTenantNotEmpty);
    }

    const uint64_t requested_quota_bytes = account.requested_quota_bytes_;
    account.requested_quota_bytes_ = 0;
    account.effective_quota_bytes_.store(0, std::memory_order_release);
    account.has_explicit_policy_ = false;
    account.EndPolicyUpdate();
    return requested_quota_bytes;
}

TenantQuotaResult TenantQuotaShard::ApplyTenantPolicies(
    const TenantQuotaPolicyMap& policies) {
    for (const auto& [_, requested_quota_bytes] : policies) {
        if (requested_quota_bytes == 0 ||
            requested_quota_bytes > TenantQuotaAccount::kMaxChargedBytes) {
            return tl::make_unexpected(TenantQuotaError::kInvalidArgument);
        }
    }

    for (auto& [tenant_id, account_ptr] : accounts_) {
        auto& account = *account_ptr;
        auto policy_it = policies.find(tenant_id);
        account.BeginPolicyUpdate();
        account.SetAdmissionClosed(true);
        if (policy_it == policies.end()) {
            account.requested_quota_bytes_ = 0;
            account.effective_quota_bytes_.store(0, std::memory_order_release);
            account.has_explicit_policy_ = false;
        } else {
            account.requested_quota_bytes_ = policy_it->second;
            account.has_explicit_policy_ = true;
        }
        account.EndPolicyUpdate();
    }

    for (const auto& [tenant_id, requested_quota_bytes] : policies) {
        auto& account = GetOrCreateAccount(tenant_id);
        if (account.has_explicit_policy_) {
            continue;
        }
        account.BeginPolicyUpdate();
        account.SetAdmissionClosed(true);
        account.requested_quota_bytes_ = requested_quota_bytes;
        account.has_explicit_policy_ = true;
        account.EndPolicyUpdate();
    }
    return {};
}

TenantQuotaPolicyMap TenantQuotaShard::GetTenantPolicies() const {
    TenantQuotaPolicyMap policies;
    for (const auto& [tenant_id, account] : accounts_) {
        if (account->has_explicit_policy_) {
            policies.emplace(tenant_id, account->requested_quota_bytes_);
        }
    }
    return policies;
}

void TenantQuotaShard::RecomputeEffectiveQuotas(
    uint64_t allocatable_capacity_bytes) {
    ApplyEffectiveQuotas(BuildEffectiveQuotaAssignments(
        ListTenantSnapshots(), allocatable_capacity_bytes));
}

bool TenantQuotaShard::IsTenantRegistered(const TenantId& tenant_id) const {
    auto it = accounts_.find(tenant_id);
    return it != accounts_.end() && it->second->has_explicit_policy_;
}

TenantQuotaHandle TenantQuotaShard::GetOrCreateTenantHandle(
    const TenantId& tenant_id) {
    return &GetOrCreateAccount(tenant_id);
}

std::optional<TenantQuotaSnapshot> TenantQuotaShard::GetTenantSnapshot(
    const TenantId& tenant_id) const {
    auto it = accounts_.find(tenant_id);
    if (it == accounts_.end() || (!it->second->has_explicit_policy_ &&
                                  it->second->ChargedBytes() == 0)) {
        return std::nullopt;
    }
    return MakeSnapshot(it->first, *it->second);
}

std::vector<TenantQuotaSnapshot> TenantQuotaShard::ListTenantSnapshots() const {
    std::vector<TenantQuotaSnapshot> snapshots;
    snapshots.reserve(accounts_.size());
    for (const auto& [tenant_id, account] : accounts_) {
        if (!account->has_explicit_policy_ && account->ChargedBytes() == 0) {
            continue;
        }
        snapshots.push_back(MakeSnapshot(tenant_id, *account));
    }
    return snapshots;
}

TenantQuotaResult TenantQuotaShard::RebuildUsage(
    const TenantQuotaUsageMap& usage) {
    for (const auto& [_, tenant_usage] : usage) {
        if (tenant_usage.charged_bytes > TenantQuotaAccount::kMaxChargedBytes) {
            return tl::make_unexpected(TenantQuotaError::kInvalidArgument);
        }
    }

    for (auto& [_, account] : accounts_) {
        account->BeginPolicyUpdate();
        account->SetChargedBytesForRebuild(0);
        account->EndPolicyUpdate();
    }

    for (const auto& [tenant_id, tenant_usage] : usage) {
        auto& account = GetOrCreateAccount(tenant_id);
        if (!account.has_explicit_policy_) {
            account.requested_quota_bytes_ = 0;
            account.effective_quota_bytes_.store(0, std::memory_order_release);
        }
        account.BeginPolicyUpdate();
        account.SetChargedBytesForRebuild(tenant_usage.charged_bytes);
        account.EndPolicyUpdate();
    }
    return {};
}

TenantQuotaAccount& TenantQuotaShard::GetOrCreateAccount(
    const TenantId& tenant_id) {
    auto [it, inserted] = accounts_.try_emplace(tenant_id);
    if (inserted) {
        it->second = std::make_unique<TenantQuotaAccount>();
    }
    return *it->second;
}

TenantQuotaSnapshot TenantQuotaShard::MakeSnapshot(
    const TenantId& tenant_id, const TenantQuotaAccount& account) const {
    const uint64_t charged_bytes = account.ChargedBytes();
    const uint64_t effective_quota_bytes = account.EffectiveQuotaBytes();
    return TenantQuotaSnapshot{
        .tenant_id = tenant_id,
        .requested_quota_bytes = account.requested_quota_bytes_,
        .effective_quota_bytes = effective_quota_bytes,
        .charged_bytes = charged_bytes,
        .admission_closed = account.AdmissionClosed(),
        .has_explicit_policy = account.has_explicit_policy_,
        .over_quota = charged_bytes > effective_quota_bytes,
    };
}

void TenantQuotaShard::ApplyEffectiveQuotas(
    const std::map<TenantId, uint64_t>& effective_quotas) {
    for (auto& [tenant_id, account] : accounts_) {
        auto it = effective_quotas.find(tenant_id);
        const uint64_t effective_quota_bytes =
            it == effective_quotas.end() ? 0 : it->second;
        account->BeginPolicyUpdate();
        account->ApplyEffectiveQuota(effective_quota_bytes);
        account->EndPolicyUpdate();
    }
}

}  // namespace mooncake
