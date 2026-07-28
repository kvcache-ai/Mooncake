#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
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
    uint64_t charged_bytes = 0;
    bool admission_closed = true;
    bool has_explicit_policy = false;
    bool over_quota = false;
};

struct TenantQuotaUsage {
    uint64_t charged_bytes = 0;
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

struct TenantQuotaChargeFailure {
    TenantQuotaError error;
    uint64_t deficit_bytes = 0;
};

using TenantQuotaResult = tl::expected<void, TenantQuotaError>;
using TenantQuotaChargeResult = tl::expected<void, TenantQuotaChargeFailure>;
using TenantQuotaPolicyResult = tl::expected<uint64_t, TenantQuotaError>;

class TenantQuotaAccount {
   public:
    static constexpr uint64_t kAdmissionClosed = 1ULL << 63;
    static constexpr uint64_t kChargedBytesMask = kAdmissionClosed - 1;
    static constexpr uint64_t kMaxChargedBytes = kChargedBytesMask;

    TenantQuotaChargeResult TryCharge(uint64_t bytes);
    TenantQuotaResult Release(uint64_t bytes);

    uint64_t ChargedBytes() const;
    uint64_t EffectiveQuotaBytes() const;
    bool AdmissionClosed() const;

   private:
    friend class TenantQuotaShard;

    void BeginPolicyUpdate();
    void EndPolicyUpdate();
    void SetAdmissionClosed(bool closed);
    void ApplyEffectiveQuota(uint64_t effective_quota_bytes);
    void SetChargedBytesForRebuild(uint64_t charged_bytes);

    // Bit 63 closes admission; bits 0..62 contain charged bytes.
    alignas(64) std::atomic<uint64_t> charged_state_{kAdmissionClosed};

    // Keep control-plane writes off the charged-state cache line.
    alignas(64) std::atomic<uint64_t> effective_quota_bytes_{0};
    std::atomic<uint64_t> policy_sequence_{0};

    // Accessed only while the owning quota-table shard is locked.
    uint64_t requested_quota_bytes_{0};
    bool has_explicit_policy_{false};
};

using TenantQuotaHandle = TenantQuotaAccount*;

template <size_t NumShards>
class ShardedTenantQuotaTable;

// Control-plane registry for stable quota accounts. Callers must provide
// external synchronization. Charge and release bypass this table and operate
// directly on TenantQuotaHandle.
class TenantQuotaShard {
   public:
    TenantQuotaResult UpsertTenantPolicy(const TenantId& tenant_id,
                                         uint64_t requested_quota_bytes);
    TenantQuotaPolicyResult DisableTenantPolicyIfEmpty(
        const TenantId& tenant_id);
    TenantQuotaResult ApplyTenantPolicies(const TenantQuotaPolicyMap& policies);
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
    TenantQuotaResult RebuildUsage(const TenantQuotaUsageMap& usage);

   private:
    template <size_t>
    friend class ShardedTenantQuotaTable;

    using AccountMap = std::map<TenantId, std::unique_ptr<TenantQuotaAccount>>;

    TenantQuotaAccount& GetOrCreateAccount(const TenantId& tenant_id);
    TenantQuotaSnapshot MakeSnapshot(const TenantId& tenant_id,
                                     const TenantQuotaAccount& account) const;
    static std::map<TenantId, uint64_t> BuildEffectiveQuotaAssignments(
        const std::vector<TenantQuotaSnapshot>& tenants,
        uint64_t allocatable_capacity_bytes);
    void ApplyEffectiveQuotas(
        const std::map<TenantId, uint64_t>& effective_quotas);

    AccountMap accounts_;
};

}  // namespace mooncake
