#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

struct TenantQuotaState {
    uint64_t requested_quota_bytes = 0;
    // Atomic fields for lock-free reads in the quota fast-path.
    // All stores use memory_order_relaxed; the shard mutex provides
    // ordering guarantees for write-side consistency.
    std::atomic<uint64_t> effective_quota_bytes{0};
    std::atomic<uint64_t> used_bytes{0};
    std::atomic<uint64_t> reserved_bytes{0};
    uint64_t committed_count = 0;
    uint64_t metadata_object_count = 0;
    bool has_explicit_policy = false;
    bool over_quota = false;

    // Explicit copy/move since std::atomic deletes implicit ones.
    TenantQuotaState() = default;
    TenantQuotaState(const TenantQuotaState& o)
        : requested_quota_bytes(o.requested_quota_bytes),
          effective_quota_bytes(
              o.effective_quota_bytes.load(std::memory_order_relaxed)),
          used_bytes(o.used_bytes.load(std::memory_order_relaxed)),
          reserved_bytes(o.reserved_bytes.load(std::memory_order_relaxed)),
          committed_count(o.committed_count),
          metadata_object_count(o.metadata_object_count),
          has_explicit_policy(o.has_explicit_policy),
          over_quota(o.over_quota) {}
    TenantQuotaState& operator=(const TenantQuotaState& o) {
        requested_quota_bytes = o.requested_quota_bytes;
        effective_quota_bytes.store(
            o.effective_quota_bytes.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
        used_bytes.store(o.used_bytes.load(std::memory_order_relaxed),
                         std::memory_order_relaxed);
        reserved_bytes.store(o.reserved_bytes.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
        committed_count = o.committed_count;
        metadata_object_count = o.metadata_object_count;
        has_explicit_policy = o.has_explicit_policy;
        over_quota = o.over_quota;
        return *this;
    }
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
    std::string tenant_id;
    uint64_t effective_quota_bytes = 0;
};

enum class TenantQuotaError {
    kQuotaExceeded,
    kInvalidArgument,
    kAccountingMismatch,
};

using TenantQuotaResult = tl::expected<void, TenantQuotaError>;

std::vector<TenantQuotaAssignment> BuildEffectiveQuotaAssignments(
    const std::map<std::string, TenantQuotaState>& tenants,
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
    TenantQuotaState& GetOrCreateState(const std::string& tenant_id);
    TenantQuotaSnapshot MakeSnapshot(const std::string& tenant_id,
                                     const TenantQuotaState& state) const;
    void RefreshOverQuota(TenantQuotaState* state) const;

    std::map<std::string, TenantQuotaState> tenants_;
};

}  // namespace mooncake
