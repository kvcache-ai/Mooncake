#pragma once

// Tenant quota control plane: the persisted per-tenant policies, the quota
// table holding one stable account per tenant, and the effective quotas the
// table derives from them and the cluster's allocatable capacity.
//
// The data plane never comes here: it charges and releases the account a
// tenant was bound to (TenantQuotaBinding, handed out by the tenant registry
// with the tenant), and each object keeps its share in its own ledger. This
// class decides how large every account is.
//
// A disabled manager meters nothing: every tenant counts as registered and no
// account is ever handed out.
//
// Lock order: the policy lock, then (outside it) the master's snapshot lock and
// an entry lock, then the recompute lock, then the table's shard locks. The
// capacity callback runs under the recompute lock and must take nothing above
// it.

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "tenant_id.h"
#include "tenant_quota.h"
#include "tenant_quota_policy_store.h"
#include "tenant_quota_sharded.h"
#include "types.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}  // namespace test

class TenantQuotaManager {
   public:
    // The bytes every tenant's effective quota is carved from.
    using CapacityFn = std::function<uint64_t()>;

    TenantQuotaManager(bool enabled, CapacityFn allocatable_capacity);

    TenantQuotaManager(const TenantQuotaManager&) = delete;
    TenantQuotaManager& operator=(const TenantQuotaManager&) = delete;

    bool Enabled() const { return enabled_; }

    // Opens the connector the policies persist to. Throws
    // std::invalid_argument when it cannot be opened; a no-op when disabled.
    void OpenPolicyStore(const std::string& type, const std::string& uri,
                         const std::string& cluster_id);
    // Replaces the table's policies with the persisted ones. Throws when the
    // store cannot be read or holds a policy out of range.
    void LoadPoliciesOrThrow();

    // The stable account of one tenant, created closed on first use. Only for
    // binding a metered tenant; a disabled manager hands out none.
    TenantQuotaAccount& AccountFor(const TenantId& tenant_id);

    // Whether writes to the tenant are admitted; always true when disabled.
    bool IsTenantRegistered(const TenantId& tenant_id) const;
    // Held across a tenant's admission check and the write it admits, so a
    // concurrent policy delete cannot land in between.
    [[nodiscard]] std::unique_lock<std::mutex> LockPolicy() const {
        return std::unique_lock<std::mutex>(policy_mutex_);
    }

    std::vector<TenantQuotaSnapshot> ListSnapshots() const;
    std::optional<TenantQuotaSnapshot> GetSnapshot(
        const TenantId& tenant_id) const;

    tl::expected<TenantQuotaSnapshot, ErrorCode> UpsertPolicy(
        const TenantId& tenant_id, uint64_t requested_quota_bytes);
    // TENANT_NOT_EMPTY while the tenant still charges anything or, by
    // `tenant_has_objects`, still holds an object.
    tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode> DeletePolicy(
        const TenantId& tenant_id,
        const std::function<bool(const TenantId&)>& tenant_has_objects);

    // Re-derives every effective quota from the current capacity, after the
    // capacity changed.
    void Recompute();
    // Overwrites every account's charged bytes with `usage`. Only while no
    // charge or release is in flight; throws when the table rejects it.
    void RebuildUsageOrThrow(const TenantQuotaUsageMap& usage);

   private:
    friend class test::MasterServiceTestPeer;

    TenantQuotaPolicySnapshot BuildPolicySnapshot() const;
    // Throws when a policy is out of the accounting range.
    void ApplyPolicies(const TenantQuotaPolicySnapshot& snapshot);

    const bool enabled_;
    const CapacityFn allocatable_capacity_;
    std::unique_ptr<TenantQuotaPolicyStore> policy_store_;
    mutable std::mutex policy_mutex_;
    // Serializes a capacity snapshot with the table update derived from it.
    mutable std::mutex recompute_mutex_;
    ShardedTenantQuotaTable<1024> table_;
};

}  // namespace mooncake
