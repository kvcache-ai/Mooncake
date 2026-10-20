#pragma once

// TenantQuotaPolicy: multi-tenancy as a NamespacePolicy. Every namespace is a
// tenant with a quota account; the policy admits writes only for registered
// tenants, charges an object's memory footprint to its tenant's account, and
// sheds the memory of a tenant over its own watermark.
//
// What an object costs is derived from the object itself, never booked by the
// code that changes it. The key keeps, in its policy words, the bytes the
// account currently holds for it and what a replaced object still owes, and
// every write lock released on the key settles the first towards what the
// object owes now:
//
//   - nothing, once the key holds no object;
//   - otherwise its size for every memory replica that holds memory: being
//     written, completed, or removed and waiting for the removal to be
//     durable (MemoryCharge);
//   - plus, while a memory replica is still being written, what the object an
//     upsert replaced under this key charged when it was replaced, so the old
//     charge stays until the new write settles.
//
// Growth is the one thing the core asks for up front (OnGrow), so a write the
// account cannot take fails before anything is allocated.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "metadata/namespace_policy.h"
#include "object_metadata.h"
#include "tenant_id.h"
#include "tenant/quota.h"
#include "tenant/quota_manager.h"
#include "types.h"

namespace mooncake {

class TenantQuotaPolicy final : public NamespacePolicy {
   public:
    // Internal settings the master passes after validating its config; both
    // ratios are in [0, 1].
    struct Options {
        // A tenant whose charge exceeds this share of its quota is evicted
        // down to (watermark - eviction_ratio); zero turns it off.
        double eviction_high_watermark_ratio = 0.0;
        double eviction_ratio = 0.0;
        // How often the watermark is checked, at most.
        std::chrono::milliseconds eviction_check_interval{1000};
    };

    // `store` outlives the policy; the allocatable capacity every quota is
    // carved from is read from it.
    TenantQuotaPolicy(StoreControl& store, Options options);

    // Opens the connector the policies persist to and loads them. Throws when
    // the store cannot be opened or read, or holds a policy out of range.
    void OpenPolicyStoreOrThrow(const std::string& type, const std::string& uri,
                                const std::string& cluster_id);

    // --- Control plane ------------------------------------------------------

    // Whether writes to the tenant are admitted. A point-in-time answer: a
    // write admitted through AdmitWrite holds it for its whole request.
    bool IsTenantRegistered(const TenantId& tenant_id) const;
    std::vector<TenantQuotaSnapshot> ListSnapshots() const;
    std::optional<TenantQuotaSnapshot> GetSnapshot(
        const TenantId& tenant_id) const;
    tl::expected<TenantQuotaSnapshot, ErrorCode> UpsertPolicy(
        const TenantId& tenant_id, uint64_t requested_quota_bytes);
    // TENANT_NOT_EMPTY while the tenant still charges anything or holds an
    // object.
    tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode> DeletePolicy(
        const TenantId& tenant_id);
    uint64_t AllocatableCapacityBytes() const;

    // What an object's memory costs: its size for every memory replica that
    // holds memory (allocated, being written, completed, or removed but not
    // yet dropped), saturating at kMaxChargedBytes.
    static uint64_t MemoryCharge(const ObjectMetadata& metadata);

    // --- NamespacePolicy ----------------------------------------------------

    std::unique_ptr<PolicyAttachment> OnNamespaceCreated(
        const TenantId& tenant_id) override;
    tl::expected<std::unique_ptr<AdmissionToken>, ErrorCode> AdmitWrite(
        const TenantId& tenant_id) override;
    tl::expected<void, ErrorCode> OnGrow(const PolicyContext& ctx,
                                         uint64_t bytes) override;
    void OnReplace(const PolicyContext& ctx) override;
    void OnTearDown(const PolicyContext& ctx) override;
    void OnWriteRelease(const PolicyContext& ctx) override;
    void OnRestored() override;
    void OnCapacityChanged() override;
    void OnMaintenance() override;

   private:
    static TenantQuotaAccount& AccountOf(const PolicyContext& ctx);

    StoreControl& store_;
    const Options options_;
    TenantQuotaManager manager_;
    // Touched only by OnMaintenance, which the eviction thread alone calls.
    std::chrono::steady_clock::time_point next_eviction_check_{};
};

}  // namespace mooncake
