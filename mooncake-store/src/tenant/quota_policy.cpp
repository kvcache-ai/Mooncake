#include "tenant/quota_policy.h"

#include <algorithm>
#include <cassert>
#include <mutex>
#include <stdexcept>
#include <utility>

#include <glog/logging.h>

#include "master_metric_manager.h"

namespace mooncake {
namespace {

// The tenant's account, hung on its namespace when the namespace was created.
// The quota table keeps one stable account per tenant id, so the reference
// outlives every namespace built for that id.
struct TenantAccount final : PolicyAttachment {
    explicit TenantAccount(TenantQuotaAccount& account) : account(account) {}
    TenantQuotaAccount& account;
};

// A write admitted while the policy lock was held, so a concurrent policy
// delete cannot land between the admission check and the write.
struct PolicyLockToken final : AdmissionToken {
    explicit PolicyLockToken(std::unique_lock<std::mutex> lock)
        : lock(std::move(lock)) {}
    std::unique_lock<std::mutex> lock;
};

ErrorCode ToErrorCode(TenantQuotaError error) {
    switch (error) {
        case TenantQuotaError::kTenantNotRegistered:
            return ErrorCode::TENANT_NOT_REGISTERED;
        case TenantQuotaError::kQuotaExceeded:
            return ErrorCode::TENANT_QUOTA_EXCEEDED;
        case TenantQuotaError::kInvalidArgument:
            return ErrorCode::INVALID_PARAMS;
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}

bool HasProcessingMemoryReplica(const ObjectMetadata& metadata) {
    return metadata.HasReplica([](const Replica& replica) {
        return replica.is_memory_replica() && replica.is_processing();
    });
}

// The key's policy words: what the account holds for the key, and what the
// object an upsert replaced under it still owes until the new write settles.
uint64_t& Held(const PolicyContext& ctx) {
    return ctx.guard.owner_state().policy_words[0];
}
uint64_t& Carried(const PolicyContext& ctx) {
    return ctx.guard.owner_state().policy_words[1];
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    return rhs > TenantQuotaAccount::kMaxChargedBytes - lhs
               ? TenantQuotaAccount::kMaxChargedBytes
               : lhs + rhs;
}

}  // namespace

TenantQuotaPolicy::TenantQuotaPolicy(StoreControl& store, Options options)
    : store_(store),
      options_(options),
      manager_([this] { return store_.AllocatableMemoryBytes(); }) {
    // The master validates these ranges in its config; within them the
    // eviction target stays below the watermark, so OnMaintenance never
    // computes a negative byte count.
    assert(options_.eviction_high_watermark_ratio >= 0.0 &&
           options_.eviction_high_watermark_ratio <= 1.0);
    assert(options_.eviction_ratio >= 0.0 && options_.eviction_ratio <= 1.0);
    assert(options_.eviction_check_interval.count() >= 0);
}

void TenantQuotaPolicy::OpenPolicyStoreOrThrow(const std::string& type,
                                               const std::string& uri,
                                               const std::string& cluster_id) {
    manager_.OpenPolicyStore(type, uri, cluster_id);
    manager_.LoadPoliciesOrThrow();
}

bool TenantQuotaPolicy::IsTenantRegistered(const TenantId& tenant_id) const {
    return manager_.IsTenantRegistered(tenant_id);
}

std::vector<TenantQuotaSnapshot> TenantQuotaPolicy::ListSnapshots() const {
    return manager_.ListSnapshots();
}

std::optional<TenantQuotaSnapshot> TenantQuotaPolicy::GetSnapshot(
    const TenantId& tenant_id) const {
    return manager_.GetSnapshot(tenant_id);
}

tl::expected<TenantQuotaSnapshot, ErrorCode> TenantQuotaPolicy::UpsertPolicy(
    const TenantId& tenant_id, uint64_t requested_quota_bytes) {
    return manager_.UpsertPolicy(tenant_id, requested_quota_bytes);
}

tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
TenantQuotaPolicy::DeletePolicy(const TenantId& tenant_id) {
    return manager_.DeletePolicy(tenant_id, [this](const TenantId& id) {
        return store_.ObjectCount(id) > 0;
    });
}

uint64_t TenantQuotaPolicy::AllocatableCapacityBytes() const {
    return store_.AllocatableMemoryBytes();
}

uint64_t TenantQuotaPolicy::MemoryCharge(const ObjectMetadata& metadata) {
    // A replica being written already holds the memory it was allocated, and
    // a removed one keeps its memory until the removal is durable and the
    // replica is dropped, so both are charged.
    const auto holding_replicas =
        metadata.CountReplicas([](const Replica& replica) {
            if (!replica.is_memory_replica()) {
                return false;
            }
            switch (replica.status()) {
                case ReplicaStatus::INITIALIZED:
                case ReplicaStatus::PROCESSING:
                case ReplicaStatus::COMPLETE:
                case ReplicaStatus::REMOVED:
                    return true;
                default:
                    return false;
            }
        });
    const unsigned __int128 charge =
        static_cast<unsigned __int128>(metadata.TotalLength()) *
        holding_replicas;
    return charge > TenantQuotaAccount::kMaxChargedBytes
               ? TenantQuotaAccount::kMaxChargedBytes
               : static_cast<uint64_t>(charge);
}

TenantQuotaAccount& TenantQuotaPolicy::AccountOf(const PolicyContext& ctx) {
    // OnNamespaceCreated hangs an account on every namespace.
    assert(ctx.attachment != nullptr);
    return static_cast<TenantAccount*>(ctx.attachment)->account;
}

std::unique_ptr<PolicyAttachment> TenantQuotaPolicy::OnNamespaceCreated(
    const TenantId& tenant_id) {
    return std::make_unique<TenantAccount>(manager_.AccountFor(tenant_id));
}

tl::expected<std::unique_ptr<AdmissionToken>, ErrorCode>
TenantQuotaPolicy::AdmitWrite(const TenantId& tenant_id) {
    auto lock = manager_.LockPolicy();
    if (!manager_.IsTenantRegistered(tenant_id)) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    return std::make_unique<PolicyLockToken>(std::move(lock));
}

tl::expected<void, ErrorCode> TenantQuotaPolicy::OnGrow(
    const PolicyContext& ctx, uint64_t bytes) {
    auto charged = AccountOf(ctx).TryCharge(bytes);
    if (!charged) {
        const ErrorCode error = ToErrorCode(charged.error().error);
        if (error == ErrorCode::TENANT_QUOTA_EXCEEDED) {
            MasterMetricManager::instance().inc_tenant_quota_reject(
                ctx.tenant_id.value(), "quota_exceeded");
        }
        return tl::make_unexpected(error);
    }
    // The account now holds these bytes for the key; the next release settles
    // them against what the object turns out to owe.
    uint64_t& held = Held(ctx);
    held = SaturatingAdd(held, bytes);
    return {};
}

void TenantQuotaPolicy::OnReplace(const PolicyContext& ctx) {
    // The replaced object's charge, and whatever it still carried from an
    // earlier replacement, is owed until the replacement's write settles.
    Carried(ctx) =
        SaturatingAdd(Carried(ctx), MemoryCharge(ctx.guard.metadata()));
}

void TenantQuotaPolicy::OnTearDown(const PolicyContext& ctx) {
    // The object goes for good, and with it any charge it carried for a
    // replaced one.
    Carried(ctx) = 0;
}

void TenantQuotaPolicy::OnWriteRelease(const PolicyContext& ctx) {
    TenantQuotaAccount& account = AccountOf(ctx);
    uint64_t& held = Held(ctx);
    uint64_t& carried = Carried(ctx);
    uint64_t owed = 0;
    if (ctx.guard.has_object()) {
        const ObjectMetadata& metadata = ctx.guard.metadata();
        if (!HasProcessingMemoryReplica(metadata)) {
            carried = 0;
        }
        owed = SaturatingAdd(MemoryCharge(metadata), carried);
    } else {
        carried = 0;
    }
    if (owed > held) {
        // Growth the core did not ask for up front, such as a write that
        // completed more than it reserved: charged regardless of the quota,
        // since the memory is already in use.
        account.ChargeUnchecked(owed - held);
    } else if (owed < held) {
        if (!account.Release(held - owed)) {
            LOG(ERROR) << "tenant quota release mismatch, tenant="
                       << ctx.tenant_id << ", key=" << ctx.guard.key()
                       << ", bytes=" << held - owed;
        }
    }
    held = owed;
}

void TenantQuotaPolicy::OnRestored() {
    // No write is in flight, so every key's words and every account can be set
    // to what the restored objects owe.
    TenantQuotaUsageMap usage;
    store_.VisitNamespaces([&](const TenantId& tenant_id,
                               route::ObjectRoute& route) {
        uint64_t charged_bytes = 0;
        for (auto object : route.WriteCursor()) {
            const uint64_t charge = MemoryCharge(object.metadata());
            if (charged_bytes > TenantQuotaAccount::kMaxChargedBytes - charge) {
                throw std::overflow_error(
                    "rebuilt tenant quota exceeds 2^63 - 1 bytes");
            }
            charged_bytes += charge;
            object.owner_state().policy_words = {charge, 0};
        }
        usage.emplace(tenant_id, charged_bytes);
    });
    manager_.RebuildUsageOrThrow(usage);
}

void TenantQuotaPolicy::OnCapacityChanged() { manager_.Recompute(); }

void TenantQuotaPolicy::OnMaintenance() {
    if (options_.eviction_high_watermark_ratio <= 0.0) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now < next_eviction_check_) {
        return;
    }
    next_eviction_check_ = now + options_.eviction_check_interval;

    // Evict down to (watermark - eviction_ratio) of the tenant's quota,
    // mirroring what the pool-wide eviction does: free a slab so subsequent
    // admissions find room already available, rather than freeing exactly the
    // deficit of the object currently being admitted.
    const double watermark = options_.eviction_high_watermark_ratio;
    const double target_ratio =
        std::max(0.0, watermark - options_.eviction_ratio);

    for (const auto& snapshot : manager_.ListSnapshots()) {
        if (snapshot.effective_quota_bytes == 0) {
            continue;
        }
        const double used_ratio =
            static_cast<double>(snapshot.charged_bytes) /
            static_cast<double>(snapshot.effective_quota_bytes);
        if (used_ratio <= watermark) {
            continue;
        }
        const double excess_ratio = used_ratio - target_ratio;
        const auto target_bytes = static_cast<uint64_t>(
            excess_ratio * static_cast<double>(snapshot.effective_quota_bytes));
        if (target_bytes == 0) {
            continue;
        }

        // VLOG rather than INFO: this pass runs once per second and emits a
        // line per over-watermark tenant, so sustained quota pressure in a
        // large multi-tenant deployment would flood the log.
        VLOG(1) << "[TENANT-EVICT-TRIGGER] tenant=" << snapshot.tenant_id
                << " used_ratio=" << used_ratio
                << " high_watermark=" << watermark
                << " target_ratio=" << target_ratio
                << " target_bytes=" << target_bytes;
        const auto result =
            store_.EvictNamespaceMemory(snapshot.tenant_id, target_bytes);
        VLOG(1) << "[TENANT-EVICT-DONE] tenant=" << snapshot.tenant_id
                << " freed_bytes=" << result.freed_bytes
                << " evicted_objects=" << result.evicted_objects;
    }
}

}  // namespace mooncake
