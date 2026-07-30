#include "master_service.h"

#include <cassert>
#include <limits>
#include <stdexcept>

namespace mooncake {

bool MasterService::IsTenantQuotaEnabled() const {
    return enable_multi_tenants_;
}

std::vector<TenantQuotaSnapshot> MasterService::ListTenantQuotaSnapshots()
    const {
    return tenant_quota_table_.ListTenantSnapshots();
}

std::optional<TenantQuotaSnapshot> MasterService::GetTenantQuotaSnapshot(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    return tenant_quota_table_.GetTenantSnapshot(tenant_id);
}

tl::expected<TenantQuotaSnapshot, ErrorCode>
MasterService::UpsertTenantQuotaPolicy(const TenantId& tenant_id,
                                       uint64_t requested_quota_bytes) {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (requested_quota_bytes == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto policy = BuildTenantQuotaPolicySnapshot();
    policy.tenant_quotas[tenant_id.value()] = requested_quota_bytes;
    auto save_result = tenant_quota_policy_store_->Save(policy);
    if (!save_result) {
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyTenantQuotaPolicies(policy);
    auto result_snapshot = GetTenantQuotaSnapshot(tenant_id);
    if (!result_snapshot.has_value()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return result_snapshot.value();
}

tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
MasterService::DeleteTenantQuotaPolicy(const TenantId& tenant_id) {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto policy = BuildTenantQuotaPolicySnapshot();
    auto policy_it = policy.tenant_quotas.find(tenant_id.value());
    if (policy_it == policy.tenant_quotas.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const uint64_t requested_quota_bytes = policy_it->second;

    auto restore_policy = [&] {
        std::lock_guard<std::mutex> recompute_lock(
            tenant_quota_recompute_mutex_);
        const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
        auto result = tenant_quota_table_.UpsertTenantPolicy(
            tenant_id, requested_quota_bytes, capacity);
        if (!result) {
            LOG(ERROR) << "failed to restore tenant quota policy tenant="
                       << tenant_id.value();
        }
    };

    auto disable_result =
        tenant_quota_table_.DisableTenantPolicyIfEmpty(tenant_id);
    if (!disable_result) {
        return tl::make_unexpected(disable_result.error() ==
                                           TenantQuotaError::kTenantNotEmpty
                                       ? ErrorCode::TENANT_NOT_EMPTY
                                       : ErrorCode::OBJECT_NOT_FOUND);
    }

    auto post_mark_snapshot = GetTenantQuotaSnapshot(tenant_id);
    if (TenantHasObjects(tenant_id) ||
        (post_mark_snapshot.has_value() &&
         (post_mark_snapshot->used_bytes != 0 ||
          post_mark_snapshot->reserved_bytes != 0 ||
          post_mark_snapshot->committed_count != 0 ||
          post_mark_snapshot->metadata_object_count != 0))) {
        restore_policy();
        return tl::make_unexpected(ErrorCode::TENANT_NOT_EMPTY);
    }

    policy.tenant_quotas.erase(policy_it);
    auto save_result = tenant_quota_policy_store_->Save(policy);
    if (!save_result) {
        restore_policy();
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyTenantQuotaPolicies(policy);
    return GetTenantQuotaSnapshot(tenant_id);
}

bool MasterService::IsTenantRegistered(const TenantId& tenant_id) const {
    if (!enable_multi_tenants_) {
        return true;
    }
    return tenant_quota_table_.IsTenantRegistered(tenant_id);
}

tl::expected<TenantId, ErrorCode> MasterService::ResolveTenantIdForWrite(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    return ResolveTenantIdForWriteLocked(tenant_id);
}

tl::expected<TenantId, ErrorCode> MasterService::ResolveTenantIdForWriteLocked(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    if (!IsTenantRegistered(tenant_id)) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    return tenant_id;
}

bool MasterService::TenantHasObjects(const TenantId& tenant_id) const {
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRO shard(this, i);
        auto tenant_it = shard->tenants.find(tenant_id);
        if (tenant_it != shard->tenants.end() &&
            !tenant_it->second.metadata.empty()) {
            return true;
        }
    }
    return false;
}

TenantQuotaPolicySnapshot MasterService::BuildTenantQuotaPolicySnapshot()
    const {
    TenantQuotaPolicySnapshot snapshot;
    for (const auto& [tenant_id, requested_quota_bytes] :
         tenant_quota_table_.GetTenantPolicies()) {
        snapshot.tenant_quotas.emplace(tenant_id.value(),
                                       requested_quota_bytes);
    }
    return snapshot;
}

void MasterService::ApplyTenantQuotaPolicies(
    const TenantQuotaPolicySnapshot& snapshot) {
    TenantQuotaPolicyMap policies;
    for (const auto& [tenant_id, requested_quota_bytes] :
         snapshot.tenant_quotas) {
        policies.emplace(TenantId(tenant_id), requested_quota_bytes);
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    auto result = tenant_quota_table_.ApplyTenantPolicies(policies, capacity);
    if (!result) {
        throw std::invalid_argument(
            "tenant quota policy contains an invalid quota");
    }
}

void MasterService::LoadTenantQuotaPoliciesFromStoreOrThrow() {
    if (!enable_multi_tenants_) {
        return;
    }
    if (!tenant_quota_policy_store_) {
        throw std::runtime_error(
            "tenant quota policy store is not initialized");
    }
    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto snapshot = tenant_quota_policy_store_->Load();
    if (!snapshot) {
        throw std::runtime_error("failed to load tenant quota policy: " +
                                 snapshot.error());
    }
    ApplyTenantQuotaPolicies(snapshot.value());
}

uint64_t MasterService::GetTenantQuotaAllocatableCapacityBytes() {
    uint64_t capacity = 0;
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::pair<Segment, UUID>> segments;
    if (segment_access.GetAllSegments(segments) != ErrorCode::OK) {
        return 0;
    }
    for (const auto& [segment, _] : segments) {
        if (capacity > std::numeric_limits<uint64_t>::max() - segment.size) {
            return std::numeric_limits<uint64_t>::max();
        }
        capacity += segment.size;
    }
    return capacity;
}

void MasterService::RecomputeTenantEffectiveQuotas() {
    if (!enable_multi_tenants_) {
        return;
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    tenant_quota_table_.RecomputeEffectiveQuotas(capacity);
}

void MasterService::RebuildTenantQuotaUsageFromMetadata() {
    if (!enable_multi_tenants_) {
        return;
    }

    TenantQuotaUsageMap usage;
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRO shard(this, i);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            for (const auto& [_, metadata] : tenant_state.metadata) {
                auto& tenant_usage = usage[tenant_id];
                if (tenant_usage.metadata_object_count ==
                    std::numeric_limits<uint64_t>::max()) {
                    throw std::overflow_error(
                        "tenant metadata object count overflow during rebuild");
                }
                ++tenant_usage.metadata_object_count;
                const uint64_t charge = CompletedMemoryQuotaCharge(metadata);
                if (charge == 0) {
                    continue;
                }
                if (tenant_usage.used_bytes >
                    std::numeric_limits<uint64_t>::max() - charge) {
                    throw std::overflow_error(
                        "tenant quota usage overflow during rebuild");
                }
                tenant_usage.used_bytes += charge;
                if (tenant_usage.committed_count ==
                    std::numeric_limits<uint64_t>::max()) {
                    throw std::overflow_error(
                        "tenant committed count overflow during rebuild");
                }
                ++tenant_usage.committed_count;
            }
        }
    }

    for (const auto& [tenant_id, _] : usage) {
        if (!tenant_quota_table_.IsTenantRegistered(tenant_id)) {
            LOG(WARNING)
                << "tenant " << tenant_id.value()
                << " exists in metadata but has no connector quota policy; "
                   "creating orphan quota state";
        }
    }

    {
        std::lock_guard<std::mutex> recompute_lock(
            tenant_quota_recompute_mutex_);
        const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
        auto rebuild_result = tenant_quota_table_.RebuildUsage(usage, capacity);
        if (!rebuild_result) {
            throw std::runtime_error("failed to rebuild tenant quota usage");
        }
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard(this, i);
        for (auto& [_, tenant_state] : shard->tenants) {
            for (auto& [_, metadata] : tenant_state.metadata) {
                metadata.reserved_quota_charge_bytes = 0;
                metadata.committed_quota_charge_bytes =
                    CompletedMemoryQuotaCharge(metadata);
                metadata.pending_replaced_quota_charge_bytes = 0;
            }
        }
    }
}

}  // namespace mooncake
