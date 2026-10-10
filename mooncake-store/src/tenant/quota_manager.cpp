#include "tenant/quota_manager.h"

#include <glog/logging.h>

#include <cassert>
#include <stdexcept>
#include <utility>

namespace mooncake {

TenantQuotaManager::TenantQuotaManager(CapacityFn allocatable_capacity)
    : allocatable_capacity_(std::move(allocatable_capacity)) {}

void TenantQuotaManager::OpenPolicyStore(const std::string& type,
                                         const std::string& uri,
                                         const std::string& cluster_id) {
    auto store = CreateTenantQuotaPolicyStore(type, uri, cluster_id);
    if (!store) {
        throw std::invalid_argument(store.error());
    }
    policy_store_ = std::move(store.value());
}

void TenantQuotaManager::LoadPoliciesOrThrow() {
    if (!policy_store_) {
        throw std::runtime_error(
            "tenant quota policy store is not initialized");
    }
    std::lock_guard<std::mutex> policy_lock(policy_mutex_);
    auto snapshot = policy_store_->Load();
    if (!snapshot) {
        throw std::runtime_error("failed to load tenant quota policy: " +
                                 snapshot.error());
    }
    ApplyPolicies(snapshot.value());
}

TenantQuotaAccount& TenantQuotaManager::AccountFor(const TenantId& tenant_id) {
    return *table_.GetOrCreateTenantHandle(tenant_id);
}

bool TenantQuotaManager::IsTenantRegistered(const TenantId& tenant_id) const {
    return table_.IsTenantRegistered(tenant_id);
}

std::vector<TenantQuotaSnapshot> TenantQuotaManager::ListSnapshots() const {
    return table_.ListTenantSnapshots();
}

std::optional<TenantQuotaSnapshot> TenantQuotaManager::GetSnapshot(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    return table_.GetTenantSnapshot(tenant_id);
}

tl::expected<TenantQuotaSnapshot, ErrorCode> TenantQuotaManager::UpsertPolicy(
    const TenantId& tenant_id, uint64_t requested_quota_bytes) {
    assert(tenant_id.IsValid());
    if (requested_quota_bytes == 0 ||
        requested_quota_bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> policy_lock(policy_mutex_);
    auto policy = BuildPolicySnapshot();
    policy.tenant_quotas[tenant_id.value()] = requested_quota_bytes;
    auto save_result = policy_store_->Save(policy);
    if (!save_result) {
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyPolicies(policy);
    auto result_snapshot = GetSnapshot(tenant_id);
    if (!result_snapshot.has_value()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return result_snapshot.value();
}

tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
TenantQuotaManager::DeletePolicy(
    const TenantId& tenant_id,
    const std::function<bool(const TenantId&)>& tenant_has_objects) {
    assert(tenant_id.IsValid());
    std::lock_guard<std::mutex> policy_lock(policy_mutex_);
    auto policy = BuildPolicySnapshot();
    auto policy_it = policy.tenant_quotas.find(tenant_id.value());
    if (policy_it == policy.tenant_quotas.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const uint64_t requested_quota_bytes = policy_it->second;

    auto restore_policy = [&] {
        std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
        auto result = table_.UpsertTenantPolicy(
            tenant_id, requested_quota_bytes, allocatable_capacity_());
        if (!result) {
            LOG(ERROR) << "failed to restore tenant quota policy tenant="
                       << tenant_id.value();
        }
    };

    auto disable_result = table_.DisableTenantPolicyIfEmpty(tenant_id);
    if (!disable_result) {
        return tl::make_unexpected(disable_result.error() ==
                                           TenantQuotaError::kTenantNotEmpty
                                       ? ErrorCode::TENANT_NOT_EMPTY
                                       : ErrorCode::OBJECT_NOT_FOUND);
    }

    if (tenant_has_objects(tenant_id)) {
        restore_policy();
        return tl::make_unexpected(ErrorCode::TENANT_NOT_EMPTY);
    }

    policy.tenant_quotas.erase(policy_it);
    auto save_result = policy_store_->Save(policy);
    if (!save_result) {
        restore_policy();
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyPolicies(policy);
    return GetSnapshot(tenant_id);
}

void TenantQuotaManager::Recompute() {
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    table_.RecomputeEffectiveQuotas(allocatable_capacity_());
}

void TenantQuotaManager::RebuildUsageOrThrow(const TenantQuotaUsageMap& usage) {
    for (const auto& [tenant_id, _] : usage) {
        if (!table_.IsTenantRegistered(tenant_id)) {
            LOG(WARNING)
                << "tenant " << tenant_id.value()
                << " exists in metadata but has no connector quota policy; "
                   "creating orphan quota state";
        }
    }
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    auto rebuild_result = table_.RebuildUsage(usage, allocatable_capacity_());
    if (!rebuild_result) {
        throw std::runtime_error("failed to rebuild tenant quota usage");
    }
}

TenantQuotaPolicySnapshot TenantQuotaManager::BuildPolicySnapshot() const {
    TenantQuotaPolicySnapshot snapshot;
    for (const auto& [tenant_id, requested_quota_bytes] :
         table_.GetTenantPolicies()) {
        snapshot.tenant_quotas.emplace(tenant_id.value(),
                                       requested_quota_bytes);
    }
    return snapshot;
}

void TenantQuotaManager::ApplyPolicies(
    const TenantQuotaPolicySnapshot& snapshot) {
    TenantQuotaPolicyMap policies;
    for (const auto& [tenant_id, requested_quota_bytes] :
         snapshot.tenant_quotas) {
        policies.emplace(TenantId(tenant_id), requested_quota_bytes);
    }
    std::lock_guard<std::mutex> recompute_lock(recompute_mutex_);
    auto result = table_.ApplyTenantPolicies(policies, allocatable_capacity_());
    if (!result) {
        throw std::invalid_argument(
            "tenant quota policy exceeds atomic accounting range");
    }
}

}  // namespace mooncake
