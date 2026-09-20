#include "weight_store_backend.h"

#include <algorithm>

#include "master_service.h"

namespace mooncake {

bool MasterStoreBackend::CanPublishWeightMutations() const {
    return master_.weight_management_mutations_enabled_;
}

PromotionQueueResult MasterStoreBackend::PromoteWeightObject(
    const TenantId& tenant_id, const std::string& key) {
    return master_.TryPushPromotionQueue(
        master_.MakeObjectIdentity(key, tenant_id), false, true);
}

std::vector<std::string> MasterStoreBackend::GetGroupMemberKeys(
    const TenantId& tenant_id, const std::string& group_id) const {
    return master_.GetGroupMemberKeys(tenant_id, group_id);
}

tl::expected<void, ErrorCode> MasterStoreBackend::RemoveObject(
    const std::string& key, const TenantId& tenant_id, bool force,
    bool allow_managed_weight) {
    return master_.RemoveObject(key, tenant_id, force, allow_managed_weight);
}

void MasterStoreBackend::EvictManagedWeightGroupToCold(
    const WeightRevisionMetadata& revision) {
    master_.EvictManagedWeightGroupToCold(revision);
}

bool MasterStoreBackend::IsOpLogEnabled() const {
    return master_.enable_oplog_;
}

bool MasterStoreBackend::IsTenantSupported(const std::string& tenant_id) const {
    const TenantId tenant(tenant_id);
    return tenant.IsValid() && master_.ResolveRequestTenantId(tenant) == tenant;
}

tl::expected<OpLogEntry, ErrorCode>
MasterStoreBackend::AppendOpLogWithDurableFinalize(OpType type,
                                                   const std::string& tenant_id,
                                                   const std::string& key,
                                                   const std::string& payload,
                                                   DurableFinalize finalize) {
    auto appended = master_.AppendOpLogWithDurableFinalize(
        type, tenant_id, key, payload,
        [finalize](const OpLogEntry& entry) { finalize(entry); });
    if (appended) {
        // Durability success precedes callback completion; only failures may
        // complete this operation through the durability waiter.
        [[maybe_unused]] auto failure_notification =
            master_.ordered_oplog_writer_->AwaitDurable(appended->sequence_id)
                .thenValue([finalize = std::move(finalize)](ErrorCode error) {
                    if (error != ErrorCode::OK) {
                        finalize(tl::make_unexpected(error));
                    }
                });
    }
    return appended;
}

WeightMetadataStore::Result<std::vector<WeightGroupMemberSnapshot>>
MasterStoreBackend::SnapshotWeightGroup(
    const WeightRevisionIdentity& identity,
    const std::string& payload_group_id) const {
    const TenantId tenant_id(identity.tenant_id);
    auto member_keys = master_.GetGroupMemberKeys(tenant_id, payload_group_id);
    if (member_keys.empty()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    std::sort(member_keys.begin(), member_keys.end());

    std::vector<WeightGroupMemberSnapshot> members;
    members.reserve(member_keys.size());
    for (const auto& key : member_keys) {
        MasterService::MetadataAccessorRO accessor(
            &master_, master_.MakeObjectIdentity(key, tenant_id));
        if (!accessor.Exists()) {
            return tl::make_unexpected(WeightManagementError::NOT_FOUND);
        }
        const auto& metadata = accessor.Get();
        if (metadata.group_id != payload_group_id) {
            return tl::make_unexpected(WeightManagementError::CONFLICT);
        }
        members.push_back(WeightGroupMemberSnapshot{
            .key = key,
            .size = metadata.size,
            .data_type = metadata.data_type,
            .readable = master_.HasReadableReplica(metadata),
            .has_memory = metadata.HasReplica([this](const Replica& replica) {
                return replica.is_memory_replica() &&
                       master_.IsReplicaReadable(replica);
            }),
            .has_cold = metadata.HasReplica([this](const Replica& replica) {
                return !replica.is_memory_replica() &&
                       master_.IsReplicaReadable(replica);
            }),
        });
    }
    return members;
}

}  // namespace mooncake
