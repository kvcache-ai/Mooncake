#include "weight_store_backend.h"

#include <algorithm>

#include "master_service.h"

namespace mooncake {

bool MasterStoreBackend::IsTenantSupported(const std::string& tenant_id) const {
    const TenantId tenant(tenant_id);
    return tenant.IsValid() && master_.ResolveRequestTenantId(tenant) == tenant;
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
        });
    }
    return members;
}

}  // namespace mooncake
