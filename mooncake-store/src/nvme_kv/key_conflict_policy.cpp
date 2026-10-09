#include "nvme_kv/key_conflict_policy.h"

#include <cstring>

namespace mooncake {

bool NvmeKvKeyConflictPolicy::ValidateResolvedRootPlacement(
    const NvmeKvObjectIdentity& identity,
    const NvmeKvStoredIdentityView& stored_view,
    const NvmeKvPhysicalKey& observed_physical_key) {
    return stored_view.resolved_physical_key == observed_physical_key &&
           EncodeNvmeKvPhysicalKey(identity, stored_view.resolved_slot) ==
               stored_view.resolved_physical_key;
}

tl::expected<NvmeKvKeyConflictPolicy::ExistingObjectDecision, ErrorCode>
NvmeKvKeyConflictPolicy::ResolveExistingObject(NvmeKvConnector& connector,
                                               const PhysicalKey& physical_key,
                                               std::string_view expected_blob) {
    auto existing_value_res = connector.Retrieve(physical_key);
    if (!existing_value_res) {
        if (existing_value_res.error() == ErrorCode::OBJECT_NOT_FOUND) {
            return ExistingObjectDecision::kNotFound;
        }
        return tl::make_unexpected(existing_value_res.error());
    }
    const auto& existing = existing_value_res.value();
    if (existing.size() == expected_blob.size() &&
        std::memcmp(existing.data(), expected_blob.data(),
                    expected_blob.size()) == 0) {
        return ExistingObjectDecision::kSameObject;
    }
    return ExistingObjectDecision::kDifferentObject;
}

}  // namespace mooncake
