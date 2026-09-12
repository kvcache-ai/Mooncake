#include "master_service.h"

namespace mooncake {

tl::expected<BeginWeightImportResponse, ErrorCode>
MasterService::BeginWeightImport(const BeginWeightImportRequest& request) {
    auto metadata = weight_metadata_store_.BeginImport(request);
    if (!metadata) {
        return tl::make_unexpected(metadata.error());
    }
    return BeginWeightImportResponse{std::move(*metadata)};
}

tl::expected<CommitWeightImportResponse, ErrorCode>
MasterService::CommitWeightImport(const CommitWeightImportRequest& request) {
    if (!request.identity.IsValid() || request.manifest_key.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // PR1: require payload objects and the manifest key to exist before
    // publishing READY+HOT. Full group-type validation lands in later PRs.
    const TenantId tenant_id(request.identity.tenant_id);
    auto manifest_exists = ExistKey(request.manifest_key, tenant_id);
    if (!manifest_exists) {
        return tl::make_unexpected(manifest_exists.error());
    }
    if (!*manifest_exists) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    for (const auto& payload_key : request.payload_keys) {
        auto exists = ExistKey(payload_key, tenant_id);
        if (!exists) {
            return tl::make_unexpected(exists.error());
        }
        if (!*exists) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
    }

    auto metadata = weight_metadata_store_.CommitImport(request);
    if (!metadata) {
        return tl::make_unexpected(metadata.error());
    }
    return CommitWeightImportResponse{std::move(*metadata)};
}

tl::expected<GetWeightMetadataResponse, ErrorCode>
MasterService::GetWeightMetadata(
    const GetWeightMetadataRequest& request) const {
    auto metadata = weight_metadata_store_.Get(request.identity);
    if (!metadata) {
        return tl::make_unexpected(metadata.error());
    }
    return GetWeightMetadataResponse{std::move(*metadata)};
}

ListWeightRevisionsResponse MasterService::ListWeightRevisions(
    const ListWeightRevisionsRequest& request) const {
    return weight_metadata_store_.List(request);
}

tl::expected<UpdateWeightPolicyResponse, ErrorCode>
MasterService::UpdateWeightPolicy(const UpdateWeightPolicyRequest& request) {
    auto metadata = weight_metadata_store_.UpdatePolicy(request);
    if (!metadata) {
        return tl::make_unexpected(metadata.error());
    }
    return UpdateWeightPolicyResponse{std::move(*metadata)};
}

namespace {

std::vector<std::string> CollectSuccessfullyRemovedKeys(
    const std::vector<std::string>& keys,
    const std::vector<tl::expected<void, ErrorCode>>& results) {
    std::vector<std::string> removed;
    removed.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        // OBJECT_NOT_FOUND is fine during cleanup of a partial transfer.
        if (results[i] || results[i].error() == ErrorCode::OBJECT_NOT_FOUND) {
            removed.push_back(keys[i]);
        }
    }
    return removed;
}

}  // namespace

tl::expected<AbortWeightImportResponse, ErrorCode>
MasterService::AbortWeightImport(const AbortWeightImportRequest& request) {
    auto result = weight_metadata_store_.AbortImport(request);
    if (!result) {
        return tl::make_unexpected(result.error());
    }

    auto& [metadata, keys] = *result;
    AbortWeightImportResponse response;
    response.metadata = std::move(metadata);
    if (!keys.empty()) {
        const TenantId tenant_id(request.identity.tenant_id);
        auto remove_results = BatchRemove(keys, tenant_id, /*force=*/true);
        response.removed_keys =
            CollectSuccessfullyRemovedKeys(keys, remove_results);
    }
    return response;
}

tl::expected<RemoveWeightRevisionResponse, ErrorCode>
MasterService::RemoveWeightRevision(
    const RemoveWeightRevisionRequest& request) {
    auto result = weight_metadata_store_.RemoveRevision(request);
    if (!result) {
        return tl::make_unexpected(result.error());
    }

    auto& [metadata, keys] = *result;
    RemoveWeightRevisionResponse response;
    response.metadata = std::move(metadata);
    if (!keys.empty()) {
        const TenantId tenant_id(request.identity.tenant_id);
        auto remove_results = BatchRemove(keys, tenant_id, /*force=*/true);
        response.removed_keys =
            CollectSuccessfullyRemovedKeys(keys, remove_results);
    }
    return response;
}

}  // namespace mooncake
