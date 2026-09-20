#include "weight_store_manager.h"

#include <chrono>
#include <condition_variable>
#include <limits>
#include <memory>

#include <glog/logging.h>

namespace mooncake {

std::unique_lock<std::mutex> WeightStoreManager::LockGroup(
    const WeightRevisionIdentity& identity) {
    const auto key = TenantId(identity.tenant_id)
                         .MakeScopedKey(MakeWeightPayloadGroupId(identity));
    return std::unique_lock(
        group_locks_[std::hash<std::string>{}(key) % group_locks_.size()]);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
WeightStoreManager::BeginWeightImport(const BeginWeightImportRequest& request) {
    auto normalized = request;
    const auto canonical_group = MakeWeightPayloadGroupId(request.identity);
    if (!backend_.IsTenantSupported(request.identity.tenant_id) ||
        canonical_group.empty() ||
        (!request.payload_group_id.empty() &&
         request.payload_group_id != canonical_group)) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    normalized.payload_group_id = canonical_group;
    auto operation_lock = LockGroup(request.identity);
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto mutation = weight_metadata_.PrepareBeginImport(normalized, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    return PersistAndPublishWeightMutation(*mutation);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
WeightStoreManager::CommitWeightImport(
    const CommitWeightImportRequest& request) {
    const auto canonical_group = MakeWeightPayloadGroupId(request.identity);
    if (canonical_group.empty() ||
        request.manifest.payload_group_id != canonical_group) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    auto operation_lock = LockGroup(request.identity);
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());

    auto mutation = weight_metadata_.PrepareCommitImport(request, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    if (mutation->no_op) {
        return PersistAndPublishWeightMutation(*mutation);
    }
    auto validation = ValidateWeightGroupForCommit(request);
    if (!validation) {
        return tl::make_unexpected(validation.error());
    }

    return PersistAndPublishWeightMutation(*mutation);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
WeightStoreManager::AbortWeightImport(const AbortWeightImportRequest& request) {
    auto operation_lock = LockGroup(request.identity);
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto mutation = weight_metadata_.PrepareAbortImport(request, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    return PersistAndPublishWeightMutation(*mutation);
}

WeightMetadataStore::Result<WeightRevisionView>
WeightStoreManager::GetWeightRevision(
    const GetWeightRevisionRequest& request) const {
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    return weight_metadata_.Get(request.identity, now_ms);
}

WeightMetadataStore::Result<ListWeightRevisionsResponse>
WeightStoreManager::ListWeightRevisions(
    const ListWeightRevisionsRequest& request) const {
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    return weight_metadata_.List(request, now_ms);
}

WeightMetadataStore::Result<void>
WeightStoreManager::ValidateWeightGroupForCommit(
    const CommitWeightImportRequest& request) const {
    const auto expected_manifest_key = MakeWeightManifestKey(request.identity);
    if (request.manifest.manifest_key != expected_manifest_key) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }

    auto members = backend_.SnapshotWeightGroup(
        request.identity, request.manifest.payload_group_id);
    if (!members) {
        return tl::make_unexpected(members.error());
    }
    if (members->size() != request.manifest.payload_count + 1) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }

    bool found_manifest = false;
    uint64_t logical_bytes = 0;
    std::vector<std::string> payload_keys;
    payload_keys.reserve(request.manifest.payload_count);
    for (const auto& member : *members) {
        if (!member.readable) {
            return tl::make_unexpected(WeightManagementError::NOT_READY);
        }
        if (member.key == request.manifest.manifest_key) {
            if (found_manifest ||
                member.data_type != ObjectDataType::METADATA) {
                return tl::make_unexpected(WeightManagementError::CONFLICT);
            }
            found_manifest = true;
            continue;
        }
        if (member.data_type != ObjectDataType::WEIGHT ||
            member.size >
                std::numeric_limits<uint64_t>::max() - logical_bytes) {
            return tl::make_unexpected(WeightManagementError::CONFLICT);
        }
        logical_bytes += member.size;
        payload_keys.push_back(member.key);
    }
    if (!found_manifest) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (logical_bytes != request.manifest.logical_bytes ||
        payload_keys.size() != request.manifest.payload_count ||
        ComputeWeightPayloadKeysSha256(payload_keys) !=
            request.manifest.payload_keys_sha256) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    return {};
}

WeightMetadataStore::Result<WeightRevisionMetadata>
WeightStoreManager::PersistAndPublishWeightMutation(
    const WeightMetadataMutation& mutation) {
    if (!mutation.no_op && !backend_.CanPublishWeightMutations()) {
        return tl::make_unexpected(WeightManagementError::DURABILITY_FAILED);
    }
    if (mutation.no_op || !backend_.IsOpLogEnabled()) {
        return weight_metadata_.Publish(mutation);
    }

    OpType type;
    std::string payload;
    if (mutation.kind == WeightMetadataMutationKind::UPSERT &&
        mutation.next.has_value()) {
        type = OpType::WEIGHT_METADATA_UPSERT;
        const auto encoded = struct_pack::serialize(*mutation.next);
        payload.assign(encoded.begin(), encoded.end());
    } else if (mutation.previous.has_value()) {
        type = OpType::WEIGHT_METADATA_DELETE;
        WeightMetadataDeleteOp deletion{
            .identity = mutation.identity,
            .metadata_generation = mutation.previous->metadata_generation,
        };
        const auto encoded = struct_pack::serialize(deletion);
        payload.assign(encoded.begin(), encoded.end());
    } else {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    struct Completion {
        std::mutex mutex;
        std::condition_variable cv;
        std::optional<WeightMetadataStore::Result<WeightRevisionMetadata>>
            result;
    };
    auto completion = std::make_shared<Completion>();
    auto persisted = backend_.AppendOpLogWithDurableFinalize(
        type, mutation.identity.tenant_id,
        MakeWeightRevisionMetadataKey(mutation.identity), payload,
        [this, mutation,
         completion](const WeightStoreBackend::DurableResult& durable_entry) {
            std::lock_guard lock(completion->mutex);
            if (completion->result.has_value()) {
                return;
            }
            if (!durable_entry) {
                completion->result = tl::make_unexpected(
                    WeightManagementError::DURABILITY_FAILED);
                completion->cv.notify_all();
                return;
            }
            completion->result = weight_metadata_.Publish(mutation);
            if (!*completion->result) {
                LOG(ERROR) << "Failed to publish durable weight metadata "
                              "mutation, sequence_id="
                           << durable_entry->sequence_id
                           << ", key=" << durable_entry->object_key
                           << ", error="
                           << static_cast<int>(completion->result->error());
            }
            completion->cv.notify_all();
        });
    if (!persisted) {
        LOG(ERROR) << "Failed to persist weight metadata mutation, key="
                   << MakeWeightRevisionMetadataKey(mutation.identity)
                   << ", error=" << static_cast<int>(persisted.error());
        return tl::make_unexpected(WeightManagementError::DURABILITY_FAILED);
    }

    std::unique_lock lock(completion->mutex);
    completion->cv.wait(lock, [&] { return completion->result.has_value(); });
    return std::move(*completion->result);
}

}  // namespace mooncake
