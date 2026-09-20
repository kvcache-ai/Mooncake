#include "weight_store_manager.h"

#include <chrono>
#include <condition_variable>
#include <limits>
#include <memory>

#include <glog/logging.h>

namespace mooncake {
namespace {

template <typename T, typename Publish>
WeightMetadataStore::Result<T> PersistAndPublish(
    WeightStoreBackend& backend, OpType type, const std::string& tenant_id,
    const std::string& key, const std::string& payload,
    const std::string& error_context, Publish publish) {
    struct Completion {
        std::mutex mutex;
        std::condition_variable cv;
        std::optional<WeightMetadataStore::Result<T>> result;
    };
    auto completion = std::make_shared<Completion>();
    auto persisted = backend.AppendOpLogWithDurableFinalize(
        type, tenant_id, key, payload,
        [completion,
         publish](const WeightStoreBackend::DurableResult& durable_entry) {
            // Arbitrate terminal callbacks and publish under the same lock.
            std::lock_guard lock(completion->mutex);
            if (completion->result.has_value()) {
                return;
            }
            if (!durable_entry) {
                completion->result = tl::make_unexpected(
                    WeightManagementError::DURABILITY_FAILED);
            } else {
                completion->result = publish(*durable_entry);
            }
            completion->cv.notify_all();
        });
    if (!persisted) {
        LOG(ERROR) << "Failed to persist " << error_context
                   << ", error=" << static_cast<int>(persisted.error());
        return tl::make_unexpected(WeightManagementError::DURABILITY_FAILED);
    }

    std::unique_lock lock(completion->mutex);
    completion->cv.wait(lock, [&] { return completion->result.has_value(); });
    return std::move(*completion->result);
}

}  // namespace

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

    const auto key = MakeWeightRevisionMetadataKey(mutation.identity);
    return PersistAndPublish<WeightRevisionMetadata>(
        backend_, type, mutation.identity.tenant_id, key, payload,
        "weight metadata mutation, key=" + key,
        [this, mutation](const OpLogEntry& durable_entry) {
            auto result = weight_metadata_.Publish(mutation);
            if (!result) {
                LOG(ERROR) << "Failed to publish durable weight metadata "
                              "mutation, sequence_id="
                           << durable_entry.sequence_id
                           << ", key=" << durable_entry.object_key
                           << ", error=" << static_cast<int>(result.error());
            }
            return result;
        });
}

WeightMetadataStore::Result<WeightRevisionLease>
WeightStoreManager::AcquireWeightRevisionLease(
    const AcquireWeightRevisionLeaseRequest& request) {
    const auto canonical_group = MakeWeightPayloadGroupId(request.identity);
    if (canonical_group.empty()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    [[maybe_unused]] auto operation_lock = LockGroup(request.identity);
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto mutation = weight_metadata_.PrepareAcquireLease(request, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    return PersistAndPublishWeightLeaseMutation(*mutation);
}

WeightMetadataStore::Result<WeightRevisionLease>
WeightStoreManager::RenewWeightRevisionLease(
    const RenewWeightRevisionLeaseRequest& request) {
    auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto mutation = weight_metadata_.PrepareRenewLease(request, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    const auto canonical_group =
        MakeWeightPayloadGroupId(mutation->previous->identity);
    if (canonical_group.empty()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    [[maybe_unused]] auto operation_lock =
        LockGroup(mutation->previous->identity);
    now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    mutation = weight_metadata_.PrepareRenewLease(request, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    return PersistAndPublishWeightLeaseMutation(*mutation);
}

WeightMetadataStore::Result<void>
WeightStoreManager::ReleaseWeightRevisionLease(
    const ReleaseWeightRevisionLeaseRequest& request) {
    auto mutation = weight_metadata_.PrepareReleaseLease(request);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    std::unique_lock<std::mutex> operation_lock;
    if (!mutation->no_op) {
        const auto canonical_group =
            MakeWeightPayloadGroupId(mutation->previous->identity);
        if (canonical_group.empty()) {
            return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
        }
        operation_lock = LockGroup(mutation->previous->identity);
        mutation = weight_metadata_.PrepareReleaseLease(request);
        if (!mutation) {
            return tl::make_unexpected(mutation.error());
        }
    }
    auto released = PersistAndPublishWeightLeaseMutation(*mutation);
    if (!released) {
        return tl::make_unexpected(released.error());
    }
    return {};
}

WeightMetadataStore::Result<WeightRevisionLease>
WeightStoreManager::PersistAndPublishWeightLeaseMutation(
    const WeightLeaseMutation& mutation) {
    if (!mutation.no_op && !backend_.CanPublishWeightMutations()) {
        return tl::make_unexpected(WeightManagementError::DURABILITY_FAILED);
    }
    if (mutation.no_op || !backend_.IsOpLogEnabled()) {
        return weight_metadata_.Publish(mutation);
    }

    OpType type;
    std::string tenant_id;
    std::string payload;
    if (mutation.kind == WeightMetadataMutationKind::UPSERT &&
        mutation.next.has_value()) {
        type = OpType::WEIGHT_LEASE_UPSERT;
        tenant_id = mutation.next->identity.tenant_id;
        const auto encoded = struct_pack::serialize(*mutation.next);
        payload.assign(encoded.begin(), encoded.end());
    } else if (mutation.previous.has_value()) {
        type = OpType::WEIGHT_LEASE_DELETE;
        tenant_id = mutation.previous->identity.tenant_id;
        WeightLeaseDeleteOp deletion{
            .lease_id = mutation.lease_id,
            .identity = mutation.previous->identity,
            .fenced_metadata_generation =
                mutation.previous->fenced_metadata_generation,
        };
        const auto encoded = struct_pack::serialize(deletion);
        payload.assign(encoded.begin(), encoded.end());
    } else {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    return PersistAndPublish<WeightRevisionLease>(
        backend_, type, tenant_id,
        MakeWeightLeaseMetadataKey(mutation.lease_id), payload,
        "weight lease mutation, lease_id=" + std::to_string(mutation.lease_id),
        [this, mutation](const OpLogEntry& durable_entry) {
            auto result = weight_metadata_.Publish(mutation);
            if (!result) {
                LOG(ERROR) << "Failed to publish durable weight lease "
                              "mutation, sequence_id="
                           << durable_entry.sequence_id
                           << ", lease_id=" << mutation.lease_id
                           << ", error=" << static_cast<int>(result.error());
            }
            return result;
        });
}

}  // namespace mooncake
