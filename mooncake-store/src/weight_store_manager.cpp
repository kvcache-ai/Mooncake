#include "weight_store_manager.h"
#include "master_metric_manager.h"

#include <chrono>
#include <algorithm>
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
        if (!error_context.empty()) {
            LOG(ERROR) << "Failed to persist " << error_context
                       << ", error=" << static_cast<int>(persisted.error());
        }
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
    std::shared_lock mutation_lock(mutation_mutex_);
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
        const auto encoded = struct_pack::serialize(WeightMetadataUpsertOp{
            .metadata = *mutation.next,
            .operation = std::nullopt,
        });
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
    std::shared_lock mutation_lock(mutation_mutex_);
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

WeightMetadataStore::Result<WeightResidencyOperation>
WeightStoreManager::StartWeightResidencyOperation(
    const StartWeightResidencyOperationRequest& request) {
    const auto canonical_group = MakeWeightPayloadGroupId(request.identity);
    if (canonical_group.empty()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    [[maybe_unused]] auto group_operation_lock =
        LockGroup(request.identity);
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto mutation = weight_metadata_.PrepareStartOperation(request, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    if (!mutation->no_op) {
        auto members = backend_.SnapshotWeightGroup(
            request.identity,
            mutation->metadata.next->manifest.payload_group_id);
        if (!members || members->size() !=
                            mutation->metadata.next->manifest.payload_count +
                                1) {
            return tl::make_unexpected(WeightManagementError::NOT_READY);
        }
        mutation->next->total_members = members->size();
    }
    return PersistAndPublishWeightOperationMutation(*mutation);
}

WeightMetadataStore::Result<WeightResidencyOperation>
WeightStoreManager::QueryWeightOperation(
    const QueryWeightOperationRequest& request) const {
    return weight_metadata_.QueryOperation(request.operation_id);
}


WeightMetadataStore::Result<WeightRevisionMetadata>
WeightStoreManager::ReconcileWeightRevision(
    const ReconcileWeightRevisionRequest& request) {
    auto group_operation_lock = LockGroup(request.identity);
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto view = weight_metadata_.Get(request.identity, now_ms);
    if (!view) {
        return tl::make_unexpected(view.error());
    }
    const auto current = view->metadata;
    if (current.availability == WeightAvailabilityState::DELETED) {
        return current;
    }
    if (current.operation == WeightOperationState::EVICTING) {
        backend_.EvictManagedWeightGroupToCold(current);
    }

    auto members = backend_.SnapshotWeightGroup(request.identity,
                                       current.manifest.payload_group_id);
    const bool absent = !members &&
                        members.error() == WeightManagementError::NOT_FOUND;
    if (!members && !absent) {
        return tl::make_unexpected(members.error());
    }

    if (current.availability == WeightAvailabilityState::DELETING) {
        if (!absent) {
            return current;
        }
        auto mutation = weight_metadata_.PrepareFinishDelete(
            current.identity, current.metadata_generation, now_ms);
        if (!mutation) {
            return tl::make_unexpected(mutation.error());
        }
        return PersistAndPublishWeightMutation(*mutation);
    }

    bool complete = !absent &&
                    members->size() == current.manifest.payload_count + 1;
    bool manifest_found = false;
    bool all_memory = complete;
    bool all_cold = complete;
    bool any_readable = false;
    uint64_t logical_bytes = 0;
    std::vector<std::string> payload_keys;
    if (!absent) {
        for (const auto& member : *members) {
            complete = complete && member.readable;
            any_readable = any_readable || member.readable;
            all_memory = all_memory && member.has_memory;
            all_cold = all_cold && member.has_cold && !member.has_memory;
            if (member.key == current.manifest.manifest_key &&
                member.data_type == ObjectDataType::METADATA) {
                manifest_found = true;
            } else if (member.data_type == ObjectDataType::WEIGHT &&
                       member.size <= std::numeric_limits<uint64_t>::max() -
                                          logical_bytes) {
                logical_bytes += member.size;
                payload_keys.push_back(member.key);
            } else {
                complete = false;
            }
        }
    }
    complete = complete && manifest_found &&
               payload_keys.size() == current.manifest.payload_count &&
               logical_bytes == current.manifest.logical_bytes &&
               ComputeWeightPayloadKeysSha256(payload_keys) ==
                   current.manifest.payload_keys_sha256;
    const auto observed_residency =
        absent || !any_readable
            ? WeightResidencyState::ABSENT
            : (all_memory ? WeightResidencyState::HOT
                          : (all_cold ? WeightResidencyState::COLD
                                      : WeightResidencyState::MIXED));

    if (current.operation != WeightOperationState::NONE) {
        auto operation = weight_metadata_.QueryOperation(current.operation_id);
        if (!operation) {
            return tl::make_unexpected(operation.error());
        }
        if (complete && observed_residency == operation->target_residency) {
            auto mutation = weight_metadata_.PrepareFinishOperation(
                operation->operation_id, observed_residency, now_ms);
            if (!mutation) {
                return tl::make_unexpected(mutation.error());
            }
            auto finished = PersistAndPublishWeightOperationMutation(*mutation);
            if (!finished) {
                return tl::make_unexpected(finished.error());
            }
            auto reconciled = weight_metadata_.Get(request.identity, now_ms);
            if (!reconciled) {
                return tl::make_unexpected(reconciled.error());
            }
            return reconciled->metadata;
        }
        uint64_t processed_members = 0;
        std::string cursor;
        if (!absent) {
            for (const auto& member : *members) {
                const bool satisfied =
                    operation->target_residency == WeightResidencyState::HOT
                        ? member.has_memory
                        : member.has_cold && !member.has_memory;
                if (satisfied) {
                    ++processed_members;
                    cursor = member.key;
                }
            }
        }
        auto progress = weight_metadata_.PrepareUpdateOperationProgress(
            operation->operation_id, processed_members,
            operation->total_members == 0 ? current.manifest.payload_count + 1
                                          : operation->total_members,
            std::move(cursor),
            complete ? WeightAvailabilityState::READY
                     : WeightAvailabilityState::DEGRADED,
            observed_residency, now_ms);
        if (!progress) {
            return tl::make_unexpected(progress.error());
        }
        auto published = PersistAndPublishWeightOperationMutation(*progress);
        if (!published) {
            return tl::make_unexpected(published.error());
        }
        auto reconciled = weight_metadata_.Get(request.identity, now_ms);
        if (!reconciled) {
            return tl::make_unexpected(reconciled.error());
        }
        return reconciled->metadata;
    }

    const auto availability = complete ? WeightAvailabilityState::READY
                                       : WeightAvailabilityState::DEGRADED;
    auto mutation = weight_metadata_.PrepareReconcile(
        current.identity, current.metadata_generation, availability,
        observed_residency, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    return PersistAndPublishWeightMutation(*mutation);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
WeightStoreManager::DeleteWeightRevision(
    const DeleteWeightRevisionRequest& request) {
    const auto canonical_group = MakeWeightPayloadGroupId(request.identity);
    if (canonical_group.empty()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    [[maybe_unused]] auto group_operation_lock = LockGroup(request.identity);
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto mutation = weight_metadata_.PrepareDelete(request, now_ms);
    if (!mutation) {
        return tl::make_unexpected(mutation.error());
    }
    // Retire leases durably before publishing a state that cannot own leases.
    for (const auto& expired :
         weight_metadata_.PrepareExpireLeases(now_ms, request.identity)) {
        auto retired = PersistAndPublishWeightLeaseMutation(expired);
        if (!retired) {
            return tl::make_unexpected(retired.error());
        }
    }
    auto deleting = PersistAndPublishWeightMutation(*mutation);
    if (!deleting) {
        return tl::make_unexpected(deleting.error());
    }

    auto keys = backend_.GetGroupMemberKeys(TenantId(request.identity.tenant_id),
                                   deleting->manifest.payload_group_id);
    std::stable_sort(keys.begin(), keys.end(), [&](const auto& lhs,
                                                   const auto& rhs) {
        return lhs != deleting->manifest.manifest_key &&
               rhs == deleting->manifest.manifest_key;
    });
    for (const auto& key : keys) {
        auto removed = backend_.RemoveObject(key, TenantId(request.identity.tenant_id),
                                    true, true);
        if (!removed && removed.error() != ErrorCode::OBJECT_NOT_FOUND) {
            return tl::make_unexpected(WeightManagementError::BUSY);
        }
    }
    group_operation_lock.unlock();
    return ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = request.identity});
}

WeightMetadataStore::Result<WeightResidencyOperation>
WeightStoreManager::PersistAndPublishWeightOperationMutation(
    const WeightOperationMutation& mutation) {
    std::shared_lock mutation_lock(mutation_mutex_);
    if (!mutation.no_op && !backend_.CanPublishWeightMutations()) {
        return tl::make_unexpected(WeightManagementError::DURABILITY_FAILED);
    }
    if (mutation.no_op || !backend_.IsOpLogEnabled()) {
        return weight_metadata_.Publish(mutation);
    }
    if (!mutation.metadata.next.has_value() || !mutation.next.has_value()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    const auto encoded = struct_pack::serialize(WeightMetadataUpsertOp{
        .metadata = *mutation.metadata.next,
        .operation = *mutation.next,
    });
    const std::string payload(encoded.begin(), encoded.end());

    return PersistAndPublish<WeightResidencyOperation>(
        backend_, OpType::WEIGHT_METADATA_UPSERT,
        mutation.metadata.identity.tenant_id,
        MakeWeightRevisionMetadataKey(mutation.metadata.identity), payload, "",
        [this, mutation](const OpLogEntry& durable_entry) {
            auto result = weight_metadata_.Publish(mutation);
            if (!result) {
                LOG(ERROR) << "Failed to publish durable weight operation, "
                              "sequence_id="
                           << durable_entry.sequence_id
                           << ", operation_id=" << mutation.next->operation_id
                           << ", error=" << static_cast<int>(result.error());
            }
            return result;
        });
}



size_t WeightStoreManager::ReconcileWeightMetadataStoreOnce(uint64_t now_ms,
                                                 size_t limit) {
    if (limit == 0) {
        return 0;
    }
    constexpr uint64_t kImportAbandonTimeoutMs = 5 * 60 * 1000;
    size_t actions = 0;

    for (const auto& mutation : weight_metadata_.PrepareExpireLeases(now_ms)) {
        if (actions == limit) {
            break;
        }
        auto group_lock = LockGroup(mutation.previous->identity);
        auto current = weight_metadata_.PrepareReleaseLease(
            ReleaseWeightRevisionLeaseRequest{
                .tenant_id = mutation.previous->identity.tenant_id,
                .lease_id = mutation.lease_id,
            });
        if (!current || current->no_op || !current->previous.has_value() ||
            current->previous->identity != mutation.previous->identity ||
            current->previous->expires_at_ms > now_ms) {
            continue;
        }
        if (PersistAndPublishWeightLeaseMutation(*current)) {
            ++actions;
        } else {
            MasterMetricManager::instance()
                .inc_weight_reconciliation_failures();
        }
    }

    const auto snapshot = weight_metadata_.ExportSnapshot();
    const size_t revision_count = snapshot.metadata.size();
    const size_t start = revision_count == 0
                             ? 0
                             : weight_reconciliation_offset_.fetch_add(
                                   std::max<size_t>(limit, 1)) %
                                   revision_count;
    for (size_t examined = 0;
         examined < revision_count && actions < limit; ++examined) {
        const auto& metadata =
            snapshot.metadata[(start + examined) % revision_count];
        if (metadata.availability == WeightAvailabilityState::DELETED) {
            continue;
        }

        if (metadata.availability == WeightAvailabilityState::IMPORTING) {
            auto group_lock = LockGroup(metadata.identity);
            auto current = weight_metadata_.Get(metadata.identity, now_ms);
            if (!current ||
                current->metadata.availability !=
                    WeightAvailabilityState::IMPORTING ||
                now_ms < current->metadata.updated_at_ms ||
                now_ms - current->metadata.updated_at_ms <
                    kImportAbandonTimeoutMs) {
                continue;
            }
            auto mutation = weight_metadata_.PrepareAbortImport(
                AbortWeightImportRequest{
                    .identity = metadata.identity,
                    .expected_metadata_generation =
                        current->metadata.metadata_generation,
                },
                now_ms);
            if (!mutation || !PersistAndPublishWeightMutation(*mutation)) {
                MasterMetricManager::instance()
                    .inc_weight_reconciliation_failures();
                continue;
            }
            ++actions;
            continue;
        }

        WeightMetadataStore::Result<WeightRevisionMetadata> reconciled =
            metadata.availability == WeightAvailabilityState::DELETING
                ? DeleteWeightRevision(DeleteWeightRevisionRequest{
                      .identity = metadata.identity,
                      .expected_metadata_generation =
                          metadata.metadata_generation,
                  })
                : ReconcileWeightRevision(
                      ReconcileWeightRevisionRequest{
                          .identity = metadata.identity,
                      });
        if (reconciled) {
            ++actions;
        } else if (reconciled.error() != WeightManagementError::STALE_GENERATION) {
            MasterMetricManager::instance()
                .inc_weight_reconciliation_failures();
        }
    }

    MasterMetricManager::instance().project_weight_metadata(
        weight_metadata_.ExportSnapshot(), now_ms);
    return actions;
}


}  // namespace mooncake
