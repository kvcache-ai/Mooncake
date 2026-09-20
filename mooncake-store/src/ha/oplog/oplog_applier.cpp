#include "ha/oplog/oplog_applier.h"

#include <glog/logging.h>

#include "ha_metric_manager.h"
#include "metadata_store.h"
#include "ha/oplog/oplog_types.h"

namespace mooncake {
namespace {

bool SameImmutableWeightReference(const WeightRevisionMetadata& lhs,
                                  const WeightRevisionMetadata& rhs) {
    if (lhs.identity != rhs.identity ||
        lhs.created_at_ms != rhs.created_at_ms) {
        return false;
    }
    if (lhs.availability == WeightAvailabilityState::IMPORTING) {
        return lhs.manifest.payload_group_id == rhs.manifest.payload_group_id &&
               lhs.manifest.payload_count == rhs.manifest.payload_count &&
               lhs.manifest.logical_bytes == rhs.manifest.logical_bytes;
    }
    return lhs.manifest == rhs.manifest;
}

bool MatchesWeightTenantAndKey(const OpLogEntry& entry,
                               const WeightRevisionIdentity& identity) {
    return NormalizeTenantId(entry.tenant_id) == identity.tenant_id &&
           entry.object_key == MakeWeightRevisionMetadataKey(identity);
}

}  // namespace

OpLogApplier::OpLogApplier(MetadataStore* metadata_store,
                           const std::string& cluster_id)
    : metadata_store_(metadata_store),
      cluster_id_(cluster_id),
      expected_sequence_id_(1) {
    if (metadata_store_ == nullptr) {
        LOG(FATAL) << "OpLogApplier: metadata_store cannot be null";
    }
    if (!NormalizeAndValidateClusterId(cluster_id_)) {
        LOG(FATAL) << "Invalid cluster_id for OpLogApplier: '" << cluster_id_
                   << "'. Allowed chars: [A-Za-z0-9_.-], max_len=128.";
    }
}

bool OpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    // Basic DoS protection: validate key/payload sizes before parsing/applying.
    std::string size_reason;
    if (!ValidateOpLogEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "OpLogApplier: entry size rejected, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return false;
    }

    // Verify checksum to detect data corruption or tampering.
    if (!VerifyOpLogChecksum(entry)) {
        LOG(ERROR)
            << "OpLogApplier: checksum mismatch, sequence_id="
            << entry.sequence_id << ", key=" << entry.object_key
            << ". Possible data corruption or tampering. Discarding entry.";
        HAMetricManager::instance().inc_oplog_checksum_failures();
        return false;
    }

    // Global ordering only.
    //
    // Retries may deliver duplicate or already-applied entries.
    const uint64_t expected = expected_sequence_id_.load();
    if (IsSequenceOlder(entry.sequence_id, expected)) {
        VLOG(2) << "OpLogApplier: skip already-applied entry, sequence_id="
                << entry.sequence_id << ", expected=" << expected
                << ", key=" << entry.object_key;
        return true;
    }
    if (IsSequenceNewer(entry.sequence_id, expected)) {
        LOG(ERROR) << "OpLogApplier: future entry rejected, sequence_id="
                   << entry.sequence_id << ", expected=" << expected
                   << ", key=" << entry.object_key;
        return false;
    }

    bool applied = true;
    switch (entry.op_type) {
        case OpType::PUT_END:
            ApplyPutEnd(entry);
            break;
        case OpType::PUT_REVOKE:
            ApplyPutRevoke(entry);
            break;
        case OpType::REMOVE:
            ApplyRemove(entry);
            break;
        case OpType::SEGMENT_MOUNT:
            ApplySegmentMount(entry);
            break;
        case OpType::SEGMENT_UNMOUNT:
            ApplySegmentUnmount(entry);
            break;
        case OpType::SEGMENT_UPDATE:
            ApplySegmentUpdate(entry);
            break;
        case OpType::WEIGHT_METADATA_UPSERT:
            applied = ApplyWeightMetadataUpsert(entry);
            break;
        case OpType::WEIGHT_METADATA_DELETE:
            applied = ApplyWeightMetadataDelete(entry);
            break;
        case OpType::WEIGHT_LEASE_UPSERT:
            applied = ApplyWeightLeaseUpsert(entry);
            break;
        case OpType::WEIGHT_LEASE_DELETE:
            applied = ApplyWeightLeaseDelete(entry);
            break;
        default:
            LOG(ERROR) << "OpLogApplier: unsupported op_type="
                       << static_cast<int>(entry.op_type)
                       << ", sequence_id=" << entry.sequence_id
                       << ", key=" << entry.object_key;
            return false;
    }
    if (!applied) {
        return false;
    }

    // Update expected sequence ID
    expected_sequence_id_.store(entry.sequence_id + 1);

    // Update metrics
    HAMetricManager::instance().inc_oplog_applied_entries();
    HAMetricManager::instance().set_oplog_applied_sequence_id(
        static_cast<int64_t>(entry.sequence_id));

    return true;
}

size_t OpLogApplier::ApplyOpLogEntries(const std::vector<OpLogEntry>& entries) {
    size_t applied_count = 0;
    for (const auto& entry : entries) {
        if (ApplyOpLogEntry(entry)) {
            applied_count++;
        }
    }
    return applied_count;
}

uint64_t OpLogApplier::GetExpectedSequenceId() const {
    return expected_sequence_id_.load();
}

void OpLogApplier::Recover(uint64_t last_applied_sequence_id) {
    expected_sequence_id_.store(last_applied_sequence_id + 1);
    LOG(INFO) << "OpLogApplier: recovered from sequence_id="
              << last_applied_sequence_id << ", expected_sequence_id set to="
              << expected_sequence_id_.load();
}

void OpLogApplier::ApplyPutEnd(const OpLogEntry& entry) {
    // Payload contains serialized metadata (replicas, size, etc.) in JSON
    // format. Deserialize the payload immediately and store structured
    // metadata. This allows Standby to serve requests immediately after
    // promotion.

    if (entry.payload.empty()) {
        // No payload - create empty metadata (legacy compatibility)
        LOG(WARNING) << "OpLogApplier: PUT_END without payload, key="
                     << entry.object_key
                     << ", sequence_id=" << entry.sequence_id;
        StandbyObjectMetadata empty_metadata;
        if (!metadata_store_->PutMetadata(entry.tenant_id, entry.object_key,
                                          empty_metadata)) {
            LOG(ERROR) << "OpLogApplier: failed to PutMetadata key="
                       << entry.object_key
                       << ", sequence_id=" << entry.sequence_id;
        }
        return;
    }

    // Deserialize payload using struct_pack (msgpack binary format)
    MetadataPayload payload;
    auto result = struct_pack::deserialize_to(payload, entry.payload);
    if (result != struct_pack::errc::ok) {
        LOG(ERROR) << "OpLogApplier: failed to deserialize payload for key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id
                   << ", payload_size=" << entry.payload.size()
                   << ", error_code=" << static_cast<int>(result);
        // Fallback to empty metadata if parsing fails
        StandbyObjectMetadata empty_metadata;
        metadata_store_->PutMetadata(entry.tenant_id, entry.object_key,
                                     empty_metadata);
        return;
    }

    // Convert to StandbyObjectMetadata and store
    StandbyObjectMetadata metadata = payload.ToStandbyMetadata();

    if (!metadata_store_->PutMetadata(entry.tenant_id, entry.object_key,
                                      metadata)) {
        LOG(ERROR) << "OpLogApplier: failed to PutMetadata key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
    } else {
        VLOG(1) << "OpLogApplier: applied PUT_END, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id
                << ", replicas=" << metadata.replicas.size()
                << ", size=" << metadata.size;
    }
}

void OpLogApplier::ApplyPutRevoke(const OpLogEntry& entry) {
    // PUT_REVOKE means the object should be removed from metadata store
    // (but the key itself may still exist if there are other replicas).
    // Current implementation removes the entire key; if we later support
    // partial replica revocation this logic will need to be refined.
    if (!metadata_store_->Remove(entry.tenant_id, entry.object_key)) {
        LOG(WARNING) << "OpLogApplier: failed to Remove key="
                     << entry.object_key
                     << " in PUT_REVOKE, sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "OpLogApplier: applied PUT_REVOKE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

void OpLogApplier::ApplyRemove(const OpLogEntry& entry) {
    if (!metadata_store_->Remove(entry.tenant_id, entry.object_key)) {
        LOG(WARNING) << "OpLogApplier: failed to Remove key="
                     << entry.object_key
                     << ", sequence_id=" << entry.sequence_id
                     << " (key may not exist)";
    } else {
        VLOG(1) << "OpLogApplier: applied REMOVE, key=" << entry.object_key
                << ", sequence_id=" << entry.sequence_id;
    }
}

bool OpLogApplier::ApplyWeightMetadataUpsert(const OpLogEntry& entry) {
    WeightMetadataUpsertOp upsert;
    if (struct_pack::deserialize_to(upsert, entry.payload) !=
            struct_pack::errc::ok ||
        !ValidateWeightRevisionMetadata(upsert.metadata).ok() ||
        !MatchesWeightTenantAndKey(entry, upsert.metadata.identity)) {
        LOG(ERROR) << "OpLogApplier: invalid weight metadata upsert, key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
        return false;
    }
    const auto& next = upsert.metadata;
    if ((next.operation != WeightOperationState::NONE &&
         (!upsert.operation.has_value() ||
          upsert.operation->identity != next.identity ||
          upsert.operation->operation_id != next.operation_id ||
          upsert.operation->operation != next.operation ||
          upsert.operation->fenced_metadata_generation !=
              next.metadata_generation ||
          upsert.operation->processed_members >
              upsert.operation->total_members ||
          (!upsert.operation->cursor.empty() &&
           !IsValidWeightComponent(upsert.operation->cursor)))) ||
        (next.operation == WeightOperationState::NONE &&
         upsert.operation.has_value() &&
         (upsert.operation->identity != next.identity ||
          upsert.operation->message != "completed" ||
          upsert.operation->target_residency != next.residency))) {
        LOG(ERROR) << "OpLogApplier: inconsistent weight operation payload, "
                      "key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
        return false;
    }
    auto publish = [&]() {
        if (!metadata_store_->PutWeightMetadata(next)) {
            return false;
        }
        return !upsert.operation.has_value() ||
               metadata_store_->PutWeightOperation(*upsert.operation);
    };
    const auto tombstone =
        metadata_store_->GetWeightMetadataTombstoneGeneration(next.identity);
    if (tombstone.has_value()) {
        LOG(ERROR) << "OpLogApplier: weight metadata is fenced by tombstone, "
                   << "key=" << entry.object_key
                   << ", sequence_id=" << entry.sequence_id;
        return false;
    }

    const auto current = metadata_store_->GetWeightMetadata(next.identity);
    if (!current.has_value()) {
        if (next.metadata_generation != 1) {
            LOG(ERROR) << "OpLogApplier: initial weight metadata generation "
                          "must be one, key="
                       << entry.object_key
                       << ", generation=" << next.metadata_generation;
            return false;
        }
        return publish();
    }
    if (*current == next) {
        return !upsert.operation.has_value() ||
               metadata_store_->GetWeightOperation(
                   upsert.operation->operation_id) == upsert.operation ||
               metadata_store_->PutWeightOperation(*upsert.operation);
    }
    if (!CanAdvanceWeightMetadataGeneration(current->metadata_generation) ||
        next.metadata_generation != current->metadata_generation + 1 ||
        next.updated_at_ms < current->updated_at_ms ||
        !SameImmutableWeightReference(*current, next) ||
        (current->availability != next.availability &&
         !IsValidWeightAvailabilityTransition(current->availability,
                                              next.availability))) {
        LOG(ERROR) << "OpLogApplier: stale or conflicting weight metadata, "
                   << "key=" << entry.object_key
                   << ", current_generation=" << current->metadata_generation
                   << ", incoming_generation=" << next.metadata_generation;
        return false;
    }
    return publish();
}

bool OpLogApplier::ApplyWeightMetadataDelete(const OpLogEntry& entry) {
    WeightMetadataDeleteOp deletion;
    if (struct_pack::deserialize_to(deletion, entry.payload) !=
            struct_pack::errc::ok ||
        !ValidateWeightRevisionIdentity(deletion.identity).ok() ||
        deletion.metadata_generation == 0 ||
        !MatchesWeightTenantAndKey(entry, deletion.identity)) {
        LOG(ERROR) << "OpLogApplier: invalid weight metadata delete, key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
        return false;
    }

    const auto current = metadata_store_->GetWeightMetadata(deletion.identity);
    if (!current.has_value()) {
        const auto tombstone =
            metadata_store_->GetWeightMetadataTombstoneGeneration(
                deletion.identity);
        return tombstone.has_value() &&
               *tombstone == deletion.metadata_generation;
    }
    if (current->metadata_generation != deletion.metadata_generation) {
        LOG(ERROR) << "OpLogApplier: stale weight metadata delete, key="
                   << entry.object_key
                   << ", current_generation=" << current->metadata_generation
                   << ", delete_generation=" << deletion.metadata_generation;
        return false;
    }
    return metadata_store_->RemoveWeightMetadata(deletion.identity,
                                                 deletion.metadata_generation);
}

bool OpLogApplier::ApplyWeightLeaseUpsert(const OpLogEntry& entry) {
    WeightRevisionLease next;
    if (struct_pack::deserialize_to(next, entry.payload) !=
            struct_pack::errc::ok ||
        next.lease_id == 0 ||
        !ValidateWeightRevisionIdentity(next.identity).ok() ||
        !IsValidWeightComponent(next.holder) || next.expires_at_ms == 0 ||
        next.fenced_metadata_generation == 0 ||
        NormalizeTenantId(entry.tenant_id) != next.identity.tenant_id ||
        entry.object_key != MakeWeightLeaseMetadataKey(next.lease_id)) {
        LOG(ERROR) << "OpLogApplier: invalid weight lease upsert, key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
        return false;
    }
    if (metadata_store_->GetWeightLeaseTombstone(next.lease_id).has_value()) {
        LOG(ERROR) << "OpLogApplier: weight lease is fenced by tombstone, id="
                   << next.lease_id;
        return false;
    }
    const auto current = metadata_store_->GetWeightLease(next.lease_id);
    if (!current.has_value()) {
        const auto revision = metadata_store_->GetWeightMetadata(next.identity);
        if (!revision.has_value() ||
            revision->metadata_generation != next.fenced_metadata_generation) {
            LOG(ERROR)
                << "OpLogApplier: new weight lease references stale revision, "
                << "id=" << next.lease_id;
            return false;
        }
        return metadata_store_->PutWeightLease(next);
    }
    if (*current == next) {
        return true;
    }
    if (current->identity != next.identity || current->holder != next.holder ||
        current->fenced_metadata_generation !=
            next.fenced_metadata_generation ||
        next.expires_at_ms < current->expires_at_ms) {
        LOG(ERROR) << "OpLogApplier: conflicting weight lease upsert, id="
                   << next.lease_id;
        return false;
    }
    const auto revision = metadata_store_->GetWeightMetadata(next.identity);
    if (!revision.has_value() ||
        revision->metadata_generation < next.fenced_metadata_generation) {
        LOG(ERROR) << "OpLogApplier: renewed weight lease references missing "
                      "revision, id="
                   << next.lease_id;
        return false;
    }
    return metadata_store_->PutWeightLease(next);
}

bool OpLogApplier::ApplyWeightLeaseDelete(const OpLogEntry& entry) {
    WeightLeaseDeleteOp deletion;
    if (struct_pack::deserialize_to(deletion, entry.payload) !=
            struct_pack::errc::ok ||
        deletion.lease_id == 0 ||
        !ValidateWeightRevisionIdentity(deletion.identity).ok() ||
        deletion.fenced_metadata_generation == 0 ||
        NormalizeTenantId(entry.tenant_id) != deletion.identity.tenant_id ||
        entry.object_key != MakeWeightLeaseMetadataKey(deletion.lease_id)) {
        LOG(ERROR) << "OpLogApplier: invalid weight lease delete, key="
                   << entry.object_key << ", sequence_id=" << entry.sequence_id;
        return false;
    }
    const auto current = metadata_store_->GetWeightLease(deletion.lease_id);
    if (!current.has_value()) {
        const auto tombstone =
            metadata_store_->GetWeightLeaseTombstone(deletion.lease_id);
        return tombstone.has_value() &&
               tombstone->identity == deletion.identity &&
               tombstone->fenced_metadata_generation ==
                   deletion.fenced_metadata_generation;
    }
    if (current->identity != deletion.identity ||
        current->fenced_metadata_generation !=
            deletion.fenced_metadata_generation) {
        LOG(ERROR) << "OpLogApplier: stale weight lease delete, id="
                   << deletion.lease_id;
        return false;
    }
    return metadata_store_->RemoveWeightLease(
        deletion.lease_id, deletion.identity,
        deletion.fenced_metadata_generation);
}

const StandbySegmentRegistry& OpLogApplier::GetSegmentRegistry() const {
    return segment_registry_;
}

void OpLogApplier::LoadSegmentRegistry(
    const std::vector<StandbySegmentInfo>& segments) {
    segment_registry_.Clear();
    for (const auto& seg : segments) {
        segment_registry_.OnSegmentMount(seg);
    }
}

void OpLogApplier::ApplySegmentMount(const OpLogEntry& entry) {
    SegmentMountOp op;
    if (struct_pack::deserialize_to(op, entry.payload) !=
        struct_pack::errc::ok) {
        LOG(ERROR) << "Failed to deserialize SEGMENT_MOUNT payload for key "
                   << entry.object_key;
        return;
    }
    StandbySegmentInfo info;
    info.segment_name = op.segment_name;
    info.transport_endpoint = op.transport_endpoint;
    info.capacity = op.capacity;
    info.is_memory_segment = op.is_memory_segment;
    info.file_path = op.file_path;
    segment_registry_.OnSegmentMount(info);
    HAMetricManager::instance().inc_oplog_applied_entries();
}

void OpLogApplier::ApplySegmentUnmount(const OpLogEntry& entry) {
    SegmentUnmountOp op;
    if (struct_pack::deserialize_to(op, entry.payload) !=
        struct_pack::errc::ok) {
        LOG(ERROR) << "Failed to deserialize SEGMENT_UNMOUNT payload for key "
                   << entry.object_key;
        return;
    }
    segment_registry_.OnSegmentUnmount(op.transport_endpoint);
    HAMetricManager::instance().inc_oplog_applied_entries();
}

void OpLogApplier::ApplySegmentUpdate(const OpLogEntry& entry) {
    SegmentUpdateOp op;
    if (struct_pack::deserialize_to(op, entry.payload) !=
        struct_pack::errc::ok) {
        LOG(ERROR) << "Failed to deserialize SEGMENT_UPDATE payload for key "
                   << entry.object_key;
        return;
    }
    StandbySegmentInfo info;
    info.segment_name = op.segment_name;
    info.transport_endpoint = op.transport_endpoint;
    info.capacity = op.capacity;
    info.is_memory_segment = op.is_memory_segment;
    info.file_path = op.file_path;
    segment_registry_.OnSegmentUpdate(info);
    HAMetricManager::instance().inc_oplog_applied_entries();
}

}  // namespace mooncake
