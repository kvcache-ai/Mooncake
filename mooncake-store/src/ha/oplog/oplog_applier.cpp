#include "ha/oplog/oplog_applier.h"

#include <glog/logging.h>

#include "ha_metric_manager.h"
#include "metadata_store.h"
#include "ha/oplog/oplog_types.h"

namespace mooncake {

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

    // Apply the operation based on type
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
        default:
            LOG(ERROR) << "OpLogApplier: unsupported op_type="
                       << static_cast<int>(entry.op_type)
                       << ", sequence_id=" << entry.sequence_id
                       << ", key=" << entry.object_key;
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
        empty_metadata.last_sequence_id = entry.sequence_id;
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
        empty_metadata.last_sequence_id = entry.sequence_id;
        metadata_store_->PutMetadata(entry.tenant_id, entry.object_key,
                                     empty_metadata);
        return;
    }

    // Convert to StandbyObjectMetadata and store
    StandbyObjectMetadata metadata =
        payload.ToStandbyMetadata(entry.sequence_id);

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
