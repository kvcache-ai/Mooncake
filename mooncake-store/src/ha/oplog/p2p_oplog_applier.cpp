#include "ha/oplog/p2p_oplog_applier.h"

#include <glog/logging.h>

#include "ha/oplog/oplog_manager.h"
#include "ha_metric_manager.h"
#include "types.h"

namespace mooncake {

P2POpLogApplier::P2POpLogApplier(P2PStandbyMetadataStore* p2p_store,
                                 const std::string& cluster_id,
                                 OpLogStore* oplog_store)
    : OpLogApplier(p2p_store, cluster_id, oplog_store), p2p_store_(p2p_store) {
    if (p2p_store_ == nullptr) {
        LOG(FATAL) << "P2POpLogApplier: p2p_store cannot be null";
    }
}

bool P2POpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    // Main branch OpTypes — delegate entirely to base class (which handles
    // validate, checksum, sequence ordering, and dispatch).
    if (entry.op_type == OpType::PUT_END ||
        entry.op_type == OpType::PUT_REVOKE ||
        entry.op_type == OpType::REMOVE ||
        entry.op_type == OpType::LEASE_RENEW) {
        return OpLogApplier::ApplyOpLogEntry(entry);
    }

    // For P2P OpTypes, perform the same validate + checksum + sequence checks
    // as the base class, then dispatch to P2P-specific apply methods.
    const bool is_p2p_op = entry.op_type == OpType_ADD_REPLICA ||
                           entry.op_type == OpType_REMOVE_REPLICA ||
                           entry.op_type == OpType_MOUNT_SEGMENT ||
                           entry.op_type == OpType_UNMOUNT_SEGMENT ||
                           entry.op_type == OpType_REMOVE_ALL ||
                           entry.op_type == OpType_REGISTER_CLIENT ||
                           entry.op_type == OpType_UNREGISTER_CLIENT;
    if (!is_p2p_op) {
        LOG(ERROR) << "P2POpLogApplier: unknown op_type="
                   << static_cast<int>(entry.op_type)
                   << ", sequence_id=" << entry.sequence_id
                   << ", key=" << entry.object_key;
        return false;
    }

    // Validate entry size (DoS protection).
    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "P2POpLogApplier: entry size rejected, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return false;
    }

    // Verify checksum.
    if (!OpLogManager::VerifyChecksum(entry)) {
        LOG(ERROR) << "P2POpLogApplier: checksum mismatch, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key;
        HAMetricManager::instance().inc_oplog_checksum_failures();
        return false;
    }

    // Sequence ordering check (mirrors base class logic).
    const uint64_t expected = GetExpectedSequenceId();
    if (IsSequenceOlder(entry.sequence_id, expected)) {
        VLOG(2) << "P2POpLogApplier: skip already-applied entry, sequence_id="
                << entry.sequence_id << ", expected=" << expected
                << ", key=" << entry.object_key;
        return true;  // consumed (no-op for duplicates)
    }
    if (IsSequenceNewer(entry.sequence_id, expected)) {
        // Future entry — reject it for later re-delivery.
        // NOTE: P2POpLogApplier does not manage pending_entries_ itself.
        // The OpLogReplicator or caller should re-deliver out-of-order
        // entries after the gap is filled. This matches the simplified
        // ordering model for P2P's initial HA implementation.
        LOG(WARNING) << "P2POpLogApplier: out-of-order entry, sequence_id="
                     << entry.sequence_id << ", expected=" << expected
                     << ", key=" << entry.object_key;
        return false;
    }

    // Dispatch to P2P-specific apply methods.
    bool ok = false;
    if (entry.op_type == OpType_ADD_REPLICA) {
        ok = ApplyAddReplica(entry);
    } else if (entry.op_type == OpType_REMOVE_REPLICA) {
        ok = ApplyRemoveReplica(entry);
    } else if (entry.op_type == OpType_MOUNT_SEGMENT) {
        ok = ApplyMountSegment(entry);
    } else if (entry.op_type == OpType_UNMOUNT_SEGMENT) {
        ok = ApplyUnmountSegment(entry);
    } else if (entry.op_type == OpType_REMOVE_ALL) {
        ok = ApplyRemoveAll(entry);
    } else if (entry.op_type == OpType_REGISTER_CLIENT) {
        ok = ApplyRegisterClient(entry);
    } else if (entry.op_type == OpType_UNREGISTER_CLIENT) {
        ok = ApplyUnregisterClient(entry);
    }

    if (ok) {
        // Advance expected sequence ID (mirrors base class).
        // Note: we access the base class atomic directly via Recover(),
        // which sets expected = last_applied + 1.
        Recover(entry.sequence_id);

        // Update metrics.
        HAMetricManager::instance().inc_oplog_applied_entries();
        HAMetricManager::instance().set_oplog_applied_sequence_id(
            static_cast<int64_t>(entry.sequence_id));
    }

    return ok;
}

bool P2POpLogApplier::ApplyAddReplica(const OpLogEntry& entry) {
    AddReplicaPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR) << "P2POpLogApplier: failed to deserialize AddReplicaPayload"
                   << ", sequence_id=" << entry.sequence_id
                   << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->AddReplica(payload.object_key, payload.client_id,
                           payload.segment_id, payload.size);
    return true;
}

bool P2POpLogApplier::ApplyRemoveReplica(const OpLogEntry& entry) {
    RemoveReplicaPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize RemoveReplicaPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->RemoveReplica(payload.object_key, payload.client_id,
                              payload.segment_id);
    return true;
}

bool P2POpLogApplier::ApplyMountSegment(const OpLogEntry& entry) {
    MountSegmentPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize MountSegmentPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->AddSegment(payload.client_id, payload.segment);
    return true;
}

bool P2POpLogApplier::ApplyUnmountSegment(const OpLogEntry& entry) {
    UnmountSegmentPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize UnmountSegmentPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->RemoveSegment(payload.segment_id, payload.client_id);
    return true;
}

bool P2POpLogApplier::ApplyRemoveAll(const OpLogEntry& entry) {
    VLOG(1) << "P2POpLogApplier::ApplyRemoveAll, sequence_id="
            << entry.sequence_id;
    p2p_store_->RemoveAllMetadata();
    return true;
}

bool P2POpLogApplier::ApplyRegisterClient(const OpLogEntry& entry) {
    RegisterClientPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize RegisterClientPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->RegisterClient(payload.client_id, payload.ip_address,
                               payload.rpc_port, payload.segments);
    return true;
}

bool P2POpLogApplier::ApplyUnregisterClient(const OpLogEntry& entry) {
    UnregisterClientPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize UnregisterClientPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->UnRegisterClient(payload.client_id);
    return true;
}

}  // namespace mooncake
