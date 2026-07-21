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

bool P2POpLogApplier::ApplyCustomOpLogEntry(const OpLogEntry& entry) {
    if (entry.op_type == OpType_ADD_REPLICA) {
        return ApplyAddReplica(entry);
    } else if (entry.op_type == OpType_REMOVE_REPLICA) {
        return ApplyRemoveReplica(entry);
    } else if (entry.op_type == OpType_MOUNT_SEGMENT) {
        return ApplyMountSegment(entry);
    } else if (entry.op_type == OpType_UNMOUNT_SEGMENT) {
        return ApplyUnmountSegment(entry);
    } else if (entry.op_type == OpType_REMOVE_ALL) {
        return ApplyRemoveAll(entry);
    } else if (entry.op_type == OpType_REGISTER_CLIENT) {
        return ApplyRegisterClient(entry);
    } else if (entry.op_type == OpType_UNREGISTER_CLIENT) {
        return ApplyUnregisterClient(entry);
    }
    return false;
}

bool P2POpLogApplier::IsBestEffortOpLogEntry(const OpLogEntry& entry) const {
    return IsBestEffortP2POpLog(entry.op_type);
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
                           payload.segment_id, payload.size, entry.sequence_id);
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
