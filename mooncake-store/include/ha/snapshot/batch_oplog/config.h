#pragma once

#include <optional>
#include <string>

#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "master_config.h"

namespace mooncake {

inline std::optional<std::string> ValidateBatchOpLogSnapshotConfig(
    const MasterConfig& config) {
    if (!config.enable_oplog_snapshot) {
        return std::nullopt;
    }
    if (!config.enable_oplog) {
        return "enable_oplog_snapshot requires enable_oplog=true";
    }
    if (!config.enable_ha || config.ha_backend_type != "etcd") {
        return "enable_oplog_snapshot requires HA with ha_backend_type=etcd";
    }
    if (config.snapshot_chunk_object_count == 0) {
        return "snapshot_chunk_object_count must be greater than 0";
    }
    if (ha::BuildBatchOpLogSnapshotRoot(config.cluster_id).empty()) {
        return "enable_oplog_snapshot requires a valid cluster_id";
    }
    try {
        ParseSnapshotObjectStoreType(config.snapshot_object_store_type);
    } catch (const std::exception& error) {
        return error.what();
    }
    return std::nullopt;
}

}  // namespace mooncake
