#include "master_snapshot_repository.h"

#include <filesystem>
#include <unordered_set>

#include <boost/algorithm/string.hpp>

#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_constants.h"
#include "ha/snapshot/snapshot_logger.h"
#include "utils/file_util.h"

namespace mooncake {

namespace fs = std::filesystem;

MasterSnapshotRepository::MasterSnapshotRepository(
    SnapshotObjectStore* object_store, ha::SnapshotCatalogStore* catalog_store,
    const std::string& backup_dir, bool use_backup_dir)
    : object_store_(object_store),
      catalog_store_(catalog_store),
      backup_dir_(backup_dir),
      use_backup_dir_(use_backup_dir) {}

tl::expected<void, SerializationError>
MasterSnapshotRepository::UploadPayloadFile(const std::vector<uint8_t>& data,
                                            const std::string& path,
                                            const std::string& local_filename,
                                            const std::string& snapshot_id) {
    SNAP_LOG_INFO("[Snapshot] Uploading {} to: {}, snapshot_id={}",
                  local_filename, path, snapshot_id);

    std::string error_msg;
    auto upload_result = object_store_->UploadBuffer(path, data);
    if (!upload_result) {
        SNAP_LOG_ERROR(
            "[Snapshot] {} upload failed, snapshot_id={}, file={}, error={}",
            local_filename, snapshot_id, path, upload_result.error());

        // Upload failed, save locally for manual recovery in exception
        // scenarios
        if (use_backup_dir_) {
            auto save_path = fs::path(backup_dir_) /
                             ha::kSnapshotBackupSaveDir / local_filename;
            auto save_result = FileUtil::SaveBinaryToFile(data, save_path);
            if (!save_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] save {} to disk failed, snapshot_id={}, "
                    "file={}",
                    local_filename, snapshot_id, save_path.string());
            }
        }

        error_msg.append(local_filename)
            .append(" upload ")
            .append(path)
            .append(" failed; ");
        return tl::make_unexpected(
            SerializationError(ErrorCode::PERSISTENT_FAIL, error_msg));
    } else {
        SNAP_LOG_INFO("[Snapshot] Upload {} success: {}, snapshot_id={}",
                      local_filename, path, snapshot_id);
    }

    return {};
}

ErrorCode MasterSnapshotRepository::PublishSnapshot(
    const ha::SnapshotDescriptor& descriptor) {
    return catalog_store_->Publish(descriptor);
}

void MasterSnapshotRepository::CleanupOldSnapshots(
    size_t keep_count, const std::string& current_snapshot_id) {
    if (!catalog_store_) {
        SNAP_LOG_ERROR(
            "[Snapshot] snapshot catalog store is not initialized, "
            "snapshot_id={}",
            current_snapshot_id);
        return;
    }

    // List() loads one descriptor per published snapshot. This remains cheap
    // because CleanupOldSnapshots() itself enforces retention count
    // and keeps the catalog single-digit in normal deployments.
    auto list_result = catalog_store_->List(ha::kUnlimitedSnapshotList);
    if (!list_result) {
        SNAP_LOG_ERROR("[Snapshot] error=list failed, snapshot_id={}, code={}",
                       current_snapshot_id, toString(list_result.error()));
        return;
    }

    const auto& snapshots = list_result.value();

    if (snapshots.size() > keep_count) {
        for (size_t i = keep_count; i < snapshots.size(); i++) {
            const std::string& old_state_dir = snapshots[i].snapshot_id;

            if (old_state_dir == current_snapshot_id) {
                SNAP_LOG_WARN(
                    "[Snapshot] Skipping deletion of current snapshot "
                    "directory {}, "
                    "snapshot_id={}",
                    old_state_dir, current_snapshot_id);
                continue;
            }

            auto delete_result = catalog_store_->Delete(old_state_dir);
            if (delete_result != ErrorCode::OK) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Failed to delete old snapshot {}, "
                    "snapshot_id={}, code={}",
                    old_state_dir, current_snapshot_id,
                    toString(delete_result));
            } else {
                SNAP_LOG_INFO(
                    "[Snapshot] Successfully deleted old snapshot {}, "
                    "snapshot_id={}",
                    old_state_dir, current_snapshot_id);
            }
        }
    }
}

tl::expected<std::vector<ha::SnapshotDescriptor>, ErrorCode>
MasterSnapshotRepository::ListSnapshots(size_t limit) {
    return catalog_store_->List(limit);
}

ErrorCode MasterSnapshotRepository::DeleteSnapshot(
    const ha::SnapshotId& snapshot_id) {
    return catalog_store_->Delete(snapshot_id);
}

std::string MasterSnapshotRepository::GetObjectStoreConnectionInfo() const {
    return object_store_->GetConnectionInfo();
}

tl::expected<ha::SnapshotDescriptor, ErrorCode>
MasterSnapshotRepository::LoadLatestSnapshot() {
    if (!catalog_store_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto latest_result = catalog_store_->GetLatest();
    if (!latest_result) {
        return tl::make_unexpected(latest_result.error());
    }

    if (!latest_result->has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    return latest_result->value();
}

tl::expected<std::vector<ha::SnapshotDescriptor>, ErrorCode>
MasterSnapshotRepository::LoadRestoreCandidates(
    const std::optional<ha::SnapshotDescriptor>& latest_snapshot) {
    std::vector<ha::SnapshotDescriptor> candidates;
    std::unordered_set<std::string> candidate_ids;

    // Add latest snapshot if provided
    if (latest_snapshot.has_value()) {
        candidates.push_back(latest_snapshot.value());
        candidate_ids.emplace(latest_snapshot->snapshot_id);
    }

    // List all snapshots as fallback
    auto list_result = ListSnapshots(ha::kUnlimitedSnapshotList);
    if (!list_result) {
        if (candidates.empty()) {
            return tl::make_unexpected(list_result.error());
        }
        // Return latest only if list failed
        return candidates;
    }

    // Filter by latest_id chronologically (snapshot IDs use timestamp format)
    for (const auto& snapshot : list_result.value()) {
        if (latest_snapshot.has_value() &&
            snapshot.snapshot_id > latest_snapshot->snapshot_id) {
            continue;
        }
        if (candidate_ids.emplace(snapshot.snapshot_id).second) {
            candidates.push_back(snapshot);
        }
    }

    return candidates;
}

tl::expected<ha::MasterSnapshotPayloads, SerializationError>
MasterSnapshotRepository::DownloadSnapshotPayloads(
    const ha::SnapshotDescriptor& descriptor) {
    const std::string& snapshot_id = descriptor.snapshot_id;
    std::string path_prefix = descriptor.object_prefix;
    if (path_prefix.empty()) {
        if (!catalog_store_) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::INVALID_PARAMS, "catalog_store is null"));
        }
        path_prefix = catalog_store_->GetSnapshotRoot() + snapshot_id + "/";
    }

    std::string manifest_path = descriptor.manifest_key;
    if (manifest_path.empty()) {
        manifest_path = path_prefix + ha::kSnapshotManifestFile;
    }

    // Download and validate manifest
    std::string manifest_content;
    auto manifest_result =
        object_store_->DownloadString(manifest_path, manifest_content);
    if (!manifest_result) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL, "failed to download manifest '" +
                                            manifest_path +
                                            "': " + manifest_result.error()));
    }

    if (use_backup_dir_) {
        auto save_result = FileUtil::SaveStringToFile(
            manifest_content, fs::path(backup_dir_) /
                                  ha::kSnapshotBackupRestoreDir /
                                  ha::kSnapshotManifestFile);
        if (!save_result) {
            SNAP_LOG_ERROR("[Restore] Failed to save manifest to file: {}",
                           save_result.error());
        }
    }

    // Parse and validate manifest
    std::vector<std::string> parts;
    boost::split(parts, manifest_content, boost::is_any_of("|"));
    if (parts.size() < 3) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::INVALID_PARAMS, "invalid snapshot manifest format"));
    }

    const std::string& protocol_type = parts[0];
    const std::string& version = parts[1];

    SNAP_LOG_INFO("[Restore] Loading snapshot: {} version: {} protocol: {}",
                  snapshot_id, version, protocol_type);

    if (protocol_type != ha::kSnapshotSerializerType) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::INVALID_PARAMS, "unsupported protocol type '" +
                                           protocol_type + "', expected '" +
                                           ha::kSnapshotSerializerType + "'"));
    }
    if (version != ha::kSnapshotSerializerVersion) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::INVALID_PARAMS,
            "incompatible snapshot version '" + version + "', expected '" +
                ha::kSnapshotSerializerVersion + "'"));
    }

    ha::MasterSnapshotPayloads payloads;

    // Download metadata
    std::string metadata_path = path_prefix + ha::kSnapshotMetadataFile;
    auto metadata_result =
        object_store_->DownloadBuffer(metadata_path, payloads.metadata);
    if (!metadata_result) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL, "failed to download metadata '" +
                                            metadata_path +
                                            "': " + metadata_result.error()));
    }

    if (use_backup_dir_) {
        auto save_result = FileUtil::SaveBinaryToFile(
            payloads.metadata, fs::path(backup_dir_) /
                                   ha::kSnapshotBackupRestoreDir /
                                   ha::kSnapshotMetadataFile);
        if (!save_result) {
            SNAP_LOG_ERROR("[Restore] Failed to save metadata to file: {}",
                           save_result.error());
        }
    }
    SNAP_LOG_INFO("[Restore] Downloaded metadata file successfully");

    // Download segments
    std::string segments_path = path_prefix + ha::kSnapshotSegmentsFile;
    auto segments_result =
        object_store_->DownloadBuffer(segments_path, payloads.segments);
    if (!segments_result) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL, "failed to download segments '" +
                                            segments_path +
                                            "': " + segments_result.error()));
    }

    if (use_backup_dir_) {
        auto save_result = FileUtil::SaveBinaryToFile(
            payloads.segments, fs::path(backup_dir_) /
                                   ha::kSnapshotBackupRestoreDir /
                                   ha::kSnapshotSegmentsFile);
        if (!save_result) {
            SNAP_LOG_ERROR("[Restore] Failed to save segments to file: {}",
                           save_result.error());
        }
    }
    SNAP_LOG_INFO("[Restore] Downloaded segments file successfully");

    // Download task_manager
    std::string task_manager_path = path_prefix + ha::kSnapshotTaskManagerFile;
    auto task_manager_result =
        object_store_->DownloadBuffer(task_manager_path, payloads.task_manager);
    if (!task_manager_result) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            "failed to download task_manager '" + task_manager_path +
                "': " + task_manager_result.error()));
    }

    if (use_backup_dir_) {
        auto save_result = FileUtil::SaveBinaryToFile(
            payloads.task_manager, fs::path(backup_dir_) /
                                       ha::kSnapshotBackupRestoreDir /
                                       ha::kSnapshotTaskManagerFile);
        if (!save_result) {
            SNAP_LOG_ERROR("[Restore] Failed to save task manager to file: {}",
                           save_result.error());
        }
    }
    SNAP_LOG_INFO("[Restore] Downloaded task manager file successfully");

    return payloads;
}

}  // namespace mooncake
