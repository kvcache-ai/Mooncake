#include "master_snapshot_repository.h"

#include <filesystem>

#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_logger.h"
#include "utils/file_util.h"

namespace mooncake {

namespace fs = std::filesystem;

namespace {
constexpr size_t kUnlimitedSnapshotList = 0;
static const std::string SNAPSHOT_BACKUP_SAVE_DIR =
    "mooncake_snapshot_save_backup";
}  // namespace

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
            auto save_path = fs::path(backup_dir_) / SNAPSHOT_BACKUP_SAVE_DIR /
                             local_filename;
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
    auto list_result = catalog_store_->List(kUnlimitedSnapshotList);
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

}  // namespace mooncake
