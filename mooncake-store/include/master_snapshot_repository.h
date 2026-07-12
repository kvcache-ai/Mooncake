#pragma once

#include <optional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "ha/ha_types.h"
#include "ha/snapshot/master_snapshot_codec.h"

namespace mooncake {

// Forward declarations
class SnapshotObjectStore;

namespace ha {
class SnapshotCatalogStore;
}

/**
 * @brief MasterSnapshotRepository handles storage and catalog operations for
 * snapshots. This includes uploading payload files to object storage,
 * publishing snapshots to the catalog, listing snapshots, deleting snapshots,
 * and enforcing retention policies.
 *
 * This class encapsulates all interactions with SnapshotObjectStore and
 * SnapshotCatalogStore, separating storage concerns from snapshot orchestration
 * logic in MasterSnapshotManager.
 */
class MasterSnapshotRepository {
   public:
    MasterSnapshotRepository(SnapshotObjectStore* object_store,
                             ha::SnapshotCatalogStore* catalog_store,
                             const std::string& backup_dir,
                             bool use_backup_dir);

    /**
     * @brief Upload a single snapshot payload file to object storage
     * @param data Binary data to upload
     * @param path Storage path/key
     * @param local_filename Filename for logging and local backup
     * @param snapshot_id Snapshot ID for logging
     * @return Empty on success, SerializationError on failure
     */
    tl::expected<void, SerializationError> UploadPayloadFile(
        const std::vector<uint8_t>& data, const std::string& path,
        const std::string& local_filename, const std::string& snapshot_id);

    /**
     * @brief Publish snapshot descriptor to catalog store
     * @param descriptor Snapshot descriptor to publish
     * @return ErrorCode::OK on success, error code on failure
     */
    ErrorCode PublishSnapshot(const ha::SnapshotDescriptor& descriptor);

    /**
     * @brief Cleanup old snapshots based on retention policy
     * @param keep_count Number of recent snapshots to keep
     * @param current_snapshot_id Current snapshot ID (for logging)
     */
    void CleanupOldSnapshots(size_t keep_count,
                             const std::string& current_snapshot_id);

    /**
     * @brief List all snapshots from catalog store
     * @param limit Maximum number of snapshots to return (0 = unlimited)
     * @return Vector of snapshot descriptors on success, error code on failure
     */
    tl::expected<std::vector<ha::SnapshotDescriptor>, ErrorCode> ListSnapshots(
        size_t limit);

    /**
     * @brief Delete a specific snapshot from catalog store
     * @param snapshot_id Snapshot ID to delete
     * @return ErrorCode::OK on success, error code on failure
     */
    ErrorCode DeleteSnapshot(const ha::SnapshotId& snapshot_id);

    /**
     * @brief Get object store connection info for logging
     * @return Connection info string
     */
    std::string GetObjectStoreConnectionInfo() const;

    /**
     * @brief Load the latest snapshot descriptor from catalog
     * @return Snapshot descriptor on success, error code on failure
     */
    tl::expected<ha::SnapshotDescriptor, ErrorCode> LoadLatestSnapshot();

    /**
     * @brief Load all restorable snapshot descriptors
     * @param latest_id Optional latest snapshot ID to filter candidates
     * @return Vector of candidate snapshots in chronological order, or error
     */
    tl::expected<std::vector<ha::SnapshotDescriptor>, ErrorCode>
    LoadRestoreCandidates(const std::optional<std::string>& latest_id);

    /**
     * @brief Download snapshot payloads from object storage
     * @param descriptor Snapshot descriptor with object paths
     * @return Structured payloads ready for decoding, or error
     */
    tl::expected<ha::MasterSnapshotPayloads, SerializationError>
    DownloadSnapshotPayloads(const ha::SnapshotDescriptor& descriptor);

   private:
    SnapshotObjectStore* object_store_;
    ha::SnapshotCatalogStore* catalog_store_;
    std::string backup_dir_;
    bool use_backup_dir_;
};

}  // namespace mooncake
