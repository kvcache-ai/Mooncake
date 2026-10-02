#pragma once

#include <cstddef>

namespace mooncake::ha {

// Snapshot file names
inline constexpr const char* kSnapshotMetadataFile = "metadata";
inline constexpr const char* kSnapshotSegmentsFile = "segments";
inline constexpr const char* kSnapshotTaskManagerFile = "task_manager";
inline constexpr const char* kSnapshotManifestFile = "manifest.txt";
inline constexpr const char* kSnapshotLatestFile = "latest.txt";

// Snapshot format
inline constexpr const char* kSnapshotSerializerType = "messagepack";
inline constexpr const char* kSnapshotSerializerVersion = "1.0.0";

// Shard slots the metadata payload's entry keys stay inside: a master that
// predates the tenant model reads each key as one of its shards and rejects a
// key at or above its own shard count. An entry holds one tenant, or several
// once the payload has more tenants than there are slots.
inline constexpr std::size_t kSnapshotShardSlots = 1024;

// Backup directories
inline constexpr const char* kSnapshotBackupSaveDir =
    "mooncake_snapshot_save_backup";
inline constexpr const char* kSnapshotBackupRestoreDir =
    "mooncake_snapshot_restore_backup";

// List limit
inline constexpr std::size_t kUnlimitedSnapshotList = 0;

}  // namespace mooncake::ha
