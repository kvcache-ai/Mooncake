#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "metadata_store.h"

namespace mooncake {

enum class SnapshotRestoreMode {
    kColdRestore,
    kStandbyCatchupWithOplog,
};

inline const char* SnapshotRestoreModeToString(SnapshotRestoreMode mode) {
    switch (mode) {
        case SnapshotRestoreMode::kColdRestore:
            return "cold-restore";
        case SnapshotRestoreMode::kStandbyCatchupWithOplog:
            return "standby-catchup-with-oplog";
    }
    return "unknown";
}

inline bool ShouldAdmitSnapshotEntryForRestore(
    SnapshotRestoreMode mode,
    const std::chrono::system_clock::time_point& lease_timeout,
    const std::chrono::system_clock::time_point& now) {
    switch (mode) {
        case SnapshotRestoreMode::kColdRestore:
            return lease_timeout > now;
        case SnapshotRestoreMode::kStandbyCatchupWithOplog:
            return true;
    }
    return false;
}

template <typename Metadata>
inline bool ShouldAdmitSnapshotEntryForRestore(
    SnapshotRestoreMode mode, const Metadata& metadata,
    const std::chrono::system_clock::time_point& now) {
    if (mode == SnapshotRestoreMode::kStandbyCatchupWithOplog) {
        return true;
    }
    auto now_copy = now;
    return !metadata.IsLeaseExpired(now_copy);
}

struct LoadedSnapshot {
    std::string snapshot_id;
    uint64_t snapshot_sequence_id{0};
    std::vector<std::pair<std::string, StandbyObjectMetadata>> metadata;
};

struct SnapshotVersionInfo {
    std::string snapshot_id;
    uint64_t snapshot_sequence_id{0};
};

/**
 * @brief SnapshotProvider is an abstraction for loading metadata snapshots.
 *
 * Assumption: snapshot functionality exists (implemented by another team), but
 * may not be synced into this repo yet. We keep Mooncake-store code progressing
 * by depending on this narrow interface.
 *
 * Snapshot semantics for hot-standby:
 * - A snapshot represents a consistent metadata baseline at
 * `snapshot_sequence_id`.
 * - Standby should: load snapshot -> recover applier to snapshot_sequence_id ->
 *   replay OpLog entries with sequence_id > snapshot_sequence_id.
 */
class SnapshotProvider {
   public:
    virtual ~SnapshotProvider() = default;

    // Resolve the current latest snapshot without loading the full payload.
    // Providers may override this for an efficient catalog-only check.
    virtual tl::expected<std::optional<SnapshotVersionInfo>, ErrorCode>
    GetLatestSnapshotVersion(const std::string& cluster_id) {
        auto snapshot =
            LoadLatestSnapshot(cluster_id, SnapshotRestoreMode::kColdRestore);
        if (!snapshot) {
            return tl::make_unexpected(snapshot.error());
        }
        if (!snapshot->has_value()) {
            return std::optional<SnapshotVersionInfo>();
        }

        SnapshotVersionInfo version;
        version.snapshot_id = snapshot->value().snapshot_id;
        version.snapshot_sequence_id = snapshot->value().snapshot_sequence_id;
        return std::optional<SnapshotVersionInfo>(std::move(version));
    }

    // Load the latest available snapshot for `cluster_id`.
    // Return values:
    // - unexpected(error): backend or parsing failure
    // - std::nullopt: no snapshot published yet
    // - LoadedSnapshot: full metadata baseline
    virtual tl::expected<std::optional<LoadedSnapshot>, ErrorCode>
    LoadLatestSnapshot(const std::string& cluster_id,
                       SnapshotRestoreMode restore_mode) = 0;
};

// Default no-op provider: behaves as if "no snapshot available".
class NoopSnapshotProvider final : public SnapshotProvider {
   public:
    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& /*cluster_id*/,
        SnapshotRestoreMode /*restore_mode*/) override {
        return std::optional<LoadedSnapshot>();
    }
};

}  // namespace mooncake
