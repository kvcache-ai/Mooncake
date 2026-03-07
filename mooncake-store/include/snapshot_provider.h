#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "metadata_store.h"

namespace mooncake {

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

    // Load the latest available snapshot for `cluster_id`.
    // Returns true on success and fills:
    // - snapshot_id: opaque identifier (e.g. timestamp/version)
    // - snapshot_sequence_id: global OpLog sequence_id at snapshot boundary
    // - snapshot: full metadata baseline as key -> StandbyObjectMetadata
    virtual bool LoadLatestSnapshot(
        const std::string& cluster_id, std::string& snapshot_id,
        uint64_t& snapshot_sequence_id,
        std::vector<std::pair<std::string, StandbyObjectMetadata>>&
            snapshot) = 0;
};

// Default no-op provider: behaves as if "no snapshot available".
class NoopSnapshotProvider final : public SnapshotProvider {
   public:
    bool LoadLatestSnapshot(
        const std::string& /*cluster_id*/, std::string& snapshot_id,
        uint64_t& snapshot_sequence_id,
        std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot)
        override {
        snapshot_id.clear();
        snapshot_sequence_id = 0;
        snapshot.clear();
        return false;
    }
};

}  // namespace mooncake
