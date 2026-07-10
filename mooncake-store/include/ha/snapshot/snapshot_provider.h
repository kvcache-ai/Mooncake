#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "metadata_store.h"

namespace mooncake {

struct LoadedSnapshot {
    std::string snapshot_id;
    uint64_t snapshot_sequence_id{0};
    std::vector<std::pair<std::string, StandbyObjectMetadata>> metadata;
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

    // Load the latest available snapshot for `cluster_id`.
    // Return values:
    // - unexpected(error): backend or parsing failure
    // - std::nullopt: no snapshot published yet
    // - LoadedSnapshot: full metadata baseline
    virtual tl::expected<std::optional<LoadedSnapshot>, ErrorCode>
    LoadLatestSnapshot(const std::string& cluster_id) = 0;
};

// Default no-op provider: behaves as if "no snapshot available".
class NoopSnapshotProvider final : public SnapshotProvider {
   public:
    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& /*cluster_id*/) override {
        return std::optional<LoadedSnapshot>();
    }
};

}  // namespace mooncake
