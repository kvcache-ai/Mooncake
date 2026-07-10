// mooncake-store/tests/ha/oplog/mock_snapshot_provider.h
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ha/snapshot/snapshot_provider.h"

namespace mooncake::test {

// In-memory SnapshotProvider for unit and integration tests.
// Allows pre-filling snapshot data and simulating load failures.
class MockSnapshotProvider : public SnapshotProvider {
   public:
    void SetSnapshot(
        const std::string& snap_id, uint64_t seq_id,
        std::vector<std::pair<std::string, StandbyObjectMetadata>> data) {
        snapshot_id_ = snap_id;
        snapshot_seq_id_ = seq_id;
        snapshot_data_ = std::move(data);
    }

    void SetLoadFail(bool fail) { should_fail_ = fail; }

    tl::expected<std::optional<LoadedSnapshot>, ErrorCode> LoadLatestSnapshot(
        const std::string& /*cluster_id*/) override {
        if (should_fail_) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (snapshot_id_.empty()) {
            return std::optional<LoadedSnapshot>();
        }
        LoadedSnapshot snap;
        snap.snapshot_id = snapshot_id_;
        snap.snapshot_sequence_id = snapshot_seq_id_;
        snap.metadata = snapshot_data_;
        return std::optional<LoadedSnapshot>(std::move(snap));
    }

   private:
    std::string snapshot_id_;
    uint64_t snapshot_seq_id_ = 0;
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot_data_;
    bool should_fail_ = false;
};

}  // namespace mooncake::test
