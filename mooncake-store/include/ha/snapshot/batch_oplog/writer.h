#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

class BatchOpLogSnapshotCapture;
class HotStandbyService;
class SnapshotObjectStore;

class BatchOpLogSnapshotWriter {
   public:
    explicit BatchOpLogSnapshotWriter(SnapshotObjectStore& object_store)
        : object_store_(object_store) {}

    tl::expected<std::string, std::string> Write(
        HotStandbyService& standby, BatchOpLogSnapshotCapture& capture,
        const std::string& snapshot_root, const std::string& snapshot_id,
        size_t chunk_object_count, int64_t created_at_ms);

   private:
    SnapshotObjectStore& object_store_;
};

}  // namespace mooncake
