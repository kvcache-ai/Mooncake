#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "metadata_store.h"

namespace mooncake {

struct BatchOpLogSnapshotObjectChunk {
    uint64_t chunk_index{0};
    std::vector<StandbyObjectEntry> objects;

    YLT_REFL(BatchOpLogSnapshotObjectChunk, chunk_index, objects);
};

std::vector<uint8_t> EncodeBatchOpLogSnapshotSegments(
    const std::vector<StandbySegmentInfo>& segments);
tl::expected<std::vector<StandbySegmentInfo>, std::string>
DecodeBatchOpLogSnapshotSegments(const std::vector<uint8_t>& encoded);

std::vector<uint8_t> EncodeBatchOpLogSnapshotObjectChunk(
    uint64_t chunk_index, std::vector<StandbyObjectEntry> objects);
tl::expected<BatchOpLogSnapshotObjectChunk, std::string>
DecodeBatchOpLogSnapshotObjectChunk(const std::vector<uint8_t>& encoded,
                                    uint64_t expected_chunk_index,
                                    uint64_t expected_object_count);

}  // namespace mooncake
