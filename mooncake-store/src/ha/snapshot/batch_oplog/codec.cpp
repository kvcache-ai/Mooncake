#include "ha/snapshot/batch_oplog/codec.h"

#include <limits>

#include <ylt/struct_pack.hpp>

namespace mooncake {

namespace {

template <typename T>
tl::expected<T, std::string> Decode(const std::vector<uint8_t>& encoded,
                                    const char* artifact) {
    T decoded;
    if (struct_pack::deserialize_to(decoded, encoded) !=
        struct_pack::errc::ok) {
        return tl::make_unexpected(std::string("Invalid ") + artifact);
    }
    return decoded;
}

template <typename T>
std::vector<uint8_t> Encode(const T& value) {
    auto encoded = struct_pack::serialize(value);
    return {encoded.begin(), encoded.end()};
}

}  // namespace

std::vector<uint8_t> EncodeBatchOpLogSnapshotSegments(
    const std::vector<StandbySegmentInfo>& segments) {
    return Encode(segments);
}

tl::expected<std::vector<StandbySegmentInfo>, std::string>
DecodeBatchOpLogSnapshotSegments(const std::vector<uint8_t>& encoded) {
    return Decode<std::vector<StandbySegmentInfo>>(encoded,
                                                   "snapshot segments");
}

std::vector<uint8_t> EncodeBatchOpLogSnapshotObjectChunk(
    uint64_t chunk_index, const std::vector<StandbyObjectEntry>& objects) {
    return Encode(BatchOpLogSnapshotObjectChunk{chunk_index, objects});
}

tl::expected<BatchOpLogSnapshotObjectChunk, std::string>
DecodeBatchOpLogSnapshotObjectChunk(const std::vector<uint8_t>& encoded,
                                    uint64_t expected_chunk_index,
                                    uint64_t expected_object_count) {
    auto decoded =
        Decode<BatchOpLogSnapshotObjectChunk>(encoded, "snapshot object chunk");
    if (!decoded) {
        return decoded;
    }
    if (decoded->chunk_index != expected_chunk_index) {
        return tl::make_unexpected("Snapshot object chunk index mismatch");
    }
    if (expected_object_count > std::numeric_limits<size_t>::max() ||
        decoded->objects.size() != expected_object_count) {
        return tl::make_unexpected("Snapshot object chunk count mismatch");
    }
    return decoded;
}

}  // namespace mooncake
