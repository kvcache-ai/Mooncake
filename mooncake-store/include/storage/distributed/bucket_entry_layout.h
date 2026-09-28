#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <string>

#include "replica.h"

namespace mooncake {

/** Canonical bucket entry layout: [value bytes][zero padding]. */
struct BucketEntryLayout {
    uint64_t offset = 0;
    uint64_t object_size = 0;
    uint64_t aligned_size = 0;

    uint64_t end() const { return offset + aligned_size; }
};

inline bool IsValidBucketAlignment(uint64_t alignment) {
    return alignment != 0 && (alignment & (alignment - 1)) == 0;
}

inline std::optional<uint64_t> CheckedAlignUp(uint64_t value,
                                              uint64_t alignment) {
    if (!IsValidBucketAlignment(alignment) ||
        value > std::numeric_limits<uint64_t>::max() - (alignment - 1)) {
        return std::nullopt;
    }
    return (value + alignment - 1) & ~(alignment - 1);
}

inline std::optional<BucketEntryLayout> ComputeBucketEntryLayout(
    uint64_t cursor, uint64_t value_size, uint64_t alignment) {
    if (value_size == 0) return std::nullopt;
    const auto offset = CheckedAlignUp(cursor, alignment);
    const auto aligned_size = CheckedAlignUp(value_size, alignment);
    if (!offset || !aligned_size ||
        *aligned_size > std::numeric_limits<uint64_t>::max() - *offset) {
        return std::nullopt;
    }
    return BucketEntryLayout{*offset, value_size, *aligned_size};
}

inline std::optional<BucketEntryLayout> RebuildBucketEntryLayout(
    uint64_t offset, uint64_t object_size, uint64_t alignment) {
    if (!IsValidBucketAlignment(alignment) || offset % alignment != 0) {
        return std::nullopt;
    }
    const auto layout =
        ComputeBucketEntryLayout(offset, object_size, alignment);
    if (!layout || layout->offset != offset) return std::nullopt;
    return layout;
}

inline DistributedFSDescriptor MakeBucketDescriptor(
    std::string data_path, const BucketEntryLayout& layout, int64_t bucket_id) {
    return DistributedFSDescriptor{std::move(data_path), layout.offset,
                                   layout.object_size, layout.aligned_size,
                                   static_cast<int>(bucket_id)};
}

inline constexpr int64_t kMaxBucketId =
    static_cast<int64_t>(std::numeric_limits<int32_t>::max());

}  // namespace mooncake
