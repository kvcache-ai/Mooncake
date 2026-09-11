#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <string>

#include "replica.h"

namespace mooncake {

/**
 * @brief Canonical on-disk layout of one bucket entry.
 *
 * Every bucket entry is laid out as
 *
 *   entry_start   = AlignUp(previous_entry_end, alignment)
 *   [value bytes][padding]
 *   value_offset  = entry_start
 *   entry_size    = value.size()
 *   reserved_size = AlignUp(entry_size, alignment)
 *
 * All code paths (Allocate, BatchAllocate, recovery, BatchWrite, BatchRead and
 * eviction candidate construction) must derive offsets through this helper so
 * a single definition governs the layout. Both the value offset and reserved
 * size are alignment-aligned, which lets aligned objects be read directly into
 * caller-owned buffers with O_DIRECT.
 */
struct BucketEntryLayout {
    uint64_t entry_start = 0;    // aligned start of the whole entry
    uint64_t value_offset = 0;   // where the value bytes begin
    uint64_t entry_size = 0;     // value bytes, without padding
    uint64_t reserved_size = 0;  // entry_size rounded up to `alignment`

    uint64_t entry_end() const { return entry_start + reserved_size; }
};

/**
 * @brief True when `alignment` is a usable power of two.
 */
inline bool IsValidBucketAlignment(uint64_t alignment) {
    return alignment != 0 && (alignment & (alignment - 1)) == 0;
}

/**
 * @brief Overflow-checked round up of `value` to a power-of-two `alignment`.
 */
inline std::optional<uint64_t> CheckedAlignUp(uint64_t value,
                                              uint64_t alignment) {
    if (!IsValidBucketAlignment(alignment)) return std::nullopt;
    if (value > std::numeric_limits<uint64_t>::max() - (alignment - 1)) {
        return std::nullopt;
    }
    return (value + alignment - 1) & ~(alignment - 1);
}

/**
 * @brief Compute the layout of an entry placed at or after `cursor`.
 *
 * Returns std::nullopt when `alignment` is invalid, when the value size is
 * zero, or when any intermediate operation would overflow. Callers are
 * responsible for the separate capacity check
 * (`layout.entry_end() <= bucket_capacity`).
 */
inline std::optional<BucketEntryLayout> ComputeBucketEntryLayout(
    uint64_t cursor, uint64_t value_size, uint64_t alignment) {
    if (!IsValidBucketAlignment(alignment)) return std::nullopt;
    if (value_size == 0) return std::nullopt;

    auto entry_start = CheckedAlignUp(cursor, alignment);
    if (!entry_start) return std::nullopt;

    auto reserved_size = CheckedAlignUp(value_size, alignment);
    if (!reserved_size) return std::nullopt;
    constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
    if (*reserved_size > kMax - *entry_start) return std::nullopt;

    BucketEntryLayout layout;
    layout.entry_start = *entry_start;
    layout.value_offset = *entry_start;
    layout.entry_size = value_size;
    layout.reserved_size = *reserved_size;
    return layout;
}

/**
 * @brief Reconstruct the layout of an already-placed entry.
 *
 * Used by recovery and by eviction-candidate construction, where the entry
 * start offset was previously persisted. Validates that the recorded start is
 * aligned and that the derived sizes do not overflow.
 */
inline std::optional<BucketEntryLayout> RebuildBucketEntryLayout(
    uint64_t entry_start, uint64_t value_size, uint64_t alignment) {
    if (!IsValidBucketAlignment(alignment)) return std::nullopt;
    if (entry_start % alignment != 0) return std::nullopt;
    auto layout = ComputeBucketEntryLayout(entry_start, value_size, alignment);
    if (!layout || layout->entry_start != entry_start) return std::nullopt;
    return layout;
}

/**
 * @brief Build the descriptor for a bucket entry.
 *
 * This is the single construction site for bucket-mode descriptors, shared by
 * Allocate, BatchAllocate, recovery and eviction so all four agree field by
 * field.
 */
inline DistributedFSDescriptor MakeBucketDescriptor(
    std::string data_path, const BucketEntryLayout& layout, uint64_t value_size,
    int64_t bucket_id) {
    DistributedFSDescriptor descriptor;
    descriptor.file_path = std::move(data_path);
    descriptor.offset = layout.value_offset;
    descriptor.object_size = value_size;
    descriptor.aligned_size = layout.reserved_size;
    descriptor.shard_idx = static_cast<int>(bucket_id);
    return descriptor;
}

/**
 * @brief Largest bucket id that survives the round-trip through
 * `DistributedFSDescriptor::shard_idx` (an `int`, serialized as int32).
 */
inline constexpr int64_t kMaxBucketId =
    static_cast<int64_t>(std::numeric_limits<int32_t>::max());

}  // namespace mooncake
