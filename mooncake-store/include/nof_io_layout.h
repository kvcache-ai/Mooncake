#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace mooncake {

/**
 * @brief How a single NoF I/O reaches the caller's buffer.
 */
enum class NoFIoSegmentKind {
    // DMA runs straight against the caller's buffer.
    kDirect,
    // DMA runs against a block sized bounce buffer, and the valid bytes are
    // copied between that buffer and the caller's buffer.
    kStaged,
};

/**
 * @brief One block aligned NVMe I/O derived from a caller request.
 *
 * The payload always starts at the beginning of the segment, because the
 * object starts at a block boundary on disk. For a staged segment the bounce
 * buffer holds `lba_count * block_size` bytes and its first `payload_size`
 * bytes map to `[buffer_offset, buffer_offset + payload_size)` of the
 * caller's buffer; the remaining bytes are padding that must never be
 * exposed to the caller.
 */
struct NoFIoSegment {
    NoFIoSegmentKind kind = NoFIoSegmentKind::kDirect;
    uint64_t lba = 0;
    uint32_t lba_count = 0;
    uint64_t buffer_offset = 0;
    uint64_t payload_size = 0;
};

/**
 * @brief The block aligned I/Os a single NoF request expands into.
 */
struct NoFIoLayout {
    std::vector<NoFIoSegment> segments;

    // True when the request can be issued without any bounce buffer.
    bool is_direct() const {
        for (const auto& segment : segments) {
            if (segment.kind != NoFIoSegmentKind::kDirect) {
                return false;
            }
        }
        return true;
    }

    // Total bounce buffer bytes required by the layout.
    uint64_t staging_bytes(uint32_t block_size) const {
        uint64_t bytes = 0;
        for (const auto& segment : segments) {
            if (segment.kind == NoFIoSegmentKind::kStaged) {
                bytes += static_cast<uint64_t>(segment.lba_count) * block_size;
            }
        }
        return bytes;
    }
};

/**
 * @brief Build the block aligned I/O layout for a NoF request.
 *
 * Callers such as LMCache and SGLang pass logical object sizes and buffer
 * addresses that are not multiples of the namespace block size. The layout
 * pads the I/O length up to a block multiple while keeping every payload
 * access inside the caller's buffer:
 *   - a block aligned buffer keeps its whole block prefix direct and only
 *     stages the partial trailing block;
 *   - an unaligned buffer address is staged in full, because the direct DMA
 *     path requires a block aligned address.
 *
 * @param block_size Namespace block size in bytes, must be non-zero.
 * @param disk_offset Byte offset of the object inside the namespace. It must
 *        be block aligned; the client cannot repair a misaligned allocation.
 * @param buffer_address Address of the caller's buffer.
 * @param size Logical object size in bytes, must be non-zero.
 * @param layout Populated on success, left untouched on failure.
 * @return true when a valid layout was produced.
 */
bool BuildNoFIoLayout(uint32_t block_size, uint64_t disk_offset,
                      uintptr_t buffer_address, size_t size,
                      NoFIoLayout& layout);

}  // namespace mooncake
