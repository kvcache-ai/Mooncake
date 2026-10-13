#include "nof_io_layout.h"

#include <limits>

namespace mooncake {

bool BuildNoFIoLayout(uint32_t block_size, uint64_t disk_offset,
                      uintptr_t buffer_address, size_t size,
                      NoFIoLayout& layout) {
    if (block_size == 0 || size == 0) {
        return false;
    }

    // The object has to start on a block boundary, otherwise its first byte
    // is not addressable by an NVMe command. Reserving the alignment is a
    // master side property of the allocation, so reject instead of guessing.
    if (disk_offset % block_size != 0) {
        return false;
    }

    const uint64_t tail_bytes = size % block_size;
    const uint64_t head_bytes = size - tail_bytes;
    const uint64_t padded_size =
        tail_bytes == 0 ? size : head_bytes + block_size;
    // SpdkNofTask counts its remaining blocks in an int, so a larger
    // request would go negative there and never be submitted or completed.
    const uint64_t padded_blocks = padded_size / block_size;
    if (padded_size < size ||
        padded_blocks >
            static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return false;
    }

    // The last LBA of the request must not wrap around.
    const uint64_t base_lba = disk_offset / block_size;
    if (base_lba > std::numeric_limits<uint64_t>::max() - padded_blocks) {
        return false;
    }
    std::vector<NoFIoSegment> segments;
    if (buffer_address % block_size != 0) {
        // The direct path needs a block aligned address, so the whole payload
        // goes through a bounce buffer.
        segments.push_back(NoFIoSegment{
            NoFIoSegmentKind::kStaged, base_lba,
            static_cast<uint32_t>(padded_size / block_size), 0, size});
    } else {
        if (head_bytes > 0) {
            segments.push_back(NoFIoSegment{
                NoFIoSegmentKind::kDirect, base_lba,
                static_cast<uint32_t>(head_bytes / block_size), 0, head_bytes});
        }
        if (tail_bytes > 0) {
            // Only the partial trailing block is staged, so at most
            // block_size - 1 payload bytes are ever copied.
            segments.push_back(NoFIoSegment{NoFIoSegmentKind::kStaged,
                                            base_lba + head_bytes / block_size,
                                            1, head_bytes, tail_bytes});
        }
    }

    layout.segments = std::move(segments);
    return true;
}

}  // namespace mooncake
