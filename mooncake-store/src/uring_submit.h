#pragma once

#include <algorithm>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>
#include <sys/types.h>

namespace mooncake::detail {

// Bytes per SQE when @p remaining bytes are spread over up to @p depth SQEs:
// a power of two, at least @p min_chunk (the queue-depth split from #1500).
// Used for every contiguous read (a read converts nothing) and for writes
// larger than single_sqe_write_max().
inline size_t split_chunk_size(size_t remaining, unsigned depth,
                               size_t min_chunk) {
    return std::max(std::bit_ceil((remaining + depth - 1) / depth), min_chunk);
}

// Bytes per SQE for one contiguous write of @p remaining bytes: the whole
// write if it is at most @p single_sqe_max bytes, otherwise the queue-depth
// split.
inline size_t write_chunk_size(size_t remaining, unsigned depth,
                               size_t min_chunk, size_t single_sqe_max) {
    return remaining <= single_sqe_max
               ? remaining
               : split_chunk_size(remaining, depth, min_chunk);
}

// Upper bound of single_sqe_write_max(), whatever the device's limit.
inline constexpr size_t kSingleSqeWriteCap = 2 * 1024 * 1024;

// The largest contiguous write to a file on block device @p dev (a stat
// st_dev) that goes out as one SQE: twice the device's max_sectors_kb, at
// most kSingleSqeWriteCap. A partition uses its disk's queue. Returns 0, so
// that every write is split, when @p dev is not a block device (tmpfs,
// overlayfs, NFS, FUSE, btrfs) or its queue cannot be read under @p sysfs.
//
// Each SQE that writes into a preallocated (unwritten) range costs its own
// unwritten-extent conversion, so splitting a small write multiplies that
// cost. Measured with first writes of OffsetAllocator records (a 4 KiB
// header page plus the value), one thread, on a virtio-blk disk backed by
// one NVMe SSD, with the guest's max_sectors_kb set to 128, 256, 512 and
// 1280: one SQE was 9% to 156% faster for every value of up to
// max_sectors_kb, gained about 1% or less for values of 2 MiB or more, and
// was up to 10% slower on ext4 for values of four or more times
// max_sectors_kb. The limit is twice max_sectors_kb, not once, because the
// header page puts a record with a value of max_sectors_kb just above it.
size_t single_sqe_write_max(dev_t dev, const std::string& sysfs = "/sys");

struct UringSubmitResult {
    unsigned submitted = 0;
    unsigned pending = 0;
    int error = 0;
};

template <typename PendingFn, typename SubmitFn, typename YieldFn>
UringSubmitResult submit_all_pending(PendingFn&& pending_fn,
                                     SubmitFn&& submit_fn, YieldFn&& yield_fn,
                                     unsigned max_transient_retries = 64) {
    UringSubmitResult result;
    unsigned transient_retries = 0;

    while ((result.pending = pending_fn()) != 0) {
        int ret = submit_fn(result.pending);
        if (ret > 0) {
            if (static_cast<unsigned>(ret) > result.pending) {
                result.error = -EIO;
                return result;
            }
            result.submitted += static_cast<unsigned>(ret);
            transient_retries = 0;
            continue;
        }

        if (ret == -EINTR || ret == -EAGAIN || ret == -ENOMEM) {
            if (transient_retries++ < max_transient_retries) {
                yield_fn();
                continue;
            }
        }

        result.error = ret == 0 ? -EIO : ret;
        result.pending = pending_fn();
        return result;
    }

    return result;
}

}  // namespace mooncake::detail
