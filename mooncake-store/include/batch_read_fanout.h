// Duplicate-key handling for batch reads.
//
// The transfer batch structures are keyed by string, so a batch that names
// the same key more than once would collapse every occurrence onto one
// destination. These helpers give every read path the same contract instead:
// transfer each unique key exactly once (first occurrence is the primary),
// then fan the verified bytes out to each duplicate's own destination with a
// device-aware copy, and mirror the primary's result (success or error).
//
// PlanDuplicateKeys decides who transfers, FanOutDuplicates propagates
// results, and CopyMaybeDevice / CopySlicesMaybeDevice move the bytes for
// contiguous and scatter-gather destinations that may live on host or device.

#pragma once

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "device/accelerator_registry.h"
#include "types.h"

namespace mooncake {

// Which ops transfer (unique keys, in first-occurrence order) and which wait
// for a local fan-out copy, as (duplicate op, primary op) pairs.
struct DuplicateKeyPlan {
    std::vector<size_t> primaries;
    std::vector<std::pair<size_t, size_t>> duplicates;
};

// Op must have a `.key` string member.
template <typename Op>
DuplicateKeyPlan PlanDuplicateKeys(const std::vector<Op> &ops) {
    DuplicateKeyPlan plan;
    std::unordered_map<std::string, size_t> first_op_by_key;
    for (size_t i = 0; i < ops.size(); ++i) {
        auto [it, inserted] = first_op_by_key.emplace(ops[i].key, i);
        if (inserted) {
            plan.primaries.push_back(i);
        } else {
            plan.duplicates.emplace_back(i, it->second);
        }
    }
    return plan;
}

// Contiguous copy between buffers that may each be host or device resident.
inline tl::expected<void, ErrorCode> CopyMaybeDevice(
    void *dst, const void *src, size_t size, const std::string &context) {
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    if (!runtime_accelerator.CopyAuto(dst, src, size)) {
        LOG(ERROR) << "copy failed: " << context;
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    return {};
}

// Copy total_size bytes between two slice lists whose chunking may differ
// (e.g. two callers carved the same object into different per-buffer
// layouts). Single-slice lists are the contiguous case.
inline tl::expected<void, ErrorCode> CopySlicesMaybeDevice(
    const std::vector<Slice> &dst, const std::vector<Slice> &src,
    uint64_t total_size, const std::string &context) {
    size_t src_idx = 0, dst_idx = 0, src_off = 0, dst_off = 0;
    uint64_t remaining = total_size;
    while (remaining > 0) {
        while (src_idx < src.size() && src_off == src[src_idx].size) {
            ++src_idx;
            src_off = 0;
        }
        while (dst_idx < dst.size() && dst_off == dst[dst_idx].size) {
            ++dst_idx;
            dst_off = 0;
        }
        if (src_idx >= src.size() || dst_idx >= dst.size()) {
            LOG(ERROR) << "slice lists underflow at "
                       << (total_size - remaining) << "/" << total_size
                       << " bytes: " << context;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const size_t chunk =
            std::min({src[src_idx].size - src_off, dst[dst_idx].size - dst_off,
                      static_cast<size_t>(remaining)});
        if (auto r = CopyMaybeDevice(
                static_cast<char *>(dst[dst_idx].ptr) + dst_off,
                static_cast<const char *>(src[src_idx].ptr) + src_off, chunk,
                context);
            !r) {
            return r;
        }
        src_off += chunk;
        dst_off += chunk;
        remaining -= chunk;
    }
    return {};
}

// One duplicate fan-out: move the primary's verified bytes into the
// duplicate's destination, then mirror the primary's result slot.
struct DuplicateFanOutJob {
    size_t dup_result_index;
    size_t primary_result_index;
    std::string key;
    uint64_t dup_bytes;
    uint64_t primary_bytes;
    std::function<tl::expected<void, ErrorCode>()> copy;
};

// Zip parallel buffer/size lists into slices.
inline std::vector<Slice> SlicesFromBuffers(const std::vector<void *> &buffers,
                                            const std::vector<size_t> &sizes) {
    std::vector<Slice> slices;
    slices.reserve(buffers.size());
    for (size_t j = 0; j < buffers.size(); ++j) {
        slices.push_back(Slice{buffers[j], sizes[j]});
    }
    return slices;
}

// A duplicate whose primary failed inherits the primary's error rather than
// a pre-set success; a size mismatch between same-key destinations fails the
// duplicate loudly instead of copying.
inline void FanOutDuplicates(
    const std::vector<DuplicateFanOutJob> &jobs,
    std::vector<tl::expected<int64_t, ErrorCode>> &results) {
    for (const auto &job : jobs) {
        const auto &primary_result = results[job.primary_result_index];
        if (!primary_result) {
            LOG(ERROR) << "Read failed for key '" << job.key
                       << "': primary read for the same key failed";
            results[job.dup_result_index] =
                tl::make_unexpected(primary_result.error());
            continue;
        }
        if (job.dup_bytes != job.primary_bytes) {
            LOG(ERROR) << "Size mismatch for duplicate key '" << job.key
                       << "': " << job.dup_bytes << " vs " << job.primary_bytes;
            results[job.dup_result_index] =
                tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            continue;
        }
        if (auto r = job.copy(); !r) {
            results[job.dup_result_index] = tl::make_unexpected(r.error());
            continue;
        }
        results[job.dup_result_index] = *primary_result;
    }
}

}  // namespace mooncake
