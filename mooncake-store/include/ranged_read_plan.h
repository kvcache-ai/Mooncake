#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <span>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Offsets and lengths are bytes within an opaque immutable object.
using ByteRange = std::pair<uint64_t, uint64_t>;

struct ReadRangeFragment {
    size_t source_offset;
    size_t destination_offset;
    size_t length;
};

struct ReadRangePlan {
    std::vector<ReadRangeFragment> fragments;
    size_t output_size = 0;
    // Source-ordered views into the final output, only for complete checksums.
    std::vector<size_t> checksum_order;
    bool canonical_read = false;
};

inline tl::expected<ReadRangePlan, ErrorCode> PlanReadRanges(
    uint64_t object_size, std::span<const ByteRange> ranges,
    bool verify_checksum) {
    ReadRangePlan plan;
    plan.fragments.reserve(ranges.size());
    constexpr auto kMaxSize =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    if (object_size > std::numeric_limits<size_t>::max()) {
        return tl::unexpected(ErrorCode::BUFFER_OVERFLOW);
    }
    for (const auto& [offset, length] : ranges) {
        if (offset >= object_size || length == 0) continue;
        const auto bytes = std::min(length, object_size - offset);
        if (bytes > kMaxSize - plan.output_size ||
            bytes > std::numeric_limits<size_t>::max() - plan.output_size) {
            return tl::unexpected(ErrorCode::BUFFER_OVERFLOW);
        }
        const auto size = static_cast<size_t>(bytes);
        if (!plan.fragments.empty() && plan.fragments.back().source_offset +
                                               plan.fragments.back().length ==
                                           offset) {
            plan.fragments.back().length += size;
        } else {
            plan.fragments.push_back(
                {static_cast<size_t>(offset), plan.output_size, size});
        }
        plan.output_size += size;
    }
    // Prefix hits need neither sorting nor a complete-object checksum.
    if (!verify_checksum || plan.output_size < object_size ||
        plan.fragments.empty()) {
        return plan;
    }
    std::vector<size_t> order(plan.fragments.size());
    std::iota(order.begin(), order.end(), 0);
    auto by_source = [&](size_t a, size_t b) {
        return plan.fragments[a].source_offset <
               plan.fragments[b].source_offset;
    };
    if (!std::is_sorted(order.begin(), order.end(), by_source)) {
        std::sort(order.begin(), order.end(), by_source);
    }
    size_t covered = 0;
    bool overlap = false;
    for (size_t index : order) {
        const auto& fragment = plan.fragments[index];
        if (fragment.source_offset > covered) return plan;
        overlap |= fragment.source_offset < covered;
        covered = std::max(covered, fragment.source_offset + fragment.length);
    }
    if (covered == object_size) {
        plan.canonical_read = overlap;
        plan.checksum_order = std::move(order);
    }
    return plan;
}

}  // namespace mooncake
