#include "storage/object_layout.h"

#include <algorithm>
#include <limits>

namespace mooncake {

tl::expected<uint64_t, ErrorCode> ObjectSlicesSize(
    std::span<const Slice> slices) {
    uint64_t total = 0;
    for (const auto& slice : slices) {
        if ((slice.ptr == nullptr && slice.size != 0) ||
            slice.size > std::numeric_limits<uint64_t>::max() - total) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total += static_cast<uint64_t>(slice.size);
    }
    return total;
}

tl::expected<ObjectShardPlan, ErrorCode> PlanObjectShards(
    std::span<const Slice> slices, ObjectLayoutLimits limits) {
    auto total_size = ObjectSlicesSize(slices);
    if (!total_size) {
        return tl::make_unexpected(total_size.error());
    }
    return PlanObjectShards(total_size.value(), limits);
}

tl::expected<ObjectShardPlan, ErrorCode> PlanObjectShards(
    uint64_t total_size, ObjectLayoutLimits limits) {
    if (limits.max_value_size == 0 ||
        limits.inline_value_size > limits.max_value_size ||
        limits.max_shards == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const uint64_t shard_count =
        total_size == 0 ? 1 : 1 + (total_size - 1) / limits.max_value_size;
    if (shard_count > limits.max_shards) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ObjectShardPlan plan;
    plan.total_size = total_size;
    plan.total_shard = static_cast<uint32_t>(shard_count);
    plan.inline_value = total_size <= limits.inline_value_size;
    plan.shards.reserve(plan.total_shard);
    for (uint32_t id = 0; id < plan.total_shard; ++id) {
        const uint64_t offset =
            static_cast<uint64_t>(id) * limits.max_value_size;
        plan.shards.push_back(ObjectShard{
            .total_shard = plan.total_shard,
            .shard_id = id,
            .offset = offset,
            .size = std::min(total_size - offset, limits.max_value_size),
        });
    }
    return plan;
}

tl::expected<std::vector<Slice>, ErrorCode> SliceObjectShard(
    std::span<const Slice> slices, const ObjectShard& shard) {
    if (shard.total_shard == 0 || shard.shard_id >= shard.total_shard) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto total_size = ObjectSlicesSize(slices);
    if (!total_size || shard.offset > *total_size ||
        shard.size > *total_size - shard.offset) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<Slice> result;
    if (shard.size == 0) return result;
    uint64_t cursor = 0;
    const uint64_t end = shard.offset + shard.size;
    for (const auto& slice : slices) {
        const uint64_t slice_begin = cursor;
        const uint64_t slice_end = cursor + slice.size;
        cursor = slice_end;
        if (slice_end <= shard.offset || slice_begin >= end) continue;

        const uint64_t begin = std::max(slice_begin, shard.offset);
        const uint64_t overlap_end = std::min(slice_end, end);
        auto* base = static_cast<const char*>(slice.ptr);
        result.push_back(Slice{
            const_cast<char*>(base + static_cast<size_t>(begin - slice_begin)),
            static_cast<size_t>(overlap_end - begin),
        });
    }
    return result;
}

}  // namespace mooncake
