#pragma once

#include <cstdint>
#include <limits>
#include <span>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Logical byte ranges only. Physical keys and on-wire manifests remain
// specific to each storage provider.
struct ObjectShard {
    uint32_t total_shard = 1;
    uint32_t shard_id = 0;
    uint64_t offset = 0;
    uint64_t size = 0;
};

struct ObjectShardPlan {
    uint64_t total_size = 0;
    uint32_t total_shard = 1;
    bool inline_value = false;
    std::vector<ObjectShard> shards;
};

struct ObjectLayoutLimits {
    uint64_t max_value_size = 0;
    // Providers without a separate root format set this to max_value_size.
    uint64_t inline_value_size = 0;
    uint32_t max_shards = std::numeric_limits<uint32_t>::max();
};

tl::expected<uint64_t, ErrorCode> ObjectSlicesSize(
    std::span<const Slice> slices);
tl::expected<ObjectShardPlan, ErrorCode> PlanObjectShards(
    std::span<const Slice> slices, ObjectLayoutLimits limits);
tl::expected<ObjectShardPlan, ErrorCode> PlanObjectShards(
    uint64_t total_size, ObjectLayoutLimits limits);
// Returned slices borrow the caller's buffers; no payload copy is made.
tl::expected<std::vector<Slice>, ErrorCode> SliceObjectShard(
    std::span<const Slice> slices, const ObjectShard& shard);

}  // namespace mooncake
