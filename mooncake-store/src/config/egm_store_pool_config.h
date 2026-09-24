// Copyright 2024 KVCache.AI

#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct EgmStorePoolConfig {
    bool enabled = false;
    bool auto_numa_nodes = true;
    std::vector<int> numa_nodes;
};

tl::expected<EgmStorePoolConfig, ErrorCode> ParseEgmStorePoolConfig(
    const ConfigDict& config, const std::string& protocol,
    size_t global_segment_size, size_t local_buffer_size);

struct EgmStorePoolCapacity {
    size_t alignment = 0;
    size_t max_chunk_size = 0;
    // Aligned capacities in the same order as the input granularities.
    std::vector<size_t> node_sizes;
};

tl::expected<EgmStorePoolCapacity, ErrorCode> CalculateEgmStorePoolCapacity(
    size_t requested_size, const std::vector<size_t>& node_granularities,
    size_t store_alignment, size_t max_mr_size);

}  // namespace mooncake
