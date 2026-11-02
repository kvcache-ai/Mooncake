// Copyright 2024 KVCache.AI

#include "egm_store_pool_config.h"

#include <algorithm>
#include <cstdlib>
#include <string_view>

#include <glog/logging.h>

#include "ascii_string.h"
#include "bool_parser.h"
#include "integer_parser.h"

namespace mooncake {
namespace {

bool IsPowerOfTwo(size_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

tl::expected<EgmStorePoolConfig, ErrorCode> ParseEgmStorePoolConfig(
    const ConfigDict& config, const std::string& protocol,
    size_t global_segment_size) {
    EgmStorePoolConfig parsed;
    const bool mnnvl = NormalizeEgmStoreProtocol(protocol) == "mnnvl";
    parsed.enabled = mnnvl;
    const auto enabled = config.find(CONFIG_KEY_ENABLE_EGM_STORE_POOL);
    if (enabled != config.end()) {
        const auto value = TryParseBool(
            enabled->second, {.token_set = BoolTokenSet::kTrueFalse});
        if (!value) {
            LOG(ERROR) << "Invalid enable_egm_store_pool: " << enabled->second;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (mnnvl && !*value && global_segment_size != 0) {
            LOG(ERROR) << "MNNVL Store capacity cannot disable EGM allocation";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        parsed.enabled = mnnvl || *value;
    }
    if (!parsed.enabled) return parsed;

    if (!mnnvl && (protocol != "nvlink" || global_segment_size == 0)) {
        LOG(ERROR) << "EGM Store Pool requires protocol=nvlink, "
                      "global_segment_size>0";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto nodes = config.find(CONFIG_KEY_EGM_NUMA_NODES);
    if (protocol == "nvlink_host") {
        if (const char* backend = std::getenv("MC_NVLINK_HOST_BACKEND")) {
            if (std::string_view(backend) != "fabric") {
                LOG(ERROR) << "nvlink_host only supports the Fabric backend";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
        }
        if (nodes == config.end()) {
            parsed.nearest_numa_node = true;
            if (const char* expression = std::getenv("MC_FABRIC_NUMA")) {
                const auto node = TryParseInteger<int>(
                    expression, {.trim_ascii_whitespace = true});
                if (!node || *node < -1) {
                    LOG(ERROR) << "Invalid MC_FABRIC_NUMA";
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                if (*node >= 0) {
                    parsed.nearest_numa_node = false;
                    parsed.auto_numa_nodes = false;
                    parsed.numa_nodes = {*node};
                }
            }
            return parsed;
        }
    }
    if (nodes == config.end() || TrimAsciiWhitespace(nodes->second) == "auto") {
        return parsed;
    }
    const std::string_view expression(nodes->second);
    size_t begin = 0;
    while (begin <= expression.size()) {
        const size_t comma = expression.find(',', begin);
        const size_t end =
            comma == std::string_view::npos ? expression.size() : comma;
        const auto token =
            TrimAsciiWhitespace(expression.substr(begin, end - begin));
        const auto node = TryParseInteger<int>(token);
        if (!node || *node < 0) {
            LOG(ERROR) << "Invalid EGM NUMA node: " << token;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        parsed.numa_nodes.push_back(*node);
        if (comma == std::string_view::npos) break;
        begin = comma + 1;
    }
    std::sort(parsed.numa_nodes.begin(), parsed.numa_nodes.end());
    parsed.numa_nodes.erase(
        std::unique(parsed.numa_nodes.begin(), parsed.numa_nodes.end()),
        parsed.numa_nodes.end());
    parsed.auto_numa_nodes = false;
    return parsed;
}

tl::expected<EgmStorePoolCapacity, ErrorCode> CalculateEgmStorePoolCapacity(
    size_t requested_size, const std::vector<size_t>& node_granularities,
    size_t store_alignment, size_t max_mr_size) {
    if (node_granularities.empty() || !IsPowerOfTwo(store_alignment)) {
        LOG(ERROR) << "EGM capacity requires NUMA nodes and a power-of-two "
                      "Store alignment";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t alignment = store_alignment;
    for (const size_t granularity : node_granularities) {
        if (!IsPowerOfTwo(granularity)) {
            LOG(ERROR) << "EGM allocation granularity must be a power of two: "
                       << granularity;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        alignment = std::max(alignment, granularity);
    }

    const size_t max_chunk_size = (max_mr_size / alignment) * alignment;
    const size_t units = requested_size / alignment;
    if (max_chunk_size == 0 || units < node_granularities.size()) {
        LOG(ERROR) << "EGM capacity must provide one aligned unit per NUMA "
                      "node and max_mr_size must fit one aligned unit";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    EgmStorePoolCapacity capacity;
    capacity.alignment = alignment;
    capacity.max_chunk_size = max_chunk_size;
    const size_t base_units = units / node_granularities.size();
    const size_t remainder = units % node_granularities.size();
    capacity.node_sizes.reserve(node_granularities.size());
    for (size_t index = 0; index < node_granularities.size(); ++index) {
        capacity.node_sizes.push_back(
            (base_units + (index < remainder ? 1 : 0)) * alignment);
    }
    return capacity;
}

}  // namespace mooncake
