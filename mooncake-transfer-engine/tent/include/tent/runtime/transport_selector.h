// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file transport_selector.h
 * @brief Configuration-driven transport selection policy
 *
 * Transport selection is driven by configuration with pattern-based rules.
 *
 * Example configuration:
 * {
 *   "policy": [
 *     {
 *       "name": "high_prio_fast",
 *       "segment_type": "memory",
 *       "priority": 0,
 *       "devices": ["mlx5_0", "mlx5_1", "mlx5_2"],
 *       "transports": ["nvlink", "rdma", "shm"]
 *     },
 *     {
 *       "name": "low_prio_slow",
 *       "segment_type": "memory",
 *       "priority": 2,
 *       "devices": ["mlx5_0"],
 *       "transports": ["rdma", "tcp"]
 *     },
 *     {
 *       "name": "file_storage",
 *       "segment_type": "file",
 *       "transports": ["gds", "io_uring", "rdma"]
 *     }
 *   ]
 * }
 *
 * If no "policy" is configured, defaults to original behavior:
 * - File: GDS → IOURING → RDMA
 * - Memory: uses buffer_transports order
 */

#ifndef TENT_TRANSPORT_SELECTOR_H
#define TENT_TRANSPORT_SELECTOR_H

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/platform.h"

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mooncake {
namespace tent {

class Transport;

/**
 * @brief Selection context for a single request
 */
struct SelectionContext {
    SegmentType segment_type;       // File or Memory
    bool same_machine;              // Local or remote
    MemoryType local_memory_type;   // CPU, CUDA, etc.
    MemoryType remote_memory_type;  // CPU, CUDA, etc. (for remote)
    const std::vector<TransportType>*
        buffer_transports;  // Pointer to transports in buffer
    size_t transfer_size;   // Transfer size in bytes
    int priority_level;     // Request priority level (higher = more urgent)
};

/**
 * @brief Transport selection policy rule
 */
struct SelectionPolicy {
    // Basic identification
    std::string name;

    // Segment type filter
    SegmentType segment_type;

    // Location filter
    std::optional<bool> same_machine;  // nullopt = don't care

    // Memory type filters (supports patterns: "cuda", "cpu", "npu", "*" for
    // any)
    std::optional<std::string> local_memory_pattern;
    std::optional<std::string> remote_memory_pattern;

    // Size filter (nullopt = no limit)
    std::optional<uint64_t> min_size;  // Minimum transfer size
    std::optional<uint64_t> max_size;  // Maximum transfer size

    // Priority filter: exact match required (request.priority == priority)
    // nullopt = match any priority level
    std::optional<int> priority;

    // Device allocation: list of device names this policy can use
    // e.g., ["mlx5_0", "mlx5_1", "rocep5s0f0"]
    // Empty = use all available devices
    std::vector<std::string> devices;

    // Transport preference list (evaluated in order)
    std::vector<TransportType> transports;
};

/**
 * @brief Result of transport selection
 */
struct SelectionResult {
    TransportType transport = UNSPEC;
    uint64_t device_mask = ~0ULL;  // Bitmask of allowed devices (~0 = all)
};

/**
 * @brief Configuration-driven transport selector
 */
class TransportSelector {
   public:
    TransportSelector(std::shared_ptr<Config> config);

    /**
     * @brief Set topology for device name to ID conversion
     */
    void setTopology(std::shared_ptr<Topology> topology) {
        topology_ = topology;
    }

    /**
     * @brief Select the best transport for a given context
     * @param context Selection context
     * @param available_transports Array of available transports (indexed by
     * TransportType)
     * @param priority_offset Priority offset for fallback (0 = first choice)
     * @return SelectionResult containing transport type and device mask
     */
    SelectionResult select(
        const SelectionContext& context,
        const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
            available_transports,
        int priority_offset = 0);

    /**
     * @brief Parse transport type from string
     */
    static TransportType parseTransportType(const std::string& str);

    /**
     * @brief Get transport type name as string
     */
    static std::string transportTypeName(TransportType type);

   private:
    std::vector<SelectionPolicy> policies_;
    std::shared_ptr<Config> config_;
    std::shared_ptr<Topology> topology_;  // For device name to ID mapping

    /**
     * @brief Load policies from configuration
     */
    void loadPolicies();

    /**
     * @brief Check if a policy matches the context
     */
    bool matchesPolicy(const SelectionPolicy& policy,
                       const SelectionContext& context) const;

    /**
     * @brief Check if memory type matches pattern
     */
    bool matchesMemoryPattern(const std::string& pattern,
                              MemoryType type) const;

    /**
     * @brief Check if transport is available for the context
     */
    bool isTransportAvailable(
        TransportType type, const SelectionContext& context,
        const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
            available_transports) const;

    /**
     * @brief Get default policies (used when no config provided)
     */
    static std::vector<SelectionPolicy> getDefaultPolicies();
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_SELECTOR_H
