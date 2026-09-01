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
 * - File: GDS -> IOURING -> RDMA
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
    int priority_level;     // Request priority level (lower = more urgent)
    std::optional<std::string>
        policy_name;  // Optional: bind to specific policy by name
    IntentType intent_type{
        IntentType::INTENT_UNSPEC};  // Business intent for policy matching
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

    // Superseded, and read by nothing: link-layer QoS is a property of the QP
    // a transfer lands on, so it is defined per pool in
    // transports/rdma/endpoint/qp_pools and applied at QP setup
    // (endpoint.cpp, setupOneQP). Still parsed so a config that sets them here
    // is reported rather than silently ignored.
    std::optional<int> service_level;
    std::optional<int> traffic_class;
    // Named QP pool this policy's traffic lands on. Routed end to end:
    // SelectionResult -> TaskInfo -> SubBatch -> RdmaTask -> selectQpInPool().
    // Unset (or a name no configured pool matches) sprays across *all* QPs --
    // there is no reserved default pool, so once qp_pools is configured such
    // traffic also lands on the named pools' QPs and inherits their SL/TC.
    std::optional<std::string> qp_pool;

    // Optional business-intent filter. nullopt preserves the historical
    // catch-all behavior; otherwise the request intent must match exactly.
    std::optional<IntentType> intent_type;

    // `devices` resolved against the topology by setTopology(), so select()
    // does not repeat a name lookup per request. ~0ULL means "all devices":
    // no device filter, no topology yet, or a filter that resolved to nothing.
    uint64_t resolved_device_mask{~0ULL};
};

/**
 * @brief Result of transport selection
 */
struct SelectionResult {
    TransportType transport = UNSPEC;
    uint64_t device_mask = ~0ULL;  // Bitmask of allowed devices (~0 = all)
    // Copied from the matched policy for diagnostics only; QP setup takes
    // SL/TC from the QP pool, not from here. See SelectionPolicy.
    std::optional<int> service_level;
    std::optional<int> traffic_class;
    // Consumed: names the QP pool the transfer is routed to.
    std::optional<std::string> qp_pool;
};

enum class TransportReachabilityMode {
    DirectEndpoint,
    InstalledOnly,
};

/**
 * @brief Configuration-driven transport selector
 */
class TransportSelector {
   public:
    TransportSelector(std::shared_ptr<Config> config);

    /**
     * @brief Set topology for device name to ID conversion
     *
     * Resolves every policy's `devices` list into a mask here, once, and
     * reports what did not resolve. Safe to call again if the topology is
     * replaced.
     */
    void setTopology(std::shared_ptr<Topology> topology);

    /**
     * @brief Select the best transport for a given context
     * @param context Selection context
     * @param available_transports Array of available transports
     * @param transport_index Transport selection index (0 = first choice, 1 =
     * second, ...)
     * @param hint The caller's preferred transport for this request;
     * UNSPEC means no hint.
     * @return Selection result with transport type and device mask
     */
    SelectionResult select(
        const SelectionContext& context,
        const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
            available_transports,
        int transport_index = 0, TransportType hint = UNSPEC);

    /**
     * @brief List policy/hint-authorized candidates in ranking order.
     *
     * DirectEndpoint preserves select() behavior by requiring the transport to
     * reach the request's original source/target memory pair. InstalledOnly
     * only requires that the transport exists, so callers that synthesize
     * staged paths can check per-hop reachability after choosing stage memory.
     */
    std::vector<SelectionResult> listCandidates(
        const SelectionContext& context,
        const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
            available_transports,
        TransportType hint = UNSPEC,
        TransportReachabilityMode reachability =
            TransportReachabilityMode::DirectEndpoint) const;

    /**
     * @brief Enable legacy mode (skip TransportSelector, use original logic)
     */
    void setLegacyMode(bool enabled) { legacy_mode_ = enabled; }

    /**
     * @brief Check if legacy mode is enabled
     */
    bool isLegacyMode() const { return legacy_mode_; }

    static std::optional<std::vector<TransportType>> reorderWithHint(
        const std::vector<TransportType>& raw, TransportType hint);

   private:
    // Width of SelectionResult::device_mask; a NIC past this index cannot be
    // named by a policy at all.
    static constexpr int kDeviceMaskBits = 64;

    std::vector<SelectionPolicy> getDefaultPolicies();
    void loadPolicies();
    void resolveDeviceMasks();
    bool matchesMemoryPattern(const std::string& pattern,
                              MemoryType type) const;
    bool matchesPolicy(const SelectionPolicy& policy,
                       const SelectionContext& context) const;
    const SelectionPolicy* findMatchingPolicy(
        const SelectionContext& context) const;
    SelectionResult policyResult(const SelectionContext& context) const;
    bool isTransportAvailable(
        TransportType type, const SelectionContext& context,
        const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
            available_transports) const;
    bool isTransportInstalled(
        TransportType type, const SelectionContext& context,
        const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
            available_transports) const;

    std::shared_ptr<Config> config_;
    std::shared_ptr<Topology> topology_;
    std::vector<SelectionPolicy> policies_;
    bool legacy_mode_{false};  // If true, skip selector and return empty result
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_SELECTOR_H
