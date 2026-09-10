// Copyright 2026 KVCache.AI
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

#ifndef MOONCAKE_TENT_TRANSPORT_UB_RAIL_MONITOR_H_
#define MOONCAKE_TENT_TRANSPORT_UB_RAIL_MONITOR_H_

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "tent/transport/ub/slice.h"

namespace mooncake::tent::ub {

// Health and learned bandwidth belong to a physical rail, rather than to one
// endpoint incarnation. Endpoint generation is therefore reported in the
// statistics but intentionally excluded from this key.
struct UbRailKey {
    Topology::NicID local_topology_id{-1};
    SegmentID remote_segment_id{LOCAL_SEGMENT_ID};
    int remote_device_id{-1};

    bool operator==(const UbRailKey&) const = default;

    [[nodiscard]] bool valid() const {
        return local_topology_id >= 0 && remote_device_id >= 0;
    }

    static UbRailKey fromPath(const UbPostPath& path) {
        return {path.local_topology_id, path.remote_segment_id,
                path.remote_device_id};
    }
};

struct UbRailKeyHash {
    size_t operator()(const UbRailKey& key) const noexcept {
        size_t seed = std::hash<int>{}(key.local_topology_id);
        auto combine = [&seed](size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<SegmentID>{}(key.remote_segment_id));
        combine(std::hash<int>{}(key.remote_device_id));
        return seed;
    }
};

struct RailMonitorConfig {
    uint32_t error_threshold{3};
    uint64_t error_window_ns{10'000'000'000ULL};
    uint64_t cooldown_ns{30'000'000'000ULL};
    // Weight assigned to the newest completion sample.
    double ewma_alpha{0.2};

    [[nodiscard]] bool valid() const {
        return error_threshold != 0 && error_window_ns != 0 &&
               cooldown_ns != 0 && ewma_alpha > 0.0 && ewma_alpha <= 1.0;
    }
};

struct RailStats {
    UbRailKey key{};
    bool paused{false};
    uint32_t errors_in_window{0};
    uint64_t successful_completions{0};
    uint64_t completed_bytes{0};
    uint64_t completion_errors{0};
    uint64_t timeouts{0};
    uint64_t recoveries{0};
    uint64_t endpoint_rebuilds{0};
    uint64_t pauses{0};
    // Bytes/second and nanoseconds respectively. -1 means no valid sample.
    double ewma_bandwidth_bytes_per_second{-1.0};
    double ewma_latency_ns{-1.0};
    uint64_t last_success_ns{0};
    uint64_t last_error_ns{0};
    uint64_t pause_started_ns{0};
    uint64_t cooldown_until_ns{0};
    uint64_t latest_endpoint_generation{0};
};

// Thread-safe rolling health and telemetry for UB posting paths.
class RailMonitor {
   public:
    explicit RailMonitor(RailMonitorConfig config = {});

    RailMonitor(const RailMonitor&) = delete;
    RailMonitor& operator=(const RailMonitor&) = delete;

    // Rejects invalid configurations without changing the active one.
    bool configure(const RailMonitorConfig& config);
    [[nodiscard]] RailMonitorConfig config() const;

    // Registration is optional; all record operations create the rail lazily.
    bool registerPath(const UbPostPath& path);
    [[nodiscard]] bool available(const UbPostPath& path, uint64_t now_ns = 0);

    void recordSuccess(const UbPostPath& path, uint64_t bytes,
                       uint64_t latency_ns, uint64_t now_ns = 0);
    void recordError(const UbPostPath& path, uint64_t now_ns = 0);
    void recordTimeout(const UbPostPath& path, uint64_t now_ns = 0);
    // Records at most one rebuild for each endpoint generation on a physical
    // rail. Returns true when telemetry advanced, allowing EndpointStore to
    // call this safely from converging/retried rebuild paths.
    bool recordEndpointRebuild(const UbPostPath& path, uint64_t now_ns = 0);

    [[nodiscard]] RailStats stats(const UbPostPath& path, uint64_t now_ns = 0);
    [[nodiscard]] std::vector<RailStats> allStats(uint64_t now_ns = 0);

    // Adds the best usable remote path sample for each local device. Returns
    // -1 until at least one valid completion sample has been observed.
    [[nodiscard]] double aggregateBandwidth(uint64_t now_ns = 0);
    [[nodiscard]] size_t pathCount() const;

   private:
    struct RailState {
        RailStats stats{};
        std::deque<uint64_t> recent_errors;
        // Event timestamps may arrive out of order from different pollers.
        // Health decisions never move behind this per-rail watermark.
        uint64_t observed_through_ns{0};
        // Late errors at or before a completed recovery epoch must not
        // resurrect an already-expired pause.
        uint64_t ignore_errors_through_ns{0};
        // Endpoint generations are process-wide monotonic. Keep a separate
        // watermark from latest_endpoint_generation because path registration
        // may observe the replacement before rebuild telemetry is emitted.
        uint64_t recorded_rebuild_generation{0};
    };

    using RailMap = std::unordered_map<UbRailKey, RailState, UbRailKeyHash>;

    static uint64_t normalizedNow(uint64_t now_ns);
    static uint64_t deadlineAfter(uint64_t now_ns, uint64_t duration_ns);
    static uint64_t observeTimeLocked(RailState& state, uint64_t event_ns);
    RailState& getOrCreateLocked(const UbPostPath& path);
    static void insertErrorLocked(RailState& state, uint64_t event_ns);
    void pruneErrorsLocked(RailState& state, uint64_t now_ns);
    void refreshCooldownLocked(RailState& state, uint64_t now_ns);
    void recordFailureLocked(const UbPostPath& path, uint64_t now_ns,
                             bool timeout);

    mutable std::mutex mutex_;
    RailMonitorConfig config_;
    RailMap rails_;
};

using UbRailMonitor = RailMonitor;

}  // namespace mooncake::tent::ub

#endif  // MOONCAKE_TENT_TRANSPORT_UB_RAIL_MONITOR_H_
