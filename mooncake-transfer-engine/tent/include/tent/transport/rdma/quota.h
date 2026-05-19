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

#ifndef TENT_SELECTOR_H
#define TENT_SELECTOR_H

#include <atomic>
#include <vector>
#include <unordered_map>
#include <cmath>
#include <algorithm>
#include <cstdint>
#include <shared_mutex>
#include <mutex>

#include "tent/common/status.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {

// Bandwidth constants (Gbps)
static constexpr double kDefaultBwGbps = 400.0;
static constexpr double kMinBwGbps = 10.0;
static constexpr double kMaxBwGbps = 800.0;

class SharedSlotManager;

/**
 * @brief DeviceSelector implements NIC selection with two modes:
 *
 * 1. Baseline mode (smart_selection_enabled=false): Simple round-robin
 *    - Deterministic, no load tracking
 *    - All devices used equally
 *
 * 2. Smart mode (smart_selection_enabled=true): EWMA-based selection
 *    - Tracks global inflight bytes per device
 *    - Learns effective bandwidth via EWMA
 *    - Selects device with minimal predicted completion time
 *    - Supports multi-path for large requests
 *
 * Selection formula:
 *     predicted_time = (inflight + length) / ewma_bandwidth
 *
 * EWMA update:
 *     ewma_bandwidth <- alpha * ewma_bandwidth + (1 - alpha) *
 * observed_bandwidth
 */
class DeviceSelector {
   public:
    // Candidate device for allocation
    struct Candidate {
        int dev_id;
        double score;
        bool is_cross_numa;
    };

    struct DeviceInfo {
        int dev_id;
        double bw_gbps;
        int numa_id;
        uint64_t padding0[5];
        std::atomic<uint64_t> inflight_bytes{0};
        uint64_t padding1[7];
        std::atomic<double> ewma_bandwidth_bps{50e9};
        uint64_t padding2[7];
        std::atomic<uint64_t> total_bytes{0};
        uint64_t padding3[5];

        uint64_t getInflightBytes() const {
            return inflight_bytes.load(std::memory_order_relaxed);
        }

        void addInflight(uint64_t bytes) {
            inflight_bytes.fetch_add(bytes, std::memory_order_relaxed);
        }

        void releaseInflight(uint64_t bytes) {
            inflight_bytes.fetch_sub(bytes, std::memory_order_relaxed);
        }

        double getEwmaBandwidth() const {
            return ewma_bandwidth_bps.load(std::memory_order_relaxed);
        }

        double getTheoreticalBandwidth() const {
            if (bw_gbps >= kMinBwGbps && bw_gbps <= kMaxBwGbps)
                return bw_gbps * 1e9 / 8.0;
            return kDefaultBwGbps * 1e9 / 8.0;
        }
    };

   public:
    DeviceSelector() = default;
    ~DeviceSelector() = default;

    DeviceSelector(const DeviceSelector &) = delete;
    DeviceSelector &operator=(const DeviceSelector &) = delete;

    Status loadTopology(std::shared_ptr<Topology> &local_topology);

    std::shared_ptr<Topology> getTopology() const { return local_topology_; }

    Status enableSharedQuota(const std::string &shm_name);

    std::shared_ptr<SharedSlotManager> getSharedSlotManager() const {
        return slot_manager_;
    }

    // Allocate devices for a request (new API)
    // slice_bytes: pre-calculated slice size from rdma_transport to ensure
    // consistency
    Status allocate(uint64_t total_length, uint32_t num_slices,
                    uint64_t slice_bytes, const std::string &location,
                    std::vector<int> &slice_dev_ids, int priority = PRIO_HIGH,
                    uint64_t device_mask = ~0ULL);

    Status allocate(uint64_t length, const std::string &location,
                    int &chosen_dev_id);

    Status release(int dev_id, uint64_t length, double latency);

    void updateTrafficStats(int dev_id, uint64_t length) {
        auto it = devices_.find(dev_id);
        if (it != devices_.end()) {
            it->second.total_bytes.fetch_add(length, std::memory_order_relaxed);
        }
    }

    void setSmartSelection(bool enable) { smart_selection_enabled_ = enable; }
    bool getSmartSelection() const { return smart_selection_enabled_; }

    void setLearningRate(double alpha) {
        sched_params_.bandwidth_learning_rate = std::clamp(alpha, 0.0, 1.0);
    }

    int getDeviceRank(const std::string &location, int dev_id) const;

    void printTrafficStats();

    void fillDevicePriorities();
    int getDevicePriority(int dev_id) const;

    struct SchedulingParams {
        // NUMA tier penalties (rank 0 = local, should be smallest)
        double numa_tier_weights[Topology::DevicePriorityRanks] = {1.0, 5.0,
                                                                   10.0};

        // EWMA bandwidth learning rate (0.0 = no learning, 1.0 = full
        // adaptation)
        double bandwidth_learning_rate = 0.01;

        // Enable priority-based filtering
        bool enable_priority_filtering = true;

        // Local device priority rotation interval (microseconds)
        uint64_t local_rotation_interval_us = 200;

        // Score random jitter range (to avoid deterministic selection)
        double score_jitter_range = 1e-9;

        // Epsilon for division by zero protection
        double score_epsilon = 1e-12;

        // EWMA bandwidth bounds (multiplier of theoretical bandwidth)
        double ewma_min_multiplier = 0.1;   // 10% of theoretical
        double ewma_max_multiplier = 10.0;  // 1000% of theoretical

        // Default bandwidth (Gbps) when topology info unavailable
        double default_bandwidth_gbps = 400.0;
        double min_bandwidth_gbps = 10.0;
        double max_bandwidth_gbps = 800.0;

        // Shared slot rotation interval (milliseconds)
        int slot_rotation_interval_ms = 2;

        std::vector<int> device_base_priorities;
    };

    void setSchedulingParams(const SchedulingParams &params) {
        sched_params_ = params;
    }

    const SchedulingParams &getSchedulingParams() const {
        return sched_params_;
    }

   private:
    std::shared_ptr<Topology> local_topology_;
    std::unordered_map<int, DeviceInfo> devices_;
    std::shared_ptr<SharedSlotManager> slot_manager_;
    bool smart_selection_enabled_ = true;
    SchedulingParams sched_params_;

    Status buildCandidates(const Topology::MemEntry *entry,
                           uint64_t slice_bytes, uint64_t device_mask,
                           std::vector<Candidate> &candidates,
                           int request_priority = PRIO_HIGH);

    void selectSinglePath(const std::vector<Candidate> &candidates,
                          uint32_t num_slices, uint64_t total_length,
                          std::vector<int> &slice_dev_ids);

    void selectMultiPath(const std::vector<Candidate> &candidates,
                         uint32_t num_slices, uint64_t total_length,
                         std::vector<int> &slice_dev_ids,
                         bool probe_mode = false);
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SELECTOR_H