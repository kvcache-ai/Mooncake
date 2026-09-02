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
 *     predicted_time = (inflight + slice_bytes) / ewma_bandwidth
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
        // Negotiated link speed in Gbps. setDeviceBandwidth() may rewrite it
        // after traffic has started while workers read it in release(), so
        // it is atomic.
        std::atomic<double> bw_gbps{0.0};
        int numa_id;
        // False for a NIC that cannot carry traffic: context never
        // constructed, or port down. Excluded from candidate construction
        // and from every aggregate; the bandwidth fields are meaningless
        // while false. Fits the alignment hole after numa_id, so the padding
        // below still puts inflight_bytes on its own cache line.
        std::atomic<bool> available{true};
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
    };

   public:
    DeviceSelector() = default;
    ~DeviceSelector() = default;

    DeviceSelector(const DeviceSelector &) = delete;
    DeviceSelector &operator=(const DeviceSelector &) = delete;

    Status loadTopology(std::shared_ptr<Topology> &local_topology);

    // Record the link speed ibv_query_port reported for dev_id and re-seed
    // its EWMA from it, replacing whatever was learned and re-deriving the
    // clamp. A value outside [min, max]_bandwidth_gbps (including 0 =
    // unknown) falls back to default_bandwidth_gbps. Call after
    // setSchedulingParams; may be called again at runtime when the link
    // renegotiates.
    Status setDeviceBandwidth(int dev_id, double gbps);

    // Mark dev_id able (or unable) to carry traffic. Unavailable devices are
    // never selected and do not count toward the aggregate bandwidth. May be
    // called from the monitor thread while workers allocate; readers see the
    // flag on their next allocate/aggregate.
    Status setDeviceAvailable(int dev_id, bool available);
    // False for an unknown device.
    bool isDeviceAvailable(int dev_id) const;

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

    // Allocate one device for the per-slice path. Small requests do not go
    // through the aggregate allocator, but must still honor the request
    // priority and transport policy's device mask. Keep the three-argument
    // overload above for source and binary compatibility.
    Status allocate(uint64_t length, const std::string &location,
                    int &chosen_dev_id, int priority, uint64_t device_mask);

    Status release(int dev_id, uint64_t length, double latency);

    Status getNicLoadStats(std::vector<NicLoadStats> &stats) const;

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

    double getAggregateEwmaBandwidth() const;

    void fillDevicePriorities();
    int getDevicePriority(int dev_id) const;

    struct SchedulingParams {
        // NUMA tier penalties (rank 0 = local, should be smallest)
        double numa_tier_weights[Topology::DevicePriorityRanks] = {1.0, 5.0,
                                                                   10.0};

        // Hard-exclude known cross-NUMA NICs; unknown NUMA keeps
        // numa_tier_weights.
        bool strict_local_numa = false;

        // EWMA bandwidth learning rate (0.0 = full adaptation, 1.0 = no
        // learning)
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

        // Bandwidth (Gbps) assumed for a device whose link speed is unknown
        // or outside [min, max]
        double default_bandwidth_gbps = 400.0;
        double min_bandwidth_gbps = 10.0;   // Minimum valid NIC bandwidth
        double max_bandwidth_gbps = 800.0;  // Maximum valid NIC bandwidth

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

    // Startup warning if the flag excludes every NIC or classifies none.
    void auditStrictLocalNuma() const;

   private:
    std::shared_ptr<Topology> local_topology_;
    std::unordered_map<int, DeviceInfo> devices_;
    std::shared_ptr<SharedSlotManager> slot_manager_;
    bool smart_selection_enabled_ = true;
    SchedulingParams sched_params_;

    // Bytes/s the device is rated for: bw_gbps when it is inside the
    // configured [min, max], default_bandwidth_gbps otherwise.
    double theoreticalBandwidth(const DeviceInfo &dev) const;

    // Known to the selector and currently able to carry traffic.
    bool usable(int dev_id) const {
        auto it = devices_.find(dev_id);
        return it != devices_.end() &&
               it->second.available.load(std::memory_order_relaxed);
    }

    const char *noEligibleDeviceReason() const {
        return sched_params_.strict_local_numa
                   ? "no eligible devices (strict_local_numa excludes "
                     "cross-NUMA NICs)"
                   : "no eligible devices";
    }

    bool isNumaEligible(const Topology::MemEntry *entry, int dev_id) const {
        if (!sched_params_.strict_local_numa) return true;
        if (!entry || !local_topology_) return true;
        return !local_topology_->isCrossNuma(*entry, dev_id);
    }

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
