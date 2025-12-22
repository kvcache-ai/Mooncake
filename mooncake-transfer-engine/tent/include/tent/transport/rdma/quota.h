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

#ifndef TENT_QUOTA_H
#define TENT_QUOTA_H

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

class SharedQuotaManager;

/**
 * @brief DeviceQuota implements NIC selection based on adaptive feedback.
 *
 * Each NIC maintains a smoothed estimate of its average service time,
 * updated after each request completes. The allocator predicts the total
 * completion time of each NIC as:
 *
 *     predicted_time = (active_bytes / bandwidth) + avg_service_time
 *
 * and selects the NIC with the smallest predicted_time.
 *
 * The estimator is updated using exponential smoothing:
 *
 *     avg_service_time <- (1 - alpha) * avg_service_time + alpha *
 * observed_time
 */
class DeviceQuota {
   public:
    struct DeviceInfo {
        int dev_id;
        double bw_gbps;
        int numa_id;
        uint64_t padding0[5];
        std::atomic<uint64_t> active_bytes{0};
        uint64_t padding1[7];
        std::atomic<uint64_t> diffusion_active_bytes{0};
        uint64_t padding2[7];
        std::atomic<double> beta0{0.0};  // Fixed latency (PCIe, setup)
        uint64_t padding3[7];
        std::atomic<double> beta1{1.0};  // Effective bandwidth correction
        uint64_t padding4[7];
    };

   public:
    DeviceQuota() = default;
    ~DeviceQuota() = default;

    DeviceQuota(const DeviceQuota &) = delete;
    DeviceQuota &operator=(const DeviceQuota &) = delete;

    Status loadTopology(std::shared_ptr<Topology> &local_topology);

    std::shared_ptr<Topology> getTopology() const { return local_topology_; }

    Status enableSharedQuota(const std::string &shm_name);

    Status allocate(uint64_t length, const std::string &location,
                    int &chosen_dev_id);

    Status release(int dev_id, uint64_t length, double latency);

    void setDiffusionActiveBytes(int dev_id, uint64_t value) {
        devices_[dev_id].diffusion_active_bytes.store(
            value, std::memory_order_relaxed);
    }

    uint64_t getActiveBytes(int dev_id) {
        return devices_[dev_id].active_bytes.load(std::memory_order_relaxed);
    }

    void setLearningRate(double alpha) { alpha_ = std::clamp(alpha, 0.0, 1.0); }

    void setLocalWeight(double local_weight) {
        local_weight_ = std::clamp(local_weight, 0.0, 1.0);
    }

    void setDiffusionInterval(uint64_t msec) {
        diffusion_interval_ = msec * 1000000ull;
    }

    void setCrossNumaAccess(bool enable = true) { allow_cross_numa_ = enable; }

   private:
    std::shared_ptr<Topology> local_topology_;
    std::unordered_map<int, DeviceInfo> devices_;
    mutable std::shared_mutex rwlock_;
    bool allow_cross_numa_ = false;
    double alpha_ = 0.01;
    double local_weight_ = 0.9;
    uint64_t diffusion_interval_ = 10 * 1000000ull;
    std::shared_ptr<SharedQuotaManager> shared_quota_;
    bool enable_quota_ = true;
    bool update_quota_params_ = true;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_QUOTA_H