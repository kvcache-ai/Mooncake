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

#include "v1/transport/rdma/quota.h"
#include "v1/transport/rdma/shared_quota.h"
#include "v1/common/utils/random.h"

#include <assert.h>
#include <unordered_set>

namespace mooncake {
namespace v1 {
Status PerThreadDeviceQuota::loadTopology(
    std::shared_ptr<Topology>& local_topology) {
    local_topology_ = local_topology;
    std::unordered_set<int> used_numa_id;
    for (size_t dev_id = 0; dev_id < local_topology->getNicCount(); ++dev_id) {
        auto entry = local_topology->getNicEntry(dev_id);
        if (entry->type != Topology::NIC_RDMA) continue;
        devices_[dev_id].dev_id = dev_id;
        devices_[dev_id].bw_gbps = 200;  // entry->bw_gbps
        devices_[dev_id].numa_id = entry->numa_node;
        used_numa_id.insert(devices_[dev_id].numa_id);
    }
    if (used_numa_id.size() == 1) allow_cross_numa_ = true;
    return Status::OK();
}

Status PerThreadDeviceQuota::enableSharedQuota(const std::string& shm_name) {
    return Status::NotImplemented("shared quota not implemented");
}

Status PerThreadDeviceQuota::allocate(uint64_t length,
                                      const std::string& location,
                                      int& chosen_dev_id) {
    auto entry = local_topology_->getMemEntry(location);
    if (!entry) return Status::InvalidArgument("Unknown location" LOC_MARK);
    static constexpr double penalty[] = {1.0, 10.0, 10.0};
    std::unordered_map<int, double> score_map;
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        for (int dev_id : entry->device_list[rank]) {
            if (!devices_.count(dev_id)) continue;
            auto& device = devices_[dev_id];
            if (!allow_cross_numa_ && device.numa_id != entry->numa_node)
                continue;
            uint64_t active_bytes = device.active_bytes + length;
            double bandwidth = device.bw_gbps * 1e9 / 8;
            double load_factor = (active_bytes * penalty[rank]) / bandwidth;
            double score = 1.0 / (1.0 + load_factor);
            score_map[dev_id] = score;
        }
    }
    if (score_map.empty())
        return Status::DeviceNotFound("no eligible devices for " + location);
    double max_score = -1.0;
    for (auto& item : score_map) max_score = std::max(max_score, item.second);
    std::vector<int> candidates;
    for (auto& item : score_map) {
        if (item.second == max_score) {
            candidates.push_back(item.first);
        }
    }
    static std::atomic<int> next_prefer_id(0);
    thread_local size_t prefer_id = next_prefer_id.fetch_add(1);
    chosen_dev_id = candidates[prefer_id % candidates.size()];
    devices_[chosen_dev_id].active_bytes += length;
    return Status::OK();
}

Status PerThreadDeviceQuota::release(int dev_id, uint64_t length,
                                     double latency) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");
    it->second.active_bytes -= length;
    return Status::OK();
}

Status DeviceQuota::loadTopology(std::shared_ptr<Topology>& local_topology) {
    local_topology_ = local_topology;
    std::unordered_set<int> used_numa_id;
    for (size_t dev_id = 0; dev_id < local_topology->getNicCount(); ++dev_id) {
        auto entry = local_topology->getNicEntry(dev_id);
        if (entry->type != Topology::NIC_RDMA) continue;
        DeviceInfo& info = devices_[dev_id];
        info.dev_id = dev_id;
        info.bw_gbps = 200.0;
        info.numa_id = entry->numa_node;
        used_numa_id.insert(entry->numa_node);
    }
    if (used_numa_id.size() == 1) allow_cross_numa_ = true;
    return Status::OK();
}

Status DeviceQuota::enableSharedQuota(const std::string& shm_name) {
    shared_quota_ = std::make_shared<SharedQuotaManager>(this);
    auto status = shared_quota_->attach(shm_name);
    if (!status.ok()) shared_quota_.reset();
    return status;
}

struct TlsDeviceInfo {
    uint64_t active_bytes{0};
    double beta0{0.0};
    double beta1{1.0};
};

thread_local std::unordered_map<int, TlsDeviceInfo> tl_device_info;

#define PER_THREAD_QUOTA

Status DeviceQuota::allocate(uint64_t length, const std::string& location,
                             int& chosen_dev_id) {
    auto entry = local_topology_->getMemEntry(location);
    if (!entry) return Status::InvalidArgument("Unknown location" LOC_MARK);

    static constexpr double penalty[] = {1.0, 1.5, 2.0};

    std::vector<int> candidates;
    double best_score = std::numeric_limits<double>::max();
    constexpr double tol = 0.999;

    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        for (int dev_id : entry->device_list[rank]) {
            if (!devices_.count(dev_id)) continue;
            auto& dev = devices_[dev_id];
            if (!allow_cross_numa_ && dev.numa_id != entry->numa_node) continue;

#ifdef PER_THREAD_QUOTA
            uint64_t overall_active_bytes =
                dev.diffusion_active_bytes.load(std::memory_order_relaxed) +
                dev.active_bytes.load(std::memory_order_relaxed);
            uint64_t active_bytes =
                local_weight_ * tl_device_info[dev_id].active_bytes +
                (1 - local_weight_) * overall_active_bytes + length;
            double bandwidth = dev.bw_gbps * 1e9 / 8;
            double predicted_time =
                (active_bytes / bandwidth) * tl_device_info[dev_id].beta1 +
                tl_device_info[dev_id].beta0;
            double score = penalty[rank] * predicted_time;
#else
            uint64_t active_bytes =
                dev.active_bytes.load(std::memory_order_relaxed) + length;
            double bandwidth = dev.bw_gbps * 1e9 / 8;
            double predicted_time =
                (active_bytes / bandwidth) *
                    dev.beta1.load(std::memory_order_relaxed) +
                dev.beta0.load(std::memory_order_relaxed);
            double score = penalty[rank] * predicted_time;
#endif

            if (score < best_score) {
                best_score = score;
                candidates.clear();
                candidates.push_back(dev_id);
            } else if (score <= best_score / tol) {
                candidates.push_back(dev_id);
            }
        }
    }

    if (candidates.empty())
        return Status::DeviceNotFound("no eligible devices for " + location);

    static std::atomic<size_t> rr_counter(0);
    size_t rr_index = rr_counter.fetch_add(1, std::memory_order_relaxed);
    chosen_dev_id = candidates[rr_index % candidates.size()];
    tl_device_info[chosen_dev_id].active_bytes += length;
    devices_[chosen_dev_id].active_bytes.fetch_add(length,
                                                   std::memory_order_relaxed);
    return Status::OK();
}

Status DeviceQuota::release(int dev_id, uint64_t length, double latency) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");
    auto& dev = it->second;
    dev.active_bytes.fetch_sub(length, std::memory_order_relaxed);
    tl_device_info[dev_id].active_bytes -= length;
    double bw = dev.bw_gbps * 1e9 / 8;
    double theory_time = length / bw;
    double obs_time = latency;
#ifdef PER_THREAD_QUOTA
    auto& tl_dev = tl_device_info[dev_id];
    double pred = tl_dev.beta0 + tl_dev.beta1 * theory_time;
    double err = obs_time - pred;
    double rel_err = (pred > 1e-9) ? (err / pred) : 0.0;
    double adapt_alpha = alpha_;
    double new_beta0 = tl_dev.beta0 + adapt_alpha * err;
    double new_beta1 = tl_dev.beta1 * (1.0 + adapt_alpha * rel_err);
    tl_dev.beta0 = std::clamp(new_beta0, 0.0, 1e-3);
    tl_dev.beta1 = std::clamp(new_beta1, 0.5, 32.0);
#else
    auto beta0 = dev.beta0.load(std::memory_order_relaxed);
    auto beta1 = dev.beta1.load(std::memory_order_relaxed);
    double pred = beta0 + beta1 * theory_time;
    if (pred < 1e-9) pred = 1e-9;
    double err = obs_time - pred;
    double rel_err = (pred > 1e-9) ? (err / pred) : 0.0;
    double adapt_alpha = alpha_;
    if (std::abs(err) > 0.1) adapt_alpha = std::min(1.0, alpha_ * 5);
    double new_beta0 = beta0 + adapt_alpha * err;
    double new_beta1 = tl_dev.beta1 * (1.0 + adapt_alpha * rel_err);
    dev.beta0.store(std::clamp(new_beta0, 0.0, 1e-3),
                    std::memory_order_relaxed);  // cap within 1ms
    dev.beta1.store(std::clamp(new_beta1, 0.5, 2.0),
                    std::memory_order_relaxed);  // bandwidth correction
#endif
    if (shared_quota_) {
        thread_local uint64_t tl_last_ts = 0;
        uint64_t now = getCurrentTimeInNano();
        if (now - tl_last_ts > diffusion_interval_) {
            tl_last_ts = now;
            return shared_quota_->diffusion();
        }
    }
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake