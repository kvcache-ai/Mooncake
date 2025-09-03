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
#include "v1/utility/random.h"

#include <assert.h>
#include <unordered_set>

namespace mooncake {
namespace v1 {
Status DeviceQuota::loadTopology(std::shared_ptr<Topology> &local_topology) {
    local_topology_ = local_topology;
    std::unordered_set<int> used_numa_id;
    for (size_t dev_id = 0; dev_id < local_topology->getDeviceList().size();
         ++dev_id) {
        devices_[dev_id].dev_id = dev_id;
        devices_[dev_id].bw_gbps = local_topology->findDeviceBandwidth(dev_id);
        devices_[dev_id].numa_id = local_topology->findDeviceNumaID(dev_id);
        devices_[dev_id].local_quota = UINT64_MAX;
        used_numa_id.insert(devices_[dev_id].numa_id);
    }
    if (used_numa_id.size() == 1) allow_cross_numa_ = true;
    return Status::OK();
}

Status DeviceQuota::enableSharedQuota(const std::string &shm_name) {
    shared_quota_ = std::make_shared<SharedQuotaManager>();
    auto status = shared_quota_->createOrAttach(shm_name, local_topology_);
    if (!status.ok()) shared_quota_.reset();
    return status;
}

Status DeviceQuota::allocate(uint64_t length, const std::string &location,
                             int &chosen_dev_id) {
    auto it_loc = local_topology_->getResolvedMatrix().find(location);
    if (it_loc == local_topology_->getResolvedMatrix().end())
        return Status::InvalidArgument("Unknown location: " + location);

    const ResolvedTopologyEntry &entry = it_loc->second;
    static constexpr double penalty[] = {1.0, 10.0, 10.0};
    std::unordered_map<int, double> score_map;
    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        for (int dev_id : entry.device_list[rank]) {
            auto &device = devices_[dev_id];
            if (!allow_cross_numa_ && device.numa_id != entry.numa_node)
                continue;
            if (shared_quota_ && device.local_quota <= 0) {
                const uint64_t acquire_size =
                    -device.local_quota +
                    std::max(length, alloc_units_ * slice_size_);
                if (shared_quota_->allocate(dev_id, acquire_size)) {
                    device.local_quota += acquire_size;
                } else {
                    continue;
                }
            }
            uint64_t active_bytes =
                device.active_bytes.load(std::memory_order_relaxed) + length;
            double bandwidth = device.bw_gbps * 1e9 / 8;
            double score =
                1.0 / (1.0 + ((active_bytes * penalty[rank]) / bandwidth));
            score_map[dev_id] = score;
        }
    }

    if (score_map.empty()) {
        const int num_devices = (int)local_topology_->getDeviceList().size();
        for (int dev_id = 0; dev_id < num_devices; ++dev_id) {
            if (devices_[dev_id].bw_gbps) {
                score_map[dev_id] = 1.0;
                break;
            }
        }
    }

    if (score_map.empty())
        return Status::DeviceNotFound("no eligible devices for " + location);

    double max_score = -1.0;
    for (auto &item : score_map) {
        max_score = std::max(max_score, item.second);
    }
    std::vector<int> candidates;
    for (auto &item : score_map) {
        if (item.second == max_score) {
            candidates.push_back(item.first);
        }
    }
    thread_local size_t rr_index = 0;
    chosen_dev_id = candidates[rr_index % candidates.size()];
    rr_index++;
    devices_[chosen_dev_id].local_quota -= length;
    devices_[chosen_dev_id].active_bytes.fetch_add(length,
                                                   std::memory_order_relaxed);
    return Status::OK();
}

Status DeviceQuota::release(int dev_id, uint64_t length) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");
    it->second.local_quota += length;
    it->second.active_bytes.fetch_sub(length, std::memory_order_relaxed);
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake