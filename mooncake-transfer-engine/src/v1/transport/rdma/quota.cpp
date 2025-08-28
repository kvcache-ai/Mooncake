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

Status DeviceQuota::allocate(uint64_t data_size, const std::string &location,
                             std::vector<AllocPlan> &plan_list) {
    plan_list.clear();
    if (data_size == 0) return Status::OK();

    auto it_loc = local_topology_->getResolvedMatrix().find(location);
    if (it_loc == local_topology_->getResolvedMatrix().end()) {
        return Status::InvalidArgument("Unknown location: " + location);
    }

    const ResolvedTopologyEntry &entry = it_loc->second;
    static constexpr double rank_weights[] = {1.0, 0.75, 0.25};

    struct Cand {
        int dev_id;
        double norm_weight;
    };
    std::vector<Cand> cands;

    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        for (int dev_id : entry.device_list[rank]) {
            auto &device = devices_[dev_id];
            double weight = device.bw_gbps * 1e9 * rank_weights[rank];
            if (!allow_cross_numa_ && device.numa_id != entry.numa_node)
                continue;
            if (weight <= 0.0) continue;
            if (shared_quota_ && device.local_quota <= 0) {
                const uint64_t acquire_size =
                    -device.local_quota +
                    std::max(data_size, alloc_units_ * slice_size_);
                if (shared_quota_->allocate(dev_id, acquire_size)) {
                    device.local_quota += acquire_size;
                } else {
                    continue;
                }
            }
            uint64_t act = device.active_bytes.load(std::memory_order_relaxed);
            double load_factor =
                1.0 + static_cast<double>(act) / (weight + 1.0);
            cands.push_back(Cand{dev_id, weight / load_factor});
        }
    }

    if (cands.empty())
        return Status::DeviceNotFound("no eligible devices for " + location);

    if (data_size < slice_size_) {
        auto it = std::max_element(cands.begin(), cands.end(),
                                   [](const Cand &a, const Cand &b) {
                                       return a.norm_weight < b.norm_weight;
                                   });
        assert(it != cands.end());
        devices_[it->dev_id].active_bytes.fetch_add(data_size,
                                                    std::memory_order_relaxed);
        plan_list.push_back(AllocPlan{it->dev_id, 0, data_size});
        return Status::OK();
    }

    size_t max_devices = static_cast<size_t>(data_size / slice_size_);
    if (max_devices == 0) max_devices = 1;
    if (max_devices > cands.size()) max_devices = cands.size();

    std::sort(cands.begin(), cands.end(), [](const Cand &a, const Cand &b) {
        return a.norm_weight > b.norm_weight;
    });
    cands.resize(max_devices);

    double total_norm_weights = 0.0;
    for (auto &c : cands) total_norm_weights += c.norm_weight;
    if (total_norm_weights <= 0.0)
        return Status::InternalError("invalid total weight");

    struct Tmp {
        int dev_id;
        uint64_t alloc;
        double frac;
    };

    std::vector<Tmp> tmp;
    tmp.reserve(cands.size());

    uint64_t allocated = 0;
    for (auto &c : cands) {
        double share_f = static_cast<double>(data_size) *
                         (c.norm_weight / total_norm_weights);
        uint64_t alloc_i = static_cast<uint64_t>(share_f);
        tmp.push_back(Tmp{c.dev_id, alloc_i, share_f - alloc_i});
        allocated += alloc_i;
    }

    uint64_t remaining = data_size - allocated;
    if (remaining > 0) {
        std::sort(tmp.begin(), tmp.end(),
                  [](const Tmp &a, const Tmp &b) { return a.frac > b.frac; });
        size_t idx = 0;
        while (remaining > 0) {
            tmp[idx % tmp.size()].alloc += 1;
            allocated += 1;
            --remaining;
            ++idx;
        }
    }

    assert(allocated == data_size);
    uint64_t offset = 0;
    for (auto &t : tmp) {
        if (t.alloc == 0) continue;
        plan_list.push_back(AllocPlan{t.dev_id, offset, t.alloc});
        offset += t.alloc;
        auto &device = devices_[t.dev_id];
        device.local_quota -= t.alloc;
        device.active_bytes.fetch_add(t.alloc, std::memory_order_relaxed);
    }

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