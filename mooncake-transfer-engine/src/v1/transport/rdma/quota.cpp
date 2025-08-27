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

namespace mooncake {
namespace v1 {
int DeviceQuota::findNumaIdByTopology(int dev_id) {
    int numa_id = -1;
    for (const auto &kv : local_topology_->getResolvedMatrix()) {
        const auto &entry = kv.second;
        for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
            if (std::find(entry.device_list[rank].begin(),
                          entry.device_list[rank].end(),
                          dev_id) != entry.device_list[rank].end()) {
                numa_id = entry.numa_node;
                break;
            }
        }
        if (numa_id != -1) break;
    }
    return numa_id >= 0 ? numa_id : 0;
}

Status DeviceQuota::loadTopology(std::shared_ptr<Topology> &local_topology) {
    local_topology_ = local_topology;
    for (size_t dev_id = 0; dev_id < local_topology->getDeviceList().size();
         ++dev_id) {
        devices_[dev_id].dev_id = dev_id;
        devices_[dev_id].bw_gbps = 200;
        devices_[dev_id].numa_id = findNumaIdByTopology(dev_id);
    }
    return Status::OK();
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
    static constexpr double rank_weights[] = {1.0, 0.5, 0.25};

    struct Cand {
        int dev_id;
        double base_w;
        uint64_t active;
        double adj_w;
    };
    std::vector<Cand> cands;

    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        double rw = rank_weights[rank];
        for (int dev_id : entry.device_list[rank]) {
            auto dit = devices_.find(dev_id);
            if (dit == devices_.end()) continue;
            const auto &d = dit->second;
            double w = d.bw_gbps * rw;
            if (!allow_cross_numa_ && d.numa_id != entry.numa_node) continue;
            if (w <= 0.0) continue;
            uint64_t act = d.active_bytes.load(std::memory_order_relaxed);
            double adj = w / (1.0 + static_cast<double>(act) / 1e9);
            cands.push_back(Cand{dev_id, w, act, adj});
        }
    }

    if (cands.empty())
        return Status::DeviceNotFound("no eligible devices for " + location);

    if (data_size < slice_size_) {
        auto it = std::max_element(
            cands.begin(), cands.end(),
            [](const Cand &a, const Cand &b) { return a.adj_w < b.adj_w; });
        assert(it != cands.end());
        devices_[it->dev_id].active_bytes.fetch_add(data_size,
                                                    std::memory_order_relaxed);
        plan_list.push_back(AllocPlan{it->dev_id, 0, data_size});
        return Status::OK();
    }

    size_t max_devices = static_cast<size_t>(data_size / slice_size_);
    if (max_devices == 0) max_devices = 1;
    if (max_devices > cands.size()) max_devices = cands.size();

    std::sort(cands.begin(), cands.end(),
              [](const Cand &a, const Cand &b) { return a.adj_w > b.adj_w; });
    cands.resize(max_devices);

    double total_w = 0.0;
    for (auto &c : cands) total_w += c.adj_w;
    if (total_w <= 0.0) return Status::InternalError("invalid total weight");

    struct Tmp {
        int dev_id;
        double share_f;
        uint64_t alloc;
        double frac;
    };
    std::vector<Tmp> tmp;
    uint64_t allocated = 0;
    for (auto &c : cands) {
        double share_f = static_cast<double>(data_size) * (c.adj_w / total_w);
        uint64_t alloc_aligned =
            (static_cast<uint64_t>(share_f) / slice_size_) * slice_size_;
        tmp.push_back(Tmp{c.dev_id, share_f, alloc_aligned,
                          share_f - static_cast<double>(alloc_aligned)});
        allocated += alloc_aligned;
    }

    uint64_t remaining = (data_size > allocated) ? (data_size - allocated) : 0;

    if (remaining >= slice_size_) {
        std::sort(tmp.begin(), tmp.end(),
                  [](const Tmp &a, const Tmp &b) { return a.frac > b.frac; });
        size_t idx = 0;
        while (remaining >= slice_size_) {
            tmp[idx % tmp.size()].alloc += slice_size_;
            remaining -= slice_size_;
            allocated += slice_size_;
            ++idx;
        }
    }

    if (remaining > 0) {
        auto it = std::max_element(
            tmp.begin(), tmp.end(),
            [](const Tmp &a, const Tmp &b) { return a.frac < b.frac; });
        if (it != tmp.end()) {
            it->alloc += remaining;
            allocated += remaining;
        }
        remaining = 0;
    }

    assert(allocated == data_size);
    uint64_t offset = 0;
    for (auto &t : tmp) {
        if (t.alloc == 0) continue;
        plan_list.push_back(AllocPlan{t.dev_id, offset, t.alloc});
        offset += t.alloc;
        devices_[t.dev_id].active_bytes.fetch_add(t.alloc,
                                                  std::memory_order_relaxed);
    }

    return Status::OK();
}

Status DeviceQuota::release(int dev_id, uint64_t length) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");
    it->second.active_bytes.fetch_sub(length, std::memory_order_relaxed);
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake