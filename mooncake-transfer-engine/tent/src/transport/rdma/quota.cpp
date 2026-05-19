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

#include "tent/transport/rdma/quota.h"
#include "tent/transport/rdma/shared_quota.h"
#include "tent/common/utils/random.h"
#include "tent/common/utils/os.h"

#include <algorithm>
#include <iostream>
#include <iomanip>

namespace mooncake {
namespace tent {
Status DeviceSelector::loadTopology(std::shared_ptr<Topology>& local_topology) {
    local_topology_ = local_topology;
    for (size_t dev_id = 0; dev_id < local_topology->getNicCount(); ++dev_id) {
        auto entry = local_topology->getNicEntry(dev_id);
        if (!entry || entry->type != Topology::NIC_RDMA) continue;
        DeviceInfo& info = devices_[dev_id];
        info.dev_id = dev_id;
        info.bw_gbps = kDefaultBwGbps;
        info.numa_id = entry->numa_node;
        info.ewma_bandwidth_bps.store(info.getTheoreticalBandwidth(),
                                      std::memory_order_relaxed);
    }
    return Status::OK();
}

Status DeviceSelector::enableSharedQuota(const std::string& shm_name) {
    slot_manager_ = std::make_shared<SharedSlotManager>(this);
    slot_manager_->setRotationIntervalMs(
        sched_params_.slot_rotation_interval_ms);
    auto status = slot_manager_->attach(shm_name);
    if (!status.ok()) slot_manager_.reset();
    return status;
}

Status DeviceSelector::allocate(uint64_t total_length, uint32_t num_slices,
                                uint64_t slice_bytes,
                                const std::string& location,
                                std::vector<int>& slice_dev_ids, int priority,
                                uint64_t device_mask) {
    slice_dev_ids.clear();
    slice_dev_ids.reserve(num_slices);
    auto entry = local_topology_->getMemEntry(location);
    if (!entry) return Status::InvalidArgument("Unknown location" LOC_MARK);

    if (!smart_selection_enabled_) {
        // Baseline mode: consistent with original TE behavior
        // Use devices from the first non-empty rank only
        thread_local uint64_t tl_rr_counter = 0;
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            thread_local std::vector<int> tl_eligible;
            tl_eligible.clear();
            for (int dev_id : entry->device_list[rank]) {
                if (!devices_.count(dev_id)) continue;
                if ((device_mask & (1ULL << dev_id)) == 0) continue;
                tl_eligible.push_back(dev_id);
            }
            if (tl_eligible.empty()) continue;

            // Found first non-empty rank, do round-robin within this rank
            uint64_t offset = 0;
            for (uint32_t i = 0; i < num_slices; ++i) {
                int dev_id = tl_eligible[tl_rr_counter % tl_eligible.size()];
                tl_rr_counter++;
                slice_dev_ids.push_back(dev_id);
                uint64_t this_slice_bytes =
                    std::min(slice_bytes, total_length - offset);
                offset += this_slice_bytes;
                devices_[dev_id].total_bytes.fetch_add(
                    this_slice_bytes, std::memory_order_relaxed);
            }
            return Status::OK();
        }
        return Status::DeviceNotFound("no eligible devices");
    }

    std::vector<DeviceSelector::Candidate> tl_candidates;
    Status status = buildCandidates(entry, slice_bytes, device_mask,
                                    tl_candidates, priority);
    if (!status.ok()) return status;
    if (num_slices == 1) {
        selectSinglePath(tl_candidates, num_slices, total_length,
                         slice_dev_ids);
    } else {
        thread_local uint64_t tl_call_count = 0;
        bool probe_mode = ((++tl_call_count % 100) == 0);
        selectMultiPath(tl_candidates, num_slices, total_length, slice_dev_ids,
                        probe_mode);
    }
    return Status::OK();
}

int DeviceSelector::getDeviceRank(const std::string& location,
                                  int dev_id) const {
    auto entry = local_topology_->getMemEntry(location);
    if (!entry) return 0;
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        for (int id : entry->device_list[rank]) {
            if (id == dev_id) return static_cast<int>(rank);
        }
    }
    return 0;
}

Status DeviceSelector::buildCandidates(const Topology::MemEntry* entry,
                                       uint64_t slice_bytes,
                                       uint64_t device_mask,
                                       std::vector<Candidate>& candidates,
                                       int request_priority) {
    // Helper lambda to add candidate device
    auto add_candidate = [&](int dev_id, size_t rank) {
        auto& dev = devices_[dev_id];
        uint64_t inflight = dev.getInflightBytes();
        double ewma_bw = dev.getEwmaBandwidth();
        double predicted_time =
            static_cast<double>(inflight + slice_bytes) / ewma_bw;
        double rank_penalty = sched_params_.numa_tier_weights[rank];
        double score = predicted_time * rank_penalty;
        score +=
            (SimpleRandom::Get().next(10) * sched_params_.score_jitter_range);
        bool is_cross_numa = (rank > 0);
        Candidate c;
        c.dev_id = dev_id;
        c.score = score;
        c.is_cross_numa = is_cross_numa;
        candidates.push_back(c);
    };

    // First pass: filter by device priority (QoS filtering)
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        for (int dev_id : entry->device_list[rank]) {
            if (!devices_.count(dev_id)) continue;
            if ((device_mask & (1ULL << dev_id)) == 0) continue;
            // QoS: Get device's current priority slot (local, per-process)
            // Device accepts request if dev_priority >= request_priority
            int dev_priority = PRIO_LOW;  // Default: accept all
            if (sched_params_.enable_priority_filtering) {
                dev_priority = getDevicePriority(dev_id);
            }
            if (dev_priority < request_priority) continue;
            add_candidate(dev_id, rank);
        }
    }

    // If no devices after filtering, fallback to all devices
    if (candidates.empty()) {
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            for (int dev_id : entry->device_list[rank]) {
                if (!devices_.count(dev_id)) continue;
                if ((device_mask & (1ULL << dev_id)) == 0) continue;
                add_candidate(dev_id, rank);
            }
        }
    }

    if (candidates.empty()) {
        return Status::DeviceNotFound("no eligible devices");
    }

    std::sort(
        candidates.begin(), candidates.end(),
        [this](const Candidate& a, const Candidate& b) {
            if (std::abs(a.score - b.score) > sched_params_.score_jitter_range)
                return a.score < b.score;
            return a.dev_id < b.dev_id;
        });
    return Status::OK();
}

void DeviceSelector::selectSinglePath(const std::vector<Candidate>& candidates,
                                      uint32_t num_slices,
                                      uint64_t total_length,
                                      std::vector<int>& slice_dev_ids) {
    if (candidates.empty()) return;

    const Candidate& best = candidates[0];
    int dev_id = best.dev_id;
    auto& dev = devices_[dev_id];

    dev.addInflight(total_length);
    dev.total_bytes.fetch_add(total_length, std::memory_order_relaxed);

    for (uint32_t i = 0; i < num_slices; ++i) {
        slice_dev_ids.push_back(dev_id);
    }
}

void DeviceSelector::selectMultiPath(const std::vector<Candidate>& candidates,
                                     uint32_t num_slices, uint64_t total_length,
                                     std::vector<int>& slice_dev_ids,
                                     bool probe_mode) {
    if (candidates.empty()) return;
    uint64_t slice_bytes = (total_length + num_slices - 1) / num_slices;
    if (probe_mode) {
        for (uint32_t i = 0; i < num_slices; ++i) {
            const Candidate& c = candidates[i % candidates.size()];
            slice_dev_ids.push_back(c.dev_id);
            devices_[c.dev_id].addInflight(slice_bytes);
            devices_[c.dev_id].total_bytes.fetch_add(slice_bytes,
                                                     std::memory_order_relaxed);
        }
    } else {
        double total_weight = 0.0;
        double max_weight = -1.0;
        int best_dev_idx = -1;
        for (size_t i = 0; i < candidates.size(); ++i) {
            double w =
                1.0 / (candidates[i].score + sched_params_.score_epsilon);
            total_weight += w;
            if (w > max_weight) {
                max_weight = w;
                best_dev_idx = static_cast<int>(i);
            }
        }
        if (best_dev_idx == -1 || num_slices == 0 || total_weight <= 0.0)
            return;
        uint32_t remaining_slices = num_slices;
        for (size_t i = 0; i < candidates.size(); ++i) {
            double w =
                1.0 / (candidates[i].score + sched_params_.score_epsilon);
            uint32_t assigned =
                static_cast<uint32_t>((w / total_weight) * num_slices);
            if (assigned > 0) {
                if (assigned > remaining_slices) assigned = remaining_slices;
                remaining_slices -= assigned;
                const Candidate& c = candidates[i];
                for (uint32_t s = 0; s < assigned; ++s) {
                    slice_dev_ids.push_back(c.dev_id);
                }
                uint64_t total_assigned_bytes =
                    static_cast<uint64_t>(slice_bytes) * assigned;
                devices_[c.dev_id].addInflight(total_assigned_bytes);
                devices_[c.dev_id].total_bytes.fetch_add(
                    total_assigned_bytes, std::memory_order_relaxed);
            }
        }
        if (remaining_slices > 0) {
            const Candidate& c = candidates[best_dev_idx];
            for (uint32_t s = 0; s < remaining_slices; ++s) {
                slice_dev_ids.push_back(c.dev_id);
            }
            uint64_t total_assigned_bytes =
                static_cast<uint64_t>(slice_bytes) * remaining_slices;
            devices_[c.dev_id].addInflight(total_assigned_bytes);
            devices_[c.dev_id].total_bytes.fetch_add(total_assigned_bytes,
                                                     std::memory_order_relaxed);
        }
    }
}

Status DeviceSelector::allocate(uint64_t length, const std::string& location,
                                int& chosen_dev_id) {
    std::vector<int> slice_dev_ids;
    Status status = allocate(length, 1, length, location, slice_dev_ids, ~0ULL);
    if (!status.ok()) return status;
    if (slice_dev_ids.empty()) {
        return Status::DeviceNotFound("allocation failed");
    }
    chosen_dev_id = slice_dev_ids[0];
    return Status::OK();
}

Status DeviceSelector::release(int dev_id, uint64_t length, double latency) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");

    auto& dev = it->second;
    dev.releaseInflight(length);

    if (!smart_selection_enabled_) {
        return Status::OK();
    }

    double observed_bw = static_cast<double>(length) / latency;
    double current_ewma = dev.getEwmaBandwidth();

    double alpha = sched_params_.bandwidth_learning_rate;
    double new_ewma = alpha * current_ewma + (1.0 - alpha) * observed_bw;

    double theoretical_bw = dev.getTheoreticalBandwidth();
    new_ewma = std::max(
        sched_params_.ewma_min_multiplier * theoretical_bw,
        std::min(sched_params_.ewma_max_multiplier * theoretical_bw, new_ewma));

    dev.ewma_bandwidth_bps.store(new_ewma, std::memory_order_relaxed);

    return Status::OK();
}

void DeviceSelector::printTrafficStats() {
    std::cout << "=== Device Traffic Statistics ===" << std::endl;
    for (const auto& [dev_id, dev] : devices_) {
        uint64_t total = dev.total_bytes.load(std::memory_order_relaxed);
        double ewma_bw_gbps = dev.getEwmaBandwidth() / 1e9 * 8.0;
        uint64_t inflight = dev.getInflightBytes();
        std::cout << "Dev " << dev_id << ": "
                  << "Total=" << (total / 1024.0 / 1024.0 / 1024.0) << " GB, "
                  << "EWMA BW=" << std::fixed << std::setprecision(2)
                  << ewma_bw_gbps << " Gbps, "
                  << "Inflight=" << inflight << " bytes" << std::endl;
    }
}

void DeviceSelector::fillDevicePriorities() {
    sched_params_.device_base_priorities.clear();
    for (const auto& [dev_id, dev] : devices_) {
        sched_params_.device_base_priorities.push_back(dev_id);
    }
}

int DeviceSelector::getDevicePriority(int dev_id) const {
    if (!sched_params_.enable_priority_filtering) return 0;
    auto it = std::find(sched_params_.device_base_priorities.begin(),
                        sched_params_.device_base_priorities.end(), dev_id);
    if (it == sched_params_.device_base_priorities.end()) return 0;
    size_t base_index =
        std::distance(sched_params_.device_base_priorities.begin(), it);
    size_t num_devices = sched_params_.device_base_priorities.size();
    if (sched_params_.local_rotation_interval_us > 0 && num_devices > 0) {
        uint64_t now = getCurrentTimeInNano();
        uint64_t offset_us = now / 1000;
        size_t rotation_offset =
            (offset_us / sched_params_.local_rotation_interval_us) %
            num_devices;
        base_index = (base_index + rotation_offset) % num_devices;
    }
    return static_cast<int>(base_index);
}

}  // namespace tent
}  // namespace mooncake