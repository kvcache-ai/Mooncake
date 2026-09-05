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
#include "tent/transport/rdma/gdr_reachability.h"
#include "tent/common/utils/random.h"
#include "tent/common/utils/os.h"

#include <glog/logging.h>

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
        info.bw_gbps.store(0.0, std::memory_order_relaxed);  // unknown
        info.numa_id = entry->numa_node;
        const double seed = theoreticalBandwidth(info);
        info.ewma_bandwidth_bps.store(seed, std::memory_order_relaxed);
        info.ewma_transmit_bps.store(seed, std::memory_order_relaxed);
    }
    // Initialize device base priorities after all devices are loaded
    fillDevicePriorities();
    return Status::OK();
}

double DeviceSelector::theoreticalBandwidth(const DeviceInfo& dev) const {
    const auto& p = sched_params_;
    double gbps = dev.bw_gbps.load(std::memory_order_relaxed);
    if (gbps < p.min_bandwidth_gbps || gbps > p.max_bandwidth_gbps)
        gbps = p.default_bandwidth_gbps;
    return gbps * 1e9 / 8.0;
}

Status DeviceSelector::setDeviceBandwidth(int dev_id, double gbps) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");
    auto& dev = it->second;
    const auto& p = sched_params_;
    if (gbps < p.min_bandwidth_gbps || gbps > p.max_bandwidth_gbps) {
        LOG(WARNING) << "Device " << local_topology_->getNicName(dev_id)
                     << " link speed " << gbps << " Gbps is "
                     << (gbps <= 0.0 ? "unknown" : "outside the valid range")
                     << ", assuming " << p.default_bandwidth_gbps << " Gbps";
    }
    dev.bw_gbps.store(gbps, std::memory_order_relaxed);
    // A worker completing between these stores clamps the old EWMA against
    // the new rate once; the seeds below overwrite it.
    const double seed = theoreticalBandwidth(dev);
    dev.ewma_bandwidth_bps.store(seed, std::memory_order_relaxed);
    dev.ewma_transmit_bps.store(seed, std::memory_order_relaxed);
    return Status::OK();
}

Status DeviceSelector::setDeviceAvailable(int dev_id, bool available) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");
    it->second.available.store(available, std::memory_order_relaxed);
    return Status::OK();
}

bool DeviceSelector::isDeviceAvailable(int dev_id) const {
    return usable(dev_id);
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

    // Exclude NICs that have proven unable to GPUDirect-DMA to this GPU. Only
    // engaged once something has actually been learned (permissive fabrics pay
    // nothing) and only for GPU/cuda locations.
    if (GdrReachability::hasAnyExclusion()) {
        LocationParser lp(location);
        if (lp.type() == "cuda" && lp.index() >= 0) {
            auto& gdr = GdrReachability::instance();
            for (const auto& kv : devices_) {
                int dev_id = kv.first;
                if (dev_id < 0 || dev_id >= 64) continue;
                const auto* nic = local_topology_->getNicEntry(dev_id);
                if (nic && !gdr.localReachable(nic->name, lp.index()))
                    device_mask &= ~(1ULL << dev_id);
            }
        }
    }

    if (!smart_selection_enabled_) {
        // Baseline mode: consistent with original TE behavior
        // Use devices from the first non-empty rank only
        thread_local uint64_t tl_rr_counter = 0;
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            thread_local std::vector<int> tl_eligible;
            tl_eligible.clear();
            for (int dev_id : entry->device_list[rank]) {
                if (!usable(dev_id)) continue;
                if ((device_mask & (1ULL << dev_id)) == 0) continue;
                if (!isNumaEligible(entry, dev_id)) continue;
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
                // Baseline mode does not track inflight (release() and
                // chargeDevice() skip it too); only lifetime traffic counts.
                devices_[dev_id].total_bytes.fetch_add(
                    this_slice_bytes, std::memory_order_relaxed);
            }
            return Status::OK();
        }
        return Status::DeviceNotFound(noEligibleDeviceReason());
    }

    std::vector<DeviceSelector::Candidate> tl_candidates;
    Status status = buildCandidates(entry, slice_bytes, device_mask,
                                    tl_candidates, priority);
    if (!status.ok()) return status;
    if (num_slices == 1) {
        selectSinglePath(tl_candidates, num_slices, total_length,
                         slice_dev_ids);
    } else {
        // Probe mode: every 100th call uses round-robin distribution
        // to ensure all devices are sampled for EWMA updates
        thread_local uint64_t tl_call_count = 0;
        bool probe_mode = ((++tl_call_count % 100) == 0);
        selectMultiPath(tl_candidates, num_slices, total_length, slice_bytes,
                        slice_dev_ids, probe_mode);
    }
    return Status::OK();
}

void DeviceSelector::auditStrictLocalNuma() const {
    if (!sched_params_.strict_local_numa || !local_topology_) return;

    size_t excludable = 0;
    for (const auto& mem : local_topology_->mem_list_) {
        bool has_nic = false, has_local_nic = false;
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            for (int dev_id : mem.device_list[rank]) {
                if (devices_.find(dev_id) == devices_.end()) continue;
                has_nic = true;
                if (local_topology_->isCrossNuma(mem, dev_id))
                    excludable++;
                else
                    has_local_nic = true;
            }
        }
        if (has_nic && !has_local_nic) {
            LOG(WARNING) << "strict_local_numa: location " << mem.name
                         << " (NUMA " << mem.numa_node
                         << ") has no same-NUMA RDMA NIC; transfers from it "
                            "will fail with DeviceNotFound";
        }
    }

    if (excludable == 0) {
        LOG(WARNING) << "strict_local_numa is enabled but no NIC can be "
                        "classified as cross-NUMA on this host (custom "
                        "priority matrix, VM, or sysfs without NUMA info), so "
                        "the flag has no effect and cross-NUMA NICs keep the "
                        "numa_penalties soft penalty";
    }
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
    // Score formula: predicted_time × numa_penalty + random_jitter
    // Lower score = better candidate
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
            if (!usable(dev_id)) continue;
            if ((device_mask & (1ULL << dev_id)) == 0) continue;
            if (!isNumaEligible(entry, dev_id)) continue;
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

    // Retry without QoS filtering; availability and NUMA exclusion stay.
    if (candidates.empty()) {
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            for (int dev_id : entry->device_list[rank]) {
                if (!usable(dev_id)) continue;
                if ((device_mask & (1ULL << dev_id)) == 0) continue;
                if (!isNumaEligible(entry, dev_id)) continue;
                add_candidate(dev_id, rank);
            }
        }
    }

    if (candidates.empty()) {
        return Status::DeviceNotFound(noEligibleDeviceReason());
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
                                     uint64_t slice_bytes,
                                     std::vector<int>& slice_dev_ids,
                                     bool probe_mode) {
    if (candidates.empty()) return;
    const size_t first = slice_dev_ids.size();
    if (probe_mode) {
        // Probe mode: round-robin distribution to ensure all devices are
        // sampled Activates every 100th call to prevent EWMA starvation
        for (uint32_t i = 0; i < num_slices; ++i) {
            const Candidate& c = candidates[i % candidates.size()];
            slice_dev_ids.push_back(c.dev_id);
        }
    } else {
        // Normal mode: weighted distribution based on inverse score
        // Lower score → higher weight → more slices
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
            }
        }
        if (remaining_slices > 0) {
            const Candidate& c = candidates[best_dev_idx];
            for (uint32_t s = 0; s < remaining_slices; ++s) {
                slice_dev_ids.push_back(c.dev_id);
            }
        }
    }
    // Charge each device what its slices actually carry, in the caller's
    // slice order, so release() (which returns the slice's length) balances
    // per device; ceil(total / n) per slice would not.
    uint64_t offset = 0;
    for (size_t i = first; i < slice_dev_ids.size(); ++i) {
        const uint64_t bytes = std::min(slice_bytes, total_length - offset);
        offset += bytes;
        auto& dev = devices_[slice_dev_ids[i]];
        dev.addInflight(bytes);
        dev.total_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }
}

Status DeviceSelector::allocate(uint64_t length, const std::string& location,
                                int& chosen_dev_id) {
    return allocate(length, location, chosen_dev_id, PRIO_HIGH, ~0ULL);
}

Status DeviceSelector::allocate(uint64_t length, const std::string& location,
                                int& chosen_dev_id, int priority,
                                uint64_t device_mask) {
    std::vector<int> slice_dev_ids;
    Status status = allocate(length, 1, length, location, slice_dev_ids,
                             priority, device_mask);
    if (!status.ok()) return status;
    if (slice_dev_ids.empty()) {
        return Status::DeviceNotFound("allocation failed");
    }
    chosen_dev_id = slice_dev_ids[0];
    return Status::OK();
}

Status DeviceSelector::chargeDevice(int dev_id, uint64_t length) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");
    // Inflight is only tracked in smart mode; see allocate() and release().
    if (smart_selection_enabled_) it->second.addInflight(length);
    // Each attempt is bytes this NIC is asked to move, so a retry -- or a
    // first attempt that fell back to another NIC -- counts again here:
    // total_bytes is a lifetime traffic figure, not a request count.
    it->second.total_bytes.fetch_add(length, std::memory_order_relaxed);
    return Status::OK();
}

void DeviceSelector::learnRate(const DeviceInfo& dev,
                               std::atomic<double>& series, double alpha,
                               double observed_bps) const {
    // EWMA update: new = α × old + (1-α) × observed
    // α = 0: always use observed (full adaptation)
    // α = 1: never update (no learning)
    // Clamped to [min_multiplier, max_multiplier] of theoretical bandwidth.
    const double theoretical_bw = theoreticalBandwidth(dev);
    const double current = series.load(std::memory_order_relaxed);
    double updated = alpha * current + (1.0 - alpha) * observed_bps;
    updated = std::max(
        sched_params_.ewma_min_multiplier * theoretical_bw,
        std::min(sched_params_.ewma_max_multiplier * theoretical_bw, updated));
    series.store(updated, std::memory_order_relaxed);
}

void DeviceSelector::notePosted(int dev_id, uint64_t bytes, uint64_t now_ns) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return;
    auto& dev = it->second;
    // The read-modify-write picks exactly one thread to see each transition,
    // so a busy stretch is opened once however many workers post at once.
    if (dev.posted_bytes.fetch_add(bytes, std::memory_order_relaxed) == 0)
        dev.busy_since.store(now_ns, std::memory_order_relaxed);
}

void DeviceSelector::notePostEnded(int dev_id, uint64_t bytes,
                                   uint64_t now_ns) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return;
    auto& dev = it->second;
    if (dev.posted_bytes.fetch_sub(bytes, std::memory_order_relaxed) != bytes)
        return;  // still busy
    // Bank the stretch that just ended. A neighbour opening the next stretch
    // between the subtraction above and this read would leave `since` ahead
    // of `now_ns`; bank nothing rather than an underflowed interval. It
    // costs one burst's busy time and reads the link fast, so it is bounded
    // and the smoothing absorbs it.
    const uint64_t since = dev.busy_since.load(std::memory_order_relaxed);
    if (now_ns > since)
        dev.busy_ns.fetch_add(now_ns - since, std::memory_order_relaxed);
}

uint64_t DeviceSelector::getBusyNs(int dev_id) const {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return 0;
    return it->second.busy_ns.load(std::memory_order_relaxed);
}

uint64_t DeviceSelector::getPostedBytes(int dev_id) const {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return 0;
    return it->second.getPostedBytes();
}

void DeviceSelector::noteCompleted(int dev_id, uint64_t bytes) {
    auto it = devices_.find(dev_id);
    if (it != devices_.end())
        it->second.completed_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

uint64_t DeviceSelector::busyNsAt(const DeviceInfo& dev, uint64_t now_ns) {
    uint64_t busy = dev.busy_ns.load(std::memory_order_relaxed);
    if (dev.posted_bytes.load(std::memory_order_relaxed) > 0) {
        const uint64_t since = dev.busy_since.load(std::memory_order_relaxed);
        if (now_ns > since) busy += now_ns - since;
    }
    return busy;
}

void DeviceSelector::resetTransmitMeter(int dev_id) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return;
    // One store, so it cannot interleave with a sampler's two baseline
    // exchanges: the next maybeSampleTransmit sees prev == 0, takes both
    // baselines itself under its CAS, and learns nothing from them.
    it->second.meter_ts.store(0, std::memory_order_relaxed);
}

void DeviceSelector::maybeSampleTransmit(int dev_id, uint64_t now_ns) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return;
    auto& dev = it->second;

    uint64_t prev = dev.meter_ts.load(std::memory_order_relaxed);
    // `now_ns <= prev`: a lane whose poll timestamp predates another lane's
    // sample; letting it through would move the baseline backwards.
    if (prev != 0 && (now_ns <= prev ||
                      now_ns - prev < sched_params_.transmit_meter_interval_ns))
        return;
    // One sampler per interval: the loser leaves the counters alone.
    if (!dev.meter_ts.compare_exchange_strong(
            prev, now_ns, std::memory_order_acq_rel, std::memory_order_relaxed))
        return;

    const uint64_t completed =
        dev.completed_bytes.load(std::memory_order_relaxed);
    const uint64_t before =
        dev.meter_completed.exchange(completed, std::memory_order_relaxed);

    // Busy time up to now: what has been banked by the stretches that ended,
    // plus the one still open. Charging the bytes to this instead of to
    // elapsed wall time is what keeps a NIC that bursts and then waits from
    // reading as a slow link -- the gap between bursts is not the link, and
    // asking whether the NIC was busy when the interval opened cannot see it
    // (a burst's first completion always has the rest of that burst behind
    // it, so every interval opens busy).
    const uint64_t busy = busyNsAt(dev, now_ns);
    const uint64_t busy_before =
        dev.meter_busy_ns.exchange(busy, std::memory_order_relaxed);

    if (prev == 0) return;  // first sample: it only sets the baseline
    // A sample spanning this much wall time describes a link too far back to
    // attribute to the link as it is now, however busy it was.
    if (now_ns - prev > sched_params_.transmit_meter_max_interval_ns) return;
    if (completed <= before || busy <= busy_before) return;

    learnRate(
        dev, dev.ewma_transmit_bps,
        sched_params_.transmit_bandwidth_learning_rate,
        static_cast<double>(completed - before) / ((busy - busy_before) / 1e9));
}

Status DeviceSelector::release(int dev_id, uint64_t length, double latency) {
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");

    auto& dev = it->second;
    // Inflight is only ever charged in smart mode; releasing in baseline mode
    // would drive the unsigned counter below zero.
    if (!smart_selection_enabled_) return Status::OK();
    dev.releaseInflight(length);

    // A release with no latency sample -- cancelled, timed out, failed,
    // swept off a queue pair, or moved to another device -- learns nothing.
    if (latency <= 0.0) return Status::OK();

    learnRate(dev, dev.ewma_bandwidth_bps,
              sched_params_.bandwidth_learning_rate,
              static_cast<double>(length) / latency);
    return Status::OK();
}

Status DeviceSelector::getNicLoadStats(std::vector<NicLoadStats>& stats) const {
    stats.reserve(stats.size() + devices_.size());
    // devices_ is populated during topology load and remains stable while
    // transfers update the per-device atomic counters below.
    for (const auto& [dev_id, dev] : devices_) {
        // Same rule as the aggregate: a NIC that cannot carry traffic has
        // no meaningful load or bandwidth to report, and with zero inflight
        // it would score as the best NIC of the lot.
        if (!dev.available.load(std::memory_order_relaxed)) continue;
        std::string device_name = local_topology_->getNicName(dev_id);
        if (device_name.empty()) device_name = std::to_string(dev_id);
        stats.push_back(NicLoadStats{
            std::move(device_name),
            dev.getInflightBytes(),
            dev.getEwmaBandwidth(),
        });
    }
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

double DeviceSelector::getAggregateEwmaBandwidth() const {
    double total = 0.0;
    for (const auto& [id, dev] : devices_) {
        if (!dev.available.load(std::memory_order_relaxed)) continue;
        total += dev.getEwmaBandwidth();
    }
    return total > 0.0 ? total : -1.0;
}

uint64_t DeviceSelector::getInflightBytes(int dev_id) const {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return 0;
    return it->second.getInflightBytes();
}

double DeviceSelector::getTransmitBandwidth(int dev_id) const {
    auto it = devices_.find(dev_id);
    if (it == devices_.end()) return -1.0;
    return it->second.getTransmitBandwidth();
}

double DeviceSelector::getAggregateTransmitBandwidth() const {
    double total = 0.0;
    for (const auto& [id, dev] : devices_) {
        if (!dev.available.load(std::memory_order_relaxed)) continue;
        total += dev.getTransmitBandwidth();
    }
    return total > 0.0 ? total : -1.0;
}

}  // namespace tent
}  // namespace mooncake
