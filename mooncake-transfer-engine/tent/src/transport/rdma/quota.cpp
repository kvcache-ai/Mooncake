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

#include <assert.h>
#include <unordered_set>

namespace mooncake {
namespace tent {
Status DeviceQuota::loadTopology(std::shared_ptr<Topology>& local_topology) {
    local_topology_ = local_topology;
    std::unordered_set<int> used_numa_id;
    for (size_t dev_id = 0; dev_id < local_topology->getNicCount(); ++dev_id) {
        auto entry = local_topology->getNicEntry(dev_id);
        if (!entry || entry->type != Topology::NIC_RDMA) continue;
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
    double beta0{0.0};  // Fixed overhead (μs), origin/main: 0.0
    double beta1{1.0};  // Bandwidth correction factor, origin/main: 1.0

    double beta0_min_observed{0.0};
    double beta0_max_observed{500.0};
    double beta1_min_observed{0.5};
    double beta1_max_observed{20.0};
    uint32_t sample_count{0};
    static constexpr double kDecayFactor = 0.98;
    static constexpr uint32_t kWarmupSamples = 50;

    void updateBounds(double b0, double b1) {
        sample_count++;
        if (sample_count <= kWarmupSamples) {
            beta0_min_observed = std::min(beta0_min_observed, b0);
            beta0_max_observed = std::max(beta0_max_observed, b0);
            beta1_min_observed = std::min(beta1_min_observed, b1);
            beta1_max_observed = std::max(beta1_max_observed, b1);
        } else {
            beta0_min_observed =
                kDecayFactor * beta0_min_observed +
                (1 - kDecayFactor) * std::min(beta0_min_observed, b0);
            beta0_max_observed =
                kDecayFactor * beta0_max_observed +
                (1 - kDecayFactor) * std::max(beta0_max_observed, b0);
            beta1_min_observed =
                kDecayFactor * beta1_min_observed +
                (1 - kDecayFactor) * std::min(beta1_min_observed, b1);
            beta1_max_observed =
                kDecayFactor * beta1_max_observed +
                (1 - kDecayFactor) * std::max(beta1_max_observed, b1);
        }
    }

    void getBounds(double& b0_min, double& b0_max, double& b1_min,
                   double& b1_max) {
        if (sample_count < kWarmupSamples) {
            b0_min = 0.0;
            b0_max = 500.0;
            b1_min = 0.5;
            b1_max = 20.0;
        } else {
            double range0 = beta0_max_observed - beta0_min_observed;
            double range1 = beta1_max_observed - beta1_min_observed;
            b0_min = std::max(0.0, beta0_min_observed);
            b0_max = beta0_max_observed + 2.0 * range0;
            b1_min = std::max(0.5, beta1_min_observed);
            b1_max = beta1_max_observed + 2.0 * range1;
        }
    }
};

thread_local std::unordered_map<int, TlsDeviceInfo> tl_device_info;

Status DeviceQuota::allocate(uint64_t length, const std::string& location,
                             int& chosen_dev_id) {
    auto entry = local_topology_->getMemEntry(location);
    if (!entry) return Status::InvalidArgument("Unknown location" LOC_MARK);

    if (!enable_quota_) {
        thread_local int id = 0;
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            auto& list = entry->device_list[rank];
            if (list.empty()) continue;
            chosen_dev_id = list[id % list.size()];
            id++;
            return Status::OK();
        }
        return Status::DeviceNotFound("no eligible devices for " + location);
    }

    static constexpr double penalty[] = {1.0, 3.0, 10.0};
    const double w = local_weight_;
    std::unordered_map<int, double> score_map;
    bool found_device = false;
    double best_score = std::numeric_limits<double>::infinity();
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        if (rank == Topology::DevicePriorityRanks - 1 && !allow_cross_numa_ &&
            found_device)
            continue;
        for (int dev_id : entry->device_list[rank]) {
            if (!devices_.count(dev_id)) continue;
            auto& dev = devices_[dev_id];
            auto& tl_dev = tl_device_info[dev_id];
            uint64_t overall_active_bytes =
                dev.diffusion_active_bytes.load(std::memory_order_relaxed) +
                dev.active_bytes.load(std::memory_order_relaxed);
            double weighted_active = w * tl_dev.active_bytes +
                                     (1.0 - w) * overall_active_bytes + length;
            double beta0_g = dev.beta0.load(std::memory_order_relaxed);
            double beta1_g = dev.beta1.load(std::memory_order_relaxed);
            double beta0 = w * tl_dev.beta0 + (1.0 - w) * beta0_g;
            double beta1 = w * tl_dev.beta1 + (1.0 - w) * beta1_g;
            double bw_bytes_per_sec = dev.bw_gbps * 1e9 / 8;
            double theory_time_us = weighted_active / bw_bytes_per_sec * 1e6;
            double predicted_time = beta0 + beta1 * theory_time_us;
            double score = penalty[rank] * predicted_time;

            // QoS penalty: lower priority gets penalized when higher priority
            // is active
            if (shared_quota_ && priority_ > PRIO_HIGH) {
                uint64_t high_load = shared_quota_->getHighPrioLoad(dev_id);
                uint64_t med_load = shared_quota_->getMediumPrioLoad(dev_id);
                constexpr uint64_t QOS_THRESHOLD = 1024 * 1024;  // 1MB

                if (priority_ == PRIO_MEDIUM && high_load > QOS_THRESHOLD) {
                    score *= 2.0;
                } else if (priority_ == PRIO_LOW &&
                           (high_load > QOS_THRESHOLD ||
                            med_load > QOS_THRESHOLD)) {
                    score *= 5.0;
                }
            }

            score_map[dev_id] = score;

            // Idle discount: give unused devices a chance (anti-starvation)
            uint64_t last_ns =
                dev.last_update_ns.load(std::memory_order_relaxed);
            if (last_ns > 0) {
                uint64_t idle_ns = getFastTimeNanos() - last_ns;
                if (idle_ns > 10e8) {  // idle > 1 second
                    double discount = std::min(0.4, idle_ns / 60e8);  // max 40%
                    score_map[dev_id] *= (1.0 - discount);
                }
            }
            best_score = std::min(best_score, score_map[dev_id]);
            found_device = true;
        }
    }

    if (!found_device) {
        return Status::DeviceNotFound("no eligible devices for " + location);
    }

    std::vector<int> filtered;
    for (const auto& [dev_id, score] : score_map) {
        if (score <= best_score * 1.05) filtered.push_back(dev_id);
    }

    std::sort(filtered.begin(), filtered.end(), [&](int a, int b) {
        if (std::abs(score_map[a] - score_map[b]) > 1e-9)
            return score_map[a] < score_map[b];
        return a < b;
    });

    thread_local size_t rr_index = 0;
    chosen_dev_id = filtered[rr_index % filtered.size()];
    rr_index++;

    tl_device_info[chosen_dev_id].active_bytes += length;
    if (local_weight_ < 1 - 1e-6)
        devices_[chosen_dev_id].active_bytes.fetch_add(
            length, std::memory_order_relaxed);
    return Status::OK();
}

Status DeviceQuota::release(int dev_id, uint64_t length, double latency) {
    if (!enable_quota_) return Status::OK();
    auto it = devices_.find(dev_id);
    if (it == devices_.end())
        return Status::InvalidArgument("device not found");

    auto& dev = it->second;
    auto& tl_dev = tl_device_info[dev_id];

    if (local_weight_ < 1 - 1e-6)
        dev.active_bytes.fetch_sub(length, std::memory_order_relaxed);
    tl_dev.active_bytes -= length;

    if (!update_quota_params_) return Status::OK();

    // Convert observed latency to microseconds
    constexpr double SEC_TO_US = 1e6;
    double obs_time = latency * SEC_TO_US;

    const double w = local_weight_;
    double beta0_g = dev.beta0.load(std::memory_order_relaxed);
    double beta1_g = dev.beta1.load(std::memory_order_relaxed);
    double beta0 = w * tl_dev.beta0 + (1.0 - w) * beta0_g;
    double beta1 = w * tl_dev.beta1 + (1.0 - w) * beta1_g;

    // Estimate active bytes at request time (for more accurate update)
    // At release time, active_bytes has already been decremented,
    // so we add length back to estimate the value at request time.
    uint64_t estimated_active = tl_dev.active_bytes + length;
    if (local_weight_ < 1 - 1e-6) {
        uint64_t overall_active =
            dev.active_bytes.load(std::memory_order_relaxed) +
            dev.diffusion_active_bytes.load(std::memory_order_relaxed);
        estimated_active = std::max(estimated_active, overall_active);
    }

    // Predict using estimated active (origin/main formula)
    double bw_bytes_per_sec = dev.bw_gbps * 1e9 / 8;
    double theory_time_us = estimated_active / bw_bytes_per_sec * SEC_TO_US;
    double pred_time = beta0 + beta1 * theory_time_us;
    double err = obs_time - pred_time;
    double rel_err = (pred_time > 1e-3) ? (err / pred_time) : 0.0;

    // Adaptive learning rate
    double adapt_alpha = alpha_;
    if (std::abs(err) > 0.05 * pred_time && pred_time > 1e-3)
        adapt_alpha = std::min(1.0, alpha_ * 5.0);

    // EWMA update (origin/main style)
    double delta0 = adapt_alpha * err;
    double new_beta0_l = tl_dev.beta0 + w * delta0;
    double new_beta0_g = beta0_g + (1.0 - w) * delta0;

    double delta1 = adapt_alpha * rel_err;
    double new_beta1_l = tl_dev.beta1 * (1.0 + w * delta1);
    double new_beta1_g = beta1_g * (1.0 + (1.0 - w) * delta1);
    double beta0_min, beta0_max, beta1_min, beta1_max;
    if (use_robust_clamp_) {
        tl_dev.updateBounds(new_beta0_l, new_beta1_l);
        tl_dev.getBounds(beta0_min, beta0_max, beta1_min, beta1_max);
    } else {
        // Static bounds: origin/main values in microseconds
        beta0_min = 0.0;
        beta0_max = 500.0;
        beta1_min = 0.5;
        beta1_max = 20.0;
    }

    tl_dev.beta0 = std::clamp(new_beta0_l, beta0_min, beta0_max);
    tl_dev.beta1 = std::clamp(new_beta1_l, beta1_min, beta1_max);

    if (local_weight_ < 1 - 1e-6) {
        dev.last_update_ns.store(getFastTimeNanos(), std::memory_order_relaxed);
        dev.beta0.store(std::clamp(new_beta0_g, beta0_min, beta0_max),
                        std::memory_order_relaxed);
        dev.beta1.store(std::clamp(new_beta1_g, beta1_min, beta1_max),
                        std::memory_order_relaxed);

        if (shared_quota_) {
            thread_local uint64_t tl_last_ts = 0;
            uint64_t now = getCurrentTimeInNano();
            if (now - tl_last_ts > diffusion_interval_) {
                tl_last_ts = now;
                return shared_quota_->diffusion();
            }
        }
    }
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake