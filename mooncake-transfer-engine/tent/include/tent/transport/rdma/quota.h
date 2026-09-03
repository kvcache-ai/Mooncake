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
 *
 * Besides the selection EWMA, each device carries a transmit estimate for
 * the deadline predictors (admission drop, NIC arbitration): the same update
 * rule and clamp, but fed from a meter -- bytes completed over the time the
 * NIC spent with work posted to it, so idle gaps inside an interval are not
 * charged -- rather than from any one completion's latency. Per-completion
 * timing cannot answer "how fast does this NIC move bytes": work requests
 * are posted in batches whose timestamps are effectively one, and a poll
 * pass timestamps their completions together, so a slice's own
 * post-to-completion grows with the depth of the batch it travelled in. The
 * selection EWMA goes on learning from each successful completion's own
 * post-to-completion time on purpose, so that a NIC backed up behind
 * earlier work requests scores worse.
 */
class DeviceSelector {
   public:
    // Candidate device for allocation
    struct Candidate {
        int dev_id;
        double score;
        bool is_cross_numa;
    };

    // 64-byte aligned so the padding below really does put each hot atomic
    // on a cache line of its own (the map's nodes would otherwise place the
    // struct at any 8-byte boundary).
    struct alignas(64) DeviceInfo {
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
        std::atomic<double> ewma_transmit_bps{50e9};
        uint64_t padding4[7];
        std::atomic<uint64_t> total_bytes{0};
        uint64_t padding3[7];
        // Bytes handed to the hardware: charged when a WR is posted and
        // returned when it leaves the queue pair. Unlike inflight_bytes
        // (charged at allocation) this is the NIC's own backlog.
        std::atomic<uint64_t> posted_bytes{0};
        uint64_t padding5[7];
        // Bytes that completed successfully, monotonic.
        std::atomic<uint64_t> completed_bytes{0};
        uint64_t padding6[7];
        // Transmit meter: the counters as of the last sample, which the
        // next one measures against. Whichever worker wins the CAS on
        // meter_ts owns that sample and is the only one touching the other
        // two. resetTransmitMeter only zeroes meter_ts, so the next sample
        // rebuilds both baselines under its own CAS instead of learning; a
        // sampler that had already won before the reset finishes on the
        // baselines it took, which are at least consistent with each other.
        std::atomic<uint64_t> meter_ts{0};
        std::atomic<uint64_t> meter_completed{0};
        std::atomic<uint64_t> meter_busy_ns{0};
        // Time this NIC has spent with something posted to it, and when the
        // stretch in progress began. Advanced only on the posted_bytes
        // 0 <-> non-zero transitions, so the cost is one timestamp per burst
        // edge rather than per slice. busy_since is never cleared: a stale
        // value can only over-count busy time, which reads the link slow,
        // whereas a zero would charge it the whole clock and read it as
        // stopped.
        std::atomic<uint64_t> busy_ns{0};
        std::atomic<uint64_t> busy_since{0};
        uint64_t padding7[3];

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

        double getTransmitBandwidth() const {
            return ewma_transmit_bps.load(std::memory_order_relaxed);
        }

        uint64_t getPostedBytes() const {
            return posted_bytes.load(std::memory_order_relaxed);
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

    // The NIC's own backlog: `notePosted` when a WR reaches the hardware,
    // `notePostEnded` when it leaves the queue pair (completion, sweep or
    // cancellation). This is the queue a slice posted now waits behind --
    // work still sitting in a worker queue is not part of it. `now_ns` marks
    // the transitions between an idle and a busy NIC, which is the time the
    // transmit meter charges its bytes to.
    void notePosted(int dev_id, uint64_t bytes, uint64_t now_ns);
    void notePostEnded(int dev_id, uint64_t bytes, uint64_t now_ns);
    uint64_t getPostedBytes(int dev_id) const;
    // Nanoseconds this device has spent with something posted to it.
    uint64_t getBusyNs(int dev_id) const;

    // A successful completion moved `bytes` on the wire.
    void noteCompleted(int dev_id, uint64_t bytes);

    // Abandon the meter's current interval without learning from it. Busy
    // time is only worth dividing into bytes that the NIC spent it moving,
    // so a posted slice that ends without its bytes being counted -- failed,
    // flushed, timed out -- makes the stretch it belonged to unusable. The
    // next sample starts over from fresh baselines. A NIC that keeps
    // producing such slices faster than the meter interval therefore never
    // closes an interval and keeps its last estimate.
    void resetTransmitMeter(int dev_id);

    // Feed the transmit estimate one throughput sample -- bytes completed
    // over the busy time accrued since the last sample -- if this device's
    // meter interval has passed. Per-completion latency cannot serve here:
    // work requests are posted in batches whose timestamps are effectively
    // one, and a poll pass timestamps every completion it reaps alike, so a
    // slice's own "post to completion" grows with the batch depth. Bytes
    // over busy time does not care how the work was batched, and charges
    // nothing for the gaps in which the NIC had nothing posted.
    void maybeSampleTransmit(int dev_id, uint64_t now_ns);

    // Charge `length` bytes to one device without running device selection,
    // for a caller that has already settled on it: a retry re-posting a
    // slice whose charge the failure path returned, or a first attempt
    // falling back from the NIC the allocator picked. release() balances
    // it. Fails only for a device the selector does not know.
    Status chargeDevice(int dev_id, uint64_t length);

    // Return a slice's inflight charge and learn from its completion.
    // `latency` is a successful attempt's post->completion time and feeds
    // the selection EWMA; <= 0 means no sample. The transmit estimate is
    // fed by the meter instead, see maybeSampleTransmit().
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

    // Bytes charged to one device and not yet released, or 0 if unknown.
    uint64_t getInflightBytes(int dev_id) const;
    // Transmit estimate (bytes/s) for one device, or -1 if unknown.
    double getTransmitBandwidth(int dev_id) const;
    // Sum of the transmit estimates over all devices, or -1 if none.
    double getAggregateTransmitBandwidth() const;

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

        // Transmit estimate learning rate, same convention. One sample per
        // meter interval below, so 0.9 is ~10 intervals (100 ms) to follow a
        // change -- slow enough that one odd interval cannot flip an
        // irreversible drop decision.
        double transmit_bandwidth_learning_rate = 0.9;

        // How often a device's throughput is sampled, and how far back a
        // sample may reach before it is dropped instead of learned from: the
        // meter charges bytes to busy time, so idle gaps do not spoil an
        // interval, but a sample spanning this much wall clock describes a
        // link too far in the past to attribute to the link as it is now.
        uint64_t transmit_meter_interval_ns = 10'000'000;      // 10 ms
        uint64_t transmit_meter_max_interval_ns = 50'000'000;  // 50 ms

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
        // Both are weights on the old EWMA value; outside [0, 1] the update
        // is meaningless (negative or runaway estimates).
        sched_params_.bandwidth_learning_rate =
            std::clamp(params.bandwidth_learning_rate, 0.0, 1.0);
        sched_params_.transmit_bandwidth_learning_rate =
            std::clamp(params.transmit_bandwidth_learning_rate, 0.0, 1.0);
        // A staleness bound shorter than the sampling interval would reject
        // every sample, silently freezing the estimate on its seed.
        sched_params_.transmit_meter_max_interval_ns =
            std::max(params.transmit_meter_max_interval_ns,
                     params.transmit_meter_interval_ns);
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

    // Nanoseconds `dev` has spent with work posted, up to `now_ns`: the
    // stretches that have ended plus the one still open. Both the sample
    // and the reset measure against this, so they cannot drift apart.
    static uint64_t busyNsAt(const DeviceInfo &dev, uint64_t now_ns);

    // EWMA step with the [min, max] x theoretical clamp: new = alpha * old
    // + (1 - alpha) * observed.
    void learnRate(const DeviceInfo &dev, std::atomic<double> &series,
                   double alpha, double observed_bps) const;

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

    // `slice_bytes` is the caller's block size: slice i carries
    // min(slice_bytes, total_length - i * slice_bytes), and that is what
    // release() will return, so it is also what gets charged.
    void selectMultiPath(const std::vector<Candidate> &candidates,
                         uint32_t num_slices, uint64_t total_length,
                         uint64_t slice_bytes, std::vector<int> &slice_dev_ids,
                         bool probe_mode = false);
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SELECTOR_H
