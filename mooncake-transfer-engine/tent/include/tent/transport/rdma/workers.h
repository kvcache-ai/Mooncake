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

#ifndef TENT_WORKERS_H
#define TENT_WORKERS_H

#include <future>
#include <memory>
#include <queue>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <numeric>

#include "context.h"
#include "rail_monitor.h"
#include "tent/common/utils/os.h"
#include "tent/common/concurrent/bounded_mpsc_queue.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {

class RdmaTransport;
class DeviceSelector;

class Workers {
    friend class RdmaTransportTestPeer;

   public:
    static constexpr size_t kCapacity = 1024 * 64;
    using BoundedSliceQueue = BoundedMPSCQueue<RdmaSliceList, kCapacity>;

   public:
    Workers(RdmaTransport* transport);

    ~Workers();

    Status start();

    Status stop();

    Status submit(RdmaSlice* slice);

    Status submit(RdmaSliceList& slice_list, int worker_id = -1);

    Status cancel(RdmaTask* task);

    DeviceSelector* getDeviceSelector() const { return device_selector_.get(); }

   private:
    using Task = std::function<void()>;
    struct WorkerContext;

    void workerThread(int thread_id);

    void asyncPostSend();

    void asyncPollCq();

    bool cancelUnpostedSlice(WorkerContext& worker, RdmaSlice* slice);

    void releaseSliceQuota(RdmaSlice* slice, double latency = 0.0);

    void monitorThread();

    // 1 Hz heartbeat from monitorThread(): drains every context's retiring
    // endpoints so reclaim is not gated on new insertions, which stall under
    // failure load.
    void reclaimEndpoints();

    // dev_id is the NicID (context_set_ index), which is also the
    // DeviceSelector id, so port events can flip that device's availability.
    int handleContextEvents(int dev_id, std::shared_ptr<RdmaContext>& context);

    // The decision half of handleContextEvents, kept apart from
    // ibv_get_async_event/ibv_ack_async_event so a synthesized event can
    // drive it in tests. Port events (PORT_ERR/PORT_ACTIVE) name their port;
    // a device's async fd delivers them for every port of the device, and a
    // context opens exactly one, so events for another port are ignored.
    void applyContextEvent(int dev_id, RdmaContext& context,
                           const ibv_async_event& event);

    // Re-read the link speed after a port event and re-seed the selector if
    // it changed; a link that returns at the same speed keeps what it
    // learned.
    void refreshLinkSpeed(int dev_id, RdmaContext& context);

    Status generatePostPath(RdmaSlice* slice);

   private:
    struct RouteHint {
        // Owning reference to the segment snapshot; keeps all raw pointers
        // below valid for the lifetime of this hint.
        SegmentDescRef pin;
        SegmentDesc* segment;
        BufferDesc* buffer;
        const Topology::MemEntry* topo_entry;
        const Topology* topo;
        std::string location;
    };

    Status getRouteHint(RouteHint& hint, SegmentID segment_id, uint64_t addr,
                        uint64_t length);

    Status selectOptimalDevice(RouteHint& source, RouteHint& target,
                               RdmaSlice* slice);

    Status selectFallbackDevice(RouteHint& source, RouteHint& target,
                                RdmaSlice* slice);

    int getDeviceByFlatIndex(const RouteHint& hint, size_t flat_idx);

    // True if the (sdev -> tdev) NIC pair is known-unable to GPUDirect-DMA to
    // the source/target GPU (learned from prior completion errors). Used to
    // steer selection away from dead rails before posting.
    bool gdrPairExcluded(const RouteHint& source, const RouteHint& target,
                         int sdev, int tdev, int src_gpu, int dst_gpu);

    int getDeviceRank(const RouteHint& hint, int device_id);

    void showLatencyInfo();

   private:
    RdmaTransport* transport_;
    size_t num_workers_;
    std::thread monitor_;

    std::atomic<bool> running_;

    struct PostPath {
        int local_device_id;
        SegmentID remote_segment_id;
        int remote_device_id;

        bool operator==(const PostPath& rhs) const {
            return local_device_id == rhs.local_device_id &&
                   remote_segment_id == rhs.remote_segment_id &&
                   remote_device_id == rhs.remote_device_id;
        }
    };

    struct PostPathHash {
        size_t operator()(const PostPath& postPath) const {
            size_t h1 = std::hash<int>{}(postPath.local_device_id);
            size_t h2 = std::hash<SegmentID>{}(postPath.remote_segment_id);
            size_t h3 = std::hash<int>{}(postPath.remote_device_id);
            return (h1 * 10007 + h2) * 10007 + h3;
        }
    };

    std::shared_ptr<RdmaEndPoint> getEndpoint(PostPath path);

    void disableEndpoint(RdmaSlice* slice);

    using GroupedRequests =
        std::unordered_map<PostPath, std::vector<RdmaSlice*>, PostPathHash>;

    struct PerfMetric {
        void add(double val) { samples.push_back(val); }
        size_t count() { return samples.size(); }
        void clear() { samples.clear(); }
        double p50() { return percentile(50.0); }
        double p95() { return percentile(95.0); }
        double p99() { return percentile(99.0); }
        double p999() { return percentile(99.9); }

        double avg() const {
            if (samples.empty()) return 0.0;
            double sum = std::accumulate(samples.begin(), samples.end(), 0.0);
            return sum / samples.size();
        }

        double min() const {
            if (samples.empty()) return 0.0;
            return *std::min_element(samples.begin(), samples.end());
        }

        double max() const {
            if (samples.empty()) return 0.0;
            return *std::max_element(samples.begin(), samples.end());
        }

        double percentile(double p) {
            if (samples.empty()) return 0.0;
            if (p <= 0) return min();
            if (p >= 100) return max();
            std::vector<double> sorted = samples;
            std::sort(sorted.begin(), sorted.end());
            double rank = (p / 100.0) * (sorted.size() - 1);
            size_t idx = static_cast<size_t>(rank);
            double frac = rank - idx;
            if (idx + 1 < sorted.size()) {
                return sorted[idx] * (1.0 - frac) + sorted[idx + 1] * frac;
            } else {
                return sorted[idx];
            }
        }

        std::vector<double> samples;
    };

    struct PerfMetricSummary {
        PerfMetric enqueue_lat;
        PerfMetric inflight_lat;
    };

    static constexpr int kNumPriorityLevels = PRIO_LOW + 1;

    struct WorkerContext {
        std::thread thread;
        BoundedSliceQueue queues[kNumPriorityLevels];  // Priority queues
        GroupedRequests requests;
        std::unordered_set<RdmaSlice*> inflight_slice_set;
        std::atomic<int64_t> inflight_slices = 0;

        std::mutex mutex;
        std::condition_variable cv;
        volatile bool in_suspend = false;

        // Next time to check for priority promotions (nanoseconds)
        uint64_t next_promotion_check_ns = 0;

        // Tick-internal re-enqueues that found their target queue full
        // (target priority, entry). Parked items stay counted in
        // inflight_slices, which both keeps the accounting continuous and
        // keeps the worker from suspending while they are pending. The
        // asyncPostSend drain consumes this list before the shared queues,
        // so parked entries can never starve behind contending producers.
        std::vector<std::pair<int, RdmaSliceList>> requeue_overflow;

        // Values are held via unique_ptr so that map rehashing does not
        // invalidate pointers into RailMonitor stored on in-flight slices
        // (see RdmaSlice::rail_monitor).
        std::unordered_map<std::string, std::unique_ptr<RailMonitor>> rails;
        PerfMetricSummary perf;
        uint64_t padding[15];
    };

    // Promote timed-out low priority requests to higher priority queues
    void promoteTimedOutRequests(WorkerContext& worker);

    // Tick-internal re-enqueue: the worker thread must never block on its own
    // queue (issue #3637), so these park into requeue_overflow on a full queue
    // and the asyncPostSend drain consumes them first.
    void submitFromTick(WorkerContext& worker, RdmaSlice* slice);

    WorkerContext* worker_context_;
    uint64_t slice_timeout_ns_;
    uint64_t priority_promotion_timeout_ns_;  // Timeout for priority promotion
    // Opt-in (issue #2528): when true, a promotion pass promotes exactly the
    // entries that have themselves timed out, instead of promoting the whole
    // queue whenever only the head has timed out. Default false keeps the
    // historical "flush the tier" behavior.
    bool priority_promotion_per_entry_ = false;

    std::unique_ptr<DeviceSelector> device_selector_;
    // File contents loaded once from workers.rail_topo_path and shared by all
    // per-worker/per-peer RailMonitor instances.
    std::string rail_topo_json_;
    bool always_tier1_ = false;
    // Opt-in deadline-aware bandwidth arbitration within a priority tier
    // (RFC #2792). Default false = original FIFO order (equal bandwidth split).
    bool deadline_bw_arbitration_ = false;
};
}  // namespace tent
}  // namespace mooncake

#endif  // WORKER_H
