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

namespace mooncake {
namespace tent {

class RdmaTransport;
class Workers {
   public:
    static constexpr size_t kCapacity = 1024 * 8;
    using BoundedSliceQueue = BoundedMPSCQueue<RdmaSliceList, kCapacity>;

   public:
    Workers(RdmaTransport *transport);

    ~Workers();

    Status start();

    Status stop();

    Status submit(RdmaSlice *slice);

    Status submit(RdmaSliceList &slice_list, int worker_id = -1);

    Status cancel(RdmaSliceList &slice_list);

   private:
    using Task = std::function<void()>;

    void workerThread(int thread_id);

    void asyncPostSend();

    void asyncPollCq();

    void monitorThread();

    int handleContextEvents(std::shared_ptr<RdmaContext> &context);

    Status generatePostPath(RdmaSlice *slice);

   private:
    struct RouteHint {
        SegmentDesc *segment;
        BufferDesc *buffer;
        const Topology::MemEntry *topo_entry;
        const Topology *topo;
    };

    Status getRouteHint(RouteHint &hint, SegmentID segment_id, uint64_t addr,
                        uint64_t length);

    Status selectOptimalDevice(RouteHint &source, RouteHint &target,
                               RdmaSlice *slice);

    Status selectFallbackDevice(RouteHint &source, RouteHint &target,
                                RdmaSlice *slice);

    int getDeviceByFlatIndex(const RouteHint &hint, size_t flat_idx);

    int getDeviceRank(const RouteHint &hint, int device_id);

    void showLatencyInfo();

   private:
    RdmaTransport *transport_;
    size_t num_workers_;
    std::thread monitor_;

    std::atomic<bool> running_;

    struct PostPath {
        int local_device_id;
        SegmentID remote_segment_id;
        int remote_device_id;

        bool operator==(const PostPath &rhs) const {
            return local_device_id == rhs.local_device_id &&
                   remote_segment_id == rhs.remote_segment_id &&
                   remote_device_id == rhs.remote_device_id;
        }
    };

    struct PostPathHash {
        size_t operator()(const PostPath &postPath) const {
            size_t h1 = std::hash<int>{}(postPath.local_device_id);
            size_t h2 = std::hash<SegmentID>{}(postPath.remote_segment_id);
            size_t h3 = std::hash<int>{}(postPath.remote_device_id);
            return (h1 * 10007 + h2) * 10007 + h3;
        }
    };

    std::shared_ptr<RdmaEndPoint> getEndpoint(PostPath path);

    void disableEndpoint(RdmaSlice *slice);

    using GroupedRequests =
        std::unordered_map<PostPath, std::vector<RdmaSlice *>, PostPathHash>;

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

    struct WorkerContext {
        std::thread thread;
        BoundedSliceQueue queue;
        GroupedRequests requests;
        std::unordered_set<RdmaSlice *> inflight_slice_set;
        std::atomic<int64_t> inflight_slices = 0;

        std::mutex mutex;
        std::condition_variable cv;
        volatile bool in_suspend = false;

        std::unordered_map<std::string, RailMonitor> rails;
        PerfMetricSummary perf;
        uint64_t padding[16];
    };

    WorkerContext *worker_context_;
    uint64_t slice_timeout_ns_;

    std::unique_ptr<DeviceQuota> device_quota_;
    bool always_tier1_ = false;
};
}  // namespace tent
}  // namespace mooncake

#endif  // WORKER_H
