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

#ifndef RDMA_WORKERS_H
#define RDMA_WORKERS_H

#include <future>
#include <queue>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <numeric>

#include "context.h"
#include "v1/utility/system.h"

namespace mooncake {
namespace v1 {

class RdmaTransport;
class Workers {
   public:
    struct BoundedSliceQueue {
        const static size_t kCapacity = 1024 * 64;
        std::atomic<uint64_t> head;
        uint64_t padding1[7];
        std::atomic<uint64_t> tail;
        uint64_t padding2[7];
        std::mutex mutex;
        RdmaSliceList *entries;

        BoundedSliceQueue() { entries = new RdmaSliceList[kCapacity]; }

        ~BoundedSliceQueue() { delete[] entries; }

        void push(RdmaSliceList &slice_list) {
            if (slice_list.num_slices == 0) return;
            std::lock_guard<std::mutex> lock(mutex);
            while (true) {
                uint64_t current_tail = tail.load(std::memory_order_relaxed);
                uint64_t next_tail = (current_tail + 1) % kCapacity;
                if (next_tail != head.load(std::memory_order_relaxed)) {
                    entries[current_tail] = slice_list;
                    tail.store(next_tail, std::memory_order_release);
                    return;
                }
            }
        }

        RdmaSliceList pop() {
            uint64_t current_head = head.load(std::memory_order_relaxed);
            if (current_head != tail.load(std::memory_order_acquire)) {
                RdmaSliceList result = entries[current_head];
                head.store((current_head + 1) % kCapacity,
                           std::memory_order_release);
                return result;
            } else {
                RdmaSliceList empty_result;
                return empty_result;
            }
        }

        void pop(std::vector<RdmaSliceList> &result) {
            uint64_t current_head = head.load(std::memory_order_relaxed);
            while (current_head != tail.load(std::memory_order_acquire)) {
                result.push_back(entries[current_head]);
                current_head = (current_head + 1) % kCapacity;
            }
            if (!result.empty())
                head.store(current_head, std::memory_order_release);
        }
    };

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

    void asyncPostSend(int thread_id);

    void asyncPollCq(int thread_id);

    int doHandshake(std::shared_ptr<RdmaEndPoint> &endpoint,
                    const std::string &peer_server_name,
                    const std::string &peer_nic_name);

    void monitorThread();

    int handleContextEvents(std::shared_ptr<RdmaContext> &context);

    Status generatePostPath(int thread_id, RdmaSlice *slice,
                            std::string &target_seg_name,
                            std::string &target_dev_name);

   private:
    struct RouteHint {
        SegmentDesc *segment;
        BufferDesc *buffer;
        const TopologyEntry *topo_entry_raw;
        const ResolvedTopologyEntry *topo_entry;
        const Topology *topo;
    };

    Status getRouteHint(RouteHint &hint, SegmentID segment_id, uint64_t addr,
                        uint64_t length);

    Status selectOptimalDevice(RouteHint &source, RouteHint &target,
                               RdmaSlice *slice, int thread_id);

    Status selectFallbackDevice(RouteHint &source, RouteHint &target,
                                RdmaSlice *slice, int thread_id);

    int getDeviceByFlatIndex(const RouteHint &hint, size_t flat_idx);

    int getDeviceRank(const RouteHint &hint, int device_id);

    void showLatencyInfo(int thread_id);

   private:
    RdmaTransport *transport_;
    size_t num_workers_;
    std::thread monitor_;

    std::atomic<bool> running_;
    std::mutex ep_mutex_;

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

    std::shared_ptr<RdmaEndPoint> getEndpoint(
        int local_device_id, const std::string &target_seg_name,
        const std::string &target_dev_name);

    Status resetEndpoint(RdmaSlice *slice);

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
        std::atomic<int64_t> inflight_slices = 0;

        std::mutex mutex;
        std::condition_variable cv;
        volatile bool in_suspend = false;

        std::unique_ptr<DeviceQuota> device_quota;
        std::unordered_map<std::string, std::shared_ptr<RailTopology>>
            rail_topology_map_;
        PerfMetricSummary perf;
    };

    WorkerContext *worker_context_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // WORKER_H
