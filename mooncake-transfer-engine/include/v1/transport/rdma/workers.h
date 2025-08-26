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
#include <vector>

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

    Status generatePostPath(int thread_id, RdmaSlice *slice);

   private:
    struct RuoteHint {
        SegmentDesc *segment;
        BufferDesc *buffer;
        const TopologyEntry *topo_entry_raw;
        const ResolvedTopologyEntry *topo_entry;
        const Topology *topo;
    };

    Status getRouteHint(RuoteHint &hint, SegmentID segment_id, uint64_t addr,
                        uint64_t length);

    Status selectOptimalDevice(RuoteHint &source, RuoteHint &target,
                               RdmaSlice *slice, int thread_id);

    Status selectFallbackDevice(RuoteHint &source, RuoteHint &target,
                                RdmaSlice *slice);

    int getDeviceByFlatIndex(const RuoteHint &hint, size_t flat_idx);

    int getDeviceRank(const RuoteHint &hint, int device_id);

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

    std::shared_ptr<RdmaEndPoint> getEndpoint(int thread_id,
                                              Workers::PostPath path);

    using GroupedRequests =
        std::unordered_map<PostPath, std::vector<RdmaSlice *>, PostPathHash>;

    struct WorkerContext {
        std::thread thread;
        BoundedSliceQueue queue;
        GroupedRequests requests;
        std::atomic<int64_t> inflight_slices = 0;

        std::mutex mutex;
        std::condition_variable cv;
        bool in_suspend = false;

        void notifyIfNeeded() {
            std::lock_guard<std::mutex> lock(mutex);
            if (in_suspend) {
                in_suspend = false;
                cv.notify_all();
            }
        }
    };

    WorkerContext *worker_context_;

    struct DeviceStats {
        DeviceStats(size_t num_devices) : num_devices(num_devices) {
            local_xfer_counts = new std::atomic<uint64_t>[num_devices];
            total_xfer_counts = new std::atomic<uint64_t>[num_devices];
            allow_remote_xfer = new std::atomic<bool>[num_devices];
        }

        ~DeviceStats() {
            delete[] local_xfer_counts;
            delete[] total_xfer_counts;
            delete[] allow_remote_xfer;
        }

        void addLocalXfer(int dev_id) {
            local_xfer_counts[dev_id].fetch_add(1, std::memory_order_relaxed);
            total_xfer_counts[dev_id].fetch_add(1, std::memory_order_relaxed);
        }

        void addRemoteXfer(int dev_id) {
            total_xfer_counts[dev_id].fetch_add(1, std::memory_order_relaxed);
        }

        bool allowRemoteXfer(int dev_id) {
            return allow_remote_xfer[dev_id].load(std::memory_order_relaxed);
        }

        void tryUpdatePeriod() {
            uint64_t current_ts = getCurrentTimeInNano();
            const uint64_t kRefreshPeriod = 10 * 1000 * 1000;  // 10ms
            if (current_ts - last_ts > kRefreshPeriod) {
                std::lock_guard<std::mutex> lock(mutex);
                if (current_ts - last_ts > kRefreshPeriod) {
                    for (size_t i = 0; i < num_devices; ++i) {
                        auto &allow = allow_remote_xfer[i];
                        if (!total_xfer_counts[i])
                            allow = true;
                        else {
                            allow = (local_xfer_counts[i] * 100 /
                                     total_xfer_counts[i]) < 5;
                        }
                        local_xfer_counts[i] = 0;
                        total_xfer_counts[i] = 0;
                    }
                    last_ts = current_ts;
                }
            }
        }

        std::mutex mutex;
        std::atomic<uint64_t> last_ts{0};
        const size_t num_devices;
        std::atomic<uint64_t> *local_xfer_counts;
        std::atomic<uint64_t> *total_xfer_counts;
        std::atomic<bool> *allow_remote_xfer;
    };

    DeviceStats device_stats_;

    bool checkAllowCrossNuma(RuoteHint &source);
};
}  // namespace v1
}  // namespace mooncake

#endif  // WORKER_H
