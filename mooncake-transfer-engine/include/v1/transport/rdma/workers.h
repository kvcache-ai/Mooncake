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

    Status submit(RdmaSliceList &slice_list);

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

    std::shared_ptr<RdmaEndPoint> getEndpoint(int thread_id, Workers::PostPath path);

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
};
}  // namespace v1
}  // namespace mooncake

#endif  // WORKER_H
