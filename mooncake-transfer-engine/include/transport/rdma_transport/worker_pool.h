// Copyright 2024 KVCache.AI
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

#ifndef WORKER_H
#define WORKER_H

#include <queue>
#include <unordered_set>

#include "config.h"
#include "rdma_context.h"

namespace mooncake {
class WorkerPool {
   public:
    WorkerPool(RdmaContext &context, int numa_socket_id = 0);

    ~WorkerPool();

    // Add slices to queue, called by Transport
    int submitPostSend(const std::vector<Transport::Slice *> &slice_list);

    // Test-only interface for fault tolerance testing
#ifdef TESTING
    static bool testOnlyShouldRetrySlice(Transport::Slice *slice) {
        return shouldRetrySlice(slice);
    }

    // Test-only: get rail state for verification
    struct TestRailState {
        int error_count;
        uint64_t pause_until_ns;
    };
    TestRailState testOnlyGetRailState(const std::string &peer_nic_path) {
        std::lock_guard<std::mutex> lock(rail_state_lock_);
        auto it = rail_states_.find(peer_nic_path);
        if (it == rail_states_.end()) {
            return {0, 0};
        }
        return {it->second.error_count, it->second.pause_until_ns};
    }

    // Test-only: check if rail is available
    bool testOnlyIsRailAvailable(const std::string &peer_nic_path) {
        return isRailAvailable(peer_nic_path);
    }

    // Test-only: get rail error threshold for verification
    static int testOnlyGetRailErrorThreshold() { return kRailErrorThreshold; }

    // Test-only: simulate path failure (used for injection testing)
    void testOnlySimulatePathFailure(const std::string &peer_nic_path,
                                     RdmaEndPoint *endpoint = nullptr) {
        handlePathFailure(peer_nic_path, endpoint);
    }

    // Test-only: get current retry counter for verification
    std::atomic<int> &testOnlyGetRedispatchCounter() {
        return redispatch_counter_;
    }

    // Test-only: Submit a single slice directly to the collective_slice_queue
    // This bypasses the Transport layer and allows direct testing of WorkerPool
    void testOnlySubmitSlice(Transport::Slice *slice, int thread_id = 0) {
        collective_slice_queue_[thread_id][slice->peer_nic_path].push_back(
            slice);
    }

    // Test-only: Perform a single post-send operation (for testing)
    void testOnlyPerformPostSend(int thread_id = 0) {
        performPostSend(thread_id);
    }

    // Test-only: Poll CQ once and return the number of completions
    int testOnlyPollCqOnce(int thread_id = 0) {
        int processed = 0;
        const static size_t kPollCount = 64;
        const int kTransferWorkerCount = globalConfig().workers_per_ctx;

        for (int cq_index = thread_id; cq_index < context_.cqCount();
             cq_index += kTransferWorkerCount) {
            ibv_wc wc[kPollCount];
            int nr_poll = context_.poll(kPollCount, wc, cq_index);
            if (nr_poll < 0) continue;

            for (int i = 0; i < nr_poll; ++i) {
                Transport::Slice *slice = (Transport::Slice *)wc[i].wr_id;
                if (wc[i].status != IBV_WC_SUCCESS) {
                    // Handle WC error
                    handlePathFailure(slice->peer_nic_path,
                                      slice->rdma.endpoint);
                    if (shouldRetrySlice(slice)) {
                        // Put into redispatch queue
                        testOnlySubmitSlice(slice, thread_id);
                    } else {
                        slice->markFailed();
                        processed_slice_count_++;
                    }
                } else {
                    slice->markSuccess();
                    processed++;
                }
            }
            if (nr_poll)
                __sync_fetch_and_sub(context_.cqOutstandingCount(cq_index),
                                     nr_poll);
        }

        if (processed > 0) processed_slice_count_.fetch_add(processed);

        return processed;
    }
#endif

   private:
    void performPostSend(int thread_id);

    void performPollCq(int thread_id);

    void redispatch(std::vector<Transport::Slice *> &slice_list, int thread_id);

    void transferWorker(int thread_id);

    void monitorWorker();

    int doProcessContextEvents();

    // Simplified rail monitor: pause problematic paths for a cooldown period
    struct RailState {
        int error_count = 0;
        uint64_t pause_until_ns = 0;  // Timestamp (ns) when pause expires
    };

    void markRailFailed(const std::string &peer_nic_path);
    bool isRailAvailable(const std::string &peer_nic_path);

    // Retry helper: increment retry count and return whether retry is allowed
    static bool shouldRetrySlice(Transport::Slice *slice);

    // Unified path failure handler: marks rail failed, notifies other workers,
    // and optionally deletes the endpoint
    void handlePathFailure(const std::string &peer_nic_path,
                           RdmaEndPoint *endpoint = nullptr);

   private:
    RdmaContext &context_;
    const int numa_socket_id_;

    std::vector<std::thread> worker_thread_;
    std::atomic<bool> workers_running_;
    std::atomic<int> suspended_flag_;

    std::atomic<int> redispatch_counter_;

    std::mutex cond_mutex_;
    std::condition_variable cond_var_;

    using SliceList = std::vector<Transport::Slice *>;

    const static int kShardCount = 8;
    std::unordered_map<std::string, SliceList> slice_queue_[kShardCount];
    std::atomic<uint64_t> slice_queue_count_[kShardCount];
    TicketLock slice_queue_lock_[kShardCount];

    std::vector<std::unordered_map<std::string, SliceList>>
        collective_slice_queue_;

    std::atomic<uint64_t> submitted_slice_count_, processed_slice_count_;

    // Rail state management: peer_nic_path -> RailState
    std::unordered_map<std::string, RailState> rail_states_;
    std::mutex rail_state_lock_;

    // Rail monitor configuration
    const static int kRailErrorThreshold = 5;            // Errors before pause
    const static uint64_t kRailPauseNs = 1000000000ull;  // 1 second pause
};
}  // namespace mooncake

#endif  // WORKER_H
