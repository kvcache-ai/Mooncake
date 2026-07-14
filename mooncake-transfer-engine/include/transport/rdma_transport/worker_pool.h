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

    void trackPostedSlices(const std::vector<Transport::Slice *> &slice_list,
                           size_t first, size_t count);
    void untrackPostedSlices(const std::vector<Transport::Slice *> &slice_list,
                             size_t first, size_t count);

   private:
    // Enqueue slices that were prepared by another WorkerPool. Used for
    // local-NIC failure handoff: the original worker keeps the remote path
    // fixed, updates the local lkey, and pushes the slice to this context's
    // worker queue.
    int submitPreparedPostSend(
        const std::vector<Transport::Slice *> &slice_list);

    void performPostSend(int thread_id);

    void performPollCq(int thread_id);

    void redispatch(std::vector<Transport::Slice *> &slice_list, int thread_id,
                    bool handoff_to_local_worker = false);

    void transferWorker(int thread_id);

    bool hasOutstandingCq(int thread_id);

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

    static bool isLocalWcFailure(const ibv_wc &wc);

    // Local-side failure handler: degrade current context so retries are
    // handed off to another local context's worker pool.
    void handleLocalFailure(const std::string &peer_nic_path,
                            RdmaEndPoint *endpoint = nullptr);

    bool tryHandoffToAnotherLocalWorker(Transport::Slice *slice);

    // Context-level health tracking for catastrophic hardware failure.
    // When all rails through a local RNIC are unavailable, increment the
    // failure counter. Reset on any success. Mark context inactive after
    // consecutive failures exceed threshold.
    bool contextHealthy() const {
        return context_failure_count_.load(std::memory_order_relaxed) <
               kContextFailureThreshold;
    }
    void markContextSuccess() {
        context_failure_count_.store(0, std::memory_order_relaxed);
    }
    void markContextFailure() {
        auto failure_count =
            context_failure_count_.fetch_add(1, std::memory_order_relaxed) + 1;
        if (failure_count >= kContextFailureThreshold) {
            LOG(WARNING) << "All rails failed for context "
                         << context_.deviceName() << " for " << failure_count
                         << " consecutive attempts, marking inactive";
            context_.set_active(false);
        }
    }

   private:
    RdmaContext &context_;
    const int numa_socket_id_;

    std::vector<std::thread> worker_thread_;
    std::atomic<bool> workers_running_;

    std::atomic<int> parked_worker_count_;

    // The poll worker updates these on every poll pass. The monitor worker
    // reads them when CQ entries stay outstanding, so a transfer timeout can
    // be distinguished from a stalled poller.
    std::atomic<uint64_t> last_poll_ts_ns_{0};
    std::atomic<uint64_t> last_poll_interval_ns_{0};
    std::atomic<uint64_t> max_poll_interval_ns_{0};

    std::mutex posted_slices_mutex_;
    std::unordered_set<Transport::Slice *> posted_slices_;

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

    // Context-level health tracking
    std::atomic<int> context_failure_count_{0};
    const static int kContextFailureThreshold =
        32;  // consecutive all-rails-failed
};
}  // namespace mooncake

#endif  // WORKER_H
