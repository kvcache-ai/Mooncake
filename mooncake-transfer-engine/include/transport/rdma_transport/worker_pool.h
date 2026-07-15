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
#include "transport/rdma_transport/context_health_tracker.h"

namespace mooncake {
class WorkerPoolTestPeer;
class WorkerPool {
    friend class WorkerPoolTestPeer;

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
    using SliceList = std::vector<Transport::Slice *>;
    const static int kShardCount = 8;

    // Enqueue slices that were prepared by another WorkerPool. Used for
    // local-NIC failure handoff: the original worker keeps the remote path
    // fixed, updates the local lkey, and pushes the slice to this context's
    // worker queue.
    int submitPreparedPostSend(
        const std::vector<Transport::Slice *> &slice_list);
    void enqueuePreparedSlices(SliceList (&slice_list_map)[kShardCount],
                               uint64_t submitted_slice_count);

    void performPostSend(int thread_id);

    void performPollCq(int thread_id);

    void redispatch(std::vector<Transport::Slice *> &slice_list, int thread_id,
                    bool handoff_to_local_worker = false);

    void transferWorker(int thread_id);

    bool hasOutstandingCq(int thread_id);

    void monitorWorker();

    int doProcessContextEvents();
    void processContextEventForTest(ibv_event_type event_type);

    // Simplified rail monitor: pause problematic paths for a cooldown period
    struct RailState {
        int error_count = 0;
        uint64_t pause_until_ns = 0;  // Timestamp (ns) when pause expires
        uint64_t last_error_ns = 0;   // Timestamp (ns) of the last error
    };

    void markRailFailed(const std::string &peer_nic_path,
                        bool immediate_pause = false);
    bool isRailAvailable(const std::string &peer_nic_path);

    // Retry helper: increment retry count and return whether retry is allowed
    static bool shouldRetrySlice(Transport::Slice *slice);

    void refreshPublishedLocalTopology();
    GidRefreshResult refreshPublishedLocalGid();
    bool handleContextEvent(ibv_event_type event_type, bool injected_for_test,
                            struct ibv_async_event *event = nullptr);
    void scheduleContextRecovery(uint64_t delay_ns = kContextRecoveryDelayNs);
    void maybeActivateRecoveredContext();
    bool hasAvailablePeerRailAlternative(Transport::Slice *slice,
                                         const std::string &failed_peer_path);

    static bool isLocalWcFailure(const ibv_wc &wc);

    // Local-side failure handler: degrade current context so retries are
    // handed off to another local context's worker pool.
    void handleLocalFailure(const std::string &peer_nic_path,
                            RdmaEndPoint *endpoint = nullptr);

    bool tryHandoffToAnotherLocalWorker(Transport::Slice *slice);

    // Context-level circuit breaker for catastrophic local RNIC failure.
    // State lives in ContextHealthTracker. The breaker trips only when the
    // consecutive all-rails-failed submit streak spans at least
    // MC_CONTEXT_FAILURE_MIN_PEERS distinct peer servers (a streak against a
    // single dead peer must never deactivate the local context). Direct local
    // completion faults remain strong local evidence and do not require
    // multiple peers. A tripped breaker auto-reactivates (half-open) after
    // MC_CONTEXT_PAUSE_TTL_MS from the monitor tick. Fatal async events reset
    // the tracker so the TTL can never resurrect an event-deactivated context.
    //
    // Every breaker-driven context-active transition -- submitter-thread
    // trips, monitor-thread TTL reactivation, and physical-event recovery --
    // updates context_.set_active() inside the tracker mutex, so the flag can
    // never diverge from the trip state. Without that, a trip's
    // set_active(false) interleaving with recovery's set_active(true) + reset()
    // could leave the context inactive with no armed trip: TTL recovery would
    // never run and the context would be skipped forever.
    bool contextHealthy() const { return !health_tracker_.tripped(); }
    void markContextSuccess() { health_tracker_.recordSuccess(); }
    bool markContextFailure(const std::unordered_set<std::string> &failed_peers,
                            int min_peers, const char *failure_kind);
    void maybeReactivateContext();
    // Fatal async event owns the inactive state: clear the breaker (cancels
    // any pending TTL reactivation) and deactivate, atomically.
    void onFatalEventDeactivate() {
        health_tracker_.reset([this] { context_.set_active(false); });
    }
    // Successful delayed port recovery: clear the breaker (streak AND any
    // armed trip) and reactivate, atomically.
    void onRecoveredContextActivate() {
        health_tracker_.reset([this] { context_.set_active(true); });
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

    std::unordered_map<std::string, SliceList> slice_queue_[kShardCount];
    std::atomic<uint64_t> slice_queue_count_[kShardCount];
    TicketLock slice_queue_lock_[kShardCount];

    std::vector<std::unordered_map<std::string, SliceList>>
        collective_slice_queue_;

    std::atomic<uint64_t> submitted_slice_count_, processed_slice_count_;
    std::atomic<uint64_t> recovery_activate_after_ns_{0};

    // Rail state management: peer_nic_path -> RailState
    std::unordered_map<std::string, RailState> rail_states_;
    std::mutex rail_state_lock_;

    // Rail monitor configuration
    const static int kRailErrorThreshold = 5;  // Errors before pause
    // Errors further apart than this are not consecutive, so error_count is
    // restarted. Without it a long-lived process accumulates isolated failures
    // until an otherwise healthy rail is paused.
    const static uint64_t kRailErrorWindowNs = 5000000000ull;  // 5 seconds
    const static uint64_t kContextRecoveryDelayNs =
        30000000000ull;  // 30 seconds before a recovered local RNIC is reused

    // Context-level circuit breaker (see markContextFailure above)
    ContextHealthTracker health_tracker_;
    const static int kContextFailureThreshold =
        32;  // consecutive all-rails-failed
};
}  // namespace mooncake

#endif  // WORKER_H
