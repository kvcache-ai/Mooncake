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

#ifndef TRANSFER_ENGINE_IMPL_H_
#define TRANSFER_ENGINE_IMPL_H_

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/runtime/admission_queue.h"
#include "tent/runtime/transport.h"
#include "tent/runtime/transport_selector.h"
#include "tent/runtime/hp_tcp_transport_config.h"

namespace mooncake {
namespace tent {

class Batch;
class Topology;
class Transport;
class SegmentDesc;
class AllocatedMemory;
class ControlService;
class SegmentTracker;
class Platform;
class ProxyManager;
class ProgressWorker;

// How long a poll loop should pause before polling again. Zero means poll
// immediately.
//
// progressBatch takes progress_mutex_ every call, so an unthrottled loop does
// not just burn a core -- it contends with every other thread's submit and
// poll path. Hot for the first iterations so short transfers keep their
// latency, then exponential backoff to a cap.
std::chrono::microseconds nextPollDelay(uint64_t poll_count);

// One backoff step: yields while nextPollDelay is zero, sleeps after that.
void waitBeforeNextPoll(uint64_t poll_count);

struct TaskInfo {
    TransportType type{UNSPEC};
    int sub_task_id{-1};
    bool derived{false};                  // merged by other tasks
    int xport_priority{0};                // transport priority (for fallback)
    int failover_count{0};                // number of failover attempts
    int metadata_refresh_retry_count{0};  // same-transport stale-cache retries
    bool suppress_failover{false};        // permanent transport result
    uint64_t device_mask{~0ULL};          // Device mask for quota allocation
    std::string qp_pool;  // Named QP pool (RFC #2568 step 3), "" = none
    Request request;
    bool staging{false};
    bool cancel_requested{false};
    TransferStatusEnum status{TransferStatusEnum::PENDING};
    std::atomic<TransferStatusEnum> staging_status{TransferStatusEnum::PENDING};
    std::chrono::steady_clock::time_point start_time{};     // Request submit
    std::chrono::steady_clock::time_point dispatch_time{};  // Initial dispatch
    std::chrono::steady_clock::time_point post_time{};      // Initial post
    // Current physical attempt. Replaced before each transport submission;
    // post_time above intentionally remains the logical request's first post.
    // attempt_type is captured at attempt start so the attempt is attributed to
    // the transport that actually ran it, even if task.type is later
    // overwritten by failover before the attempt is finished.
    std::chrono::steady_clock::time_point attempt_post_time{};
    TransportType attempt_type{UNSPEC};
    bool attempt_active{false};
    // Failure attribution for tent_task_failures_total (first failure wins):
    // -1 = none, 0 = submit-stage, 1 = poll-stage. Set where the failure
    // originates and never overwritten, so a poll-observed failure stays
    // "poll" even when a later failover resubmit is synchronously rejected.
    // Invariant for integrators: mark submit failures before any recovery
    // attempt, so a task that recovers and later fails at poll still
    // attributes its root cause to submit.
    int8_t failure_stage{-1};

    TaskInfo() = default;

    TaskInfo(const TaskInfo& other)
        : type(other.type),
          sub_task_id(other.sub_task_id),
          derived(other.derived),
          xport_priority(other.xport_priority),
          failover_count(other.failover_count),
          metadata_refresh_retry_count(other.metadata_refresh_retry_count),
          suppress_failover(other.suppress_failover),
          device_mask(other.device_mask),
          qp_pool(other.qp_pool),
          request(other.request),
          staging(other.staging),
          cancel_requested(other.cancel_requested),
          status(other.status),
          staging_status(other.staging_status.load(std::memory_order_relaxed)),
          start_time(other.start_time),
          dispatch_time(other.dispatch_time),
          post_time(other.post_time),
          attempt_post_time(other.attempt_post_time),
          attempt_type(other.attempt_type),
          attempt_active(other.attempt_active),
          failure_stage(other.failure_stage) {}

    TaskInfo(TaskInfo&& other) noexcept
        : type(other.type),
          sub_task_id(other.sub_task_id),
          derived(other.derived),
          xport_priority(other.xport_priority),
          failover_count(other.failover_count),
          metadata_refresh_retry_count(other.metadata_refresh_retry_count),
          suppress_failover(other.suppress_failover),
          device_mask(other.device_mask),
          qp_pool(std::move(other.qp_pool)),
          request(std::move(other.request)),
          staging(other.staging),
          cancel_requested(other.cancel_requested),
          status(other.status),
          staging_status(other.staging_status.load(std::memory_order_relaxed)),
          start_time(other.start_time),
          dispatch_time(other.dispatch_time),
          post_time(other.post_time),
          attempt_post_time(other.attempt_post_time),
          attempt_type(other.attempt_type),
          attempt_active(other.attempt_active),
          failure_stage(other.failure_stage) {}

    TaskInfo& operator=(const TaskInfo& other) {
        if (this != &other) {
            type = other.type;
            sub_task_id = other.sub_task_id;
            derived = other.derived;
            xport_priority = other.xport_priority;
            failover_count = other.failover_count;
            metadata_refresh_retry_count = other.metadata_refresh_retry_count;
            suppress_failover = other.suppress_failover;
            device_mask = other.device_mask;
            qp_pool = other.qp_pool;
            request = other.request;
            staging = other.staging;
            cancel_requested = other.cancel_requested;
            status = other.status;
            staging_status.store(
                other.staging_status.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
            start_time = other.start_time;
            dispatch_time = other.dispatch_time;
            post_time = other.post_time;
            attempt_post_time = other.attempt_post_time;
            attempt_type = other.attempt_type;
            attempt_active = other.attempt_active;
            failure_stage = other.failure_stage;
        }
        return *this;
    }

    TaskInfo& operator=(TaskInfo&& other) noexcept {
        if (this != &other) {
            type = other.type;
            sub_task_id = other.sub_task_id;
            derived = other.derived;
            xport_priority = other.xport_priority;
            failover_count = other.failover_count;
            metadata_refresh_retry_count = other.metadata_refresh_retry_count;
            suppress_failover = other.suppress_failover;
            device_mask = other.device_mask;
            qp_pool = std::move(other.qp_pool);
            request = std::move(other.request);
            staging = other.staging;
            cancel_requested = other.cancel_requested;
            status = other.status;
            staging_status.store(
                other.staging_status.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
            start_time = other.start_time;
            dispatch_time = other.dispatch_time;
            post_time = other.post_time;
            attempt_post_time = other.attempt_post_time;
            attempt_type = other.attempt_type;
            attempt_active = other.attempt_active;
            failure_stage = other.failure_stage;
        }
        return *this;
    }
};

class TransferEngineImpl {
    friend class ProxyManager;

   public:
    TransferEngineImpl();

    TransferEngineImpl(std::shared_ptr<Config> config);

    ~TransferEngineImpl();

    TransferEngineImpl(const TransferEngineImpl&) = delete;

    TransferEngineImpl& operator=(const TransferEngineImpl&) = delete;

   public:
    bool available() const { return available_; }

    const std::string getSegmentName() const;

    const std::string getRpcServerAddress() const;

    uint16_t getRpcServerPort() const;

    // Local topology discovered (or loaded from custom matrix) at construct.
    std::shared_ptr<Topology> getLocalTopology() const;

   public:
    Status exportLocalSegment(std::string& shared_handle);

    Status importRemoteSegment(SegmentID& handle,
                               const std::string& shared_handle);

    Status openSegment(SegmentID& handle, const std::string& segment_name);

    Status closeSegment(SegmentID handle);

    Status getSegmentInfo(SegmentID handle, SegmentInfo& info);

   public:
    Status allocateLocalMemory(void** addr, size_t size,
                               Location location = kWildcardLocation);

    Status allocateLocalMemory(void** addr, size_t size, Location location,
                               bool internal);

    Status freeLocalMemory(void* addr);

    Status registerLocalMemory(void* addr, size_t size,
                               Permission permission = kGlobalReadWrite);

    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               Permission permission = kGlobalReadWrite);

    Status unregisterLocalMemory(void* addr, size_t size = 0,
                                 bool update_metadata = true);

    Status unregisterLocalMemory(std::vector<void*> addr_list,
                                 std::vector<size_t> size_list = {},
                                 bool update_metadata = true);

    // advanced buffer allocate function
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions& options);

    // advanced buffer register function
    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               MemoryOptions& options);

   public:
    BatchID allocateBatch(size_t batch_size);

    Status freeBatch(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list,
                          const Notification& notifi);

    Status cancelTransfer(BatchID batch_id, size_t task_id);

    Status sendNotification(SegmentID target_id, const Notification& notifi);

    Status receiveNotification(std::vector<Notification>& notifi_list);

    Status probePeerAliveByID(SegmentID target_id);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status);

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus>& status_list);

    Status getTransferStatus(BatchID batch_id, TransferStatus& overall_status);

    Status progressBatch(BatchID batch_id, TransferStatus& overall_status);

    Status getNicLoadStats(std::vector<NicLoadStats>& stats) const;

    Status waitTransferCompletion(BatchID batch_id);

    Status transferSync(const std::vector<Request>& request_list);

    uint64_t lockStageBuffer(const std::string& location);

    Status unlockStageBuffer(uint64_t addr);

    // Test-only hook: replace the transport in a given slot after construct().
    // Production code never calls this. Used by failover integration tests to
    // inject a FaultProxyTransport without bypassing resubmitTransferTask,
    // resolveTransport, or any other engine state. Not thread-safe with any
    // in-flight transfer on that slot.
    void swapTransportForTest(TransportType type,
                              std::shared_ptr<Transport> xport) {
        if (type >= 0 && type < (TransportType)kSupportedTransportTypes) {
            transport_list_[type] = std::move(xport);
        }
    }

    // Test-only hook: how many batches are still alive. Lets a test assert
    // that a failed transfer released its batch rather than leaking it.
    size_t aliveBatchCountForTest() {
        size_t count = 0;
        for (auto& shard : batch_shards_) {
            std::lock_guard<std::recursive_mutex> lk(shard.mtx);
            count += shard.alive_batches.size();
        }
        return count;
    }

    // Test-only hook: how many undrained staging batches the ProxyManager
    // handed over for deferred teardown. Lets a regression test assert the
    // ownership-transfer path ran instead of freeing memory the transport
    // workers could still touch.
    size_t deferredStageTeardownBatchCountForTest() {
        std::lock_guard<std::recursive_mutex> lk(progress_mutex_);
        return deferred_stage_teardown_.batches.size();
    }

    // Wake the optional event-driven progress worker for `batch_id`. No-op if
    // enable_progress_worker is false. Transport completion paths use this as
    // an idempotent "maybe ready" signal.
    void notifyBatchMaybeReady(BatchID batch_id);

   private:
    friend class ProgressWorker;

    Status construct();

    Status deconstruct();

    Status setupLocalSegment();

    Status lazyFreeBatch();

    SelectionResult getTransportType(const Request& request,
                                     int transport_index = 0);

    std::vector<TransportType> getSupportedTransports(
        TransportType request_type);

    Status resubmitTransferTask(Batch* batch, size_t task_id);

    // Submit-stage failover: recover a task whose synchronous
    // submitTransferTasks() failed by walking the remaining candidate
    // transports (bounded by max_failover_attempts_), mirroring the poll-time
    // failover in updateTaskStatusAfterPoll. Returns true when the task is
    // PENDING again on a fallback transport; otherwise the task is
    // terminally FAILED with task.type left at the last attempted transport
    // so failure metrics attribute to a real transport instead of UNSPEC.
    bool attemptSubmitStageFailover(Batch* batch, size_t task_id);

    Status retainBatch(BatchID batch_id, Batch*& batch);

    Status releaseBatch(Batch* batch);

    // Called by ProxyManager when its shutdown drain deadline expires: takes
    // ownership of the batches (and the local stage buffers they target) that
    // never reached a terminal state, detaching them from batch_set_ /
    // alive_batches_ so deconstruct() does not hand their SubBatch/Slice
    // objects back to the Slab while transport workers may still complete
    // them. Released only after the transports quiesce.
    struct DeferredStageTeardown;

    void adoptDeferredStageTeardown(DeferredStageTeardown&& deferred);

    class BatchRef;

    struct PreparedSubmit;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list,
                          const Notification* notifi,
                          QueueOwnerKind owner_kind);

    Status submitStagingTransfer(BatchID batch_id,
                                 const std::vector<Request>& request_list);

    Status enqueuePreparedSubmit(Batch* batch, const PreparedSubmit& prepared,
                                 QueueOwnerKind owner_kind);

    bool shouldQueueSubmit(const PreparedSubmit& prepared,
                           QueueOwnerKind owner_kind) const;

    Status prepareSubmit(Batch* batch, const std::vector<Request>& request_list,
                         PreparedSubmit& prepared);

    Status commitPreparedSubmit(Batch* batch, const PreparedSubmit& prepared);

    void attachProgressNotifier(Batch* batch, Transport::SubBatchRef sub_batch);

    uint64_t nextBatchToken();

    Status refillDispatchWindow();

    Status progressRuntimeQueue();

    bool hasActiveRuntimeQueue();

    void notifyRuntimeQueueReady();

    Status dispatchQueuedOwner(QueueOwnerId owner_id);

    Status markQueuedOwnerSubmitted(QueueOwnerId owner_id);

    Status finishQueuedOwner(QueueOwnerId owner_id,
                             TransferStatusEnum terminal_status);

    Status cancelQueuedOwner(QueueOwnerId owner_id);

    Status retireQueueForBatch(Batch* batch);

    Status pollTaskStatus(Batch* batch, size_t task_id,
                          TransferStatus& task_status);

    void updateTaskStatusAfterPoll(Batch* batch, size_t task_id,
                                   TransferStatus& task_status,
                                   bool allow_failover);

    Status getBatchStatus(BatchID batch_id, TransferStatus& overall_status,
                          bool allow_failover);

    SelectionResult resolveTransport(const Request& req, int transport_index,
                                     bool invalidate_on_fail = true);

    // Verify that req.transport_hint is usable for this request
    Status validateTransportHint(const Request& req, size_t request_index);

    Status loadTransports();

    void findStagingPolicy(const Request& req,
                           std::vector<std::string>& policy);

    Status maybeFireSubmitHooks(Batch* batch, bool check = true);

    void addSubmitHook(Batch* batch, size_t start_task_id,
                       const std::vector<Request>& request_list,
                       const Notification& notifi);

    void recordTaskCompletionMetrics(TaskInfo& task,
                                     TransferStatusEnum prev_status,
                                     TransferStatusEnum new_status);

    void startTransportAttempt(TaskInfo& task, TransportType type,
                               std::chrono::steady_clock::time_point post_time);

    void finishTransportAttempt(TaskInfo& task, TransferStatusEnum status,
                                std::chrono::steady_clock::time_point end_time);

   private:
    struct AllocatedMemory {
        void* addr;
        size_t size;
        Transport* transport;
        MemoryOptions options;
    };

    // Staging resources that ProxyManager could not drain before its shutdown
    // deadline. Their slices may still be written by transport workers, so
    // nothing here may be freed before transport_list_ is reset (which joins
    // the workers and drains the completion queues).
    struct DeferredStageTeardown {
        struct StageBuffers {
            std::string location;
            void* chunks{nullptr};
            std::atomic_flag* bitmap{nullptr};
        };
        std::vector<BatchID> batches;
        std::vector<StageBuffers> stage_buffers;
        bool empty() const { return batches.empty() && stage_buffers.empty(); }
    };

    struct RuntimeQueueConfig {
        bool enabled{false};
        QueueLimits limits{};
        size_t max_dispatch_owners{0};
        size_t max_dispatch_bytes{0};
        std::chrono::microseconds progress_fallback_interval{50000};
    };

    struct QueuedOwnerState {
        Batch* batch{nullptr};
        size_t owner_task_id{0};
        std::vector<size_t> public_task_ids;
        size_t byte_charge{0};
        bool in_dispatch_window{false};
    };

   private:
    std::shared_ptr<Config> conf_;
    HpTcpTransportConfig hp_tcp_transport_config_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Topology> topology_;
    std::unique_ptr<TransportSelector> transport_selector_;
    bool available_;

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transport_list_;
    std::unique_ptr<SegmentTracker> local_segment_tracker_;

    std::vector<AllocatedMemory> allocated_memory_;
    std::mutex mutex_;

    std::string hostname_;
    uint16_t port_;
    bool ipv6_;
    std::string local_segment_name_;

    std::unique_ptr<ProxyManager> staging_proxy_;
    bool merge_requests_;
    int max_failover_attempts_{3};
    bool enable_auto_failover_on_poll_{true};
    bool enable_progress_worker_{false};
    RuntimeQueueConfig runtime_queue_config_;
    std::unique_ptr<LocalTransferAdmissionQueue> runtime_queue_;
    std::unordered_map<QueueOwnerId, QueuedOwnerState> queued_owners_;
    size_t dispatch_inflight_owners_{0};
    size_t dispatch_inflight_bytes_{0};
    uint64_t next_batch_token_{1};

    // Serializes engine-global progress: runtime queue, lazyFreeBatch
    // freelist, and ProgressWorker. Recursive because freeBatch ->
    // lazyFreeBatch -> getTransferStatus can re-enter. See issue #2116.
    std::recursive_mutex progress_mutex_;
    // Guarded by progress_mutex_. Emptied (abandoned on purpose) at the end of
    // deconstruct(), after the transports have been destroyed.
    DeferredStageTeardown deferred_stage_teardown_;
    std::unique_ptr<ProgressWorker> progress_worker_;

    // Per-batch hot paths (poll / free / retain) lock only the shard for
    // that BatchID instead of progress_mutex_, so N threads polling N
    // independent batches do not serialize.
    //
    // Exception: enable_runtime_queue=true keeps those paths on
    // progress_mutex_ because queued_owners_ / dispatch window are
    // engine-global. The runtime queue is off by default.
    //
    // The batch registry (alive / active membership) lives INSIDE the same
    // shard: membership changes and lookups are O(1) set ops taken while
    // already holding the shard lock, so no separate global registry lock
    // sits on the poll/alloc/free hot path.
    //
    // Lock order (never reversed):
    //   progress_mutex_ -> shard
    // With the runtime queue enabled, hot paths hold progress_mutex_ and
    // take the shard only for membership ops:
    //   progress_mutex_ -> shard (membership only)
    static constexpr size_t kBatchLockShards = 64;
    struct BatchShard {
        std::recursive_mutex mtx;
        std::unordered_set<Batch*> active_batches;
        std::unordered_set<BatchID> alive_batches;
    };
    BatchShard& batchShard(BatchID batch_id) {
        return batch_shards_[reinterpret_cast<uintptr_t>(batch_id) %
                             kBatchLockShards];
    }
    std::recursive_mutex& batchShardMutex(BatchID batch_id) {
        return batchShard(batch_id).mtx;
    }
    std::recursive_mutex& progressLockFor(BatchID batch_id) {
        if (runtime_queue_config_.enabled) return progress_mutex_;
        return batchShardMutex(batch_id);
    }
    // Caller must hold progressLockFor(batch_id). In default mode that is
    // the shard itself, so the membership check is a lock-free read under
    // an already-held lock. In queue mode the caller holds progress_mutex_
    // and we take the shard briefly (progress -> shard order is allowed).
    bool isBatchAlive(BatchID batch_id) {
        BatchShard& shard = batchShard(batch_id);
        if (runtime_queue_config_.enabled) {
            std::lock_guard<std::recursive_mutex> lk(shard.mtx);
            return shard.alive_batches.count(batch_id) != 0;
        }
        return shard.alive_batches.count(batch_id) != 0;
    }
    std::array<BatchShard, kBatchLockShards> batch_shards_;
    // Deferred-free queue, guarded by progress_mutex_. Only used when the
    // fast path cannot free inline (tasks still in flight, runtime refs,
    // or runtime queue enabled).
    std::vector<Batch*> batch_freelist_;
};
}  // namespace tent
}  // namespace mooncake

#endif
