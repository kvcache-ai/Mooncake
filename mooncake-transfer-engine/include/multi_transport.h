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

#ifndef MULTI_TRANSPORT_H_
#define MULTI_TRANSPORT_H_

#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "transport/transport.h"
#include "task_congestion_status.h"

namespace mooncake {

// Snapshot of a batch that reached a terminal state. Drain copies this out
// of the ready queue and does not look the batch up again.
struct BatchCompletion {
    Transport::BatchID batch_id = 0;
    Transport::TransferStatus status{};
};

class TransferEngineImpl;
class TransferEngineImplTestPeer;
class MultiTransportTestPeer;
class MultiTransportBatchTestPeer;

class MultiTransport {
    friend class TransferEngineImpl;
    friend class TransferEngineImplTestPeer;
    friend class MultiTransportTestPeer;
    friend class MultiTransportBatchTestPeer;

   public:
    using BatchID = Transport::BatchID;
    using TransferRequest = Transport::TransferRequest;
    using TransferStatus = Transport::TransferStatus;
    using TransferStatusEnum = Transport::TransferStatusEnum;
    using BatchDesc = Transport::BatchDesc;

    MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                   std::string &local_server_name);

    ~MultiTransport();

    struct ScatterSubmission {
        BatchID batch_id = static_cast<BatchID>(-1);
        std::vector<size_t> task_sizes;
    };

    BatchID allocateBatchID(size_t batch_size);

    // An OK or BatchCleanupDeferred return invalidates batch_id. A deferred
    // cleanup means in-flight transport work may still own transfer buffers.
    Status freeBatchID(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries);

    Status submitScatter(const std::vector<TransferRequest> &entries,
                         ScatterSubmission &submission);

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_submitTransfer(BatchID batch_id,
                             const std::vector<TransferRequest> &entries,
                             std::string &proto);
#endif

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    Status getTaskCongestionState(BatchID batch_id, size_t task_id,
                                  TaskCongestionState &state) const;

    Status getTaskCongestionDetail(BatchID batch_id, size_t task_id,
                                   TaskCongestionDetail &detail) const;

    Status getScatterRequestStatuses(
        BatchID batch_id, size_t task_id,
        std::vector<TransferStatusEnum> &request_statuses);

    // walk_tasks skips the finished-task counter shortcut and asks every task.
    Status getBatchTransferStatus(BatchID batch_id, TransferStatus &status,
                                  bool walk_tasks = false);

    // See TransferEngine::completionFd() / RFC #5164.
    int completionFd() const { return completion_fd_; }

    // See TransferEngine::enableBatchCompletionNotification().
    Status enableBatchCompletionNotification(BatchID batch_id);

    // See TransferEngine::drainCompletions().
    Status drainCompletions(std::vector<BatchCompletion> &out);

    Transport *installTransport(const std::string &proto,
                                std::shared_ptr<Topology> topo);

    Transport *getTransport(const std::string &proto);

    /**
     * @brief Check if TCP is the only installed host transport.
     *
     * When only TCP is available (no RDMA, NVLink, etc.), local memcpy is
     * preferred over TCP loopback for same-host transfers. POSIX SHM is
     * intra-node only and does not change this classification.
     */
    bool isTcpOnly() const;

    std::vector<Transport *> listTransports();

    void *getBaseAddr();

   private:
    Status freeBatchID(BatchID batch_id,
                       const std::function<void()> &before_delete);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries,
                          std::vector<size_t> *task_sizes);

    Status selectTransports(const std::vector<TransferRequest> &entries,
                            std::vector<Transport *> &transports);

    // Stamp a task's submission time and direction for the metrics, before it
    // is posted to its transport.
    static void recordTaskStart(Transport::TransferTask &task,
                                const TransferRequest &request);

    // Record `task` in the metrics once, on the first poll or batch free that
    // observes a terminal `status` (COMPLETED, FAILED, CANCELED or TIMEOUT).
    static void recordTaskEnd(Transport::TransferTask &task,
                              const TransferStatus &status);

    // Record a finished `task` from the outcome the event-driven completion
    // callback saved, or else from its slice counters.
    static void recordFinishedTask(Transport::TransferTask &task);

    // True while the metrics record tasks.
    static bool metricsEnabled();

    // If `allows_reuse` is non-null it is written on every return. True means
    // this segment's transport is a function of `target_id` only, so the
    // caller may reuse the returned pointer for later requests with the same
    // `target_id`. False for mixed-protocol segments (comma in `protocol`) and
    // invalid IDs. Passing nullptr skips the flag.
    Status selectTransport(const TransferRequest &entry, Transport *&transport,
                           bool *allows_reuse = nullptr);

    Status tryFreeBatchID(BatchID batch_id,
                          const std::function<void()> &before_delete);

    void deferredCleanupLoop();

    // Drops a still-queued id and ignores a later terminal callback. The
    // batch must still be alive. Called only once freeBatchID has committed
    // to invalidating the handle.
    void discardQueuedCompletion(BatchID batch_id);

    void enqueueTerminal(BatchID batch_id, TransferStatus status);

    static void onTerminal(void *ctx, BatchID batch_id, TransferStatus status);

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_selectTransport(const TransferRequest &entry,
                              Transport *&transport,
                              std::string &preferred_proto);
#endif

   private:
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::map<std::string, std::shared_ptr<Transport>> transport_map_;
    RWSpinlock batch_desc_lock_;
    std::unordered_map<BatchID, std::shared_ptr<BatchDesc>> batch_desc_set_;

    std::mutex deferred_cleanup_mutex_;
    std::condition_variable deferred_cleanup_cv_;
    std::unordered_map<BatchID, std::function<void()>>
        deferred_cleanup_batches_;
    bool stop_deferred_cleanup_ = false;
    std::thread deferred_cleanup_thread_;

    // completion_mu_ guards ready_ and BatchDesc::terminal_suppressed.
    // The eventfd is written only when ready_ goes from empty to non-empty.
    // The write stays outside the lock and is not retried.
    std::mutex completion_mu_;
    std::vector<BatchCompletion> ready_;
    int completion_fd_ = -1;
};
}  // namespace mooncake

#endif  // MULTI_TRANSPORT_H_
