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

#include "multi_transport.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <string>
#include <thread>
#ifdef __linux__
#include <sys/eventfd.h>
#include <unistd.h>
#endif

#include "config.h"
#include "multi_transport_locality.h"
#ifdef WITH_METRICS
#include "transfer_engine_metrics.h"
#endif
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_twosided/rdma_twosided_transport.h"
#ifdef USE_BAREX
#include "transport/barex_transport/barex_transport.h"
#endif
#ifdef USE_TCP
#include "transport/tcp_transport/tcp_transport.h"
#endif
#include "transport/transport.h"
#ifdef USE_NVMEOF
#include "transport/nvmeof_transport/nvmeof_transport.h"
#endif
#ifdef USE_NCCL_HOST
#include "transport/nccl_transport/nccl_transport.h"
#endif
#ifdef USE_ASCEND_DIRECT
#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"
#endif
#if defined(USE_ASCEND) && !defined(USE_ASCEND_DIRECT)
#include "transport/ascend_transport/hccl_transport/hccl_transport.h"
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
#include "transport/ascend_transport/heterogeneous_rdma_transport.h"
#endif
#ifdef USE_INTRA_NVLINK
#include "transport/intranode_nvlink_transport/intranode_nvlink_transport.h"
#endif
#ifdef USE_HIP
#include "transport/hip_transport/hip_transport.h"
#endif
#ifdef USE_HYLINK
#include "transport/hylink_transport/hylink_transport.h"
#endif
#ifdef USE_MACA
#include "transport/maca_transport/maca_transport.h"
#endif
#ifdef USE_MUSA
#include "transport/musa_transport/musa_transport.h"
#endif
#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
#endif
#ifdef USE_CXL
#include "transport/cxl_transport/cxl_transport.h"
#endif
#include "transport/shm_transport/shm_transport.h"
#ifdef USE_UBSHMEM
#include "transport/ascend_transport/ubshmem_transport/ubshmem_transport.h"
#endif
#ifdef USE_FLAGCX
#include "transport/flagcx_transport/flagcx_transport.h"
#endif
#ifdef USE_EFA
#include "transport/efa_transport/efa_transport.h"
#endif
#ifdef USE_CXI
#include "transport/cxi_transport/cxi_transport.h"
#endif
#ifdef USE_SUNRISE
#include "transport/sunrise_link_transport/sunrise_link_transport.h"
#endif
#ifdef USE_UB
#include "transport/kunpeng_transport/ub_transport.h"
#endif

#include <cassert>

namespace mooncake {
MultiTransport::MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                               std::string& local_server_name)
    : metadata_(metadata), local_server_name_(local_server_name) {
#ifdef __linux__
    // Counter mode, not EFD_SEMAPHORE. Drain reads the sum once.
    completion_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (completion_fd_ < 0) {
        LOG(WARNING) << "completion eventfd failed: errno " << errno;
    }
#endif
}

MultiTransport::~MultiTransport() {
    {
        std::lock_guard<std::mutex> guard(deferred_cleanup_mutex_);
        stop_deferred_cleanup_ = true;
    }
    deferred_cleanup_cv_.notify_one();
    if (deferred_cleanup_thread_.joinable()) {
        deferred_cleanup_thread_.join();
    }
#ifdef __linux__
    if (completion_fd_ >= 0) {
        ::close(completion_fd_);
        completion_fd_ = -1;
    }
#endif
}

MultiTransport::BatchID MultiTransport::allocateBatchID(size_t batch_size) {
    auto batch_desc = new BatchDesc();
    if (!batch_desc) return ERR_MEMORY;
    batch_desc->id = BatchID(batch_desc);
    batch_desc->batch_size = batch_size;
    batch_desc->task_list.reserve(batch_size);
    batch_desc->context = NULL;
    // terminal_notify is installed in syncTerminalNotify after an opted-in
    // submit on a counter-capable path. Non-opted-in batches never hook the
    // wake (RFC #5164).
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_lock_.lock();
    batch_desc_set_[batch_desc->id] = batch_desc;
    batch_desc_lock_.unlock();
#endif
    return batch_desc->id;
}

Status MultiTransport::freeBatchID(BatchID batch_id) {
    return freeBatchID(batch_id, std::function<void()>());
}

Status MultiTransport::freeBatchID(BatchID batch_id,
                                   const std::function<void()>& before_delete) {
    std::lock_guard<std::mutex> guard(deferred_cleanup_mutex_);
    if (deferred_cleanup_batches_.count(batch_id) != 0) {
        return Status::BatchCleanupDeferred(
            "BatchID is invalid because cleanup has already been deferred");
    }

    Status status = tryFreeBatchID(batch_id, before_delete);
    if (!status.IsBatchBusy()) {
        return status;
    }

    // The handle is already invalid for the caller. Drop a queued wake
    // before the deferred thread deletes the descriptor.
    discardQueuedCompletion(batch_id);
    deferred_cleanup_batches_.emplace(batch_id, before_delete);
    if (!deferred_cleanup_thread_.joinable()) {
        // Start the cleanup worker lazily after taking ownership of a busy
        // batch. BatchCleanupDeferred tells callers that in-flight transport
        // work may still own buffers but the batch handle is already invalid.
        deferred_cleanup_thread_ =
            std::thread(&MultiTransport::deferredCleanupLoop, this);
    }
    deferred_cleanup_cv_.notify_one();
    LOG(WARNING) << "Batch " << batch_id
                 << " is still busy; cleanup has been deferred";
    return Status::BatchCleanupDeferred(
        "BatchID is invalid because cleanup has been deferred");
}

Status MultiTransport::tryFreeBatchID(
    BatchID batch_id, const std::function<void()>& before_delete) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    bool busy = false;
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        auto& task = batch_desc.task_list[task_id];
        if (__atomic_load_n(&task.is_finished, __ATOMIC_ACQUIRE)) {
            // Event-driven completion can finish a task without a poll
            // (e.g. mooncake-store waits on completion_cv, then frees the
            // batch).
            if (metricsEnabled()) recordFinishedTask(task);
            continue;
        }
        if (task.slice_count == 0) {
            task.is_finished = true;
            continue;
        }
        TransferStatus status;
        Status query_status = getTransferStatus(batch_id, task_id, status);
        if (!query_status.ok()) {
            return query_status;
        }
        // A terminal transfer status only means the task published completion;
        // the completion callback may still be unwinding while holding raw
        // BatchDesc access. Keep the batch alive until the explicit quiescence
        // counter reaches zero below.
        if (status.s != Transport::TransferStatusEnum::COMPLETED &&
            status.s != Transport::TransferStatusEnum::FAILED) {
            busy = true;
            // With metrics, keep going so finished tasks behind a busy one
            // are still recorded.
            if (!metricsEnabled()) break;
        }
    }
    if (busy) {
        return Status::BatchBusy(
            "BatchID cannot be freed until all tasks are done");
    }
    // A terminal status is visible to pollers before CompletionCallbackPin
    // drops: publishTerminalIfDone (eventfd wake) still runs under that pin.
    // Return BatchBusy so the deferred-cleanup path retries; do not spin.
    if (batch_desc.active_completion_callbacks.load(
            std::memory_order_acquire) != 0) {
        return Status::BatchBusy(
            "BatchID cannot be freed until completion callbacks are "
            "quiescent");
    }
    // Remove the id before the descriptor is deleted. A completion callback
    // still holding a pin cannot pass the pin check above; one that runs
    // after this sees terminal_suppressed and does not touch the queue.
    discardQueuedCompletion(batch_id);
    if (before_delete) {
        before_delete();
    }
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_.erase(batch_id);
#endif
    return Status::OK();
}

void MultiTransport::discardQueuedCompletion(BatchID batch_id) {
    std::lock_guard<std::mutex> lock(completion_mu_);
    Transport::toBatchDesc(batch_id).terminal_suppressed = true;
    ready_.erase(std::remove_if(ready_.begin(), ready_.end(),
                                [batch_id](const BatchCompletion& item) {
                                    return item.batch_id == batch_id;
                                }),
                 ready_.end());
}

void MultiTransport::clearQueuedTerminal(BatchID batch_id) {
    std::lock_guard<std::mutex> lock(completion_mu_);
    auto& batch = Transport::toBatchDesc(batch_id);
    if (batch.terminal_suppressed) return;
    batch.terminal_enqueued = false;
    ready_.erase(std::remove_if(ready_.begin(), ready_.end(),
                                [batch_id](const BatchCompletion& item) {
                                    return item.batch_id == batch_id;
                                }),
                 ready_.end());
}

Status MultiTransport::enableBatchCompletionNotification(BatchID batch_id) {
    if (completion_fd_ < 0) {
        return Status::NotSupportedTransport(
            "completion eventfd is not available");
    }
    if (globalConfig().slice_timeout > 0) {
        return Status::NotSupportedTransport(
            "completion notification requires slice_timeout disabled; "
            "timeouts are enforced only by foreground status polling");
    }
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (!batch_desc.task_list.empty()) {
        return Status::InvalidArgument(
            "enableBatchCompletionNotification must be called before "
            "submitTransfer");
    }
    batch_desc.event_enabled = true;
    return Status::OK();
}

void MultiTransport::syncTerminalNotify(BatchDesc& batch_desc) {
    // Opt-in plus a counter-capable submit path. Non-opted-in batches keep
    // the legacy fast path and never write the eventfd.
    if (batch_desc.event_enabled && batch_desc.counter_eligible &&
        completion_fd_ >= 0) {
        batch_desc.terminal_notify = &MultiTransport::onTerminal;
        batch_desc.terminal_notify_ctx = this;
    } else {
        batch_desc.terminal_notify = nullptr;
        batch_desc.terminal_notify_ctx = nullptr;
    }
}

void MultiTransport::onTerminal(void* ctx, BatchID batch_id,
                                TransferStatus status) {
    static_cast<MultiTransport*>(ctx)->enqueueTerminal(batch_id, status);
}

void MultiTransport::enqueueTerminal(BatchID batch_id, TransferStatus status) {
    bool wake = false;
    {
        std::lock_guard<std::mutex> lock(completion_mu_);
        auto& batch = Transport::toBatchDesc(batch_id);
        if (batch.terminal_suppressed || batch.terminal_enqueued) return;
        batch.terminal_enqueued = true;
        ready_.push_back(BatchCompletion{batch_id, status});
        wake = completion_fd_ >= 0;
    }
    // Local completion only. Peer notify stays on getBatchTransferStatus;
    // do not sendNotify from the CQ / completion path (RFC #5164).
#ifdef __linux__
    if (!wake) return;
    const uint64_t one = 1;
    // Wake only. EAGAIN or a short write leaves the id on the queue.
    // Do not retry here: this runs on the completion path.
    const ssize_t n = ::write(completion_fd_, &one, sizeof(one));
    (void)n;
#else
    (void)wake;
#endif
}

Status MultiTransport::drainCompletions(std::vector<BatchCompletion>& out) {
    out.clear();
    if (completion_fd_ < 0) {
        return Status::NotSupportedTransport(
            "completion eventfd is not available");
    }
#ifdef __linux__
    uint64_t value = 0;
    const ssize_t n = ::read(completion_fd_, &value, sizeof(value));
    if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        // The queue is still the record. Surface the ids, then the error
        // is visible on the next wait because the counter was not cleared.
        PLOG(WARNING) << "completion eventfd read failed";
    }
#endif
    std::lock_guard<std::mutex> lock(completion_mu_);
    out.swap(ready_);
    return Status::OK();
}

void MultiTransport::deferredCleanupLoop() {
    constexpr auto kCleanupInterval = std::chrono::milliseconds(100);
    std::unique_lock<std::mutex> lock(deferred_cleanup_mutex_);
    while (true) {
        if (deferred_cleanup_batches_.empty()) {
            if (stop_deferred_cleanup_) {
                return;
            }
            deferred_cleanup_cv_.wait(lock, [this] {
                return stop_deferred_cleanup_ ||
                       !deferred_cleanup_batches_.empty();
            });
            continue;
        }

        for (auto it = deferred_cleanup_batches_.begin();
             it != deferred_cleanup_batches_.end();) {
            Status status = tryFreeBatchID(it->first, it->second);
            if (status.ok()) {
                it = deferred_cleanup_batches_.erase(it);
            } else if (!status.IsBatchBusy()) {
                // A permanent error (e.g. a stale task reference) will never
                // resolve on retry; drop the entry instead of polling forever.
                LOG(ERROR) << "Dropping deferred cleanup for batch "
                           << it->first << ": " << status.ToString();
                it = deferred_cleanup_batches_.erase(it);
            } else {
                ++it;
            }
        }

        if (stop_deferred_cleanup_) {
            if (!deferred_cleanup_batches_.empty()) {
                LOG(WARNING)
                    << "Deferred cleanup stopped with "
                    << deferred_cleanup_batches_.size()
                    << " busy batch(es); leaving them allocated to avoid "
                       "blocking shutdown while transport callbacks are "
                       "still pending";
            }
            return;
        }
        deferred_cleanup_cv_.wait_for(lock, kCleanupInterval);
    }
}

#ifdef WITH_METRICS

void MultiTransport::recordTaskStart(Transport::TransferTask& task,
                                     const TransferRequest& request) {
    if (!TransferEngineMetrics::isEnabled()) return;
    if (task.start_time.time_since_epoch().count() != 0) return;
    // Scatter grouping keeps every request in a task on the same opcode.
    task.metrics_opcode = request.opcode;
    task.start_time = std::chrono::steady_clock::now();
}

void MultiTransport::recordTaskEnd(Transport::TransferTask& task,
                                   const TransferStatus& status) {
    const auto start = task.start_time;
    if (start.time_since_epoch().count() == 0) return;

    // Record the first terminal status once; completion after TIMEOUT
    // does not change the metrics outcome.
    if (status.s != Transport::TransferStatusEnum::COMPLETED &&
        status.s != Transport::TransferStatusEnum::FAILED &&
        status.s != Transport::TransferStatusEnum::CANCELED &&
        status.s != Transport::TransferStatusEnum::TIMEOUT) {
        return;
    }
    // A task refused before any slice was created reads as COMPLETED with 0
    // bytes but no transfer happened.
    if (__atomic_load_n(&task.slice_count, __ATOMIC_ACQUIRE) == 0) return;
    if (__atomic_exchange_n(&task.export_metrics_recorded, true,
                            __ATOMIC_ACQ_REL)) {
        return;
    }

    auto& metrics = TransferEngineMetrics::instance();
    auto direction = task.metrics_opcode == Transport::TransferRequest::READ
                         ? TransferEngineMetrics::Direction::Read
                         : TransferEngineMetrics::Direction::Write;
    if (status.s == Transport::TransferStatusEnum::COMPLETED) {
        // Prefer the completion callback's stamp over this observation time.
        using Clock = std::chrono::steady_clock;
        const int64_t end_ticks =
            __atomic_load_n(&task.end_ticks, __ATOMIC_RELAXED);
        const auto end = end_ticks != 0
                             ? Clock::time_point(Clock::duration(end_ticks))
                             : Clock::now();
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        metrics.recordCompleted(direction, status.transferred_bytes,
                                static_cast<uint64_t>(latency.count()));
    } else if (status.s == Transport::TransferStatusEnum::TIMEOUT) {
        metrics.recordTimeout(direction);
    } else {
        metrics.recordFailed(direction);
    }
}

void MultiTransport::recordFinishedTask(Transport::TransferTask& task) {
    // Usually a poll already recorded this task.
    if (__atomic_load_n(&task.export_metrics_recorded, __ATOMIC_ACQUIRE))
        return;
    TransferStatus status;
    status.s = static_cast<Transport::TransferStatusEnum>(
        __atomic_load_n(&task.end_status, __ATOMIC_RELAXED));
    if (status.s == Transport::TransferStatusEnum::WAITING) {
        // No completion callback saved the outcome; the slice counters hold
        // it once they are complete.
        const uint64_t failed =
            __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
        const uint64_t succeeded =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
        if (succeeded + failed !=
            __atomic_load_n(&task.slice_count, __ATOMIC_ACQUIRE))
            return;
        status.s = failed ? Transport::TransferStatusEnum::FAILED
                          : Transport::TransferStatusEnum::COMPLETED;
    }
    status.transferred_bytes =
        __atomic_load_n(&task.transferred_bytes, __ATOMIC_RELAXED);
    recordTaskEnd(task, status);
}

bool MultiTransport::metricsEnabled() {
    return TransferEngineMetrics::isEnabled();
}

#else

void MultiTransport::recordTaskStart(Transport::TransferTask&,
                                     const TransferRequest&) {}

void MultiTransport::recordTaskEnd(Transport::TransferTask&,
                                   const TransferStatus&) {}

void MultiTransport::recordFinishedTask(Transport::TransferTask&) {}

bool MultiTransport::metricsEnabled() { return false; }

#endif  // WITH_METRICS

Status MultiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    return submitTransfer(batch_id, entries, nullptr);
}

Status MultiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    std::vector<size_t>* task_sizes) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (!task_sizes &&
        batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests(
            "Exceed the limitation of batch capacity");
    }

    std::vector<Transport*> transports;
    auto select_status = selectTransports(entries, transports);
    if (!select_status.ok()) return select_status;

    if (entries.empty()) return Status::OK();
    auto& task_list = batch_desc.task_list;
    const bool append = !task_list.empty();
    // RFC #5164: an event-enabled batch is one submit and at most one
    // completion record. Legacy (non-opted-in) append stays unchanged.
    if (batch_desc.event_enabled && append) {
        return Status::InvalidArgument(
            "event-enabled batch allows only one submitTransfer; "
            "freeBatchID and allocateBatchID again");
    }
    const bool all_counter = std::all_of(
        transports.begin(), transports.end(),
        [](Transport* t) { return t->supportsBatchCompletionCounter(); });
    if (batch_desc.event_enabled && !all_counter) {
        return Status::NotSupportedTransport(
            "event-enabled batch requires a transport that publishes "
            "completion without foreground polling");
    }
    batch_desc.counter_eligible =
        (task_list.empty() || batch_desc.counter_eligible) && all_counter;
    batch_desc.status_cached.store(false, std::memory_order_relaxed);
    batch_desc.is_finished.store(false, std::memory_order_relaxed);
    batch_desc.finished_transfer_bytes.store(0, std::memory_order_relaxed);
    // A seal inside submitTransferTask can finish a task before this call
    // returns. Hold that record until the status is known: publish only after
    // success, and drop it if submit fails. Publishing first and then erasing
    // loses the race with a dispatcher already waiting on the fd.
    if (batch_desc.event_enabled) {
        std::lock_guard<std::mutex> lock(completion_mu_);
        batch_desc.terminal_suppressed = true;
    }
    syncTerminalNotify(batch_desc);

    task_list.reserve(task_list.size() + entries.size());
    std::unordered_map<Transport*, std::vector<Transport::TransferTask*> >
        submit_tasks;
    if (task_sizes) task_sizes->reserve(entries.size());
    for (size_t i = 0; i < entries.size();) {
        size_t count = 1;
        const auto group_id = entries[i].task_group_id;
        if (task_sizes && group_id != TransferRequest::kNoTaskGroup &&
            transports[i]->supportsGroupedScatter()) {
            while (i + count < entries.size() &&
                   entries[i + count].task_group_id == group_id &&
                   entries[i + count].opcode == entries[i].opcode &&
                   transports[i + count] == transports[i])
                ++count;
        }
        auto& task = task_list.emplace_back();
        task.batch_id = batch_id;
        task.transport_ = transports[i];
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<Transport::TransferRequest*>(&entries[i]);
#else
        task.request = &entries[i];
#endif
        task.request_count = count;
        if (transports[i]->supportsBatchCompletionCounter())
            Transport::Slice::unsealTaskSubmission(&task);
#ifdef USE_EVENT_DRIVEN_COMPLETION
        if (count > 1) Transport::Slice::unsealTaskSubmission(&task);
#endif
        recordTaskStart(task, entries[i]);
        submit_tasks[transports[i]].push_back(&task);
        if (task_sizes) task_sizes->push_back(count);
        i += count;
    }
    if (task_sizes) batch_desc.batch_size = task_list.size();
    Status overall_status = Status::OK();
    for (auto& entry : submit_tasks) {
        auto status = entry.first->submitTransferTask(entry.second);
        for (auto* task : entry.second) {
            if ((__atomic_load_n(&task->completion_state, __ATOMIC_RELAXED) &
                 Transport::Slice::kCompletionSealBit) == 0)
                Transport::Slice::sealTaskSubmission(task);
        }
        if (!status.ok()) {
            batch_desc.counter_eligible = false;
            // LOG(ERROR) << "Failed to submit transfer task to "
            //            << entry.first->getName();
            overall_status = status;
        }
    }
    syncTerminalNotify(batch_desc);
    const bool publish = overall_status.ok() && batch_desc.event_enabled &&
                         batch_desc.counter_eligible && completion_fd_ >= 0;
    {
        std::lock_guard<std::mutex> lock(completion_mu_);
        if (!publish) {
            batch_desc.terminal_notify = nullptr;
            batch_desc.terminal_notify_ctx = nullptr;
            batch_desc.terminal_suppressed = true;
            batch_desc.terminal_enqueued = false;
            auto same_batch = [batch_id](const BatchCompletion& item) {
                return item.batch_id == batch_id;
            };
            ready_.erase(
                std::remove_if(ready_.begin(), ready_.end(), same_batch),
                ready_.end());
        } else {
            batch_desc.terminal_suppressed = false;
        }
    }
    if (publish) {
        const uint64_t done =
            batch_desc.finished_task_count.load(std::memory_order_acquire);
        if (!batch_desc.task_list.empty() &&
            done == batch_desc.task_list.size()) {
            batch_desc.publishTerminalIfDone(done - 1);
        }
    }
    return overall_status;
}

Status MultiTransport::submitScatter(
    const std::vector<TransferRequest>& entries,
    ScatterSubmission& submission) {
    submission = {};
    if (entries.empty())
        return Status::InvalidArgument("scatter transfer is empty");
    submission.batch_id = allocateBatchID(0);
    if (submission.batch_id == static_cast<BatchID>(-1))
        return Status::InvalidArgument(
            "failed to allocate scatter transfer batch");
    return submitTransfer(submission.batch_id, entries, &submission.task_sizes);
}

#ifdef ENABLE_MULTI_PROTOCOL
Status MultiTransport::mp_submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    std::string& proto) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests(
            "Exceed the limitation of batch capacity");
    }

    if (entries.empty()) return Status::OK();
    // Explicit-protocol dispatch retains transport-specific polling and is
    // not a polling-free eventfd path.
    if (batch_desc.event_enabled) {
        return Status::NotSupportedTransport(
            "event-enabled batch does not support mp_submitTransfer");
    }
    const bool append = !batch_desc.task_list.empty();
    batch_desc.counter_eligible = false;
    batch_desc.status_cached.store(false, std::memory_order_relaxed);
    batch_desc.is_finished.store(false, std::memory_order_relaxed);
    batch_desc.finished_transfer_bytes.store(0, std::memory_order_relaxed);
    (void)append;
    syncTerminalNotify(batch_desc);
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    std::unordered_map<Transport*, std::vector<Transport::TransferTask*> >
        submit_tasks;
    for (auto& request : entries) {
        Transport* transport = nullptr;
        auto status = mp_selectTransport(request, transport, proto);
        if (!status.ok()) return status;
        assert(transport);
        auto& task = batch_desc.task_list[task_id];
        task.batch_id = batch_id;
        task.transport_ = transport;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<Transport::TransferRequest*>(&request);
#else
        task.request = &request;
#endif
        if (transport->supportsBatchCompletionCounter())
            Transport::Slice::unsealTaskSubmission(&task);
        recordTaskStart(task, request);
        ++task_id;
        submit_tasks[transport].push_back(&task);
    }
    Status overall_status = Status::OK();
    for (auto& entry : submit_tasks) {
        auto status = entry.first->submitTransferTask(entry.second);
        for (auto* task : entry.second) {
            if ((__atomic_load_n(&task->completion_state, __ATOMIC_RELAXED) &
                 Transport::Slice::kCompletionSealBit) == 0)
                Transport::Slice::sealTaskSubmission(task);
        }
        if (!status.ok()) {
            // LOG(ERROR) << "Failed to submit transfer task to "
            //            << entry.first->getName();
            overall_status = status;
        }
    }
    syncTerminalNotify(batch_desc);
    clearQueuedTerminal(batch_id);
    return overall_status;
}
#endif

Status MultiTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument("Task ID out of range");
    }
    auto& task = batch_desc.task_list[task_id];

    // Helper: check if any slice has exceeded the configured timeout.
    // Returns true if a timeout was detected (and logs it).
    auto checkSliceTimeout = [&](const Transport::TransferTask& t) -> bool {
        if (globalConfig().slice_timeout <= 0) return false;
        auto current_ts = getCurrentTimeInNano();
        const int64_t kPacketDeliveryTimeout =
            globalConfig().slice_timeout * 1000000000;
        for (auto& slice : t.slice_list) {
            auto ts = slice->ts;
            if (ts > 0 && current_ts > ts &&
                current_ts - ts > kPacketDeliveryTimeout) {
                LOG(INFO) << "Slice timeout detected";
                return true;
            }
        }
        return false;
    };

    // If the task has an associated transport, delegate to its
    // getTransferStatus() to trigger transport-specific completion
    // polling. For example, the NVLink async transport polls CUDA
    // streams via cudaStreamQuery() here; without this call the
    // slice statuses (and therefore success/failed_slice_count)
    // would never be updated.
    if (task.transport_) {
        auto ret =
            task.transport_->getTransferStatus(batch_id, task_id, status);
        if (!ret.ok()) return ret;

        // Apply timeout check on top of the transport result.
        if (status.s == Transport::TransferStatusEnum::WAITING &&
            checkSliceTimeout(task)) {
            status.s = Transport::TransferStatusEnum::TIMEOUT;
        }
        recordTaskEnd(task, status);
        return Status::OK();
    }

    // Fallback for tasks without a transport pointer (legacy path)
    uint64_t success_slice_count =
        __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
    uint64_t failed_slice_count =
        __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
    // Completion counters publish the byte updates made by
    // Slice::markSuccess().
    status.transferred_bytes =
        __atomic_load_n(&task.transferred_bytes, __ATOMIC_RELAXED);
    assert(task.slice_count);
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = Transport::TransferStatusEnum::FAILED;
        } else {
            status.s = Transport::TransferStatusEnum::COMPLETED;
        }
        __atomic_store_n(&task.is_finished, true, __ATOMIC_RELAXED);
    } else {
        if (checkSliceTimeout(task)) {
            status.s = Transport::TransferStatusEnum::TIMEOUT;
        } else {
            status.s = Transport::TransferStatusEnum::WAITING;
        }
    }
    recordTaskEnd(task, status);
    return Status::OK();
}

Status MultiTransport::getScatterRequestStatuses(
    BatchID batch_id, size_t task_id,
    std::vector<TransferStatusEnum>& request_statuses) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (task_id >= batch_desc.task_list.size())
        return Status::InvalidArgument("Task ID out of range");

    const auto& task = batch_desc.task_list[task_id];
    if (!task.request || task.request_count == 0)
        return Status::InvalidArgument("Invalid grouped scatter task");
    const auto success_count =
        __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE);
    const auto failed_count =
        __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE);
    if (success_count + failed_count != task.slice_count)
        return Status::InvalidArgument("Grouped scatter task is not complete");

    request_statuses.assign(task.request_count, TransferStatusEnum::COMPLETED);
    size_t slice_index = 0;
    for (size_t i = 0; i < task.request_count; ++i) {
        size_t remaining = task.request[i].length;
        while (remaining != 0 && slice_index < task.slice_list.size()) {
            const auto* slice = task.slice_list[slice_index++];
            if (slice->length > remaining)
                return Status::InvalidArgument(
                    "Invalid grouped scatter slice layout");
            remaining -= slice->length;
            if (slice->status != Transport::Slice::SUCCESS)
                request_statuses[i] = TransferStatusEnum::FAILED;
        }
        if (remaining != 0) request_statuses[i] = TransferStatusEnum::FAILED;
    }
    if (slice_index != task.slice_list.size())
        return Status::InvalidArgument("Invalid grouped scatter slice layout");
    return Status::OK();
}

Status MultiTransport::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    status.transferred_bytes = 0;

    if (batch_desc.status_cached.load(std::memory_order_acquire) ||
        task_count == 0) {
        status.s = Transport::TransferStatusEnum::COMPLETED;
        status.transferred_bytes =
            batch_desc.finished_transfer_bytes.load(std::memory_order_relaxed);
        return Status::OK();
    }

    // Capability, not a nonzero counter, determines whether polling can be
    // skipped. Mixed/legacy transports may need polling to make progress.
    // Timeout and failure inspection still use the per-task path.
    const uint64_t finished =
        batch_desc.finished_task_count.load(std::memory_order_acquire);
    if (batch_desc.counter_eligible && globalConfig().slice_timeout <= 0 &&
        !batch_desc.has_failure.load(std::memory_order_acquire) &&
        finished < task_count) {
        // Skip the per-task walk. transferred_bytes stays 0 (a valid lower
        // bound) until COMPLETED, when the walk below fills the real total.
        status.s = Transport::TransferStatusEnum::WAITING;
        return Status::OK();
    }
    // On completion, walk once to aggregate bytes and let getTransferStatus
    // set is_finished. The WAITING fast path does not write is_finished.

    size_t success_count = 0;
    bool any_failed = false;
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        // Once a failure is seen, the walk continues only to record metrics
        // for tasks that finished later. Tasks already recorded have nothing
        // left to report, so skip the transport query for them.
#ifdef WITH_METRICS
        if (any_failed &&
            __atomic_load_n(
                &batch_desc.task_list[task_id].export_metrics_recorded,
                __ATOMIC_ACQUIRE))
            continue;
#endif
        TransferStatus task_status;
        auto ret = getTransferStatus(batch_id, task_id, task_status);

        if (!ret.ok()) {
            status.s = Transport::TransferStatusEnum::FAILED;
            return Status::OK();
        }

        if (task_status.s == Transport::TransferStatusEnum::COMPLETED) {
            status.transferred_bytes += task_status.transferred_bytes;
            success_count++;
        } else if (task_status.s == Transport::TransferStatusEnum::FAILED ||
                   task_status.s == Transport::TransferStatusEnum::TIMEOUT) {
            any_failed = true;
            // With metrics, keep going so every finished task is recorded.
            if (!metricsEnabled()) break;
        }
    }
    if (any_failed) {
        status.s = Transport::TransferStatusEnum::FAILED;
        return Status::OK();
    }

    status.s = (success_count == task_count)
                   ? Transport::TransferStatusEnum::COMPLETED
                   : Transport::TransferStatusEnum::WAITING;
    if (status.s == Transport::TransferStatusEnum::COMPLETED) {
        batch_desc.finished_transfer_bytes.store(status.transferred_bytes,
                                                 std::memory_order_relaxed);
        batch_desc.status_cached.store(true, std::memory_order_release);
        batch_desc.is_finished.store(true, std::memory_order_release);
    }
    return Status::OK();
}

Transport* MultiTransport::installTransport(const std::string& proto,
                                            std::shared_ptr<Topology> topo) {
#ifdef USE_NCCL_HOST
    if ((proto == "nccl" && !transport_map_.empty()) ||
        (proto != "nccl" && transport_map_.count("nccl") != 0)) {
        LOG(ERROR) << "NCCL host transport must be the only transport "
                      "installed in a Transfer Engine instance";
        return nullptr;
    }
#endif
    Transport* transport = nullptr;
    if (std::string(proto) == "rdma" || std::string(proto) == "rdma_twosided") {
        if ((proto == "rdma" && transport_map_.count("rdma_twosided")) ||
            (proto == "rdma_twosided" && transport_map_.count("rdma"))) {
            LOG(ERROR) << "Cannot install both rdma and rdma_twosided "
                          "transports in the same Transfer Engine instance";
            return nullptr;
        }
        if (std::string(proto) == "rdma") {
            transport = new RdmaTransport();
        } else {
            transport = new RdmaTwoSidedTransport();
        }
    }
#ifdef USE_UB
    else if (std::string(proto) == "ub") {
        transport = new UbTransport();
    }
#endif
#ifdef USE_BAREX
    else if (std::string(proto) == "barex") {
        transport = new BarexTransport();
    }
#endif
#ifdef USE_TCP
    else if (std::string(proto) == "tcp") {
        transport = new TcpTransport();
    }
#endif
#ifdef USE_NVMEOF
    else if (std::string(proto) == "nvmeof") {
        transport = new NVMeoFTransport();
    }
#endif
#ifdef USE_NCCL_HOST
    else if (std::string(proto) == "nccl") {
        transport = new NcclHostTransport();
    }
#endif
#ifdef USE_ASCEND_DIRECT
    else if (std::string(proto) == "ascend") {
        transport = new AscendDirectTransport();
    }
#endif
#ifdef USE_ASCEND
    else if (std::string(proto) == "ascend") {
        transport = new HcclTransport();
    }
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
    else if (std::string(proto) == "ascend") {
        transport = new HeterogeneousRdmaTransport();
    }
#endif

#ifdef USE_INTRA_NVLINK
    else if (std::string(proto) == "nvlink_intra") {
        transport = new IntraNodeNvlinkTransport();
    }
#endif

#ifdef USE_HIP
    else if (std::string(proto) == "hip") {
        transport = new HipTransport();
    }
#endif
#ifdef USE_HYLINK
    else if (std::string(proto) == "hylink") {
        transport = new HylinkTransport();
    }
#endif
#ifdef USE_MACA
    else if (std::string(proto) == "maca") {
        transport = new MacaTransport();
    }
#endif
#ifdef USE_MUSA
    else if (std::string(proto) == "musa") {
        transport = new MusaTransport();
    }
#endif
#ifdef USE_MNNVL
    else if (std::string(proto) == "nvlink") {
        transport = new NvlinkTransport();
    }
#endif  // USE_MNNVL
#ifdef USE_CXL
    else if (std::string(proto) == "cxl") {
        transport = new CxlTransport();
    }
#endif
    else if (std::string(proto) == "shm") {
        transport = new ShmTransport();
    }
#ifdef USE_UBSHMEM
    else if (std::string(proto) == "ubshmem") {
        transport = new UBShmemTransport();
    }
#endif
#ifdef USE_FLAGCX
    else if (std::string(proto) == "flagcx") {
        transport = new FlagCxTransport();
    }
#endif
#ifdef USE_EFA
    else if (std::string(proto) == "efa") {
        transport = new EfaTransport();
    }
#endif
#ifdef USE_CXI
    else if (std::string(proto) == "cxi") {
        transport = new CxiTransport();
    }
#endif
#ifdef USE_SUNRISE
    else if (std::string(proto) == "sunrise_link") {
        transport = new SunriseLinkTransport();
    }
#endif

    if (!transport) {
        LOG(ERROR) << "Unsupported transport " << proto
                   << ", please rebuild Mooncake";
        return nullptr;
    }

#ifdef USE_BAREX
    bool use_eic = false;
    for (auto& dev : topo->getHcaList()) {
        if (dev.find("soe") != std::string::npos ||
            dev.find("solar") != std::string::npos) {
            use_eic = true;
        }
    }

    if (std::string(proto) == "barex") {
        std::string nics;
        for (auto& dev : topo->getHcaList()) {
            if (use_eic) {
                if (dev.find("soe") == std::string::npos &&
                    dev.find("solar") == std::string::npos) {
                    // ignore no eic nics
                    continue;
                }
            }
            nics += dev;
            nics += ",";
        }

        // Remove the last extra comma
        if (!nics.empty()) {
            nics.pop_back();
        }

        if (!nics.empty()) {
            LOG(INFO) << "ACCL_USE_NICS is set to " << nics;
            setenv("ACCL_USE_NICS", nics.c_str(), 1);
        }
    }
#endif
    if (transport->install(local_server_name_, metadata_, topo)) {
        delete transport;
        return nullptr;
    }

    transport_map_[proto] = std::shared_ptr<Transport>(transport);
    return transport;
}

Status MultiTransport::selectTransports(
    const std::vector<TransferRequest>& entries,
    std::vector<Transport*>& transports) {
    transports.clear();
    transports.reserve(entries.size());
    Transport* reused_transport = nullptr;
    Transport::SegmentID reused_target = 0;
    bool reuse_allowed = false;
    const bool metacache = globalConfig().metacache;
    for (const auto& request : entries) {
        Transport* transport = nullptr;
        // Reuse only address-independent routes within this submission. Mixed
        // protocol segments must still resolve each address independently, and
        // disabling the metadata cache must retain the explicit refresh
        // behavior. This restores MultiTransport routing (protocol to
        // Transport*), not RDMA's per-target SegmentDesc fetch inside
        // submitTransferTask.
        if (reuse_allowed && reused_transport && metacache &&
            request.target_id == reused_target) {
            transport = reused_transport;
        } else {
            auto status = selectTransport(request, transport, &reuse_allowed);
            if (!status.ok()) return status;
            assert(transport);
            reused_transport = transport;
            reused_target = request.target_id;
        }
        transports.push_back(transport);
    }
    return Status::OK();
}

Status MultiTransport::selectTransport(const TransferRequest& entry,
                                       Transport*& transport,
                                       bool* allows_reuse) {
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        if (allows_reuse) *allows_reuse = false;
        return Status::InvalidArgument("Invalid target segment ID " +
                                       std::to_string(entry.target_id));
    }
#ifdef ENABLE_MULTI_PROTOCOL
    // Offset-based routing is only compiled for mixed-protocol segments.
    // A homogeneous batch can reuse this Transport*.
    if (allows_reuse) {
        *allows_reuse =
            target_segment_desc->protocol.find(',') == std::string::npos;
    }
#else
    if (allows_reuse) *allows_reuse = true;
#endif

    auto proto = target_segment_desc->protocol;
#ifdef ENABLE_MULTI_PROTOCOL
    // Multi-protocol segment (e.g. "rdma,hip"): a single batch may target
    // buffers owned by different transports (the device KV pool via hip, the
    // host aux/metadata buffers via rdma). Route each request to the transport
    // that owns the buffer covering the target address. When a buffer is
    // registered under more than one protocol (the device KV pool is registered
    // by both rdma and hip), pick the highest-performance transport by a fixed
    // priority instead of relying on buffer registration order.
    if (proto.find(',') != std::string::npos) {
        auto protocol_priority = [](const std::string& p) {
            // hip is intra-node GPU-IPC only. On a cross-node request a
            // hip+rdma segment must fall through to rdma; allow deployments
            // that know they need the cross-node path to de-prioritize hip.
            if (p == "hylink") return std::getenv("MC_DISABLE_HYLINK") ? 0 : 5;
            if (p == "hip") return std::getenv("MC_DISABLE_HIP") ? 0 : 4;
            if (p == "maca") return std::getenv("MC_DISABLE_MACA") ? 0 : 4;
            if (p == "musa") return std::getenv("MC_DISABLE_MUSA") ? 0 : 4;
            if (p == "shm") return 4;
            if (p == "cxl") return 3;
            if (p == "rdma") return 2;
            if (p == "tcp") return 1;
            return 0;
        };
        // hip transport uses GPU IPC, which cannot reach a GPU on another host.
        // The device KV pool is registered under both rdma and hip, so a
        // cross-host target must skip its hip buffers and fall back to rdma.
        // This makes the intra-node fast path (hip) and the cross-node path
        // (rdma) work automatically from a single multi-protocol segment,
        // without requiring the operator to set MC_DISABLE_HIP.
        const bool local_ipc_reachable = isLocalIpcReachableTarget(
            target_segment_desc->name, local_server_name_);
        std::string chosen;
        int chosen_priority = -1;
        for (const auto& buffer : target_segment_desc->buffers) {
            // CXL buffers locate via offset + cxl_base_addr; all other
            // protocols use the absolute virtual address in buffer.addr.
            uint64_t start =
                (buffer.protocol == "cxl")
                    ? buffer.offset + target_segment_desc->cxl_base_addr
                    : buffer.addr;
            if (entry.target_offset >= start &&
                entry.target_offset < start + buffer.length) {
                if ((buffer.protocol == "hip" || buffer.protocol == "musa" ||
                     buffer.protocol == "shm") &&
                    !local_ipc_reachable) {
                    continue;
                }
                int priority = protocol_priority(buffer.protocol);
                if (priority > chosen_priority) {
                    chosen = buffer.protocol;
                    chosen_priority = priority;
                }
            }
        }
        if (chosen.empty()) {
            return Status::InvalidArgument(
                "No matching buffer for target offset in multi-protocol "
                "segment " +
                std::to_string(entry.target_id));
        }
        if (!transport_map_.count(chosen) && chosen == "rdma" &&
            transport_map_.count("rdma_twosided")) {
            chosen = "rdma_twosided";
        }
        if (!transport_map_.count(chosen)) {
            return Status::NotSupportedTransport("Transport " + chosen +
                                                 " not installed");
        }
        if (globalConfig().trace) {
            LOG(INFO) << "MultiTransport::selectTransport route: target_id="
                      << entry.target_id << " segment_protocol=\"" << proto
                      << "\" local_ipc_reachable=" << local_ipc_reachable
                      << " chosen=" << chosen;
        }
        transport = transport_map_[chosen].get();
        return Status::OK();
    }
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
    // When USE_ASCEND_HETEROGENEOUS is enabled:
    // - Target side directly reuses RDMA Transport
    // - Initiator side uses heterogeneous_rdma_transport
    if (target_segment_desc->protocol == "rdma") {
        proto = "ascend";
    }
#endif
    // RdmaTwoSidedTransport still publishes segment protocol "rdma" (one-sided
    // memory path). Route those segments to the installed twosided transport.
    if (!transport_map_.count(proto) && proto == "rdma" &&
        transport_map_.count("rdma_twosided")) {
        transport = transport_map_["rdma_twosided"].get();
        return Status::OK();
    }
    if (!transport_map_.count(proto)) {
        return Status::NotSupportedTransport("Transport " + proto +
                                             " not installed");
    }
    transport = transport_map_[proto].get();
    return Status::OK();
}

#ifdef ENABLE_MULTI_PROTOCOL
Status MultiTransport::mp_selectTransport(const TransferRequest& entry,
                                          Transport*& transport,
                                          std::string& preferred_proto) {
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        return Status::InvalidArgument("Invalid target segment ID " +
                                       std::to_string(entry.target_id));
    }
    // Parse comma-separated protocols
    std::vector<std::string> protos;
    std::stringstream ss(target_segment_desc->protocol);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) protos.push_back(item);
    }

    // hip GPU IPC and POSIX SHM cannot reach a remote host; downgrade an
    // explicit intra-node preference to a cross-host-capable transport
    // (mirrors the locality gate in selectTransport). Prefer rdma, then tcp.
    if ((preferred_proto == "hip" || preferred_proto == "musa" ||
         preferred_proto == "shm") &&
        !isLocalIpcReachableTarget(target_segment_desc->name,
                                   local_server_name_)) {
        std::string fallback;
        for (const char* candidate : {"rdma", "tcp"}) {
            if (std::find(protos.begin(), protos.end(), candidate) !=
                protos.end()) {
                fallback = candidate;
                break;
            }
        }
        if (fallback.empty()) {
            return Status::NotSupportedTransport(
                preferred_proto + " target is cross-host but segment " +
                std::to_string(entry.target_id) +
                " offers no cross-host transport (rdma/tcp)");
        }
        preferred_proto = fallback;
    }

#ifdef USE_ASCEND_HETEROGENEOUS
    // When USE_ASCEND_HETEROGENEOUS is enabled:
    // - Target side directly reuses RDMA Transport
    // - Initiator side uses heterogeneous_rdma_transport
    if (preferred_proto == "rdma" &&
        std::find(protos.begin(), protos.end(), "rdma") != protos.end()) {
        preferred_proto = "ascend";
    }
#endif
    if (!transport_map_.count(preferred_proto) && preferred_proto == "rdma" &&
        transport_map_.count("rdma_twosided")) {
        preferred_proto = "rdma_twosided";
    }
    if (!transport_map_.count(preferred_proto)) {
        return Status::NotSupportedTransport("Transport " + preferred_proto +
                                             " not installed");
    }
    // Segment metadata still advertises "rdma" for the twosided install.
    const std::string segment_proto =
        (preferred_proto == "rdma_twosided") ? "rdma" : preferred_proto;
    if (std::find(protos.begin(), protos.end(), segment_proto) ==
        protos.end()) {
        return Status::NotSupportedTransport(
            "Transport " + preferred_proto +
            " not supported by target segment");
    }
    transport = transport_map_[preferred_proto].get();
    return Status::OK();
}
#endif

Transport* MultiTransport::getTransport(const std::string& proto) {
    if (!transport_map_.count(proto)) return nullptr;
    return transport_map_[proto].get();
}

bool MultiTransport::isTcpOnly() const {
    // SHM is intra-node DRAM IPC, not a cross-host fabric. Ignore it so a
    // tcp+shm engine still auto-enables Store same-process memcpy.
    size_t n = transport_map_.size();
    if (transport_map_.count("shm")) --n;
    return n == 1 && transport_map_.count("tcp") == 1;
}

std::vector<Transport*> MultiTransport::listTransports() {
    std::vector<Transport*> transport_list;
    for (auto& entry : transport_map_)
        transport_list.push_back(entry.second.get());
    return transport_list;
}

void* MultiTransport::getBaseAddr() {
#ifdef USE_CXL
    Transport* transport = getTransport("cxl");
    if (transport) {
        auto* cxl_transport = dynamic_cast<CxlTransport*>(transport);
        return cxl_transport ? cxl_transport->getCxlBaseAddr() : 0;
    }
#endif
    return 0;
}

}  // namespace mooncake
