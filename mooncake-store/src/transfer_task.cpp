#include "transfer_task.h"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>
#include "gpu_staging_utils.h"
#include "transfer_engine.h"
#include "transport/transport.h"

namespace mooncake {

// ============================================================================
// FilereadWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 10 worker
// threads.
constexpr int kDefaultFilereadWorkers = 10;

FilereadWorkerPool::FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend)
    : shutdown_(false) {
    VLOG(1) << "Creating FilereadWorkerPool with " << kDefaultFilereadWorkers
            << " workers";

    // Start worker threads
    workers_.reserve(kDefaultFilereadWorkers);
    for (int i = 0; i < kDefaultFilereadWorkers; ++i) {
        workers_.emplace_back(&FilereadWorkerPool::workerThread, this);
    }
    backend_ = backend;
}

FilereadWorkerPool::~FilereadWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "FilereadWorkerPool destroyed";
}

void FilereadWorkerPool::submitTask(FilereadTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown FilereadWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void FilereadWorkerPool::workerThread() {
    VLOG(2) << "FilereadWorkerPool worker thread started";

    while (true) {
        FilereadTask task("", 0, {}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                if (!backend_) {
                    LOG(ERROR)
                        << "Backend is not initialized, cannot load object";
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                    continue;
                }

                auto load_result = backend_->LoadObject(
                    task.file_path, task.slices, task.object_size);
                if (load_result) {
                    VLOG(2) << "Fileread task completed successfully with "
                            << task.file_path;
                    task.state->set_completed(ErrorCode::OK);
                } else {
                    LOG(ERROR)
                        << "Fileread task failed for file: " << task.file_path
                        << " with error: " << toString(load_result.error());
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async fileread: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "FilereadWorkerPool worker thread exiting";
}

// ============================================================================
// MemcpyWorkerPool Implementation
// ============================================================================
// Since memcpy is bound by memory bandwidth, we only need one worker thread.
constexpr int kDefaultMemcpyWorkers = 1;

MemcpyWorkerPool::MemcpyWorkerPool() : shutdown_(false) {
    VLOG(1) << "Creating MemcpyWorkerPool with " << kDefaultMemcpyWorkers
            << " workers";

    // Start worker threads
    workers_.reserve(kDefaultMemcpyWorkers);
    for (int i = 0; i < kDefaultMemcpyWorkers; ++i) {
        workers_.emplace_back(&MemcpyWorkerPool::workerThread, this);
    }
}

MemcpyWorkerPool::~MemcpyWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "MemcpyWorkerPool destroyed";
}

void MemcpyWorkerPool::submitTask(MemcpyTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown MemcpyWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void MemcpyWorkerPool::workerThread() {
    VLOG(2) << "MemcpyWorkerPool worker thread started";

    while (true) {
        MemcpyTask task({}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                bool ok = true;
                for (const auto& op : task.operations) {
                    int src_dev = -1, dst_dev = -1;
                    bool src_on_gpu =
                        gpu_staging::IsDevicePointer(op.src, &src_dev);
                    bool dst_on_gpu =
                        gpu_staging::IsDevicePointer(op.dest, &dst_dev);

                    if (!src_on_gpu && !dst_on_gpu) {
                        std::memcpy(op.dest, op.src, op.size);
                    } else {
                        int dev = src_on_gpu ? src_dev : dst_dev;
                        gpu_staging::SetDevice(dev);
                        if (!gpu_staging::CopyAuto(op.dest, op.src, op.size)) {
                            LOG(ERROR)
                                << "GPU memcpy failed: src_dev=" << src_dev
                                << " dst_dev=" << dst_dev
                                << " size=" << op.size;
                            ok = false;
                            break;
                        }
                    }
                }

                VLOG(2) << "Memcpy task completed with "
                        << task.operations.size() << " operations"
                        << (ok ? "" : " (with GPU copy failure)");
                task.state->set_completed(ok ? ErrorCode::OK
                                             : ErrorCode::TRANSFER_FAIL);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async memcpy: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "MemcpyWorkerPool worker thread exiting";
}

// ============================================================================
// TransferEngineOperationState Implementation
// ============================================================================

bool TransferEngineOperationState::is_completed() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (result_.has_value()) {
        return true;
    }

    check_task_status();
    return result_.has_value();
}

void TransferEngineOperationState::check_task_status() {
    // Check all transfers in the batch.
    // Wait for ALL tasks to reach a terminal state before setting the result,
    // even if some have already failed. This prevents the caller from seeing
    // "completed" while background transfers are still in progress, which
    // could cause issues when freeBatchID is called in the destructor.
    bool all_terminated = true;
    std::vector<size_t> failed_task_ids;

    for (size_t i = 0; i < batch_size_; ++i) {
        TransferStatus status;
        Status s = engine_.getTransferStatus(batch_id_, i, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status for batch "
                       << batch_id_ << " task " << i << " with error "
                       << s.message();
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        switch (status.s) {
            case TransferStatusEnum::COMPLETED:
                // This transfer is done successfully
                break;
            case TransferStatusEnum::FAILED:
            case TransferStatusEnum::CANCELED:
            case TransferStatusEnum::INVALID:
#ifndef USE_ASCEND_DIRECT
                VLOG(1) << "Transfer failed for batch " << batch_id_ << " task "
                        << i << " with status " << static_cast<int>(status.s);
#endif
                failed_task_ids.push_back(i);
                break;
            default:
                // Transfer is still in progress (WAITING, PENDING, etc.)
                all_terminated = false;
                break;
        }
    }

    if (!all_terminated) {
        // Some tasks are still in progress; wait for next poll iteration.
        // Do NOT set result yet, even if some tasks have already failed.
        return;
    }

    // All tasks have reached a terminal state.
    ErrorCode ec = ErrorCode::OK;
    if (!failed_task_ids.empty()) {
        std::ostringstream oss;
        for (size_t j = 0; j < failed_task_ids.size(); ++j) {
            if (j > 0) oss << ", ";
            oss << failed_task_ids[j];
        }
        LOG(ERROR) << "Batch " << batch_id_
                   << " completed with task failures: task_ids=[" << oss.str()
                   << "]";
        ec = ErrorCode::TRANSFER_FAIL;
    }

    set_result_internal(ec);
}

void TransferEngineOperationState::set_result_internal(ErrorCode error_code) {
    if (result_.has_value()) {
        LOG(ERROR) << "Attempting to set result multiple times for batch "
                   << batch_id_
                   << ". Previous result: " << static_cast<int>(result_.value())
                   << ", attempted new result: " << static_cast<int>(error_code)
                   << ". This indicates a race condition or logic error.";
        return;  // Don't crash, just return early
    }

    VLOG(1) << "Setting transfer result for batch " << batch_id_ << " to "
            << static_cast<int>(error_code);
    result_.emplace(error_code);
}

void TransferEngineOperationState::wait_for_completion() {
    if (is_completed()) {
        return;
    }

    // 60 seconds
    constexpr int64_t timeout_milliseconds = 60 * 1000;

#ifdef USE_EVENT_DRIVEN_COMPLETION
    VLOG(1) << "Waiting for transfer engine completion for batch " << batch_id_;

    // Wait directly on BatchDesc's condition variable.
    auto& batch_desc = Transport::toBatchDesc(batch_id_);
    bool completed;
    bool failed = false;

    completed = batch_desc.is_finished.load(std::memory_order_acquire);
    if (!completed) {
        // Use the same mutex as the notifier when updating the predicate to
        // avoid missed notifications. The predicate is re-checked under the
        // lock. Under the mutex, relaxed is sufficient; the mutex acquire
        // orders prior writes.
        std::unique_lock<std::mutex> lock(batch_desc.completion_mutex);
        const int64_t elapsed_milliseconds =
            getCurrentTimeInMilli() - start_ts_;
        if (elapsed_milliseconds < timeout_milliseconds) {
            completed = batch_desc.completion_cv.wait_for(
                lock,
                std::chrono::milliseconds(timeout_milliseconds -
                                          elapsed_milliseconds),
                [&batch_desc] {
                    return batch_desc.is_finished.load(
                        std::memory_order_relaxed);
                });
        }
    }  // Explicitly release completion_mutex before acquiring mutex_

    // Once completion is observed, read failure flag.
    if (completed) {
        failed = batch_desc.has_failure.load(std::memory_order_relaxed);
    }

    ErrorCode error_code =
        completed ? (failed ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK)
                  : ErrorCode::TRANSFER_FAIL;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        set_result_internal(error_code);
    }

    if (completed) {
        VLOG(1) << "Transfer engine operation completed for batch " << batch_id_
                << " with result: " << static_cast<int>(error_code);
    } else {
        LOG(ERROR) << "Failed to complete transfers after "
                   << timeout_milliseconds << " milliseconds for batch "
                   << batch_id_;
    }
#else
    VLOG(1) << "Starting transfer engine futex wait for batch " << batch_id_;

    auto& batch_desc = Transport::toBatchDesc(batch_id_);
    auto* futex_word = reinterpret_cast<uint32_t*>(&batch_desc.progress_futex);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_milliseconds);

    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            check_task_status();
            if (result_.has_value()) {
                VLOG(1) << "Transfer engine operation completed for batch "
                        << batch_id_ << " with result: "
                        << static_cast<int>(result_.value());
                break;
            }
        }

        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            LOG(ERROR) << "Failed to complete transfers after "
                       << timeout_milliseconds << " milliseconds for batch "
                       << batch_id_;
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        uint32_t snapshot = __atomic_load_n(futex_word, __ATOMIC_ACQUIRE);
        // 100ms timeout as fallback for transports (e.g. TENT) that may not
        // wake via the standard Slice completion path.
        struct timespec ts = {0, 100 * 1000 * 1000};
        ::syscall(SYS_futex, futex_word, FUTEX_WAIT_PRIVATE, snapshot, &ts,
                  nullptr, 0);
    }
#endif
}

// ============================================================================
// ProgressiveGetHandle Implementation
// ============================================================================

namespace {
constexpr size_t kProgressiveGetWindowChunks = 8;
constexpr int64_t kProgressiveGetTimeoutSeconds = 60;

TransferTaskTestHooks* g_transfer_task_test_hooks = nullptr;

}  // namespace

void SetTransferTaskTestHooks(TransferTaskTestHooks* hooks) {
    g_transfer_task_test_hooks = hooks;
}

namespace {

using ScatterRange = std::tuple<size_t, size_t, size_t>;
using ScatterKeyRanges =
    std::vector<std::pair<Replica::Descriptor, std::vector<ScatterRange>>>;

struct ScatterReadBuildResult {
    std::vector<TransferRequest> flat_requests;
    std::vector<std::vector<TransferRequest>> logical_groups;
    std::vector<SegmentHandle> opened_segments;
};

std::optional<ScatterReadBuildResult> buildScatterReadRequests(
    TransferEngine& engine, void* dest_buffer,
    const ScatterKeyRanges& key_ranges, bool enable_task_grouping,
    const char* log_context) {
    ScatterReadBuildResult result;
    size_t total_ranges = 0;
    for (const auto& [_, ranges] : key_ranges) {
        total_ranges += ranges.size();
    }
    result.flat_requests.reserve(total_ranges);
    if (enable_task_grouping) {
        result.logical_groups.reserve(key_ranges.size());
    }
    result.opened_segments.reserve(key_ranges.size());

    uint64_t next_task_group_id = 0;
    char* dest_base = static_cast<char*>(dest_buffer);

    for (const auto& [replica, ranges] : key_ranges) {
        if (!replica.is_memory_replica()) {
            LOG(ERROR) << log_context << ": disk replicas not supported";
            return std::nullopt;
        }
        const auto& handle = replica.get_memory_descriptor().buffer_descriptor;
        if (handle.transport_endpoint_.empty()) {
            LOG(ERROR) << log_context << ": empty transport endpoint";
            return std::nullopt;
        }

        SegmentHandle seg = engine.openSegment(handle.transport_endpoint_);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << log_context << ": failed to open segment for "
                       << handle.transport_endpoint_;
            return std::nullopt;
        }
        result.opened_segments.push_back(seg);

        uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
        std::vector<TransferRequest> group_requests;
        if (enable_task_grouping) {
            group_requests.reserve(ranges.size());
        }
        const uint64_t task_group_id = enable_task_grouping
                                           ? next_task_group_id++
                                           : TransferRequest::kNoTaskGroup;

        for (const auto& [dest_offset, src_offset, size] : ranges) {
            if (size == 0) {
                continue;
            }
            TransferRequest request;
            request.opcode = TransferRequest::READ;
            request.source = dest_base + dest_offset;
            request.target_id = seg;
            request.target_offset = base_address + src_offset;
            request.length = size;
            request.task_group_id = task_group_id;
            result.flat_requests.emplace_back(request);
            if (enable_task_grouping) {
                group_requests.emplace_back(request);
            }
        }

        if (enable_task_grouping && !group_requests.empty()) {
            result.logical_groups.emplace_back(std::move(group_requests));
        }
    }

    return result;
}
}  // namespace

using ChunkSubmitFailureHook = std::function<bool()>;

size_t sum_request_bytes(const std::vector<TransferRequest>& requests) {
    size_t total_bytes = 0;
    for (const auto& request : requests) {
        total_bytes += request.length;
    }
    return total_bytes;
}

class ChunkedReadSession {
   public:
    ChunkedReadSession(size_t num_chunks, ErrorCode result)
        : num_chunks_(num_chunks),
          chunk_done_(num_chunks, 1),
          chunk_results_(num_chunks, result),
          completed_count_(num_chunks),
          submitted_chunks_(num_chunks),
          sealed_(true),
          precompleted_(true) {}

    ChunkedReadSession(TransferEngine& engine, BatchID batch_id,
                       std::vector<std::vector<TransferRequest>> chunk_groups,
                       std::vector<SegmentHandle> opened_segments,
                       size_t initial_window, std::string submit_context,
                       ChunkSubmitFailureHook fail_next_submit)
        : engine_(&engine),
          batch_id_(batch_id),
          num_chunks_(chunk_groups.size()),
          chunk_groups_(std::move(chunk_groups)),
          opened_segments_(std::move(opened_segments)),
          chunk_done_(num_chunks_, 0),
          chunk_results_(num_chunks_, ErrorCode::OK),
          completed_count_(0),
          window_size_(std::max<size_t>(1, initial_window)),
          submit_context_(std::move(submit_context)),
          fail_next_submit_(std::move(fail_next_submit)) {}

    ~ChunkedReadSession() { cleanup(); }

    bool submit_initial_window() {
        if (precompleted_) {
            return true;
        }
        maybe_submit_more(window_size_);
        return !submit_failed_;
    }

    size_t num_chunks() const { return num_chunks_; }

    bool is_chunk_ready(size_t chunk_index) {
        if (chunk_index >= num_chunks_) {
            return false;
        }
        maybe_submit_more(chunk_index + 1);
        std::lock_guard<std::mutex> lock(state_mutex_);
        return poll_chunk_locked(chunk_index);
    }

    size_t completed_count() {
        if (precompleted_) {
            return completed_count_.load(std::memory_order_relaxed);
        }
        maybe_submit_more(submitted_chunks_.load(std::memory_order_acquire) +
                          window_size_);
        std::lock_guard<std::mutex> lock(state_mutex_);
        size_t current = completed_count_.load(std::memory_order_relaxed);
        if (current == num_chunks_) {
            return current;
        }
        size_t observed_finished = observed_batch_finished_count();
        if (observed_finished > current) {
            return drain_completed_chunks_locked(observed_finished);
        }
        return current;
    }

    ErrorCode wait_chunk(size_t chunk_index) {
        if (chunk_index >= num_chunks_) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (precompleted_) {
            return chunk_results_[chunk_index];
        }

        maybe_submit_more(chunk_index + 1);
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (poll_chunk_locked(chunk_index)) {
                return chunk_results_[chunk_index];
            }
        }

        auto& batch_desc = Transport::toBatchDesc(batch_id_);
        auto* futex_word =
            reinterpret_cast<uint32_t*>(&batch_desc.progress_futex);
        const auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(kProgressiveGetTimeoutSeconds);

        while (true) {
            uint32_t snapshot = __atomic_load_n(futex_word, __ATOMIC_ACQUIRE);

            maybe_submit_more(chunk_index + 1);
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                size_t observed_finished = observed_batch_finished_count();
                if (observed_finished >
                    completed_count_.load(std::memory_order_relaxed)) {
                    drain_completed_chunks_locked(observed_finished);
                    if (chunk_done_[chunk_index]) {
                        return chunk_results_[chunk_index];
                    }
                }

                if (materialize_chunk_result_locked(chunk_index)) {
                    return chunk_results_[chunk_index];
                }
            }

            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                break;
            }
            auto remaining =
                std::chrono::duration_cast<std::chrono::nanoseconds>(deadline -
                                                                     now);
            struct timespec ts;
            ts.tv_sec = remaining.count() / 1000000000LL;
            ts.tv_nsec = remaining.count() % 1000000000LL;

            ::syscall(SYS_futex, futex_word, FUTEX_WAIT_PRIVATE, snapshot, &ts,
                      nullptr, 0);
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            mark_chunk_done_locked(chunk_index, ErrorCode::TRANSFER_FAIL);
        }
        seal_batch();
        return ErrorCode::TRANSFER_FAIL;
    }

    ErrorCode wait_all() {
        if (precompleted_) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            return aggregate_result_locked();
        }

        maybe_submit_more(num_chunks_);
        auto& batch_desc = Transport::toBatchDesc(batch_id_);
        auto* futex_word =
            reinterpret_cast<uint32_t*>(&batch_desc.progress_futex);
        const auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(kProgressiveGetTimeoutSeconds);

        size_t current = completed_count_.load(std::memory_order_relaxed);
        while (current < num_chunks_) {
            maybe_submit_more(num_chunks_);
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                size_t observed_finished = observed_batch_finished_count();
                if (observed_finished > current) {
                    current = drain_completed_chunks_locked(observed_finished);
                    continue;
                }
            }

            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                break;
            }

            uint32_t snapshot = __atomic_load_n(futex_word, __ATOMIC_ACQUIRE);
            auto remaining =
                std::chrono::duration_cast<std::chrono::nanoseconds>(deadline -
                                                                     now);
            struct timespec ts;
            ts.tv_sec = remaining.count() / 1000000000LL;
            ts.tv_nsec = remaining.count() % 1000000000LL;

            ::syscall(SYS_futex, futex_word, FUTEX_WAIT_PRIVATE, snapshot, &ts,
                      nullptr, 0);
            current = completed_count_.load(std::memory_order_relaxed);
        }

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (observed_batch_finished_count() ==
                submitted_chunks_.load(std::memory_order_acquire)) {
                drain_completed_chunks_locked(num_chunks_, true);
            }

            if (completed_count_.load(std::memory_order_relaxed) !=
                num_chunks_) {
                mark_remaining_chunks_failed_locked();
            }

            seal_batch();
            return aggregate_result_locked();
        }
    }

   private:
    void cleanup() {
        if (precompleted_ || !engine_ || batch_id_ == INVALID_BATCH_ID) {
            return;
        }

        wait_all();
        BatchID batch_id = batch_id_;
        Status free_status = engine_->freeBatchID(batch_id);
        if (!free_status.ok()) {
            LOG(ERROR) << "Failed to free chunked read batch " << batch_id
                       << ": " << free_status.message();
        } else if (g_transfer_task_test_hooks &&
                   g_transfer_task_test_hooks->on_batch_freed) {
            g_transfer_task_test_hooks->on_batch_freed(batch_id);
        }
        for (SegmentHandle seg : opened_segments_) {
            if (engine_->closeSegment(seg) != 0) {
                LOG(ERROR) << "Failed to close chunked read segment " << seg;
            }
        }
        opened_segments_.clear();
        chunk_groups_.clear();
        engine_ = nullptr;
        batch_id_ = INVALID_BATCH_ID;
    }

    void maybe_submit_more(size_t required_chunks) {
        if (precompleted_) {
            return;
        }

        std::lock_guard<std::mutex> lock(submit_mutex_);
        if (sealed_) {
            return;
        }

        size_t submitted = submitted_chunks_.load(std::memory_order_acquire);
        size_t target = std::min(
            num_chunks_, std::max(required_chunks, submitted + window_size_));
        if (target <= submitted) {
            return;
        }

        std::vector<TransferRequest> window;
        size_t raw_request_count = 0;
        for (size_t i = submitted; i < target; ++i) {
            raw_request_count += chunk_groups_[i].size();
        }
        window.reserve(raw_request_count);
        for (size_t i = submitted; i < target; ++i) {
            window.insert(window.end(), chunk_groups_[i].begin(),
                          chunk_groups_[i].end());
        }

        if (fail_next_submit_ && fail_next_submit_()) {
            submit_failed_ = true;
            for (size_t i = submitted; i < num_chunks_; ++i) {
                mark_chunk_done_locked(i, ErrorCode::TRANSFER_FAIL);
            }
            submitted_chunks_.store(num_chunks_, std::memory_order_release);
            seal_batch_locked();
            return;
        }

        Status s = engine_->submitTransfer(batch_id_, window);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to submit " << submit_context_
                       << " window for batch " << batch_id_ << ": "
                       << s.message();
            submit_failed_ = true;
            for (size_t i = submitted; i < num_chunks_; ++i) {
                mark_chunk_done_locked(i, ErrorCode::TRANSFER_FAIL);
            }
            submitted_chunks_.store(num_chunks_, std::memory_order_release);
            seal_batch_locked();
            return;
        }

        submitted_chunks_.store(target, std::memory_order_release);
        if (target == num_chunks_) {
            seal_batch_locked();
        }
    }

    void seal_batch() {
        if (precompleted_) {
            return;
        }
        std::lock_guard<std::mutex> lock(submit_mutex_);
        seal_batch_locked();
    }

    void seal_batch_locked() {
        if (sealed_) {
            return;
        }
        auto& batch_desc = Transport::toBatchDesc(batch_id_);
        {
            std::lock_guard<std::mutex> lifecycle_lock(
                batch_desc.lifecycle_mutex);
            batch_desc.sealed.store(true, std::memory_order_release);
            batch_desc.publish_completion_if_ready_locked();
        }
#ifdef USE_EVENT_DRIVEN_COMPLETION
        batch_desc.completion_cv.notify_all();
#endif
        sealed_ = true;
    }

    size_t observed_batch_finished_count() const {
        const auto& batch_desc = Transport::toBatchDesc(batch_id_);
        size_t finished = static_cast<size_t>(
            batch_desc.finished_task_count.load(std::memory_order_acquire));
        size_t submitted = submitted_chunks_.load(std::memory_order_acquire);
        return std::min(finished, submitted);
    }

    void mark_chunk_done_locked(size_t chunk_index, ErrorCode result) {
        if (chunk_done_[chunk_index]) {
            return;
        }
        chunk_done_[chunk_index] = 1;
        chunk_results_[chunk_index] = result;
        completed_count_.fetch_add(1, std::memory_order_relaxed);
    }

    bool materialize_chunk_result_locked(size_t chunk_index) {
        if (chunk_done_[chunk_index]) {
            return true;
        }
        if (chunk_index >= submitted_chunks_.load(std::memory_order_acquire)) {
            return false;
        }

        TransferStatus status;
        Status s = engine_->getTransferStatus(batch_id_, chunk_index, status);
        if (!s.ok()) {
            LOG(ERROR)
                << "Failed to get chunked read transfer status for batch "
                << batch_id_ << " chunk " << chunk_index;
            mark_chunk_done_locked(chunk_index, ErrorCode::TRANSFER_FAIL);
            return true;
        }

        switch (status.s) {
            case TransferStatusEnum::COMPLETED:
                mark_chunk_done_locked(chunk_index, ErrorCode::OK);
                return true;
            case TransferStatusEnum::FAILED:
            case TransferStatusEnum::CANCELED:
            case TransferStatusEnum::INVALID:
            case TransferStatusEnum::TIMEOUT:
                mark_chunk_done_locked(chunk_index, ErrorCode::TRANSFER_FAIL);
                return true;
            default:
                return false;
        }
    }

    size_t drain_completed_chunks_locked(size_t observed_finished_count,
                                         bool force_full_scan = false) {
        size_t completed = completed_count_.load(std::memory_order_relaxed);
        if (completed >= num_chunks_) {
            return completed;
        }
        if (observed_finished_count <= completed && !force_full_scan) {
            return completed;
        }

        size_t submitted = submitted_chunks_.load(std::memory_order_acquire);
        size_t scan_limit =
            force_full_scan ? submitted : observed_finished_count;
        if (scan_limit == 0) {
            return completed;
        }

        size_t idx = drain_cursor_ < submitted ? drain_cursor_ : 0;
        size_t scanned = 0;
        while (scanned < submitted &&
               (force_full_scan || completed < scan_limit)) {
            if (!chunk_done_[idx] && materialize_chunk_result_locked(idx)) {
                completed = completed_count_.load(std::memory_order_relaxed);
            }
            idx = (idx + 1) % submitted;
            ++scanned;
        }

        drain_cursor_ = submitted == 0 ? 0 : idx;
        return completed_count_.load(std::memory_order_relaxed);
    }

    void mark_remaining_chunks_failed_locked() {
        for (size_t i = 0; i < num_chunks_; ++i) {
            if (!chunk_done_[i]) {
                mark_chunk_done_locked(i, ErrorCode::TRANSFER_FAIL);
            }
        }
        drain_cursor_ = num_chunks_;
    }

    ErrorCode aggregate_result_locked() const {
        for (size_t i = 0; i < num_chunks_; ++i) {
            if (chunk_results_[i] != ErrorCode::OK) {
                return chunk_results_[i];
            }
        }
        return ErrorCode::OK;
    }

    bool poll_chunk_locked(size_t chunk_index) {
        if (chunk_done_[chunk_index]) {
            return true;
        }
        maybe_submit_more(chunk_index + 1);

        size_t observed_finished = observed_batch_finished_count();
        size_t completed = completed_count_.load(std::memory_order_relaxed);
        if (observed_finished > completed) {
            completed = drain_completed_chunks_locked(observed_finished);
        }

        if (chunk_done_[chunk_index]) {
            return true;
        }
        if (completed >= num_chunks_) {
            return false;
        }
        if (observed_finished == completed) {
            return false;
        }

        return materialize_chunk_result_locked(chunk_index);
    }

    TransferEngine* engine_ = nullptr;
    BatchID batch_id_ = INVALID_BATCH_ID;
    size_t num_chunks_ = 0;
    std::vector<std::vector<TransferRequest>> chunk_groups_;
    std::vector<SegmentHandle> opened_segments_;
    std::vector<uint8_t> chunk_done_;
    std::vector<ErrorCode> chunk_results_;
    std::atomic<size_t> completed_count_{0};
    std::atomic<size_t> submitted_chunks_{0};
    size_t drain_cursor_{0};
    size_t window_size_ = 1;
    bool sealed_ = false;
    bool precompleted_ = false;
    bool submit_failed_ = false;
    std::string submit_context_;
    ChunkSubmitFailureHook fail_next_submit_;
    std::mutex submit_mutex_;
    std::mutex state_mutex_;
};

template <typename Handle>
std::optional<Handle> create_chunked_read_handle(
    TransferEngine& engine,
    std::vector<std::vector<TransferRequest>> chunk_groups,
    std::vector<SegmentHandle> opened_segments, size_t initial_window,
    const char* submit_context, ChunkSubmitFailureHook fail_next_submit) {
    BatchID batch_id = engine.allocateBatchID(chunk_groups.size());
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << submit_context << ": failed to allocate batch ID";
        return std::nullopt;
    }
    if (g_transfer_task_test_hooks &&
        g_transfer_task_test_hooks->on_batch_allocated) {
        g_transfer_task_test_hooks->on_batch_allocated(batch_id);
    }

    auto session = std::make_shared<ChunkedReadSession>(
        engine, batch_id, std::move(chunk_groups), std::move(opened_segments),
        initial_window, submit_context, std::move(fail_next_submit));
    if (!session->submit_initial_window()) {
        return std::nullopt;
    }
    return Handle(std::move(session));
}

ProgressiveGetHandle::ProgressiveGetHandle(
    std::shared_ptr<ChunkedReadSession> session)
    : session_(std::move(session)) {}

ProgressiveGetHandle::~ProgressiveGetHandle() = default;

ProgressiveGetHandle::ProgressiveGetHandle(
    ProgressiveGetHandle&& other) noexcept = default;

ProgressiveGetHandle& ProgressiveGetHandle::operator=(
    ProgressiveGetHandle&& other) noexcept = default;

size_t ProgressiveGetHandle::num_chunks() const {
    return session_->num_chunks();
}

bool ProgressiveGetHandle::is_chunk_ready(size_t chunk_index) {
    return session_->is_chunk_ready(chunk_index);
}

size_t ProgressiveGetHandle::completed_count() {
    return session_->completed_count();
}

ErrorCode ProgressiveGetHandle::wait_chunk(size_t chunk_index) {
    return session_->wait_chunk(chunk_index);
}

ErrorCode ProgressiveGetHandle::wait_all() { return session_->wait_all(); }

ScatterReadHandle::ScatterReadHandle(
    std::shared_ptr<ChunkedReadSession> session)
    : session_(std::move(session)) {}

ScatterReadHandle::~ScatterReadHandle() = default;

ScatterReadHandle::ScatterReadHandle(ScatterReadHandle&& other) noexcept =
    default;

ScatterReadHandle& ScatterReadHandle::operator=(
    ScatterReadHandle&& other) noexcept = default;

size_t ScatterReadHandle::num_chunks() const { return session_->num_chunks(); }

bool ScatterReadHandle::is_chunk_ready(size_t chunk_index) {
    return session_->is_chunk_ready(chunk_index);
}

size_t ScatterReadHandle::completed_count() {
    return session_->completed_count();
}

ErrorCode ScatterReadHandle::wait_chunk(size_t chunk_index) {
    return session_->wait_chunk(chunk_index);
}

ErrorCode ScatterReadHandle::wait_all() { return session_->wait_all(); }

// ============================================================================
// TransferFuture Implementation
// ============================================================================

TransferFuture::TransferFuture(std::shared_ptr<OperationState> state)
    : state_(std::move(state)) {
    if (!state_) {
        LOG(ERROR) << "TransferFuture requires valid state";
        throw std::invalid_argument("TransferFuture requires valid state");
    }
}

bool TransferFuture::isReady() const { return state_->is_completed(); }

ErrorCode TransferFuture::wait() {
    if (!isReady()) {
        state_->wait_for_completion();
    }
    return state_->get_result();
}

ErrorCode TransferFuture::get() { return wait(); }

TransferStrategy TransferFuture::strategy() const {
    return state_->get_strategy();
}

// ============================================================================
// TransferSubmitter Implementation
// ============================================================================

TransferSubmitter::TransferSubmitter(TransferEngine& engine,
                                     std::shared_ptr<StorageBackend>& backend,
                                     const std::string& local_hostname,
                                     TransferMetric* transfer_metric)
    : engine_(engine),
      local_endpoint_(engine.getLocalIpAndPort()),
      memcpy_pool_(std::make_unique<MemcpyWorkerPool>()),
      fileread_pool_(std::make_unique<FilereadWorkerPool>(backend)),
      local_hostname_(local_hostname),
      transfer_metric_(transfer_metric) {
    // Read MC_STORE_MEMCPY environment variable.
    // When not set, auto-detect based on transport type:
    //   - TCP-only environment: enable memcpy (avoids TCP loopback overhead)
    //   - RDMA/other transports: disable memcpy (RDMA is more efficient)
    const char* env_value = std::getenv("MC_STORE_MEMCPY");
    if (env_value == nullptr) {
        memcpy_enabled_ = engine_.isTcpOnly();
        LOG(INFO) << "MC_STORE_MEMCPY not set, auto-detected: "
                  << (memcpy_enabled_ ? "TCP-only environment, memcpy enabled"
                                      : "non-TCP transport available, memcpy "
                                        "disabled");
    } else {
        std::string env_str(env_value);
        // Convert to lowercase for case-insensitive comparison
        std::transform(env_str.begin(), env_str.end(), env_str.begin(),
                       ::tolower);
        if (env_str == "false" || env_str == "0" || env_str == "no" ||
            env_str == "off") {
            memcpy_enabled_ = false;
        } else if (env_str == "true" || env_str == "1" || env_str == "yes" ||
                   env_str == "on") {
            memcpy_enabled_ = true;
        } else {
            LOG(WARNING) << "Invalid value for MC_STORE_MEMCPY: " << env_str
                         << ", defaulting to enabled";
            memcpy_enabled_ = true;
        }
    }

    VLOG(1) << "TransferSubmitter initialized with memcpy_enabled="
            << memcpy_enabled_;
}

std::optional<TransferFuture> TransferSubmitter::submit(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        if (!validateTransferParams(handle, slices)) {
            return std::nullopt;
        }

        if (op_code == TransferRequest::READ) {
            future = submitMemoryReadOperation(handle, slices, 0);
        } else {
            TransferStrategy strategy = selectStrategy(handle, slices);

            switch (strategy) {
                case TransferStrategy::LOCAL_MEMCPY:
                    future = submitMemcpyOperation(handle, slices, op_code);
                    break;
                case TransferStrategy::TRANSFER_ENGINE:
                    future =
                        submitTransferEngineOperation(handle, slices, op_code);
                    break;
                default:
                    LOG(ERROR) << "Unknown transfer strategy: " << strategy;
                    return std::nullopt;
            }
        }
    } else {
        future = submitFileReadOperation(replica, slices, op_code);
    }

    // Update metrics on successful submission
    if (future.has_value()) {
        updateTransferMetrics(slices, op_code);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeRead(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t src_offset) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (src_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range read overflow: src_offset=" << src_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        TransferStrategy strategy = selectStrategy(handle, slices);

        if (strategy == TransferStrategy::LOCAL_MEMCPY) {
            future = submitMemcpyOperation(handle, slices,
                                           TransferRequest::READ, src_offset);
        } else if (strategy == TransferStrategy::TRANSFER_ENGINE) {
            future = submitTransferEngineOperation(
                handle, slices, TransferRequest::READ, src_offset);
        } else {
            LOG(ERROR) << "Range read only supports LOCAL_MEMCPY or "
                          "TRANSFER_ENGINE, got: "
                       << strategy;
            return std::nullopt;
        }
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range read not supported for disk replicas (use full read)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::READ);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submit_batch(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    TransferRequest::OpCode op_code) {
    std::optional<TransferFuture> future;
    std::vector<TransferRequest> requests;
    for (size_t i = 0; i < replicas.size(); ++i) {
        auto& replica = replicas[i];
        auto& slices = all_slices[i];
        auto& mem_desc = replica.get_memory_descriptor();
        if (!validateTransferParams(mem_desc.buffer_descriptor, slices)) {
            return std::nullopt;
        }
        auto& handle = mem_desc.buffer_descriptor;
        uint64_t offset = 0;
        SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment "
                       << handle.transport_endpoint_;
            return std::nullopt;
        }
        for (auto slice : slices) {
            TransferRequest request;
            request.opcode = op_code;
            request.source = static_cast<char*>(slice.ptr);
            request.target_id = seg;
            request.target_offset = handle.buffer_address_ + offset;
            request.length = slice.size;
            requests.emplace_back(request);
            offset += slice.size;
        }
    }
    future = submitTransfer(requests);
    // Update metrics on successful submission
    if (future.has_value()) {
        for (auto& slices : all_slices) {
            updateTransferMetrics(slices, op_code);
        }
    }
    return future;
}

std::optional<TransferFuture>
TransferSubmitter::submit_batch_get_offload_object(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys, const std::vector<uint64_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batched_slices) {
    std::optional<TransferFuture> future;
    std::vector<TransferRequest> requests;
    // Open the segment once — all keys share the same transfer_engine_addr.
    SegmentHandle seg = engine_.openSegment(transfer_engine_addr);
    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
        // nullopt = failure (caller checks !future).  The function returns
        // std::optional so tl::unexpected is not available here.
        return std::nullopt;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        const uint64_t pointer = pointers[i];
        auto it = batched_slices.find(key);
        if (it == batched_slices.end()) {
            LOG(ERROR) << "Key not found in batched_slices: " << key;
            return std::nullopt;  // fail closed
        }
        // Emit one TransferRequest per slice: the on-disk blob is read
        // sequentially while slices may point to non-contiguous GPU memory.
        uint64_t offset = 0;
        for (const auto& slice : it->second) {
            TransferRequest request;
            request.opcode = TransferRequest::READ;
            request.source = static_cast<char*>(slice.ptr);
            request.target_id = seg;
            request.target_offset = pointer + offset;
            request.length = slice.size;
            requests.emplace_back(request);
            offset += slice.size;
        }
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitBatchReadRanges(
    void* dest_buffer,
    const std::vector<std::pair<
        Replica::Descriptor, std::vector<std::tuple<size_t, size_t, size_t>>>>&
        key_ranges,
    bool enable_task_grouping) {
    auto build_result =
        buildScatterReadRequests(engine_, dest_buffer, key_ranges,
                                 enable_task_grouping, "submitBatchReadRanges");
    if (!build_result) {
        return std::nullopt;
    }

    auto& requests = build_result->flat_requests;
    if (requests.empty()) {
        return TransferFuture(std::make_shared<EmptyOperationState>());
    }

    const size_t grouped_task_count = build_result->logical_groups.size();
    auto future =
        submitTransfer(requests, enable_task_grouping ? grouped_task_count : 0);
    if (future) {
        updateReadRequestMetrics(requests);
    }
    return future;
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    auto state = std::make_shared<MemcpyOperationState>();

    // Create memcpy operations
    std::vector<MemcpyOperation> operations;
    operations.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];

        if (slice.ptr == nullptr) continue;

        void* dest;
        const void* src;

        if (op_code == TransferRequest::READ) {
            // READ: from handle (remote buffer) to slice (local buffer)
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(base_address + offset);
        } else {
            // WRITE: from slice (local buffer) to handle (remote buffer)
            dest = reinterpret_cast<void*>(base_address + offset);
            src = slice.ptr;
        }
        offset += slice.size;

        operations.emplace_back(dest, src, slice.size);
    }

    // Submit memcpy operations to worker pool for async execution
    MemcpyTask task(std::move(operations), state);
    memcpy_pool_->submitTask(std::move(task));

    VLOG(1) << "Memcpy transfer submitted to worker pool with " << slices.size()
            << " operations";

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransfer(
    std::vector<TransferRequest>& requests, size_t batch_task_count) {
    // Allocate batch ID using the actual logical task count when callers
    // intentionally group multiple requests into one transfer task.
    const size_t batch_size =
        batch_task_count == 0 ? requests.size() : batch_task_count;
    BatchID batch_id = engine_.allocateBatchID(batch_size);
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        return std::nullopt;
    }

    // Submit transfer
    Status s = engine_.submitTransfer(batch_id, requests);
    if (!s.ok()) {
        LOG(ERROR) << "Failed to submit all transfers, error code is "
                   << s.code();
        // Note: batch_id will be freed by TransferEngineOperationState
        // destructor if we create the state object, otherwise we need to free
        // it here
        engine_.freeBatchID(batch_id);
        return std::nullopt;
    }

    if (batch_id == INVALID_BATCH_ID) {  // INVALID_BATCH_ID
        LOG(ERROR) << "Invalid batch ID for transfer engine operation";
        return std::nullopt;
    }

    // Create state with transfer engine context - no polling thread
    // needed
    auto state = std::make_shared<TransferEngineOperationState>(
        engine_, batch_id, batch_size);

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransferEngineOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    if (handle.transport_endpoint_.empty()) {
        LOG(ERROR) << "Transport endpoint is empty for handle with address "
                   << handle.buffer_address_;
        return std::nullopt;
    }
    SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);

    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "Failed to open segment for endpoint='"
                   << handle.transport_endpoint_ << "'";
        return std::nullopt;
    }

    // Create transfer requests
    std::vector<TransferRequest> requests;
    requests.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) continue;

        TransferRequest request;
        request.opcode = op_code;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = base_address + offset;
        request.length = slice.size;

        offset += slice.size;
        requests.emplace_back(request);
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryReadOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t src_offset) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::READ,
                                     src_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(handle, slices,
                                             TransferRequest::READ, src_offset);
    }

    LOG(ERROR) << "Read only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitFileReadOperation(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code) {
    auto state = std::make_shared<FilereadOperationState>();
    auto disk_replica = replica.get_disk_descriptor();
    std::string file_path = disk_replica.file_path;
    size_t file_length = disk_replica.object_size;

    // Submit memcpy operations to worker pool for async execution
    FilereadTask task(file_path, file_length, slices, state);
    fileread_pool_->submitTask(std::move(task));

    VLOG(1) << "Fileread transfer submitted to worker pool with " << file_path;

    return TransferFuture(state);
}

TransferStrategy TransferSubmitter::selectStrategy(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    // Check if memcpy operations are enabled via environment variable
    if (!memcpy_enabled_) {
        VLOG(2) << "Memcpy operations disabled via MC_STORE_MEMCPY environment "
                   "variable";
        return TransferStrategy::TRANSFER_ENGINE;
    }

    // Check conditions for local memcpy optimization
    if (isLocalTransfer(handle)) {
        return TransferStrategy::LOCAL_MEMCPY;
    }

    return TransferStrategy::TRANSFER_ENGINE;
}

namespace {
// Helper function to extract IP address from endpoint string (ip:port format).
// Supports both IPv4 (ip:port) and IPv6 ([ipv6]:port) formats.
std::string extractIpAddress(const std::string& endpoint) {
    if (endpoint.empty()) {
        return "";
    }

    // Handle IPv6 format: [ipv6]:port
    if (endpoint[0] == '[') {
        size_t closing_bracket = endpoint.find(']');
        if (closing_bracket == std::string::npos) {
            LOG(WARNING) << "Invalid IPv6 endpoint format: " << endpoint;
            return "";
        }
        return endpoint.substr(1, closing_bracket - 1);
    }

    // Handle IPv4 or hostname:port format.
    size_t colon_pos = endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        return endpoint.substr(0, colon_pos);
    }

    // No colon found, return the whole string (might be just IP or hostname).
    return endpoint;
}
}  // namespace

bool TransferSubmitter::isSameProcessEndpoint(
    const std::string& handle_endpoint, const std::string& local_endpoint) {
    // Local memcpy requires that handle.buffer_address_ is a virtual address
    // valid in THIS process. Same host is not enough: two processes on the
    // same host share an IP but have distinct virtual address spaces, so a
    // memcpy on a peer process's address would segfault. Require the full
    // transport endpoint to match, which uniquely identifies the owning
    // process.
    if (handle_endpoint.empty() || local_endpoint.empty()) {
        return false;
    }
    if (handle_endpoint == local_endpoint) {
        return true;
    }

    const std::string handle_ip = extractIpAddress(handle_endpoint);
    const std::string local_ip = extractIpAddress(local_endpoint);
    if (!handle_ip.empty() && handle_ip == local_ip) {
        VLOG(2) << "Disabling local memcpy for same-host endpoints with "
                   "different process endpoints: handle="
                << handle_endpoint << ", local=" << local_endpoint;
    }

    return false;
}

bool TransferSubmitter::isLocalTransfer(
    const AllocatedBuffer::Descriptor& handle) const {
    return isSameProcessEndpoint(handle.transport_endpoint_, local_hostname_) ||
           isSameProcessEndpoint(handle.transport_endpoint_, local_endpoint_);
}

bool TransferSubmitter::validateTransferParams(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    uint64_t all_slice_len = 0;
    for (auto slice : slices) {
        all_slice_len += slice.size;
    }
    if (handle.size_ != all_slice_len) {
        LOG(ERROR) << "handles len:" << handle.size_
                   << ", all_slice_len:" << all_slice_len;
        return false;
    }
    return true;
}

std::optional<ScatterReadHandle>
TransferSubmitter::submitStreamingBatchReadRanges(
    void* dest_buffer,
    const std::vector<std::pair<
        Replica::Descriptor, std::vector<std::tuple<size_t, size_t, size_t>>>>&
        key_ranges,
    bool enable_task_grouping) {
    auto build_result = buildScatterReadRequests(
        engine_, dest_buffer, key_ranges, enable_task_grouping,
        "submitStreamingBatchReadRanges");
    if (!build_result) {
        return std::nullopt;
    }

    auto chunk_groups = std::move(build_result->logical_groups);
    auto& requests = build_result->flat_requests;
    if (requests.empty()) {
        return ScatterReadHandle(
            std::make_shared<ChunkedReadSession>(0, ErrorCode::OK));
    }

    if (!enable_task_grouping) {
        chunk_groups.reserve(requests.size());
        for (auto& request : requests) {
            chunk_groups.push_back({request});
        }
    }

    auto handle = create_chunked_read_handle<ScatterReadHandle>(
        engine_, std::move(chunk_groups),
        std::move(build_result->opened_segments),
        std::min<size_t>(requests.size(), kProgressiveGetWindowChunks),
        "submitStreamingBatchReadRanges", []() {
            return g_transfer_task_test_hooks &&
                   g_transfer_task_test_hooks
                       ->fail_next_streaming_scatter_submit &&
                   g_transfer_task_test_hooks
                       ->fail_next_streaming_scatter_submit();
        });
    if (handle) {
        updateReadRequestMetrics(requests);
    }
    return handle;
}

std::optional<ProgressiveGetHandle> TransferSubmitter::submitProgressiveRead(
    const Replica::Descriptor& replica, void* dest_buffer, size_t total_size,
    size_t chunk_size) {
    if (!replica.is_memory_replica()) {
        LOG(ERROR) << "submitProgressiveRead: only memory replicas supported";
        return std::nullopt;
    }
    if (chunk_size == 0 || total_size == 0) {
        LOG(ERROR) << "submitProgressiveRead: invalid sizes (total_size="
                   << total_size << ", chunk_size=" << chunk_size << ")";
        return std::nullopt;
    }

    const auto& handle = replica.get_memory_descriptor().buffer_descriptor;
    if (handle.transport_endpoint_.empty()) {
        LOG(ERROR) << "submitProgressiveRead: empty transport endpoint";
        return std::nullopt;
    }

    size_t num_chunks = (total_size + chunk_size - 1) / chunk_size;
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    char* dest_base = static_cast<char*>(dest_buffer);

    SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);
    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "submitProgressiveRead: failed to open segment for "
                   << handle.transport_endpoint_;
        return std::nullopt;
    }

    std::vector<std::vector<TransferRequest>> chunk_groups;
    chunk_groups.reserve(num_chunks);
    std::vector<TransferRequest> requests;
    requests.reserve(num_chunks);

    for (size_t i = 0; i < num_chunks; ++i) {
        size_t offset = i * chunk_size;
        size_t length = std::min(chunk_size, total_size - offset);

        TransferRequest request;
        request.opcode = TransferRequest::READ;
        request.source = dest_base + offset;
        request.target_id = seg;
        request.target_offset = base_address + offset;
        request.length = length;
        requests.emplace_back(request);
        chunk_groups.push_back({request});
    }

    auto progressive_handle = create_chunked_read_handle<ProgressiveGetHandle>(
        engine_, std::move(chunk_groups), {seg},
        std::min(num_chunks, kProgressiveGetWindowChunks),
        "submitProgressiveRead", []() {
            return g_transfer_task_test_hooks &&
                   g_transfer_task_test_hooks->fail_next_progressive_submit &&
                   g_transfer_task_test_hooks->fail_next_progressive_submit();
        });
    if (progressive_handle) {
        updateReadBytes(total_size);
    }
    return progressive_handle;
}

void TransferSubmitter::updateTransferMetrics(const std::vector<Slice>& slices,
                                              TransferRequest::OpCode op_code) {
    size_t total_bytes = 0;
    for (const auto& slice : slices) {
        total_bytes += slice.size;
    }

    if (transfer_metric_ == nullptr) {
        return;
    }

    if (op_code == TransferRequest::READ) {
        transfer_metric_->total_read_bytes.inc(total_bytes);

    } else if (op_code == TransferRequest::WRITE) {
        transfer_metric_->total_write_bytes.inc(total_bytes);
    }
}

void TransferSubmitter::updateReadRequestMetrics(
    const std::vector<TransferRequest>& requests) {
    updateReadBytes(sum_request_bytes(requests));
}

void TransferSubmitter::updateReadBytes(size_t total_bytes) {
    if (transfer_metric_ == nullptr) {
        return;
    }
    transfer_metric_->total_read_bytes.inc(total_bytes);
}

// ============================================================================
// ProgressivePutSession + ProgressivePutHandle Implementation
// ============================================================================

ErrorCode ProgressivePutSession::write_chunk(void* data, size_t offset,
                                             size_t size) {
    if (data == nullptr || size == 0) {
        LOG(ERROR) << "ProgressivePutSession: invalid params (data=" << data
                   << ", size=" << size << ")";
        return ErrorCode::INVALID_PARAMS;
    }
    if (size > total_size_ || offset > total_size_ - size) {
        LOG(ERROR) << "ProgressivePutSession: overflow offset=" << offset
                   << " + size=" << size << " > total=" << total_size_;
        return ErrorCode::INVALID_PARAMS;
    }

    std::lock_guard<std::mutex> lock(write_mutex_);

    if (sealed_.load(std::memory_order_relaxed)) {
        LOG(ERROR) << "ProgressivePutSession: cannot write after seal";
        return ErrorCode::INVALID_PARAMS;
    }
    if (completed_count_.load(std::memory_order_relaxed) >= num_chunks_) {
        LOG(ERROR) << "ProgressivePutSession: all chunks already written";
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode err = submitter_->submitChunkWrite(replica_, data, offset, size);
    if (err != ErrorCode::OK) {
        return err;
    }

    uint64_t new_count =
        completed_count_.fetch_add(1, std::memory_order_release) + 1;

    // Progress callback inside the lock to guarantee monotonic counter
    // updates. This is acceptable because the progress key is forced to
    // the local segment (LOCAL_MEMCPY path — sub-microsecond).
    if (progress_cb_) {
        progress_cb_(new_count);
    }
    return ErrorCode::OK;
}

ErrorCode ProgressivePutSession::seal() {
    std::lock_guard<std::mutex> lock(write_mutex_);
    sealed_.store(true, std::memory_order_release);
    if (seal_cb_) {
        return seal_cb_();
    }
    return ErrorCode::OK;
}

ProgressivePutHandle::ProgressivePutHandle(
    std::shared_ptr<ProgressivePutSession> session)
    : session_(std::move(session)) {}

ProgressivePutHandle::~ProgressivePutHandle() {
    if (session_ && !session_->is_sealed() &&
        session_->completed_count() < session_->num_chunks()) {
        LOG(WARNING) << "ProgressivePutHandle destroyed without seal: "
                     << session_->completed_count() << "/"
                     << session_->num_chunks() << " chunks written";
    }
}

ProgressivePutHandle::ProgressivePutHandle(
    ProgressivePutHandle&& other) noexcept = default;

ProgressivePutHandle& ProgressivePutHandle::operator=(
    ProgressivePutHandle&& other) noexcept = default;

size_t ProgressivePutHandle::num_chunks() const {
    return session_->num_chunks();
}

size_t ProgressivePutHandle::completed_count() const {
    return session_->completed_count();
}

ErrorCode ProgressivePutHandle::write_chunk(void* data, size_t offset,
                                            size_t size) {
    return session_->write_chunk(data, offset, size);
}

ErrorCode ProgressivePutHandle::seal() { return session_->seal(); }

bool ProgressivePutHandle::is_sealed() const { return session_->is_sealed(); }

ErrorCode TransferSubmitter::submitChunkWrite(
    const Replica::Descriptor& replica, void* data, size_t offset,
    size_t size) {
    const auto& handle = replica.get_memory_descriptor().buffer_descriptor;
    if (size > handle.size_ || offset > handle.size_ - size) {
        LOG(ERROR) << "submitChunkWrite: overflow offset=" << offset
                   << " + size=" << size << " > buffer=" << handle.size_;
        return ErrorCode::INVALID_PARAMS;
    }

    Slice slice{data, size};
    std::vector<Slice> slices{slice};

    std::optional<TransferFuture> future;
    TransferStrategy strategy = selectStrategy(handle, slices);
    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        future = submitMemcpyOperation(handle, slices, TransferRequest::WRITE,
                                       offset);
    } else {
        future = submitTransferEngineOperation(handle, slices,
                                               TransferRequest::WRITE, offset);
    }

    if (!future) {
        return ErrorCode::TRANSFER_FAIL;
    }

    ErrorCode result = future->get();
    if (result == ErrorCode::OK && transfer_metric_) {
        transfer_metric_->total_write_bytes.inc(size);
    }
    return result;
}

}  // namespace mooncake
