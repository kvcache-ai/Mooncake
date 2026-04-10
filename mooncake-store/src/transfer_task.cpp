#include "transfer_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include "transfer_engine.h"
#include "transport/transport.h"
#include "tracing_facade.h"

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

size_t CalculateRequestBytes(const std::vector<TransferRequest>& requests) {
    size_t total_bytes = 0;
    for (const auto& request : requests) {
        total_bytes += request.length;
    }
    return total_bytes;
}

const char* ToStoreTransferStatusName(TransferStatusEnum status) {
    switch (status) {
        case TransferStatusEnum::WAITING:
            return "WAITING";
        case TransferStatusEnum::PENDING:
            return "PENDING";
        case TransferStatusEnum::INVALID:
            return "INVALID";
        case TransferStatusEnum::CANCELED:
            return "CANCELED";
        case TransferStatusEnum::COMPLETED:
            return "COMPLETED";
        case TransferStatusEnum::TIMEOUT:
            return "TIMEOUT";
        case TransferStatusEnum::FAILED:
            return "FAILED";
    }
    return "UNKNOWN";
}

TransferTraceSession TransferTraceSession::Start(
    mooncake::tracing::TracingFacade& tracing,
    const mooncake::tracing::TraceContext* trace_context, size_t batch_size,
    size_t total_bytes) {
    TransferTraceSession session;
    session.batch_size_ = batch_size;

    if (trace_context != nullptr && trace_context->valid()) {
        session.parent_context_ = *trace_context;
        return session;
    }

    session.operation_span_ =
        tracing.StartSpan("mooncake.transfer.operation", nullptr,
                          {{"batch.size", std::to_string(batch_size)},
                           {"sampling.priority", "structural"},
                           {"bytes.total", std::to_string(total_bytes)}});
    if (session.operation_span_.valid()) {
        session.parent_context_ = session.operation_span_.context();
    }
    return session;
}

void TransferTraceSession::RecordBatch(BatchID batch_id) {
    batch_id_ = batch_id;
    if (!operation_span_.valid()) {
        return;
    }
    operation_span_.SetAttribute("te.batch_id", std::to_string(batch_id_));
}

void TransferTraceSession::StartSubmitGap(
    mooncake::tracing::TracingFacade& tracing, BatchID batch_id,
    size_t batch_size) {
    if (submit_gap_context_.valid() || wait_context_.valid()) {
        return;
    }

    auto span =
        tracing.StartSpan("mooncake.transfer.submit_gap", parent_context(),
                          {{"te.batch_id", std::to_string(batch_id)},
                           {"batch.size", std::to_string(batch_size)}});
    if (!span.valid()) {
        return;
    }
    submit_gap_context_ = span.context();
    submit_gap_span_ = std::move(span);
}

void TransferTraceSession::MarkSubmitError() {
    if (!operation_span_.valid()) {
        return;
    }
    if (batch_id_ != INVALID_BATCH_ID) {
        operation_span_.AddEvent("transfer submit failed",
                                 {{"te.batch_id", std::to_string(batch_id_)}});
    }
    operation_span_.SetStatus("ERROR");
}

void TransferTraceSession::FinishSubmitGap(ErrorCode error_code) {
    if (!submit_gap_context_.valid()) {
        return;
    }

    submit_gap_span_.AddEvent(
        "submit gap finished",
        {{"result.code", std::to_string(static_cast<int>(error_code))}});
    if (error_code != ErrorCode::OK) {
        submit_gap_span_.SetStatus("ERROR");
    }
    submit_gap_span_.End();
    submit_gap_span_ = mooncake::tracing::Span();
    submit_gap_context_ = {};
}

mooncake::tracing::Span* TransferTraceSession::EnsureWaitSpan(
    mooncake::tracing::TracingFacade& tracing, BatchID batch_id,
    size_t batch_size) {
    if (wait_context_.valid()) {
        return &wait_span_;
    }

    FinishSubmitGap(ErrorCode::OK);

    auto span =
        tracing.StartSpan("mooncake.transfer.wait_completion", parent_context(),
                          {{"te.batch_id", std::to_string(batch_id)},
                           {"batch.size", std::to_string(batch_size)}});
    if (!span.valid()) {
        return nullptr;
    }
    wait_context_ = span.context();
    wait_span_ = std::move(span);
    return &wait_span_;
}

void TransferTraceSession::FinishWait(ErrorCode error_code) {
    if (!wait_context_.valid()) {
        return;
    }

    wait_span_.AddEvent(
        "wait completion finished",
        {{"result.code", std::to_string(static_cast<int>(error_code))}});
    if (error_code != ErrorCode::OK) {
        wait_span_.SetStatus("ERROR");
    }
    wait_span_.End();
    wait_span_ = mooncake::tracing::Span();
    wait_context_ = {};
}

void TransferTraceSession::Finish(mooncake::tracing::TracingFacade& tracing,
                                  ErrorCode error_code) {
    FinishSubmitGap(error_code);
    auto complete_span = tracing.StartSpan(
        "mooncake.transfer.complete", parent_context(),
        {{"batch.size", std::to_string(batch_size_)},
         {"result.code", std::to_string(static_cast<int>(error_code))}});
    if (complete_span.valid()) {
        if (batch_id_ != INVALID_BATCH_ID) {
            complete_span.SetAttribute("te.batch_id",
                                       std::to_string(batch_id_));
        }
        complete_span.AddEvent(
            "transfer operation completed",
            {{"result.code", std::to_string(static_cast<int>(error_code))}});
        if (error_code != ErrorCode::OK) {
            complete_span.SetStatus("ERROR");
        }
    }

    if (!operation_span_.valid()) {
        return;
    }
    if (batch_id_ != INVALID_BATCH_ID) {
        operation_span_.SetAttribute("te.batch_id", std::to_string(batch_id_));
    }
    operation_span_.SetAttribute("batch.size", std::to_string(batch_size_));
    operation_span_.SetAttribute("result.code",
                                 std::to_string(static_cast<int>(error_code)));
    operation_span_.AddEvent(
        "transfer operation completed",
        {{"result.code", std::to_string(static_cast<int>(error_code))}});
    if (error_code != ErrorCode::OK) {
        operation_span_.SetStatus("ERROR");
    }
    operation_span_.End();
}

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
                for (const auto& op : task.operations) {
                    std::memcpy(op.dest, op.src, op.size);
                }

                VLOG(2) << "Memcpy task completed successfully with "
                        << task.operations.size() << " operations";
                task.state->set_completed(ErrorCode::OK);
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
    auto& tracing = mooncake::tracing::TracingFacade::Instance("mooncake-store",
                                                               "transfer-task");
    auto* wait_span =
        trace_session_.EnsureWaitSpan(tracing, batch_id_, batch_size_);
    // Check all transfers in the batch
    bool all_completed = true;
    bool has_failure = false;

    for (size_t i = 0; i < batch_size_; ++i) {
        TransferStatus status;
        Status s = engine_.getTransferStatus(batch_id_, i, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status for batch "
                       << batch_id_ << " task " << i << " with error "
                       << s.message();
            if (wait_span != nullptr) {
                wait_span->AddEvent(
                    "status query failed",
                    {{"task.id", std::to_string(i)},
                     {"error.message", std::string(s.message())}});
                wait_span->SetStatus("ERROR");
            }
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        switch (status.s) {
            case TransferStatusEnum::COMPLETED:
                // This transfer is done, continue checking others
                break;
            case TransferStatusEnum::FAILED:
            case TransferStatusEnum::CANCELED:
            case TransferStatusEnum::INVALID:
#ifndef USE_ASCEND_DIRECT
                LOG(ERROR) << "Transfer failed for batch " << batch_id_
                           << " task " << i << " with status "
                           << static_cast<int>(status.s);
#endif
                has_failure = true;
                if (wait_span != nullptr) {
                    wait_span->AddEvent(
                        "transfer failed",
                        {{"task.id", std::to_string(i)},
                         {"status",
                          std::string(ToStoreTransferStatusName(status.s))}});
                }
                break;
            default:
                // Transfer is still pending (PENDING, RUNNING, etc.)
                all_completed = false;
                break;
        }
    }

    if (has_failure) {
        VLOG(1) << "Setting batch " << batch_id_
                << " result to TRANSFER_FAIL due to task failures";
        set_result_internal(ErrorCode::TRANSFER_FAIL);
        return;
    }

    if (all_completed) {
        if (wait_span != nullptr) {
            wait_span->AddEvent("all transfers completed");
        }
        set_result_internal(ErrorCode::OK);
        return;
    }

    return;
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
    trace_session_.FinishWait(error_code);
    auto& tracing = mooncake::tracing::TracingFacade::Instance("mooncake-store",
                                                               "transfer-task");
    trace_session_.Finish(tracing, error_code);
}

void TransferEngineOperationState::wait_for_completion() {
    if (is_completed()) {
        return;
    }

    constexpr int64_t timeout_seconds = 60;

#ifdef USE_EVENT_DRIVEN_COMPLETION
    VLOG(1) << "Waiting for transfer engine completion for batch " << batch_id_;
    auto& tracing = mooncake::tracing::TracingFacade::Instance("mooncake-store",
                                                               "transfer-task");
    trace_session_.EnsureWaitSpan(tracing, batch_id_, batch_size_);

    // Wait directly on BatchDesc's condition variable.
    auto& batch_desc = Transport::toBatchDesc(batch_id_);
    bool completed;
    bool failed = false;

    // Fast path: if already finished, avoid taking the mutex and waiting.
    // Use acquire here to pair with the writer's release-store, because this
    // path may skip taking the mutex. It ensures all prior updates are visible.
    completed = batch_desc.is_finished.load(std::memory_order_acquire);
    if (!completed) {
        // Use the same mutex as the notifier when updating the predicate to
        // avoid missed notifications. The predicate is re-checked under the
        // lock. Under the mutex, relaxed is sufficient; the mutex acquire
        // orders prior writes.
        std::unique_lock<std::mutex> lock(batch_desc.completion_mutex);
        completed = batch_desc.completion_cv.wait_for(
            lock, std::chrono::seconds(timeout_seconds), [&batch_desc] {
                return batch_desc.is_finished.load(std::memory_order_relaxed);
            });
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
        LOG(ERROR) << "Failed to complete transfers after " << timeout_seconds
                   << " seconds for batch " << batch_id_;
    }
#else
    VLOG(1) << "Starting transfer engine polling for batch " << batch_id_;

    constexpr int64_t kOneSecondInNano = 1000 * 1000 * 1000;
    const int64_t start_ts = getCurrentTimeInNano();

    while (true) {
        if (getCurrentTimeInNano() - start_ts >
            timeout_seconds * kOneSecondInNano) {
            LOG(ERROR) << "Failed to complete transfers after "
                       << timeout_seconds << " seconds for batch " << batch_id_;
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        check_task_status();
        if (result_.has_value()) {
            VLOG(1) << "Transfer engine operation completed for batch "
                    << batch_id_
                    << " with result: " << static_cast<int>(result_.value());
            break;
        }
        // Continue polling
        VLOG(1) << "Transfer engine operation still pending for batch "
                << batch_id_;
    }
#endif
}

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
                                     TransferMetric* transfer_metric)
    : engine_(engine),
      memcpy_pool_(std::make_unique<MemcpyWorkerPool>()),
      fileread_pool_(std::make_unique<FilereadWorkerPool>(backend)),
      transfer_metric_(transfer_metric) {
    // Read MC_STORE_MEMCPY environment variable, default to false (disabled)
    const char* env_value = std::getenv("MC_STORE_MEMCPY");
    if (env_value == nullptr) {
        memcpy_enabled_ = false;  // Default: disabled
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
    TransferRequest::OpCode op_code,
    const tracing::TraceContext* trace_context) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        if (!validateTransferParams(handle, slices)) {
            return std::nullopt;
        }

        TransferStrategy strategy = selectStrategy(handle, slices);

        switch (strategy) {
            case TransferStrategy::LOCAL_MEMCPY:
                future = submitMemcpyOperation(handle, slices, op_code,
                                               trace_context);
                break;
            case TransferStrategy::TRANSFER_ENGINE:
                future = submitTransferEngineOperation(handle, slices, op_code,
                                                       trace_context);
                break;
            default:
                LOG(ERROR) << "Unknown transfer strategy: " << strategy;
                return std::nullopt;
        }
    } else {
        future =
            submitFileReadOperation(replica, slices, op_code, trace_context);
    }

    // Update metrics on successful submission
    if (future.has_value()) {
        updateTransferMetrics(slices, op_code);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submit_batch(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    TransferRequest::OpCode op_code,
    const tracing::TraceContext* trace_context) {
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
    future = submitTransfer(requests, trace_context);
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
    const std::unordered_map<std::string, Slice>& batched_slices) {
    std::optional<TransferFuture> future;
    std::vector<TransferRequest> requests;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i];
        auto pointer = pointers[i];
        SegmentHandle seg = engine_.openSegment(transfer_engine_addr);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
            return std::nullopt;
        }
        const auto& slice = batched_slices.find(key)->second;
        TransferRequest request;
        request.opcode = TransferRequest::READ;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = pointer;
        request.length = slice.size;
        requests.emplace_back(request);
    }
    return submitTransfer(requests, nullptr);
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code,
    const tracing::TraceContext* trace_context) {
    (void)trace_context;
    auto state = std::make_shared<MemcpyOperationState>();

    // Create memcpy operations
    std::vector<MemcpyOperation> operations;
    operations.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = 0;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];

        if (slice.ptr == nullptr) continue;

        void* dest;
        const void* src;

        if (op_code == TransferRequest::READ) {
            // READ: from handle (remote buffer) to slice (local
            // buffer)
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(base_address + offset);
        } else {
            // WRITE: from slice (local buffer) to handle (remote
            // buffer)
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
    std::vector<TransferRequest>& requests,
    const tracing::TraceContext* trace_context) {
    auto& tracing =
        tracing::TracingFacade::Instance("mooncake-store", "transfer-task");
    const size_t batch_size = requests.size();
    const size_t total_bytes = CalculateRequestBytes(requests);
    auto trace_session = TransferTraceSession::Start(tracing, trace_context,
                                                     batch_size, total_bytes);

    auto span = tracing.StartSpan(
        "mooncake.transfer.prepare", trace_session.parent_context(),
        {{"batch.size", std::to_string(batch_size)},
         {"bytes.total", std::to_string(total_bytes)}});
    // Allocate batch ID
    BatchID batch_id = engine_.allocateBatchID(batch_size);
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        span.SetStatus("ERROR");
        trace_session.MarkSubmitError();
        return std::nullopt;
    }
    span.SetAttribute("te.batch_id", std::to_string(batch_id));
    trace_session.RecordBatch(batch_id);

    // Submit transfer
    Status s = engine_.submitTransfer(batch_id, requests,
                                      trace_session.parent_context());
    if (!s.ok()) {
        LOG(ERROR) << "Failed to submit all transfers, error code is "
                   << s.code();
        span.SetStatus("ERROR");
        trace_session.MarkSubmitError();
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

    trace_session.StartSubmitGap(tracing, batch_id, batch_size);

    // Create state with transfer engine context - no polling thread
    // needed
    auto state = std::make_shared<TransferEngineOperationState>(
        engine_, batch_id, batch_size, std::move(trace_session));

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransferEngineOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code,
    const tracing::TraceContext* trace_context) {
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
    uint64_t offset = 0;

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
    return submitTransfer(requests, trace_context);
}

std::optional<TransferFuture> TransferSubmitter::submitFileReadOperation(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code,
    const tracing::TraceContext* trace_context) {
    (void)trace_context;
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
// Helper function to extract IP address from endpoint string (ip:port format)
// Supports both IPv4 (ip:port) and IPv6 ([ipv6]:port) formats
std::string extractIpAddress(const std::string& endpoint) {
    if (endpoint.empty()) {
        return "";
    }

    // Handle IPv6 format: [ipv6]:port
    if (endpoint[0] == '[') {
        size_t closing_bracket = endpoint.find(']');
        if (closing_bracket == std::string::npos) {
            LOG(WARNING) << "Invalid IPv6 endpoint format: " << endpoint;
            // Return empty to disable local memcpy optimization
            return "";
        }
        return endpoint.substr(1, closing_bracket - 1);  // Extract IPv6 address
    }

    // Handle IPv4 or hostname:port format
    // Find the last colon (to handle IPv6 addresses without brackets)
    size_t colon_pos = endpoint.rfind(':');
    if (colon_pos != std::string::npos) {
        return endpoint.substr(0, colon_pos);
    }

    // No colon found, return the whole string (might be just IP or hostname)
    return endpoint;
}
}  // namespace

bool TransferSubmitter::isLocalTransfer(
    const AllocatedBuffer::Descriptor& handle) const {
    std::string local_ep = engine_.getLocalIpAndPort();
    std::string local_ip = extractIpAddress(local_ep);

    if (!local_ep.empty()) {
        std::string handle_ip = extractIpAddress(handle.transport_endpoint_);
        return !handle.transport_endpoint_.empty() && handle_ip == local_ip;
    }

    // Without a local IP we cannot prove locality; disable memcpy.
    return false;
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

}  // namespace mooncake
