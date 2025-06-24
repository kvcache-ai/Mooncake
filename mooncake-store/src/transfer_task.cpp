#include "transfer_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>

#include "utils.h"

namespace mooncake {

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
                LOG(ERROR) << "Transfer failed for batch " << batch_id_
                           << " task " << i << " with status "
                           << static_cast<int>(status.s);
                has_failure = true;
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

    cv_.notify_all();
}

void TransferEngineOperationState::wait_for_completion() {
    if (is_completed()) {
        return;
    }

    VLOG(1) << "Starting transfer engine polling for batch " << batch_id_;
    constexpr int64_t timeout_seconds = 60;
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
}

// ============================================================================
// TransferFuture Implementation
// ============================================================================

TransferFuture::TransferFuture(std::shared_ptr<OperationState> state)
    : state_(std::move(state)) {
    CHECK(state_) << "TransferFuture requires valid state";
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
                                     const std::string& local_hostname)
    : engine_(engine),
      local_hostname_(local_hostname),
      memcpy_pool_(std::make_unique<MemcpyWorkerPool>()) {
    CHECK(!local_hostname_.empty()) << "Local hostname cannot be empty";

    // Read MC_STORE_MEMCPY environment variable, default to true (enabled)
    const char* env_value = std::getenv("MC_STORE_MEMCPY");
    if (env_value == nullptr) {
        memcpy_enabled_ = true;  // Default: enabled
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
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    std::vector<Slice>& slices, Transport::TransferRequest::OpCode op_code) {
    if (!validateTransferParams(handles, slices)) {
        return std::nullopt;
    }

    TransferStrategy strategy = selectStrategy(handles, slices);

    switch (strategy) {
        case TransferStrategy::LOCAL_MEMCPY:
            return submitMemcpyOperation(handles, slices, op_code);
        case TransferStrategy::TRANSFER_ENGINE:
            return submitTransferEngineOperation(handles, slices, op_code);
        default:
            LOG(ERROR) << "Unknown transfer strategy: " << strategy;
            return std::nullopt;
    }
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    std::vector<Slice>& slices, Transport::TransferRequest::OpCode op_code) {
    auto state = std::make_shared<MemcpyOperationState>();

    // Create memcpy operations
    std::vector<MemcpyOperation> operations;
    operations.reserve(handles.size());

    for (size_t i = 0; i < handles.size(); ++i) {
        const auto& handle = handles[i];
        const auto& slice = slices[i];

        void* dest;
        const void* src;

        if (op_code == Transport::TransferRequest::READ) {
            // READ: from handle (remote buffer) to slice (local
            // buffer)
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(handle.buffer_address_);
        } else {
            // WRITE: from slice (local buffer) to handle (remote
            // buffer)
            dest = reinterpret_cast<void*>(handle.buffer_address_);
            src = slice.ptr;
        }

        operations.emplace_back(dest, src, handle.size_);
    }

    // Submit memcpy operations to worker pool for async execution
    MemcpyTask task(std::move(operations), state);
    memcpy_pool_->submitTask(std::move(task));

    VLOG(1) << "Memcpy transfer submitted to worker pool with "
            << handles.size() << " operations";

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransferEngineOperation(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    std::vector<Slice>& slices, Transport::TransferRequest::OpCode op_code) {
    // Create transfer requests
    std::vector<Transport::TransferRequest> requests;
    requests.reserve(handles.size());

    for (size_t i = 0; i < handles.size(); ++i) {
        const auto& handle = handles[i];
        const auto& slice = slices[i];

        Transport::SegmentHandle seg =
            engine_.openSegment(handle.segment_name_);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment " << handle.segment_name_;
            return std::nullopt;
        }

        Transport::TransferRequest request;
        request.opcode = op_code;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = handle.buffer_address_;
        request.length = handle.size_;

        requests.emplace_back(request);
    }

    // Allocate batch ID
    const size_t batch_size = requests.size();
    BatchID batch_id = engine_.allocateBatchID(batch_size);
    if (batch_id == Transport::INVALID_BATCH_ID) {
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

    // Create state with transfer engine context - no polling thread
    // needed
    auto state = std::make_shared<TransferEngineOperationState>(
        engine_, batch_id, batch_size);

    return TransferFuture(state);
}

TransferStrategy TransferSubmitter::selectStrategy(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    const std::vector<Slice>& slices) const {
    // Check if memcpy operations are enabled via environment variable
    if (!memcpy_enabled_) {
        VLOG(2) << "Memcpy operations disabled via MC_STORE_MEMCPY environment "
                   "variable";
        return TransferStrategy::TRANSFER_ENGINE;
    }

    // Check conditions for local memcpy optimization
    if (isLocalTransfer(handles)) {
        return TransferStrategy::LOCAL_MEMCPY;
    }

    return TransferStrategy::TRANSFER_ENGINE;
}

bool TransferSubmitter::isLocalTransfer(
    const std::vector<AllocatedBuffer::Descriptor>& handles) const {
    return std::all_of(handles.begin(), handles.end(),
                       [this](const auto& handle) {
                           return handle.segment_name_ == local_hostname_;
                       });
}

bool TransferSubmitter::validateTransferParams(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    const std::vector<Slice>& slices) const {
    if (handles.empty()) {
        LOG(ERROR) << "handles is empty";
        return false;
    }

    if (handles.size() > slices.size()) {
        LOG(ERROR) << "invalid_partition_count handles_size=" << handles.size()
                   << " slices_size=" << slices.size();
        return false;
    }

    for (size_t i = 0; i < handles.size(); ++i) {
        if (handles[i].size_ != slices[i].size) {
            LOG(ERROR) << "Size of replica partition " << i << " ("
                       << handles[i].size_
                       << ") does not match provided buffer (" << slices[i].size
                       << ")";
            return false;
        }
    }

    return true;
}

}  // namespace mooncake
