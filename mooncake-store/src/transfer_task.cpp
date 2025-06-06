#include "transfer_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <thread>

#include "utils.h"

namespace mooncake {

// ============================================================================
// TransferEngineOperationState Implementation
// ============================================================================

void TransferEngineOperationState::wait_for_completion() {
    if (is_completed()) {
        return;
    }

    VLOG(1) << "Starting transfer engine polling for batch " << batch_id_;

    constexpr uint32_t max_try_num = 3;
    constexpr int64_t timeout_seconds = 60;
    constexpr int64_t kOneSecondInNano = 1000 * 1000 * 1000;

    const int64_t start_ts = getCurrentTimeInNano();
    uint32_t try_num = 0;

    // RAII wrapper for batch ID cleanup
    auto batch_guard = [this](void*) { engine_->freeBatchID(batch_id_); };
    std::unique_ptr<void, decltype(batch_guard)> guard(
        reinterpret_cast<void*>(1), batch_guard);

    while (try_num < max_try_num && !is_completed()) {
        if (getCurrentTimeInNano() - start_ts >
            timeout_seconds * kOneSecondInNano) {
            LOG(ERROR) << "Failed to complete transfers after "
                       << timeout_seconds << " seconds";
            set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }

        bool has_err = false;
        bool all_ready = true;

        for (size_t i = 0; i < batch_size_; ++i) {
            TransferStatus status;
            Status s = engine_->getTransferStatus(batch_id_, i, status);
            if (!s.ok()) {
                LOG(ERROR) << "Transfer " << i
                           << " error, error_code=" << s.code();
                set_completed(ErrorCode::TRANSFER_FAIL);
                return;
            }

            if (status.s != TransferStatusEnum::COMPLETED) {
                all_ready = false;
            }

            if (status.s == TransferStatusEnum::FAILED ||
                status.s == TransferStatusEnum::CANCELED ||
                status.s == TransferStatusEnum::INVALID) {
                LOG(ERROR) << "Transfer failed for task " << i
                           << " with status " << static_cast<int>(status.s);
                has_err = true;
            }
        }

        if (all_ready) {
            VLOG(1) << "Transfer engine operation completed successfully";
            set_completed(ErrorCode::OK);
            return;
        }

        if (has_err) {
            LOG(WARNING) << "Transfer incomplete, retrying... (attempt "
                         << try_num + 1 << "/" << max_try_num << ")";
            ++try_num;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } else {
            // Still waiting for completion, brief sleep
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    LOG(ERROR) << "Transfer incomplete after max attempts: " << max_try_num;
    set_completed(ErrorCode::TRANSFER_FAIL);
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
    : engine_(engine), local_hostname_(local_hostname) {
    CHECK(!local_hostname_.empty()) << "Local hostname cannot be empty";
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
            LOG(ERROR) << "Unknown transfer strategy: "
                       << static_cast<int>(strategy);
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
            // READ: from handle (remote buffer) to slice (local buffer)
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(handle.buffer_address_);
        } else {
            // WRITE: from slice (local buffer) to handle (remote buffer)
            dest = reinterpret_cast<void*>(handle.buffer_address_);
            src = slice.ptr;
        }

        operations.emplace_back(dest, src, handle.size_);
    }

    // Execute memcpy operations asynchronously
    std::thread([state, operations = std::move(operations)]() {
        VLOG(1) << "Executing async memcpy transfer with " << operations.size()
                << " operations";

        try {
            for (const auto& op : operations) {
                std::memcpy(op.dest, op.src, op.size);
            }

            VLOG(1) << "Async memcpy transfer completed successfully";
            state->set_completed(ErrorCode::OK);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception during async memcpy transfer: "
                       << e.what();
            state->set_completed(ErrorCode::TRANSFER_FAIL);
        }
    }).detach();

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
        engine_.freeBatchID(batch_id);
        return std::nullopt;
    }

    // Create state with transfer engine context - no polling thread needed
    auto state = std::make_shared<TransferEngineOperationState>(
        &engine_, batch_id, batch_size);

    return TransferFuture(state);
}

TransferStrategy TransferSubmitter::selectStrategy(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    const std::vector<Slice>& slices) const {
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
