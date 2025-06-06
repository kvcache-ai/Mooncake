#pragma once

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Transfer strategy enumeration
 */
enum class TransferStrategy {
    LOCAL_MEMCPY,    // Local memory copy using memcpy
    TRANSFER_ENGINE  // Remote transfer using transfer engine
};

/**
 * @brief Abstract base class for operation state management
 *
 * This class encapsulates the common state and behavior for async transfer
 * operations. Derived classes implement strategy-specific waiting logic.
 */
class OperationState {
   public:
    explicit OperationState(TransferStrategy strategy) : strategy_(strategy) {}
    virtual ~OperationState() = default;

    // Non-copyable, non-movable
    OperationState(const OperationState&) = delete;
    OperationState& operator=(const OperationState&) = delete;
    OperationState(OperationState&&) = delete;
    OperationState& operator=(OperationState&&) = delete;

    /**
     * @brief Check if the operation has completed
     */
    bool is_completed() const { return completed_.load(); }

    /**
     * @brief Get the operation result
     */
    ErrorCode get_result() const { return result_.load(); }

    /**
     * @brief Get the transfer strategy
     */
    TransferStrategy get_strategy() const { return strategy_; }

    /**
     * @brief Mark the operation as completed with the given result
     */
    void set_completed(ErrorCode error_code) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            result_.store(error_code);
            completed_.store(true);
        }
        cv_.notify_all();
    }

    /**
     * @brief Wait for the operation to complete (strategy-specific
     * implementation)
     */
    virtual void wait_for_completion() = 0;

   protected:
    std::atomic<bool> completed_{false};
    std::atomic<ErrorCode> result_{ErrorCode::OK};
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    const TransferStrategy strategy_;
};

/**
 * @brief Operation state for local memcpy transfers
 */
class MemcpyOperationState : public OperationState {
   public:
    MemcpyOperationState() : OperationState(TransferStrategy::LOCAL_MEMCPY) {}

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return completed_.load(); });
    }
};

/**
 * @brief Operation state for transfer engine operations
 */
class TransferEngineOperationState : public OperationState {
   public:
    TransferEngineOperationState(TransferEngine* engine, BatchID batch_id,
                                 size_t batch_size)
        : OperationState(TransferStrategy::TRANSFER_ENGINE),
          engine_(engine),
          batch_id_(batch_id),
          batch_size_(batch_size) {
        CHECK(engine_) << "TransferEngine cannot be null";
    }

    void wait_for_completion() override;

   private:
    TransferEngine* engine_;
    BatchID batch_id_;
    size_t batch_size_;
};

/**
 * @brief Represents the future result of an asynchronous transfer operation
 *
 * This class provides a std::future-like interface for asynchronous transfer
 * operations. Users can check completion status, wait for results, or get the
 * final error code.
 */
class TransferFuture {
   public:
    explicit TransferFuture(std::shared_ptr<OperationState> state);

    // Non-copyable but movable
    TransferFuture(const TransferFuture&) = delete;
    TransferFuture& operator=(const TransferFuture&) = delete;
    TransferFuture(TransferFuture&&) = default;
    TransferFuture& operator=(TransferFuture&&) = default;

    /**
     * @brief Check if the operation has completed (non-blocking)
     * @return true if the operation is finished, false otherwise
     */
    bool isReady() const;

    /**
     * @brief Wait for the operation to complete (blocking)
     * @return ErrorCode indicating success or failure
     */
    ErrorCode wait();

    /**
     * @brief Get the result, waiting if necessary (blocking)
     * @return ErrorCode indicating success or failure
     */
    ErrorCode get();

    /**
     * @brief Get the transfer strategy used by this operation
     * @return TransferStrategy enum value
     */
    TransferStrategy strategy() const;

   private:
    std::shared_ptr<OperationState> state_;
};

/**
 * @brief Memory copy operation descriptor
 */
struct MemcpyOperation {
    void* dest;
    const void* src;
    size_t size;

    MemcpyOperation(void* d, const void* s, size_t sz)
        : dest(d), src(s), size(sz) {}
};

/**
 * @brief Submitter class for asynchronous transfer operations
 *
 * This class analyzes transfer requirements, selects optimal strategies, and
 * immediately submits operations returning TransferFuture objects for result
 * tracking.
 */
class TransferSubmitter {
   public:
    explicit TransferSubmitter(TransferEngine& engine,
                               const std::string& local_hostname);

    /**
     * @brief Submit an asynchronous transfer operation
     *
     * Analyzes the transfer requirements, selects the optimal strategy,
     * and immediately submits the operation. Returns a TransferFuture
     * that can be used to track completion and get results.
     *
     * @param handles Buffer descriptors for the transfer
     * @param slices Memory slices for the transfer
     * @param op_code Transfer operation (READ/WRITE)
     * @return TransferFuture representing the async operation, or nullopt on
     * failure
     */
    std::optional<TransferFuture> submit(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        std::vector<Slice>& slices, Transport::TransferRequest::OpCode op_code);

   private:
    TransferEngine& engine_;
    const std::string local_hostname_;

    /**
     * @brief Select the optimal transfer strategy
     */
    TransferStrategy selectStrategy(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        const std::vector<Slice>& slices) const;

    /**
     * @brief Check if all handles refer to local segments
     */
    bool isLocalTransfer(
        const std::vector<AllocatedBuffer::Descriptor>& handles) const;

    /**
     * @brief Validate transfer parameters
     */
    bool validateTransferParams(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        const std::vector<Slice>& slices) const;

    /**
     * @brief Submit memcpy operation asynchronously
     */
    std::optional<TransferFuture> submitMemcpyOperation(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        std::vector<Slice>& slices, Transport::TransferRequest::OpCode op_code);

    /**
     * @brief Submit transfer engine operation asynchronously
     */
    std::optional<TransferFuture> submitTransferEngineOperation(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        std::vector<Slice>& slices, Transport::TransferRequest::OpCode op_code);
};

}  // namespace mooncake
