#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "types.h"
#include "replica.h"
#include "storage_backend.h"
#include "client_metric.h"

namespace mooncake {

/**
 * @brief Transfer strategy enumeration
 */
enum class TransferStrategy {
    LOCAL_MEMCPY = 0,     // Local memory copy using memcpy
    TRANSFER_ENGINE = 1,  // Remote transfer using transfer engine
    FILE_READ = 2,        // File read operation
    EMPTY = 3
};

/**
 * @brief Stream operator for TransferStrategy
 */
inline std::ostream& operator<<(std::ostream& os,
                                const TransferStrategy& strategy) noexcept {
    switch (strategy) {
        case TransferStrategy::LOCAL_MEMCPY:
            return os << "LOCAL_MEMCPY";
        case TransferStrategy::TRANSFER_ENGINE:
            return os << "TRANSFER_ENGINE";
        case TransferStrategy::FILE_READ:
            return os << "FILE_READ";
        default:
            return os << "UNKNOWN";
    }
}

/**
 * @brief Abstract base class for operation state management
 *
 * This class encapsulates the common state and behavior for async transfer
 * operations. Derived classes implement strategy-specific waiting logic.
 */
class OperationState {
   public:
    OperationState() = default;
    virtual ~OperationState() = default;

    // Non-copyable, non-movable
    OperationState(const OperationState&) = delete;
    OperationState& operator=(const OperationState&) = delete;
    OperationState(OperationState&&) = delete;
    OperationState& operator=(OperationState&&) = delete;

    /**
     * @brief Check if the operation has completed
     */
    virtual bool is_completed() = 0;

    /**
     * @brief Get the operation result. Make sure to call is_completed() first.
     */
    ErrorCode get_result() const {  // lock mutex
        std::lock_guard<std::mutex> lock(mutex_);
        assert(result_.has_value() &&
               "get_result() called on an incomplete or failed-to-set "
               "operation state.");
        return result_.value_or(ErrorCode::INVALID_PARAMS);
    }

    /**
     * @brief Get the transfer strategy
     */
    virtual TransferStrategy get_strategy() const = 0;

    /**
     * @brief Wait for the operation to complete (strategy-specific
     * implementation)
     */
    virtual void wait_for_completion() = 0;

   protected:
    std::optional<ErrorCode> result_ = std::nullopt;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

/**
 * @brief Operation state for local memcpy transfers
 */
class EmptyOperationState : public OperationState {
   public:
    bool is_completed() override { return true; }

    void wait_for_completion() override {}

    TransferStrategy get_strategy() const override {
        return TransferStrategy::EMPTY;
    }
};

/**
 * @brief Operation state for local memcpy transfers
 */
class MemcpyOperationState : public OperationState {
   public:
    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(ErrorCode error_code) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
        }
        cv_.notify_all();
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::LOCAL_MEMCPY;
    }
};

class FilereadOperationState : public OperationState {
   public:
    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(ErrorCode error_code) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
        }
        cv_.notify_all();
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::FILE_READ;
    }
};

/**
 * @brief Operation state for transfer engine operations
 */
class TransferEngineOperationState : public OperationState {
   public:
    TransferEngineOperationState(TransferEngine& engine, BatchID batch_id,
                                 size_t batch_size)
        : engine_(engine), batch_id_(batch_id), batch_size_(batch_size) {}

    ~TransferEngineOperationState() { engine_.freeBatchID(batch_id_); }

    bool is_completed() override;

    void wait_for_completion() override;

    TransferStrategy get_strategy() const override {
        return TransferStrategy::TRANSFER_ENGINE;
    }

   private:
    /**
     * @brief Check the current completion status of the task, make sure to lock
     * the mutex before calling this function.
     * Updates the internal state and returns true if the task is completed.
     */
    void check_task_status();

    void set_result_internal(ErrorCode error_code);

    TransferEngine& engine_;
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
 * @brief Memcpy task for async execution
 */
struct MemcpyTask {
    std::vector<MemcpyOperation> operations;
    std::shared_ptr<MemcpyOperationState> state;

    MemcpyTask(std::vector<MemcpyOperation> ops,
               std::shared_ptr<MemcpyOperationState> s)
        : operations(std::move(ops)), state(std::move(s)) {}
};

/**
 * @brief Thread pool for asynchronous memcpy operations
 *
 * This class manages a single worker thread that executes memcpy operations
 * asynchronously.
 */
class MemcpyWorkerPool {
   public:
    explicit MemcpyWorkerPool();
    ~MemcpyWorkerPool();

    // Non-copyable, non-movable
    MemcpyWorkerPool(const MemcpyWorkerPool&) = delete;
    MemcpyWorkerPool& operator=(const MemcpyWorkerPool&) = delete;
    MemcpyWorkerPool(MemcpyWorkerPool&&) = delete;
    MemcpyWorkerPool& operator=(MemcpyWorkerPool&&) = delete;

    /**
     * @brief Submit a memcpy task for async execution
     * @param task The memcpy task to execute
     */
    void submitTask(MemcpyTask task);

   private:
    void workerThread();

    std::vector<std::thread> workers_;
    std::queue<MemcpyTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> shutdown_;
};

/**
 * @brief Fileread task for async execution
 */
struct FilereadTask {
    std::string file_path;
    size_t object_size;
    std::vector<Slice> slices;
    std::shared_ptr<FilereadOperationState> state;

    FilereadTask(const std::string& path, size_t size,
                 const std::vector<Slice>& slices_ref,
                 std::shared_ptr<FilereadOperationState> s)
        : file_path(path),
          object_size(size),
          slices(slices_ref),
          state(std::move(s)) {}
};

/**
 * @brief Thread pool for asynchronous memcpy operations
 *
 * This class manages a single worker thread that executes memcpy operations
 * asynchronously.
 */
class FilereadWorkerPool {
   public:
    explicit FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend);
    ~FilereadWorkerPool();

    // Non-copyable, non-movable
    FilereadWorkerPool(const FilereadWorkerPool&) = delete;
    FilereadWorkerPool& operator=(const FilereadWorkerPool&) = delete;
    FilereadWorkerPool(FilereadWorkerPool&&) = delete;
    FilereadWorkerPool& operator=(FilereadWorkerPool&&) = delete;

    /**
     * @brief Submit a memcpy task for async execution
     * @param task The memcpy task to execute
     */
    void submitTask(FilereadTask task);

   private:
    void workerThread();

    std::vector<std::thread> workers_;
    std::queue<FilereadTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> shutdown_;
    std::shared_ptr<StorageBackend> backend_;
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
                               std::shared_ptr<StorageBackend>& backend,
                               TransferMetric* transfer_metric = nullptr);

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
    std::optional<TransferFuture> submit(const Replica::Descriptor& replica,
                                         std::vector<Slice>& slices,
                                         TransferRequest::OpCode op_code);

    std::optional<TransferFuture> submit_batch(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        TransferRequest::OpCode op_code);

    std::optional<TransferFuture> submit_batch_get_offload_object(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& pointers,
        const std::unordered_map<std::string, Slice>& batched_slices);

   private:
    TransferEngine& engine_;
    std::unique_ptr<MemcpyWorkerPool> memcpy_pool_;
    std::unique_ptr<FilereadWorkerPool> fileread_pool_;
    bool memcpy_enabled_;
    TransferMetric* transfer_metric_;

    /**
     * @brief Select the optimal transfer strategy
     */
    TransferStrategy selectStrategy(const AllocatedBuffer::Descriptor& handle,
                                    const std::vector<Slice>& slices) const;

    /**
     * @brief Check if all handles refer to local segments
     */
    bool isLocalTransfer(const AllocatedBuffer::Descriptor& handle) const;

    /**
     * @brief Validate transfer parameters
     */
    bool validateTransferParams(const AllocatedBuffer::Descriptor& handle,
                                const std::vector<Slice>& slices) const;

    /**
     * @brief Submit memcpy operation asynchronously
     */
    std::optional<TransferFuture> submitMemcpyOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices,
        const TransferRequest::OpCode op_code);

    /**
     * @brief Submit transfer engine operation asynchronously
     */
    std::optional<TransferFuture> submitTransferEngineOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices,
        const TransferRequest::OpCode op_code);

    std::optional<TransferFuture> submitFileReadOperation(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        TransferRequest::OpCode op_code);

    /**
     * @brief Calculate total bytes for transfer operation and update metrics
     */
    void updateTransferMetrics(const std::vector<Slice>& slices,
                               TransferRequest::OpCode op);

    std::optional<TransferFuture> submitTransfer(
        std::vector<TransferRequest>& requests);
};

}  // namespace mooncake