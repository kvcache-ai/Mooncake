#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <stack>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "types.h"
#include "replica.h"
#include "storage_backend.h"
#include "client_metric.h"
#include "nof/nvmeof_initiator.h"

namespace mooncake {

/**
 * @brief Transfer strategy enumeration
 */
enum class TransferStrategy {
    LOCAL_MEMCPY = 0,     // Local memory copy using memcpy
    TRANSFER_ENGINE = 1,  // Remote transfer using transfer engine
    FILE_READ = 2,        // File read operation
    EMPTY = 3,
    SPDK_NVMF = 4  // Spdk nvmf operation
};

enum class OffloadBufferAccess {
    kTransferEngine,
    kLocalAddress,
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
        case TransferStrategy::SPDK_NVMF:
            return os << "SPDK_NVMF";
        case TransferStrategy::FILE_READ:
            return os << "FILE_READ";
        case TransferStrategy::EMPTY:
            return os << "EMPTY";
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

/**
 * @brief Operation state for NoF (NVMe-oF) transfers
 */
class NofOperationState : public OperationState {
   public:
    bool is_completed() override {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.has_value();
    }

    void set_completed(ErrorCode error_code, std::string error_detail = "") {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!result_.has_value());
            result_.emplace(error_code);
            error_detail_ = std::move(error_detail);
        }
        cv_.notify_all();
    }

    // Data-plane error detail (sc/sct/status string), empty on success.
    // Surfaced to logs today; available here for future API plumbing.
    [[nodiscard]] std::string error_detail() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return error_detail_;
    }

    void wait_for_completion() override {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return result_.has_value(); });
    }

    TransferStrategy get_strategy() const override {
        return TransferStrategy::SPDK_NVMF;
    }

   private:
    std::string error_detail_;
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
        : engine_(engine),
          batch_id_(batch_id),
          batch_size_(batch_size),
          start_ts_(getCurrentTimeInMilli()) {}

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
    const int64_t start_ts_;
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

struct NofQos;

/**
 * @brief NoF operation descriptor (byte-oriented, opaque handle)
 */
struct NofTask {
    NofSegmentHandle* seg_handle;
    void* ptr;
    uint64_t byte_offset;
    uint64_t byte_length;
    uint64_t remaining_bytes;
    int outstanding_sub_io;
    NofIOOp op;
    int idx;  // subop idx
    bool failed;
    bool on_chain;
    std::shared_ptr<NofOperationState> state;
    // First failure's error detail (sc/sct/status string); written only on
    // the failure path, empty in steady state — zero hot-path cost.
    std::string error_detail;
    // Formerly a raw pointer to a worker-thread stack variable (C2).
    // Now shared ownership: worker holds one ref, each task holds one.
    std::shared_ptr<std::atomic<int64_t>> io_count;
    NofQos* nof_qos;
    NofTask* nxt;

    NofTask(NofSegmentHandle* handle, void* buf, uint64_t byte_off,
            uint64_t byte_len, NofIOOp op_code,
            std::shared_ptr<NofOperationState> s)
        : seg_handle(handle),
          ptr(buf),
          byte_offset(byte_off),
          byte_length(byte_len),
          remaining_bytes(byte_len),
          outstanding_sub_io(0),
          op(op_code),
          idx(0),
          failed(false),
          on_chain(false),
          state(std::move(s)),
          io_count(nullptr),
          nof_qos(nullptr),
          nxt(nullptr) {}
};

struct NofSubTask {
    // Two-slot adaptor embedded in pooled sub-task storage: steady-state
    // submit/completion performs no per-sub-IO heap allocation (RFC §5.5).
    NofIOAdaptor adaptor;
    NofTask* task;
    uint32_t submit_bytes;
    std::stack<NofSubTask*>* sub_task_pool;
};

constexpr int kDefaultNofSubmitChunkBytes = (1 << 17);    // 128k
constexpr int kDefaultNofInflightBytesLimit = (1 << 25);  // 32M

struct NofQos {
    // Block size cached at QoS creation — removes the per-poll-loop
    // GetBlockSize query that exists today (transfer_task.cpp:487-488).
    const uint32_t block_size;
    const int blocks_per_chunk;
    const int inflight_blocks_limit;
    int inflight_blocks[static_cast<size_t>(NofIOOp::kNum)];
    NofTask* head[static_cast<size_t>(NofIOOp::kNum)];
    NofTask* tail[static_cast<size_t>(NofIOOp::kNum)];

    explicit NofQos(uint32_t bs);

    bool Empty() const {
        return (head[static_cast<size_t>(NofIOOp::kRead)] == nullptr &&
                head[static_cast<size_t>(NofIOOp::kWrite)] == nullptr);
    }

    void PushTask(NofTask* task) {
        size_t op = static_cast<size_t>(task->op);
        if (head[op] == nullptr) {
            head[op] = task;
            tail[op] = task;
        } else {
            tail[op]->nxt = task;
            tail[op] = task;
        }
    }

    void PopTask(NofIOOp op) {
        size_t i = static_cast<size_t>(op);
        if (head[i]) {
            head[i] = head[i]->nxt;
        }
    }
};

constexpr int kDefaultNofWorkers = 4;
class NofWorkerPool {
   public:
    explicit NofWorkerPool(std::shared_ptr<NVMeoFInitiator> initiator,
                           int numa_socket_id = 0);
    ~NofWorkerPool();

    NofWorkerPool(const NofWorkerPool&) = delete;
    NofWorkerPool& operator=(const NofWorkerPool&) = delete;
    NofWorkerPool(NofWorkerPool&&) = delete;
    NofWorkerPool& operator=(NofWorkerPool&&) = delete;

    void submitTask(NofTask task);

   private:
    void workerThread(int work_idx);

    std::shared_ptr<NVMeoFInitiator> initiator_;
    int worker_count_;
    int numa_socket_id_;
    std::vector<std::thread> workers_;
    std::unique_ptr<std::queue<NofTask>[]> task_queue_;
    std::unique_ptr<std::mutex[]> queue_mutex_;
    std::unique_ptr<std::condition_variable[]> queue_cv_;
    std::atomic<bool> shutdown_;
    std::mutex seg_mutex_;
    int seg_num = 0;
    std::map<NofSegmentHandle*, int> seg_to_worker_;
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
    explicit TransferSubmitter(
        TransferEngine& engine, std::shared_ptr<StorageBackend>& backend,
        const std::string& local_hostname,
        TransferMetric* transfer_metric = nullptr, int numa_socket_id = 0,
        std::shared_ptr<NVMeoFInitiator> nof_initiator = nullptr);

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
                                         TransferRequest::OpCode op_code,
                                         void* ptr = nullptr, size_t size = 0);

    /**
     * @brief Submit a range read: read [src_offset, src_offset+size) from
     * object into slice.ptr. Slices must total exactly `size` bytes.
     */
    std::optional<TransferFuture> submitRangeRead(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        uint64_t src_offset);

    std::optional<TransferFuture> submitRangeWrite(
        const Replica::Descriptor& replica, std::vector<Slice>& slices,
        uint64_t dst_offset);

    TransferEngine::ScatterTransferOperation submitScatter(
        const std::vector<TransferEngine::ScatterTransferRange>& transfers);

    std::optional<TransferFuture> submit_batch(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        TransferRequest::OpCode op_code);

    std::optional<TransferFuture> submit_batch_get_offload_object(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& pointers,
        const std::unordered_map<std::string, std::vector<Slice>>&
            batched_slices,
        OffloadBufferAccess buffer_access);

    [[nodiscard]] bool canUseLocalMemcpy(const std::string& endpoint) const;

    /**
     * @brief Pure comparison helper: returns true iff both endpoints are
     * non-empty and identical. Exposed for unit testing of the locality
     * decision without instantiating a full TransferEngine.
     *
     * Two endpoints identify the same process only when their ip:port (or
     * full hostname) match exactly; same-host different-process pairs share
     * an IP but not a port and must NOT be treated as locally addressable.
     */
    static bool isSameProcessEndpoint(const std::string& handle_endpoint,
                                      const std::string& local_endpoint);

   private:
    TransferEngine& engine_;
    // Cached at construction: the local transport endpoint never changes for
    // the lifetime of the TransferSubmitter, so we avoid calling
    // engine_.getLocalIpAndPort() (which allocates a string) on every transfer.
    const std::string local_endpoint_;
    std::unique_ptr<MemcpyWorkerPool> memcpy_pool_;
    std::shared_ptr<NVMeoFInitiator> nof_initiator_;
    std::unique_ptr<NofWorkerPool> nof_pool_;
    std::unique_ptr<FilereadWorkerPool> fileread_pool_;
    bool memcpy_enabled_;
    const std::string local_hostname_;
    TransferMetric* transfer_metric_;

    /**
     * @brief Select the optimal transfer strategy
     */
    TransferStrategy selectStrategy(const AllocatedBuffer::Descriptor& handle,
                                    const std::vector<Slice>& slices) const;

    /**
     * @brief Validate transfer parameters
     */
    bool validateTransferParams(const AllocatedBuffer::Descriptor& handle,
                                const std::vector<Slice>& slices) const;

    void appendMemcpyOperations(const AllocatedBuffer::Descriptor& handle,
                                const std::vector<Slice>& slices,
                                TransferRequest::OpCode op_code,
                                uint64_t buffer_offset,
                                std::vector<MemcpyOperation>& operations);

    /**
     * @brief Submit memcpy operation asynchronously
     */
    std::optional<TransferFuture> submitMemcpyOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, const TransferRequest::OpCode op_code,
        uint64_t src_offset = 0);

    std::optional<TransferFuture> submitMemcpyOperations(
        std::vector<MemcpyOperation> operations);

    /**
     * @brief Submit NoF (NVMe-oF) operation asynchronously
     */
    std::optional<TransferFuture> submitNofOperation(
        const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
        const TransferRequest::OpCode op_code);

    /**
     * @brief Submit transfer engine operation asynchronously
     * @param src_offset Optional offset in source buffer (default 0)
     */
    std::optional<TransferFuture> submitTransferEngineOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, const TransferRequest::OpCode op_code,
        uint64_t src_offset = 0);

    std::optional<TransferFuture> submitMemoryReadOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, uint64_t src_offset);

    std::optional<TransferFuture> submitMemoryWriteOperation(
        const AllocatedBuffer::Descriptor& handle,
        const std::vector<Slice>& slices, uint64_t dst_offset);

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
