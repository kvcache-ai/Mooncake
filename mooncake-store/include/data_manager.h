#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <ylt/util/tl/expected.hpp>
#include "async_memcpy_executor.h"
#include "client_buffer.hpp"
#include "client_config_builder.h"
#include "task_handle.h"
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"
#include "client_rpc_types.h"

namespace mooncake {

/**
 * @struct ReadTaskHandle
 * @brief Handle for a read operation
 */
struct ReadTaskHandle {
    std::unique_ptr<TaskHandle<void>> task_handle;
    int64_t data_size;

    // if user use zero-copy get(), the var is useless;
    // if user provides allocator, the var is the buffer allocated by allocator;
    std::shared_ptr<BufferHandle> read_buf;
};

/**
 * @struct LocalTransferConfig
 * @brief Configuration for local data transfer operations
 */
struct LocalTransferConfig {
    LocalTransferMode mode = LocalTransferMode::TE;

    // When mode == TE, the following parameters are used:
    std::string te_endpoint;

    // When mode == MEMCPY, the following parameters are used:
    // 0 means forbid async memcpy (fall back to synchronous).
    size_t local_memcpy_async_worker_num = 32;
};

/**
 * @class DataManager
 * @brief Manages data access operations using TieredBackend and TransferEngine
 *
 * Provides unified interface for local and remote data operations, handling
 * tiered storage access and zero-copy transfers.
 */
class DataManager {
    // Allow test class to access private methods for testing
    friend class DataManagerTest;

   public:
    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    struct PreWriteResult {
        RemoteBufferDesc remote_buffer;
        uint64_t deadline_ms = 0;
        UUID pending_write_token{0, 0};
    };

    struct PinKeyResult {
        RemoteBufferDesc remote_buffer;
        uint64_t deadline_ms = 0;
        UUID pin_token{0, 0};
    };

    /**
     * @brief Constructor
     * @param tiered_backend Unique pointer to TieredBackend instance (takes
     * ownership)
     * @param transfer_engine Shared pointer to TransferEngine instance (shared
     * with Client)
     */
    DataManager(std::unique_ptr<TieredBackend> tiered_backend,
                std::shared_ptr<TransferEngine> transfer_engine,
                size_t lock_shard_count = 1024,
                const LocalTransferConfig& local_transfer_config = {});

    ~DataManager();

    void Stop();

    /**
     * @brief Cleanup: delegates to TieredBackend::Destroy().
     */
    void Destroy() {
        if (tiered_backend_) {
            tiered_backend_->Destroy();
        }
    }

    // ================================================================
    // Public local read/write interface
    // Internally selects TE or Memcpy path based on config.
    // ================================================================

    // The Put operation consists of three phases:
    // 1. Allocation: allocate memory from the tiered backend for the data
    // 2. Write: write the data to the allocated memory
    // 3. Commit: commit the data to the tiered backend
    //
    // IMPORTANT: The caller must keep the memory referenced by `slices` alive
    // from the time Put() returns until TaskHandle::Wait() completes. The
    // returned TaskHandle may capture raw pointers from the slices for
    // asynchronous transfer.
    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> Put(
        std::string_view key, std::vector<Slice>& slices);

    // Attention!!!
    // Get() method run without key lock.
    // It works based on two assumptions:
    // 1. We assume that each key will not be updated after they are created.
    // 2. The key is acquired by handle which protect the data accessibility
    //    based on ref count. Once the method acquire handle successfully, the
    //    accessor can safely access the data until the handle is released.
    //
    // IMPORTANT: The caller must keep the memory referenced by `slices` alive
    // from the time Get() returns until TaskHandle::Wait() completes. The
    // returned TaskHandle may capture raw pointers from the slices for
    // asynchronous data copy.
    tl::expected<ReadTaskHandle, ErrorCode> Get(
        std::string_view key, const std::vector<Slice>& slices);

    tl::expected<ReadTaskHandle, ErrorCode> Get(
        std::string_view key, std::shared_ptr<ClientBufferAllocator> allocator);

    /**
     * @brief Query the size of an object.
     * @param key Object key
     * @return Object size in bytes, or ErrorCode on failure
     */
    tl::expected<size_t, ErrorCode> QueryObjectSize(std::string_view key);

    tl::expected<void, ErrorCode> Delete(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt,
        bool notify_master = true);

    /**
     * @brief Get tier views from underlying tiered storage
     */
    std::vector<TierView> GetTierViews() const;

    // ================================================================
    // Remote data transfer — called by RPC service layer
    // ================================================================

    /**
     * @brief Iterate all keys in batches.
     * Delegates to TieredBackend::ForEachKeyBatch().
     */
    void ForEachKeyBatch(
        const std::function<bool(std::vector<ReplicaLocation>&&)>& callback)
        const;

    /**
     * @brief Get hot key statistics
     */
    AccessStats GetHotKeyStats() const;

    /**
     * @brief Get all tier IDs where a key has replicas.
     */
    std::vector<UUID> GetReplicaTierIds(std::string_view key) const;

    /**
     * @brief Read data and transfer to remote destination buffers
     *
     * This is the core method for remote data access:
     * 1. Get data handle from TieredBackend
     * 2. Use TransferEngine to transfer data via RDMA to destination buffers
     *
     * @param key Object key to read
     * @param dest_buffers Destination buffers on remote client (Client A)
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> ReadRemoteData(
        std::string_view key,
        const std::vector<RemoteBufferDesc>& dest_buffers);

    /**
     * @brief Write data from remote source buffers
     * @param key Object key to write
     * @param src_buffers Source buffers on remote client (Client A)
     * @param tier_id Optional tier ID (nullopt = use default tier selection)
     * @return UUID of the tier (segment) where data was written, or ErrorCode
     */
    tl::expected<UUID, ErrorCode> WriteRemoteData(
        std::string_view key, const std::vector<RemoteBufferDesc>& src_buffers,
        std::optional<UUID> tier_id = std::nullopt);

    tl::expected<PreWriteResult, ErrorCode> PreWrite(
        std::string_view key, size_t size_bytes,
        std::optional<UUID> tier_id = std::nullopt);

    tl::expected<void, ErrorCode> WriteCommit(
        std::string_view key, const UUID& pending_write_token);

    tl::expected<PinKeyResult, ErrorCode> PinKey(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt);

    tl::expected<void, ErrorCode> UnPinKey(std::string_view key,
                                           const UUID& pin_token);

    // ================================================================
    // Utilities
    // ================================================================

    /**
     * @brief Rectify stale read route by checking local key existence
     * and removing replica from master if key is not found locally.
     *
     * @param key Object key to rectify
     * @param tier_id Optional tier ID. If specified, only checks the given
     *        tier; if nullopt, checks all tiers.
     */
    void RectifyReadRoute(std::string_view key,
                          std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Set the callback for rectifying read routes in Master.
     * @param fn Callback invoked when key not found locally.
     */
    void SetRectifyCallback(
        std::function<void(std::string_view, std::optional<UUID>)> fn);

    bool Exist(std::string_view key,
               std::optional<UUID> tier_id = std::nullopt) const;

   private:
    void ClearLeaseRecords();

    // Forward declarations for nested shard structs used by internal helpers.
    struct PendingWriteShard;
    struct PinnedKeyShard;

    struct KeyCtx {
        std::string_view key;
        std::string key_string;
        size_t hash = 0;
        size_t pending_write_shard_idx = 0;
        size_t pinned_key_shard_idx = 0;
    };

    KeyCtx BuildKeyCtx(std::string_view key) const;
    PendingWriteShard& GetPendingWriteShard(const KeyCtx& ctx);
    PinnedKeyShard& GetPinnedKeyShard(const KeyCtx& ctx);

    tl::expected<PreWriteResult, ErrorCode> PreWriteInternal(
        const KeyCtx& ctx, size_t size_bytes, std::optional<UUID> tier_id);
    tl::expected<void, ErrorCode> WriteCommitInternal(const KeyCtx& ctx,
                                                      const UUID& pending_write_token);
    tl::expected<PinKeyResult, ErrorCode> PinKeyInternal(
        const KeyCtx& ctx, std::optional<UUID> tier_id);
    tl::expected<void, ErrorCode> UnPinKeyInternal(const KeyCtx& ctx,
                                                   const UUID& pin_token);

    tl::expected<AllocationHandle, ErrorCode> LookupPendingWriteHandleInternal(
        const KeyCtx& ctx, const UUID& pending_write_token);
    tl::expected<AllocationHandle, ErrorCode> LookupPinnedKeyHandleInternal(
        const KeyCtx& ctx, const UUID& pin_token);
    void AbortPendingWriteInternal(const KeyCtx& ctx,
                                   const UUID& pending_write_token);

    std::shared_mutex& GetKeyLock(std::string_view key) {
        size_t hash = std::hash<std::string_view>{}(key);
        return lock_shards_[hash % lock_shard_count_];
    }

    /**
     * @brief Transfer data from local source to remote destination buffers
     * @param handle Local allocation handle (source)
     * @param dest_buffers Remote destination buffers
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> TransferDataToRemote(
        AllocationHandle handle,
        const std::vector<RemoteBufferDesc>& dest_buffers);

    /**
     * @brief Transfer data from remote source buffers to local allocated space
     * @param handle Local allocation handle (destination)
     * @param src_buffers Remote source buffers
     * @return ErrorCode indicating success or failure. Any segment failure
     *         will result in error (no partial success).
     */
    tl::expected<void, ErrorCode> TransferDataFromRemote(
        AllocationHandle handle,
        const std::vector<RemoteBufferDesc>& src_buffers);

    tl::expected<ReadTaskHandle, ErrorCode> BuildDataCopier(
        const AllocationHandle& handle, std::string_view key,
        const std::vector<Slice>& slices);

    tl::expected<ReadTaskHandle, ErrorCode> BuildDataCopierViaTe(
        const AllocationHandle& handle, const std::vector<Slice>& slices);

    tl::expected<ReadTaskHandle, ErrorCode> BuildDataCopierViaMemcpy(
        const AllocationHandle& handle, std::string_view key,
        const std::vector<Slice>& slices);

    // --- Put dispatch by transfer mode ---
    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> PutViaTe(
        std::string_view key, std::vector<Slice>& slices);

    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> PutViaMemcpy(
        std::string_view key, std::vector<Slice>& slices);

    // --- Conversion helpers ---

    std::vector<RemoteBufferDesc> SlicesToRemoteBufferDescs(
        const std::vector<Slice>& slices) const;

    // --- TE transfer helpers ---
    struct TeSubmitResult {
        std::vector<std::tuple<Transport::BatchID, size_t, std::string>>
            transfer_batches;
        // Temp DRAM buffer used when source or destination is non-DRAM.
        // Non-null means a post-copy (for Write) or pre-copy (for Read) is
        // required.
        std::shared_ptr<void> temp_buffer;
        AllocationHandle handle;  // Ensure local memory is not released
    };

    tl::expected<TeSubmitResult, ErrorCode> SubmitTeTransferInternal(
        const AllocationHandle& handle,
        const std::vector<RemoteBufferDesc>& remote_buffers,
        Transport::TransferRequest::OpCode opcode);

    /**
     * @brief Helper to wait for a transfer batch to complete
     * @param batch_id Batch ID to poll
     * @param num_tasks Number of tasks in the batch
     * @param segment_name Name of the segment for logging
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> WaitTransferBatch(
        Transport::BatchID batch_id, size_t num_tasks,
        const std::string& segment_endpoint);

    /**
     * @brief Validate remote buffer descriptors
     * @param buffers Buffer descriptors to validate
     * @return ErrorCode if validation fails, otherwise OK
     */
    tl::expected<void, ErrorCode> ValidateRemoteBuffers(
        const std::vector<RemoteBufferDesc>& buffers);

    /**
     * @brief Prepare DRAM buffer for transfer from non-DRAM source
     * @param source_ptr Source data pointer
     * @param source_type Source memory type
     * @param total_size Total data size
     * @param backend TieredBackend for DataCopier access
     * @return Pair of (transfer_source_ptr, temp_buffer_owner) or error
     */
    tl::expected<std::pair<void*, std::unique_ptr<void, void (*)(void*)>>,
                 ErrorCode>
    PrepareDRAMTransferBuffer(void* source_ptr, MemoryType source_type,
                              size_t total_size, TieredBackend* backend);

    /**
     * @brief Prepare DRAM buffer for receiving data to non-DRAM destination
     * @param dest_ptr Destination data pointer
     * @param dest_type Destination memory type
     * @param total_size Total data size
     * @return Pair of (transfer_dest_ptr, temp_buffer_owner) or error
     */
    tl::expected<std::pair<void*, std::unique_ptr<void, void (*)(void*)>>,
                 ErrorCode>
    PrepareDRAMReceiveBuffer(void* dest_ptr, MemoryType dest_type,
                             size_t total_size);

    /**
     * @brief Copy data from DRAM buffer to non-DRAM tier
     * @param temp_buffer Temp DRAM buffer pointer
     * @param dest_ptr Destination pointer
     * @param dest_type Destination memory type
     * @param total_size Total data size
     * @param backend TieredBackend for DataCopier access
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> CopyFromDRAMBuffer(void* temp_buffer,
                                                     void* dest_ptr,
                                                     MemoryType dest_type,
                                                     size_t total_size,
                                                     TieredBackend* backend);

    /**
     * @brief Submit transfer requests for a segment (without waiting)
     * @param segment_name Segment name (for logging)
     * @param seg Segment handle (already opened)
     * @param requests Transfer requests to submit
     * @return BatchID if successful, or error
     */
    tl::expected<Transport::BatchID, ErrorCode> SubmitTransferRequests(
        const std::string& segment_endpoint, Transport::SegmentHandle seg,
        const std::vector<Transport::TransferRequest>& requests);

    /**
     * @brief Wait for multiple transfer batches to complete
     * @param batches Vector of (batch_id, num_tasks, segment_endpoint) tuples
     * @return ErrorCode indicating success or failure. If any batch fails,
     *         remaining batch IDs are freed and error is returned immediately.
     */
    tl::expected<void, ErrorCode> WaitAllTransferBatches(
        const std::vector<std::tuple<Transport::BatchID, size_t, std::string>>&
            batches);

    // Wait for all tasks to reach a terminal state, then free the batch.
    void CancelBatchTETask(Transport::BatchID batch_id, size_t num_tasks);

    using OrderedDeadlineList = std::list<std::pair<std::string, TimePoint>>;
    using OrderedDeadlineListIt = OrderedDeadlineList::iterator;

    struct PendingWriteRecord {
        UUID pending_write_token{0, 0};
        TimePoint deadline{};
        AllocationHandle handle;
        OrderedDeadlineListIt list_it;
    };

    struct PinnedKeyRecord {
        UUID pin_token{0, 0};
        TimePoint deadline{};
        AllocationHandle handle;
        uint32_t ref_count = 1;
        OrderedDeadlineListIt list_it;
    };

    struct PendingWriteShard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, PendingWriteRecord> by_key;
        OrderedDeadlineList ordered_list;
    };

    struct PinnedKeyShard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, PinnedKeyRecord> by_key;
        OrderedDeadlineList ordered_list;
    };

    size_t HashKey(std::string_view key) const;
    PendingWriteShard& GetPendingWriteShard(std::string_view key);
    PinnedKeyShard& GetPinnedKeyShard(std::string_view key);
    const std::chrono::milliseconds& lease_duration() const {
        return lease_duration_;
    }
    uint64_t TimePointToDeadlineMs(TimePoint deadline) const;
    TimePoint DeadlineMsToTimePoint(uint64_t deadline_ms) const;
    bool IsExpired(TimePoint deadline) const;
    RemoteBufferDesc BuildRemoteBufferDesc(const AllocationHandle& handle) const;
    void LeaseScannerMain();
    void ShutdownLeaseScanner();
    size_t ScanExpiredPendingWrites(PendingWriteShard& shard, TimePoint now);
    size_t ScanExpiredPinnedKeys(PinnedKeyShard& shard, TimePoint now);
    bool ErasePendingWriteLocked(PendingWriteShard& shard,
                                 const std::string& key);
    bool ErasePinnedKeyLocked(PinnedKeyShard& shard, const std::string& key);
    bool RemoveExpiredPendingWriteLocked(PendingWriteShard& shard,
                                         const std::string& key,
                                         TimePoint now);
    bool RemoveExpiredPinnedKeyLocked(PinnedKeyShard& shard,
                                      const std::string& key, TimePoint now);
    void TouchOrderedDeadlineNode(OrderedDeadlineList& ordered_list,
                                  OrderedDeadlineListIt it,
                                  const std::string& key, TimePoint deadline);

    tl::expected<AllocationHandle, ErrorCode> LookupPendingWriteHandle(
        std::string_view key, const UUID& pending_write_token);
    tl::expected<AllocationHandle, ErrorCode> LookupPinnedKeyHandle(
        std::string_view key, const UUID& pin_token);
    void AbortPendingWrite(std::string_view key, const UUID& pending_write_token);

   private:
    std::unique_ptr<TieredBackend> tiered_backend_;    // Owned by DataManager
    std::shared_ptr<TransferEngine> transfer_engine_;  // Shared with Client

    // Sharded locks for concurrent access
    // Configurable via MOONCAKE_DM_LOCK_SHARD_COUNT environment variable
    // (default: 1024)
    size_t lock_shard_count_;
    std::vector<std::shared_mutex> lock_shards_;
    std::vector<PendingWriteShard> pending_write_shards_;
    std::vector<PinnedKeyShard> pinned_key_shards_;

    // Callback for rectifying stale read routes
    std::function<void(std::string_view, std::optional<UUID>)>
        rectify_wrong_route_fn_;

    LocalTransferConfig local_transfer_config_;
    std::unique_ptr<AsyncMemcpyExecutor> async_memcpy_executor_;
    std::chrono::milliseconds lease_duration_;
    std::chrono::milliseconds lease_scan_interval_;
    std::atomic<bool> lease_scanner_stop_requested_{false};
    std::condition_variable lease_scanner_cv_;
    std::mutex lease_scanner_mutex_;
    std::thread lease_scanner_thread_;
};

}  // namespace mooncake
