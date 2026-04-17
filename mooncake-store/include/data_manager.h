#pragma once

#include <array>
#include <shared_mutex>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <optional>
#include <ylt/util/tl/expected.hpp>
#include "async_memcpy_executor.h"
#include "client_buffer.hpp"
#include "client_config_builder.h"
#include "task_handle.h"
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"
#include "client_rpc_types.h"
#include "client_config_builder.h"

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
    size_t local_memcpy_async_worker_num = 32;
    size_t local_memcpy_async_queue_depth = 2048;
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

    void Stop() {
        if (async_memcpy_executor_) {
            async_memcpy_executor_->Shutdown();
        }
        if (tiered_backend_) {
            tiered_backend_->Stop();
        }
    }

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
    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> Put(
        const std::string& key, std::vector<Slice>& slices);

    // Attention!!!
    // Get() method run without key lock.
    // It works based on two assumptions:
    // 1. We assume that each key will not be updated after they are created.
    // 2. The key is acquired by handle which protect the data accessibility
    //    based on ref count. Once the method acquire handle successfully, the
    //    accessor can safely access the data until the handle is released.
    tl::expected<ReadTaskHandle, ErrorCode> Get(
        const std::string& key, const std::vector<Slice>& slices);

    tl::expected<ReadTaskHandle, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator);

    tl::expected<void, ErrorCode> Delete(
        const std::string& key, std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Get tier views from underlying tiered storage
     */
    std::vector<TierView> GetTierViews() const;

    // ================================================================
    // Remote data transfer — called by RPC service layer
    // ================================================================

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
        const std::string& key,
        const std::vector<RemoteBufferDesc>& dest_buffers);

    /**
     * @brief Write data from remote source buffers
     * @param key Object key to write
     * @param src_buffers Source buffers on remote client (Client A)
     * @param tier_id Optional tier ID (nullopt = use default tier selection)
     * @return UUID of the tier (segment) where data was written, or ErrorCode
     */
    tl::expected<UUID, ErrorCode> WriteRemoteData(
        const std::string& key,
        const std::vector<RemoteBufferDesc>& src_buffers,
        std::optional<UUID> tier_id = std::nullopt);

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
    void RectifyReadRoute(const std::string& key,
                          std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Set the callback for rectifying read routes in Master.
     * @param fn Callback invoked when key not found locally.
     */
    void SetRectifyCallback(
        std::function<void(const std::string&, std::optional<UUID>)> fn);

    bool Exist(const std::string& key,
               std::optional<UUID> tier_id = std::nullopt) const;

   private:
    std::shared_mutex& GetKeyLock(const std::string& key) {
        size_t hash = std::hash<std::string>{}(key);
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
        const AllocationHandle& handle, const std::string& key,
        const std::vector<Slice>& slices);

    tl::expected<ReadTaskHandle, ErrorCode> BuildDataCopierViaTe(
        const AllocationHandle& handle, const std::vector<Slice>& slices);

    tl::expected<ReadTaskHandle, ErrorCode> BuildDataCopierViaMemcpy(
        const AllocationHandle& handle, const std::string& key,
        const std::vector<Slice>& slices);

    tl::expected<LocalCopyPlan, ErrorCode> BuildLocalCopyPlan(
        const std::string& key, const AllocationHandle& handle,
        const std::vector<Slice>& slices) const;
    // --- Put dispatch by transfer mode ---

    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> PutViaTe(
        const std::string& key, std::vector<Slice>& slices);

    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> PutViaMemcpy(
        const std::string& key, std::vector<Slice>& slices);

    // --- Conversion helpers ---

    std::vector<RemoteBufferDesc> SlicesToRemoteBufferDescs(
        const std::vector<Slice>& slices) const;

    // --- TE transfer helpers ---
    struct TeSubmitResult {
        std::vector<std::tuple<BatchID, size_t, std::string>> transfer_batches;
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

   private:
    std::unique_ptr<TieredBackend> tiered_backend_;    // Owned by DataManager
    std::shared_ptr<TransferEngine> transfer_engine_;  // Shared with Client

    // Sharded locks for concurrent access
    // Configurable via MOONCAKE_DM_LOCK_SHARD_COUNT environment variable
    // (default: 1024)
    size_t lock_shard_count_;
    std::vector<std::shared_mutex> lock_shards_;

    // Callback for rectifying stale read routes
    std::function<void(const std::string&, std::optional<UUID>)>
        rectify_wrong_route_fn_;

    LocalTransferConfig local_transfer_config_;
    std::unique_ptr<AsyncMemcpyExecutor> async_memcpy_executor_;
};

}  // namespace mooncake
