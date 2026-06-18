#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>

#include "client_service.h"
#include "client_buffer.hpp"
#include "pinned_buffer_pool.h"
#include "storage_backend.h"

namespace mooncake {

struct SsdMetric;

class FileStorage {
   public:
    FileStorage(const FileStorageConfig& config, std::shared_ptr<Client> client,
                const std::string& local_rpc_addr,
                SsdMetric* ssd_metric = nullptr);
    ~FileStorage();

    tl::expected<void, ErrorCode> Init();

    /**
     * @brief Result of BatchGet operation containing batch_id and buffer
     * pointers.
     */
    struct BatchGetResult {
        uint64_t batch_id;
        std::vector<uint64_t> pointers;
    };

    /**
     * @brief Reads multiple key-value (KV) entries from local storage and
     * forwards them to a remote node.
     * @param keys                 List of keys to read from the local KV store
     * @param sizes                Expected size in bytes for each value
     * @return tl::expected<BatchGetResult, ErrorCode> containing batch_id and
     * buffer pointers.
     */
    tl::expected<BatchGetResult, ErrorCode> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    FileStorageConfig config_;

    /**
     * @brief Releases buffer associated with a specific batch_id.
     * Called by remote client after transfer completion.
     * @param batch_id The unique identifier of the batch to release
     * @return true if batch was found and released, false otherwise
     */
    bool ReleaseBuffer(uint64_t batch_id);

   private:
    friend class FileStorageTest;
    friend class FileStoragePromotionTest;

    struct PromotionExecutionResult {
        bool alloc_attempted = false;
        bool write_attempted = false;
        bool notify_success_attempted = false;
        bool notify_failure_attempted = false;
        bool completed = false;
        ErrorCode terminal_error = ErrorCode::OK;
    };

    struct AllocatedBatch {
        uint64_t batch_id;
        std::vector<BufferHandle> handles;
        std::unordered_map<std::string, Slice> slices;
        std::chrono::steady_clock::time_point lease_timeout;
        std::vector<uint64_t> pointers;
        uint64_t total_size;

        AllocatedBatch() : batch_id(0), total_size(0) {}
        AllocatedBatch(AllocatedBatch&&) = default;
        AllocatedBatch& operator=(AllocatedBatch&&) = default;

        AllocatedBatch(const AllocatedBatch&) = delete;
        AllocatedBatch& operator=(const AllocatedBatch&) = delete;

        ~AllocatedBatch() = default;
    };

    /**
     * @brief Offload object data and metadata.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> OffloadObjects(
        const std::vector<OffloadTaskItem>& offloading_objects);

    /**
     * @brief Performs a heartbeat operation for the FileStorage component.
     * 1. Sends object status (e.g., access frequency, size) to the master via
     * client.
     * 2. Receives feedback on which objects should be offloaded.
     * 3. Triggers asynchronous offloading of pending objects.
     * 4. Pulls and processes any pending L2->L1 promotion tasks queued by the
     *    master (mirror of step 1+2 in the reverse direction).
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Heartbeat();

    /**
     * @brief Drives the L2->L1 promotion pipeline for one heartbeat tick.

     * * Pulls promotion work from the master, stages a MEMORY replica for each

     * * key, copies the bytes from local SSD into that replica, and notifies
     * the
     * master on success. A failure on any single key is logged and
     * skipped;
     * FileStorage eagerly notifies the master to release the
     * promotion slot,
     * with the master-side reaper acting as the
     * long-stop on missed failures.
     *
     * @return tl::expected<void,
     * ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> ProcessPromotionTasks();

    PromotionExecutionResult ProcessPromotionTask(
        const PromotionTaskItem& task,
        const std::vector<std::string>& preferred_segments);

    bool EnqueuePromotionTask(const PromotionTaskItem& task,
                              bool allow_over_capacity_for_pulled_task = false);

    void ReleasePromotionTask(const std::string& key,
                              const std::string& tenant_id);

    void PromotionWorkerThreadFunc();

    tl::expected<bool, ErrorCode> IsEnableOffloading();

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batch_object);

    tl::expected<void, ErrorCode> BatchQuerySegmentSlices(
        const std::vector<std::string>& keys, const std::string& tenant_id,
        std::unordered_map<std::string, std::vector<Slice>>& batched_slices);

    tl::expected<void, ErrorCode> RegisterLocalMemory();

    tl::expected<std::shared_ptr<AllocatedBatch>, ErrorCode> AllocateBatch(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    void ClientBufferGCThreadFunc();

    /**
     * @brief Re-registers all offloaded objects with the master.
     * Called after master restart recovery to sync SSD object metadata.
     * This is the same logic as the ScanMeta step in Init().
     */
    tl::expected<void, ErrorCode> ReRegisterOffloadedObjects();

    std::shared_ptr<Client> client_;
    SsdMetric* ssd_metric_{nullptr};
    std::string local_rpc_addr_;
    // Pinned host memory pool for GPU D2H staging in OffloadObjects
    std::unique_ptr<PinnedBufferPool> pinned_buffer_pool_;
    std::shared_ptr<StorageBackendInterface> storage_backend_;
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_;
    mutable Mutex client_buffer_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<AllocatedBatch>> GUARDED_BY(
        client_buffer_mutex_) client_buffer_allocated_batches_;
    std::atomic<uint64_t> next_batch_id_{1};

    mutable Mutex offloading_mutex_;
    bool GUARDED_BY(offloading_mutex_) enable_offloading_;
    std::atomic<bool> heartbeat_running_;
    std::thread heartbeat_thread_;
    std::atomic<bool> client_buffer_gc_running_;
    std::thread client_buffer_gc_thread_;
    std::atomic<bool> promotion_workers_running_{false};
    std::vector<std::thread> promotion_worker_threads_;
    std::mutex promotion_queue_mutex_;
    std::condition_variable promotion_queue_cv_;
    std::deque<PromotionTaskItem> promotion_task_queue_;
    std::future<void> rescan_future_;
    std::atomic<bool> metadata_resync_pending_{false};
};

}  // namespace mooncake
