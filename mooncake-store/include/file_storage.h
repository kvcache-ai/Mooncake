#pragma once

#include <unordered_set>

#include "client_service.h"
#include "client_buffer.h"
#include "storage_backend.h"
#include "pinned_buffer_pool.h"

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
     * 4. If offload work was returned, pulls and processes any pending L2->L1
     *    promotion tasks queued by the master (mirror of step 1+2 in the
     *    reverse direction).
     * 5. Runs proactive local-disk watermark eviction.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Heartbeat();

    /**
     * @brief Drives the L2->L1 promotion pipeline for one heartbeat tick.
     * Pulls promotion work from the master, stages a MEMORY replica for each
     * key, copies the bytes from local SSD into that replica, and notifies the
     * master on success. A failure on any single key is logged and skipped;
     * the master-side reaper decrements the source replica's refcnt and
     * erases the task entry on TTL expiry, and any orphaned PROCESSING
     * MEMORY replica is reaped via the standard discarded-replicas path.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> ProcessPromotionTasks();

    tl::expected<bool, ErrorCode> IsEnableOffloading();

    tl::expected<void, ErrorCode> RunDiskWatermarkEviction();

    tl::expected<void, ErrorCode> NotifyEvictedDiskReplicas(
        const std::vector<std::string>& evicted_keys);

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

    /**
     * @brief Partition drained-but-unbucketed offload tasks into NACKs vs
     * carries (pure function; extracted for direct unit testing).
     *
     * A key absent from all_bucket_keys was either deliberately skipped by
     * the backend (size over bucket limit / already on disk) — those must be
     * NACKed so the master releases the task and source-replica refcount — or
     * DEFERRED to the backend's ungrouped pool to be written as part of a
     * full bucket next cycle. Deferred keys must NOT be NACKed: the backend
     * carries only key+size across the deferral, so their tasks must be
     * carried here or the re-emitted keys arrive task-less and are silently
     * dropped (upstream #3006 — measured as ~12% of pool entries never
     * reaching the SSD tier).
     *
     * Previously carried keys must also be RETAINED while un-emitted: the
     * backend's pool only moves on a call with non-empty input, so a cycle
     * can pass without the pooled keys being either emitted or re-reported
     * (empty effective input after the carry merge). Rebuilding the carry
     * from deferred_keys alone would NACK such keys while the pool still
     * holds them — re-introducing the task-less drop.
     *
     * @param task_by_storage_key All tasks known this cycle (drained +
     * carried).
     * @param all_bucket_keys Storage keys emitted in a bucket this cycle.
     * @param deferred_keys Storage keys the backend deferred this cycle.
     * @param previously_carried Carry map from the previous cycle; entries
     * not emitted this cycle are retained in carried_tasks.
     * @param failed_tasks Output: tasks to NACK (appended).
     * @param carried_tasks Output: tasks to carry to the next cycle.
     */
    static void PartitionUnbucketedTasks(
        const std::unordered_map<std::string, OffloadTaskItem>&
            task_by_storage_key,
        const std::unordered_set<std::string>& all_bucket_keys,
        const std::vector<std::string>& deferred_keys,
        const std::unordered_map<std::string, OffloadTaskItem>&
            previously_carried,
        std::vector<OffloadTaskItem>& failed_tasks,
        std::unordered_map<std::string, OffloadTaskItem>& carried_tasks);

    // Tasks for keys the bucket backend deferred to its ungrouped pool, keyed
    // by tenant-scoped storage key. Consumed (merged into task_by_storage_key)
    // on the next OffloadObjects call, when the backend re-emits those keys in
    // a bucket. Only touched from the offload worker path (single-threaded);
    // lost on restart together with the backend's in-memory ungrouped pool,
    // in which case the master's TTL reaper reclaims the orphaned tasks.
    std::unordered_map<std::string, OffloadTaskItem>
        deferred_task_by_storage_key_;

    mutable Mutex offloading_mutex_;
    bool GUARDED_BY(offloading_mutex_) enable_offloading_;
    std::atomic<bool> heartbeat_running_;
    std::thread heartbeat_thread_;
    std::atomic<bool> client_buffer_gc_running_;
    std::thread client_buffer_gc_thread_;
    std::future<void> rescan_future_;
    std::atomic<bool> metadata_resync_pending_{false};
};

}  // namespace mooncake
