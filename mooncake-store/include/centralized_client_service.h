#pragma once

#include "client_service.h"
#include "centralized_master_client.h"
#include "storage_backend.h"
#include "file_storage.h"
#include "transfer_task.h"
#include "thread_pool.h"
#include "rpc_types.h"
#include <chrono>

namespace mooncake {

class PutOperation;
class FileStorage;
class RealClient;

/**
 * @brief RPC client pool for inter-client offload communication.
 * Used to call remote clients' batch_get_offload_object RPC handler.
 */
class ClientRequester {
   public:
    ClientRequester();

    /**
     * @brief Retrieves multiple objects from a remote Transfer Engine (TE)
     * @param client_addr Network address (e.g., "ip:port") of the remote
     * Transfer Engine service.
     * @param keys Map from object key to size (bytes);
     */
    tl::expected<BatchGetOffloadObjectResponse, ErrorCode>
    batch_get_offload_object(const std::string& client_addr,
                             const std::vector<std::string>& keys,
                             const std::vector<int64_t> sizes);

   private:
    /**
     * @brief A batch of allocated memory buffers, tracking both handles and
     * mapped addresses. This struct holds a collection of buffer resources
     * obtained from a memory allocator. It includes:
     * - `handles`: Opaque handles used to manage lifetime and deallocation.
     * - `pointers`: Direct virtual addresses where the buffers are accessible.
     */
    struct AllocatedBatch {
        std::vector<BufferHandle>
            handles;  ///< Unique handles for each buffer (used for release)
        std::vector<uintptr_t>
            pointers;  ///< Virtual memory addresses where buffers are mapped

        // Allow move semantics
        AllocatedBatch() = default;
        AllocatedBatch(AllocatedBatch&&) = default;
        AllocatedBatch& operator=(AllocatedBatch&&) = default;

        // Prevent copying (because BufferHandle is move-only)
        AllocatedBatch(const AllocatedBatch&) = delete;
        AllocatedBatch& operator=(const AllocatedBatch&) = delete;

        ~AllocatedBatch() =
            default;  // Automatically releases all handles via RAII
    };

    mutable std::shared_mutex client_pool_mutex_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;

    /**
     * @brief Generic RPC invocation helper for single-result operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ReturnType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param args Arguments to pass to the RPC call
     * @return The result of the RPC call
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc(
        const std::string& client_addr, Args&&... args);
};

/**
 * @brief Centralized-specific query result with lease timeout information
 */
class CentralizedQueryResult final : public QueryResult {
   public:
    /** @brief Time point when the lease for this key expires */
    const std::chrono::steady_clock::time_point lease_timeout;

    CentralizedQueryResult(
        std::vector<Replica::Descriptor>&& replicas_param,
        std::chrono::steady_clock::time_point lease_timeout_param)
        : QueryResult(std::move(replicas_param)),
          lease_timeout(lease_timeout_param) {}

    bool IsLeaseExpired() const {
        return std::chrono::steady_clock::now() >= lease_timeout;
    }

    bool IsLeaseExpired(std::chrono::steady_clock::time_point& now) const {
        return now >= lease_timeout;
    }
};

class CentralizedClientService
    : public ClientService,
      public std::enable_shared_from_this<CentralizedClientService> {
   public:
    CentralizedClientService(
        const std::string& metadata_connstring, uint16_t metrics_port = 9003,
        bool enable_metrics_http = true,
        const std::map<std::string, std::string>& labels = {});

    ~CentralizedClientService() override;

    ErrorCode Init(const CentralizedClientConfig& config);
    void Stop() override;
    void Destroy() override;

    tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) override;

    DeploymentMode deployment_mode() const override {
        return DeploymentMode::CENTRALIZATION;
    }

    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name);

    tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const ReadRouteConfig& config = {},
        bool aggregate_same_segment_task = false) override;

    tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {}) override;

    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const WriteConfig& config) override;

    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) override;

    tl::expected<void, ErrorCode> Remove(const ObjectKey& key) override;

    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str) override;

    tl::expected<long, ErrorCode> RemoveAll() override;

    tl::expected<void, ErrorCode> MountSegment(const void* buffer,
                                               size_t size) override;

    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size) override;

    /**
     * @brief Mounts a local disk segment into the master.
     * @param enable_offloading If true, enables offloading (write-to-file).
     */
    tl::expected<void, ErrorCode> MountLocalDiskSegment(bool enable_offloading);

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-offloaded objects.
     * @param enable_offloading Indicates whether offloading is enabled for this
     * segment.
     * @param offloading_objects On return, contains a map from object key to
     * size (in bytes) for all objects that require offload.
     */
    tl::expected<void, ErrorCode> OffloadObjectHeartbeat(
        bool enable_offloading,
        std::unordered_map<std::string, int64_t>& offloading_objects);

    /**
     * @brief Performs a batched read of multiple objects using a
     * high-throughput Transfer Engine.
     * @param transfer_engine_addr Address of the Transfer Engine service (e.g.,
     * "ip:port").
     * @param keys List of keys identifying the data objects to be transferred
     * @param pointers Array of destination memory addresses on the remote node
     *                         where data will be written (one per key)
     * @param batch_slices Map from object key to its data slice
     * (`mooncake::Slice`), containing raw bytes to be written.
     */
    tl::expected<void, ErrorCode> BatchGetOffloadObject(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uintptr_t>& pointers,
        const std::unordered_map<std::string, Slice>& batch_slices);

    /**
     * @brief Notifies the master that offloading of specified objects has
     * succeeded.
     * @param keys         A list of object keys (names) that were successfully
     * offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);

    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient() override;

    tl::expected<BatchGetOffloadObjectResponse, ErrorCode>
    BatchGetOffloadObjectFromStorage(const std::vector<std::string>& keys,
                                     const std::vector<int64_t>& sizes);

    /**
     * @brief Create a copy task to copy an object's replicas to target segments
     * @param key Object key
     * @param targets Target segments
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key,
        const std::vector<std::string>& targets) override;

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& source,
        const std::string& target) override;

    /**
     * @brief Query a task by task id
     * @param task_id Task ID to query
     * @return tl::expected<QueryTaskResponse, ErrorCode> Task basic info
     * on success, ErrorCode on failure
     */
    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id) override;

    /**
     * @brief Fetch tasks assigned to a client
     * @param batch_size Number of tasks to fetch
     * @return tl::expected<std::vector<TaskAssignment>, ErrorCode> list of
     * tasks on success, ErrorCode on failure
     */
    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        size_t batch_size) override;

    /**
     * @brief Mark the task as complete
     * @param task_complete Task complete request
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete) override;

   protected:
    HeartbeatRequest build_heartbeat_request() override;

    MasterClient& GetMasterClient() override { return master_client_; }

    ClientMetric* GetMetrics() override { return metrics_.get(); }

   private:
    void InitTransferSubmitter();

    std::vector<tl::expected<void, ErrorCode>> BatchGetWhenPreferSameNode(
        const std::vector<std::string>& object_keys,
        const std::vector<std::unique_ptr<QueryResult>>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    tl::expected<void, ErrorCode> InnerGet(const std::string& object_key,
                                           const QueryResult& query_result,
                                           std::vector<Slice>& slices);

    std::vector<tl::expected<void, ErrorCode>> InnerBatchGet(
        const std::vector<std::string>& object_keys,
        const std::vector<std::unique_ptr<QueryResult>>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices,
        bool prefer_same_node = false);

    std::vector<PutOperation> CreatePutOperations(
        const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);
    void StartBatchPut(std::vector<PutOperation>& ops,
                       const ReplicateConfig& config);
    void SubmitTransfers(std::vector<PutOperation>& ops);
    void WaitForTransfers(std::vector<PutOperation>& ops);
    void FinalizeBatchPut(std::vector<PutOperation>& ops);
    std::vector<tl::expected<void, ErrorCode>> CollectResults(
        const std::vector<PutOperation>& ops);

    std::vector<tl::expected<void, ErrorCode>> BatchPutWhenPreferSameNode(
        std::vector<PutOperation>& ops);

    void PrepareStorageBackend(const std::string& storage_root_dir,
                               const std::string& fsdir,
                               bool enable_eviction = true,
                               uint64_t quota_bytes = 0);

    void PutToLocalFile(const std::string& object_key,
                        const std::vector<Slice>& slices,
                        const DiskDescriptor& disk_descriptor);

    ErrorCode TransferData(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices,
                           TransferRequest::OpCode op_code);
    ErrorCode TransferWrite(const Replica::Descriptor& replica_descriptor,
                            std::vector<Slice>& slices);
    ErrorCode TransferRead(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices);

    tl::expected<void, ErrorCode> InnerUnmountSegment(const void* buffer,
                                                      size_t size);

    ErrorCode GetPreferredReplica(
        const std::vector<Replica::Descriptor>& replica_list,
        Replica::Descriptor& replica);

    tl::expected<void, ErrorCode> BatchGetIntoOffloadObjectInternal(
        const std::string& target_rpc_service_addr,
        std::unordered_map<std::string, Slice>& objects);

   private:
    // Client-side metrics (must be initialized before master_client_)
    std::unique_ptr<ClientMetric> metrics_;

    // Global segment memory management
    struct SegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) free(ptr);
        }
    };
    struct AscendSegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) free_memory("ascend", ptr);
        }
    };
    struct HugepageSegmentDeleter {
        size_t size = 0;
        void operator()(void* ptr) const {
            if (ptr && size > 0) free_buffer_mmap_memory(ptr, size);
        }
    };
    std::vector<std::unique_ptr<void, SegmentDeleter>> segment_ptrs_;
    std::vector<std::unique_ptr<void, AscendSegmentDeleter>>
        ascend_segment_ptrs_;
    std::vector<std::unique_ptr<void, HugepageSegmentDeleter>>
        hugepage_segment_ptrs_;

    CentralizedMasterClient master_client_;
    std::unique_ptr<TransferSubmitter> transfer_submitter_;

    // Mutex to protect mounted_segments_
    SharedMutex mounted_segments_mutex_;
    std::unordered_map<UUID, Segment, boost::hash<UUID>> mounted_segments_;

    // File storage for offloading
    std::shared_ptr<FileStorage> file_storage_;

    // RPC client pool for inter-client offload communication
    std::shared_ptr<ClientRequester> client_requester_;

    // Client persistent thread pool for async operations
    ThreadPool write_thread_pool_;
    std::shared_ptr<StorageBackend> storage_backend_;
};

}  // namespace mooncake
