#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <ylt/util/tl/expected.hpp>
#include <chrono>
#include <unordered_set>

#include "client_metric.h"
#include "ha_helper.h"
#include "master_client.h"
#include "storage_backend.h"
#include "thread_pool.h"
#include "transfer_engine.h"
#include "transfer_task.h"
#include "types.h"
#include "replica.h"
#include "master_metric_manager.h"

namespace mooncake {

class PutOperation;

/**
 * @brief Result of a query operation containing replica information and lease
 * timeout
 */
class QueryResult {
   public:
    /** @brief List of available replicas for the queried key */
    const std::vector<Replica::Descriptor> replicas;
    /** @brief Time point when the lease for this key expires */
    const std::chrono::steady_clock::time_point lease_timeout;

    QueryResult(std::vector<Replica::Descriptor>&& replicas_param,
                std::chrono::steady_clock::time_point lease_timeout_param)
        : replicas(std::move(replicas_param)),
          lease_timeout(lease_timeout_param) {}

    bool IsLeaseExpired() const {
        return std::chrono::steady_clock::now() >= lease_timeout;
    }

    bool IsLeaseExpired(std::chrono::steady_clock::time_point& now) const {
        return now >= lease_timeout;
    }
};

/**
 * @brief Client for interacting with the mooncake distributed object store
 */
class Client {
   public:
    ~Client();

    /**
     * @brief Creates and initializes a new Client instance
     * @param local_hostname Local host address (IP:Port)
     * @param metadata_connstring Connection string for metadata service
     * @param protocol Transfer protocol ("rdma" or "tcp")
     * @param device_names Comma-separated RDMA device names.
     *        Optional with default auto-discovery. Only required when
     *        auto-discovery is disabled (set env `MC_MS_AUTO_DISC=0`).
     * @param master_server_entry The entry of master server (IP:Port of master
     *        address for non-HA mode, etcd://IP:Port;IP:Port;...;IP:Port for
     *        HA mode)
     * @return std::optional containing a shared_ptr to Client if successful,
     * std::nullopt otherwise
     */
    static std::optional<std::shared_ptr<Client>> Create(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& device_names = std::nullopt,
        const std::string& master_server_entry = kDefaultMasterAddress,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        std::map<std::string, std::string> labels = {});

    /**
     * @brief Retrieves data for a given key
     * @param object_key Key to retrieve
     * @param slices Vector of slices to store the retrieved data
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      std::vector<Slice>& slices);

    /**
     * @brief Batch retrieve data for multiple keys
     * @param object_keys Keys to query
     * @param slices Map of object keys to their data slices
     */
    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure.
     */
    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @return QueryResult containing replicas and lease timeout, or ErrorCode
     * indicating failure
     */
    tl::expected<QueryResult, ErrorCode> Query(const std::string& object_key);

    /**
     * @brief Queries replica lists for object keys that match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    QueryByRegex(const std::string& str);

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @return Vector of QueryResult objects containing replicas and lease
     * timeouts
     */
    std::vector<tl::expected<QueryResult, ErrorCode>> BatchQuery(
        const std::vector<std::string>& object_keys);

    /**
     * @brief Batch clear KV cache for specified object keys on a specific
     * segment for a given client.
     * @param object_keys Vector of object key strings to clear.
     * @param client_id The UUID of the client that owns the object keys.
     * @param segment_name The name of the segment (storage device) to clear
     * from.
     * @return An expected object containing a vector of successfully cleared
     * object keys on success, or an ErrorCode on failure.
     */
    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name);

    /**
     * @brief Transfers data using pre-queried object information
     * @param object_key Key of the object
     * @param query_result Previously queried object metadata containing
     * replicas and lease timeout
     * @param slices Vector of slices to store the data
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      const QueryResult& query_result,
                                      std::vector<Slice>& slices);
    /**
     * @brief Transfers data using pre-queried object information
     * @param object_keys Keys of the objects
     * @param query_results Previously queried object metadata for each key
     * @param slices Map of object keys to their data slices
     * @return Vector of ErrorCode results for each object
     */
    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        const std::vector<QueryResult>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices,
        bool prefer_same_node = false);

    /**
     * @brief Stores data with replication
     * @param key Object key
     * @param slices Vector of data slices to store
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const ReplicateConfig& config);

    /**
     * @brief Batch put data with replication
     * @param keys Object keys
     * @param batched_slices Vector of vectors of data slices to store (indexed
     * to match keys)
     * @param config Replication configuration
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @param force If true, skip lease and replication task checks
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                         bool force = false);

    /**
     * @brief Removes objects from the store whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                bool force = false);

    /**
     * @brief Removes all objects and all its replicas
     * @param force If true, skip lease and replication task checks
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    tl::expected<long, ErrorCode> RemoveAll(bool force = false);

    /**
     * @brief Registers a memory segment to master for allocation
     * @param buffer Memory buffer to register
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> MountSegment(
        const void* buffer, size_t size, const std::string& protocol = "tcp");

    /**
     * @brief Unregisters a memory segment from master
     * @param buffer Memory buffer to unregister
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size);

    /**
     * @brief Registers memory buffer with TransferEngine for data transfer
     * @param addr Memory address to register
     * @param length Size of the memory region
     * @param location Device location (e.g. "cpu:0")
     * @param remote_accessible Whether the memory can be accessed remotely
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    /**
     * @brief Unregisters memory buffer from TransferEngine
     * @param addr Memory address to unregister
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    /**
     * @brief Checks if an object exists
     * @param key Key to check
     * @return ErrorCode::OK if exists, ErrorCode::OBJECT_NOT_FOUND if not
     * exists, other ErrorCode for errors
     */
    tl::expected<bool, ErrorCode> IsExist(const std::string& key);

    /**
     * @brief Checks if multiple objects exist
     * @param keys Vector of keys to check
     * @return Vector of existence results for each key
     */
    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys);

    /**
     * @brief Create a copy task to copy an object's replicas to target segments
     * @param key Object key
     * @param targets Target segments
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets);

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const std::string& source,
                                                 const std::string& target);

    /**
     * @brief Query a task by task id
     * @param task_id Task ID to query
     * @return tl::expected<QueryTaskResponse, ErrorCode> Task basic info
     * on success, ErrorCode on failure
     */
    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);

    /**
     * @brief Get global segment base address for cxl protocol
     * @return Global segment base address
     */
    void* GetBaseAddr();

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

    /**
     * @brief Fetch tasks assigned to a client
     * @param batch_size Number of tasks to fetch
     * @return tl::expected<std::vector<TaskAssignment>, ErrorCode> list of
     * tasks on success, ErrorCode on failure
     */
    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        size_t batch_size);

    /**
     * @brief Mark the task as complete
     * @param task_complete Task complete request
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete);

    // For human-readable metrics
    tl::expected<std::string, ErrorCode> GetSummaryMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return metrics_->summary_metrics();
    }

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats() {
        return master_client_.CalcCacheStats();
    }

    // For Prometheus-style metrics
    tl::expected<std::string, ErrorCode> SerializeMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::string str;
        metrics_->serialize(str);
        return str;
    }

    [[nodiscard]] std::string GetTransportEndpoint() {
        return transfer_engine_->getLocalIpAndPort();
    }

    tl::expected<Replica::Descriptor, ErrorCode> GetPreferredReplica(
        const std::vector<Replica::Descriptor>& replica_list);

   private:
    /**
     * @brief Private constructor to enforce creation through Create() method
     */
    Client(const std::string& local_hostname,
           const std::string& metadata_connstring, const std::string& protocol,
           const std::map<std::string, std::string>& labels = {});

    /**
     * @brief Internal helper functions for initialization and data transfer
     */
    ErrorCode ConnectToMaster(const std::string& master_server_entry);
    ErrorCode InitTransferEngine(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& device_names);
    void InitTransferSubmitter();
    ErrorCode TransferData(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices,
                           TransferRequest::OpCode op_code);
    ErrorCode TransferWrite(const Replica::Descriptor& replica_descriptor,
                            std::vector<Slice>& slices);
    ErrorCode TransferRead(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices);

    /**
     * @brief Prepare and use the storage backend for persisting data
     */
    void PrepareStorageBackend(const std::string& storage_root_dir,
                               const std::string& fsdir,
                               bool enable_eviction = true,
                               uint64_t quota_bytes = 0);

    void PutToLocalFile(const std::string& object_key,
                        const std::vector<Slice>& slices,
                        const DiskDescriptor& disk_descriptor);

    /**
     * @brief Find the first complete replica from a replica list
     * @param replica_list List of replicas to search through
     * @param replica the first complete replica (file or memory)
     * @return ErrorCode::OK if found, ErrorCode::INVALID_REPLICA if no complete
     * replica
     */
    ErrorCode FindFirstCompleteReplica(
        const std::vector<Replica::Descriptor>& replica_list,
        Replica::Descriptor& replica);

    /**
     * @brief Batch put helper methods for structured approach
     */
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
    std::vector<tl::expected<void, ErrorCode>> BatchGetWhenPreferSameNode(
        const std::vector<std::string>& object_keys,
        const std::vector<QueryResult>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    // Client identification
    const UUID client_id_;

    // Client-side metrics
    std::unique_ptr<ClientMetric> metrics_;

    // Core components
    std::shared_ptr<TransferEngine> transfer_engine_;
    MasterClient master_client_;
    std::unique_ptr<TransferSubmitter> transfer_submitter_;

    // Mutex to protect mounted_segments_
    std::mutex mounted_segments_mutex_;
    std::unordered_map<UUID, Segment, boost::hash<UUID>> mounted_segments_;

    // Configuration
    const std::string local_hostname_;
    const std::string metadata_connstring_;
    const std::string protocol_;

    // Client persistent thread pool for async operations
    ThreadPool write_thread_pool_;
    std::shared_ptr<StorageBackend> storage_backend_;

    // For high availability
    MasterViewHelper master_view_helper_;
    std::thread ping_thread_;
    std::atomic<bool> ping_running_{false};
    void PingThreadMain(bool is_ha_mode, std::string current_master_address);
};

}  // namespace mooncake
