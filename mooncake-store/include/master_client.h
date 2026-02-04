#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstdlib>
#include <boost/functional/hash.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>

#include "client_metric.h"
#include "replica.h"
#include "types.h"
#include "rpc_types.h"
#include "master_metric_manager.h"
#include "task_manager.h"

namespace mooncake {

static const std::string kDefaultMasterAddress = "localhost:50051";

/**
 * @brief Client for interacting with the mooncake master service
 */
class MasterClient {
   public:
    MasterClient(const UUID& client_id, MasterClientMetric* metrics = nullptr)
        : client_id_(client_id), metrics_(metrics) {
        coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config
            pool_conf{};
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            pool_conf.client_config.socket_config =
                coro_io::ib_socket_t::config_t{};
        }
        client_pools_ =
            std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
                pool_conf);
    }
    ~MasterClient();

    MasterClient(const MasterClient&) = delete;
    MasterClient& operator=(const MasterClient&) = delete;

    /**
     * @brief Connects to the master service
     * @param master_addr Master service address (IP:Port)
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = kDefaultMasterAddress);

    /**
     * @brief Checks if an object exists
     * @param object_key Key to query
     * @return tl::expected<bool, ErrorCode> indicating exist or not
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> ExistKey(
        const std::string& object_key);

    /**
     * @brief Checks if multiple objects exist
     * @param object_keys Vector of keys to query
     * @return Vector containing existence status for each key
     */
    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& object_keys);

    /**
     * @brief Calculate cache hit rate metrics
     * @param object_keys None
     * @return Map containing metrics
     */
    [[nodiscard]] tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

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
    [[nodiscard]] tl::expected<std::vector<std::string>, ErrorCode>
    BatchReplicaClear(const std::vector<std::string>& object_keys,
                      const UUID& client_id, const std::string& segment_name);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaList(const std::string& object_key);

    /**
     * @brief Retrieves replica lists for object keys that match a regex
     * pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]]
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& object_keys);

    /**
     * @brief Starts a put operation
     * @param key Object key
     * @param slice_lengths Vector of slice lengths
     * @param value_length Total value length
     * @param config Replication configuration
     * @return tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
     * indicating success/failure
     */
    [[nodiscard]] tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    PutStart(const std::string& key, const std::vector<size_t>& slice_lengths,
             const ReplicateConfig& config);

    /**
     * @brief Starts a batch of put operations for N objects
     * @param keys Vector of object key
     * @param value_lengths Vector of total value lengths
     * @param slice_lengths Vector of vectors of slice lengths
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config);

    /**
     * @brief Ends a put operation
     * @param key Object key
     * @param replica_type Type of replica (memory or disk)
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutEnd(
        const std::string& key, ReplicaType replica_type);

    /**
     * @brief Ends a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<std::string>& keys);

    /**
     * @brief Revokes a put operation
     * @param key Object key
     * @param replica_type Type of replica (memory or disk)
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutRevoke(
        const std::string& key, ReplicaType replica_type);

    /**
     * @brief Revokes a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @param force If true, skip lease and replication task checks
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> Remove(const std::string& key,
                                                       bool force = false);

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveByRegex(
        const std::string& str, bool force = false);

    /**
     * @brief Removes all objects and all its replicas
     * @param force If true, skip lease and replication task checks
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveAll(bool force = false);

    /**
     * @brief Registers a segment to master for allocation
     * @param segment Segment to register
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment);

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @param segments Segments to remount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments);

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_id ID of the segment to unmount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id);

    /**
     * @brief Gets the cluster ID for the current client to use as subdirectory
     * name
     * @return GetClusterIdResponse containing the cluster ID
     */
    [[nodiscard]] tl::expected<std::string, ErrorCode> GetFsdir();

    [[nodiscard]] tl::expected<GetStorageConfigResponse, ErrorCode>
    GetStorageConfig();

    /**
     * @brief Pings master to check its availability
     * @return tl::expected<PingResponse, ErrorCode>
     * containing view version and client status
     */
    [[nodiscard]] tl::expected<PingResponse, ErrorCode> Ping();

    /**
     * @brief Mounts a local disk segment into the master.
     * @param enable_offloading If true, enables offloading (write-to-file).
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountLocalDiskSegment(
        const UUID& client_id, bool enable_offloading);

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-persisted objects.
     * @param enable_offloading Indicates whether persistence is enabled for
     * this segment.
     */
    [[nodiscard]] tl::expected<std::unordered_map<std::string, int64_t>,
                               ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading);

    /**
     * @brief Adds multiple new objects to a specified client in batch.
     * @param keys         A list of object keys (names) that were successfully
     * offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);

    /**
     * @brief Start a copy operation
     * @param key Object key
     * @param src_segment Source segment name
     * @param tgt_segments Target segment names
     * @return tl::expected<CopyStartResponse, ErrorCode> indicating
     * success/failure
     */
    [[nodiscard]] tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const std::string& key, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);

    /**
     * @brief End a copy operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> CopyEnd(const std::string& key);

    /**
     * @brief Revoke a copy operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> CopyRevoke(
        const std::string& key);

    /**
     * @brief Start a move operation
     * @param key Object key
     * @param src_segment Source segment name
     * @param tgt_segment Target segment name
     * @return tl::expected<MoveStartResponse, ErrorCode> indicating
     * success/failure
     */
    [[nodiscard]] tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const std::string& key, const std::string& src_segment,
        const std::string& tgt_segment);

    /**
     * @brief End a move operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MoveEnd(const std::string& key);

    /**
     * @brief Revoke a move operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MoveRevoke(
        const std::string& key);

    /**
     * @brief Create a task to copy an object's replica to target segments
     * @param key Object key
     * @param targets Target segments
     * @return tl::expected<UUID, ErrorCode> Copy task ID on success,
     * ErrorCode on failure
     */
    [[nodiscard]] tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets);

    /**
     * @brief Create a task to move an object's replica from source segment to
     * target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<UUID, ErrorCode> Move task ID on success,
     * ErrorCode on failure
     */
    [[nodiscard]] tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& source,
        const std::string& target);

    /**
     * @brief Query a task by task id
     * @param task_id Task ID to query
     * @return tl::expected<QueryTaskResponse, ErrorCode> Task basic info
     * on success, ErrorCode on failure
     */
    [[nodiscard]] tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id);

    /**
     * @brief Fetch tasks assigned to a client
     * @param batch_size Number of tasks to fetch
     * @return tl::expected<std::vector<TaskAssignment>, ErrorCode> list of
     * tasks on success, ErrorCode on failure
     */
    [[nodiscard]] tl::expected<std::vector<TaskAssignment>, ErrorCode>
    FetchTasks(size_t batch_size);

    /**
     * @brief Mark the task as complete
     * @param task_complete Task complete request
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete);

   private:
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
        Args&&... args);

    /**
     * @brief Generic RPC invocation helper for batch operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ResultType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param input_size Size of input batch for error handling
     * @param args Arguments to pass to the RPC call
     * @return Vector of results from the batch RPC call
     */
    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args);

    /**
     * @brief Accessor for the coro_rpc_client pool. Since coro_rpc_client pool
     * cannot reconnect to a different address, a new coro_rpc_client pool is
     * created if the address is different from the current one.
     */
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_pool_ = client_pool;
        }

        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };
    RpcClientAccessor client_accessor_;

    // The client identification.
    const UUID client_id_;

    // Metrics for tracking RPC operations
    MasterClientMetric* metrics_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;

    // Mutex to insure the Connect function is atomic.
    mutable Mutex connect_mutex_;
    // The address which is passed to the coro_rpc_client
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);
};

}  // namespace mooncake
