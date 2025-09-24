#pragma once

#include <memory>
#include <string>
#include <vector>
#include <cstdlib>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_io/client_pool.hpp>

#include "client_metric.h"
#include "replica.h"
#include "types.h"
#include "rpc_types.h"

namespace mooncake {

static const std::string kDefaultMasterAddress = "localhost:50051";

/**
 * @brief Client for interacting with the mooncake master service
 */
class MasterClient {
   public:
    MasterClient(MasterClientMetric* metrics = nullptr) : metrics_(metrics) {
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
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> Remove(const std::string& key);

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveByRegex(
        const std::string& str);

    /**
     * @brief Removes all objects and all its replicas
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveAll();

    /**
     * @brief Registers a segment to master for allocation
     * @param segment Segment to register
     * @param client_id The uuid of the client
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment, const UUID& client_id);

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @param segments Segments to remount
     * @param client_id The uuid of the client
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id);

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_id ID of the segment to unmount
     * @param client_id The uuid of the client
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id, const UUID& client_id);

    /**
     * @brief Gets the cluster ID for the current client to use as subdirectory
     * name
     * @return GetClusterIdResponse containing the cluster ID
     */
    [[nodiscard]] tl::expected<std::string, ErrorCode> GetFsdir();

    /**
     * @brief Pings master to check its availability
     * @param client_id The uuid of the client
     * @return tl::expected<PingResponse, ErrorCode>
     * containing view version and client status
     */
    [[nodiscard]] tl::expected<PingResponse, ErrorCode> Ping(
        const UUID& client_id);

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
