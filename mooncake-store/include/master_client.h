#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "rpc_service.h"
#include "types.h"

using namespace async_simple;
using namespace coro_rpc;

namespace mooncake {

static const std::string kDefaultMasterAddress = "localhost:50051";

/**
 * @brief Client for interacting with the mooncake master service
 */
class MasterClient {
   public:
    MasterClient();
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
    [[nodiscard]] tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    GetReplicaList(const std::string& object_key);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]]
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
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
             size_t value_length, const ReplicateConfig& config);

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
                  const std::vector<uint64_t>& value_lengths,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config);

    /**
     * @brief Ends a put operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutEnd(const std::string& key);

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
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutRevoke(
        const std::string& key);

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
     * @return tl::expected<std::pair<ViewVersionId, ClientStatus>, ErrorCode>
     * containing view version and client status
     */
    [[nodiscard]] tl::expected<std::pair<ViewVersionId, ClientStatus>,
                               ErrorCode>
    Ping(const UUID& client_id);

   private:
    /**
     * @brief Accessor for the coro_rpc_client. Since coro_rpc_client cannot
     * reconnect to a different address, a new coro_rpc_client is created if
     * the address is different from the current one.
     */
    class RpcClientAccessor {
       public:
        void SetClient(std::shared_ptr<coro_rpc_client> client) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_ = client;
        }

        std::shared_ptr<coro_rpc_client> GetClient() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_rpc_client> client_;
    };
    RpcClientAccessor client_accessor_;

    // Mutex to insure the Connect function is atomic.
    mutable Mutex connect_mutex_;
    // The address which is passed to the coro_rpc_client
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);
};

}  // namespace mooncake
