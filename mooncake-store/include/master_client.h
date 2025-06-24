#pragma once

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
     * @return ErrorCode indicating exist or not
     */
    [[nodiscard]] ExistKeyResponse ExistKey(const std::string& object_key);

    /**
     * @brief Checks if multiple objects exist
     * @param object_keys Vector of keys to query
     * @return BatchExistResponse containing existence status for each key
     */
    [[nodiscard]] BatchExistResponse BatchExistKey(
        const std::vector<std::string>& object_keys);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] GetReplicaListResponse GetReplicaList(
        const std::string& object_key);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] BatchGetReplicaListResponse BatchGetReplicaList(
        const std::vector<std::string>& object_keys);

    /**
     * @brief Starts a put operation
     * @param key Object key
     * @param slice_lengths Vector of slice lengths
     * @param value_length Total value length
     * @param config Replication configuration
     * @param start_response Output parameter for put start response
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] PutStartResponse PutStart(
        const std::string& key, const std::vector<size_t>& slice_lengths,
        size_t value_length, const ReplicateConfig& config);

    /**
     * @brief Starts a batch of put operations for N objects
     * @param keys Vector of object key
     * @param value_lengths Vector of total value lengths
     * @param slice_lengths Vector of vectors of slice lengths
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] BatchPutStartResponse BatchPutStart(
        const std::vector<std::string>& keys,
        const std::unordered_map<std::string, uint64_t>& value_lengths,
        const std::unordered_map<std::string, std::vector<uint64_t>>&
            slice_lengths,
        const ReplicateConfig& config);

    /**
     * @brief Ends a put operation
     * @param key Object key
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] PutEndResponse PutEnd(const std::string& key);

    /**
     * @brief Ends a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] BatchPutEndResponse BatchPutEnd(
        const std::vector<std::string>& keys);

    /**
     * @brief Revokes a put operation
     * @param key Object key
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] PutRevokeResponse PutRevoke(const std::string& key);

    /**
     * @brief Revokes a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] BatchPutRevokeResponse BatchPutRevoke(
        const std::vector<std::string>& keys);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] RemoveResponse Remove(const std::string& key);

    /**
     * @brief Removes all objects and all its replicas
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] RemoveAllResponse RemoveAll();

    /**
     * @brief Registers a segment to master for allocation
     * @param segment Segment to register
     * @param client_id The uuid of the client
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] MountSegmentResponse MountSegment(const Segment& segment,
                                                    const UUID& client_id);

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @param segments Segments to remount
     * @param client_id The uuid of the client
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ReMountSegmentResponse ReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id);

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_id ID of the segment to unmount
     * @param client_id The uuid of the client
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] UnmountSegmentResponse UnmountSegment(const UUID& segment_id,
                                                        const UUID& client_id);

    /**
     * @brief Pings master to check its availability
     * @param client_id The uuid of the client
     * @return current master view version
     * @return client status from the master
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] PingResponse Ping(const UUID& client_id);

   private:
    coro_rpc_client client_;
};

}  // namespace mooncake
