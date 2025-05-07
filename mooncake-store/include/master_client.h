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
    [[nodiscard]] ExistKeyResponse ExistKey(
        const std::string& object_key);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] GetReplicaListResponse GetReplicaList(
        const std::string& object_key);

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
     * @brief Ends a put operation
     * @param key Object key
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] PutEndResponse PutEnd(const std::string& key);

    /**
     * @brief Revokes a put operation
     * @param key Object key
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] PutRevokeResponse PutRevoke(const std::string& key);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] RemoveResponse Remove(const std::string& key);

    /**
     * @brief Registers a segment to master for allocation
     * @param segment_name hostname:port of the segment
     * @param buffer Buffer address of the segment
     * @param size Size of the segment in bytes
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] MountSegmentResponse MountSegment(
        const std::string& segment_name, const void* buffer, size_t size);

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_name Name which is used to register the segment
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] UnmountSegmentResponse UnmountSegment(
        const std::string& segment_name);

   private:
    coro_rpc_client client_;
};

}  // namespace mooncake
