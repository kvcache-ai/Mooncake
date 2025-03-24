#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include <vector>

#include "master.grpc.pb.h"
#include "types.h"

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
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode GetReplicaList(
        const std::string& object_key,
        mooncake_store::GetReplicaListResponse& object_info) const;

    /**
     * @brief Starts a put operation
     * @param key Object key
     * @param slice_lengths Vector of slice lengths
     * @param value_length Total value length
     * @param config Replication configuration
     * @param start_response Output parameter for put start response
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode PutStart(
        const std::string& key, const std::vector<size_t>& slice_lengths,
        size_t value_length, const ReplicateConfig& config,
        mooncake_store::PutStartResponse& start_response) const;

    /**
     * @brief Ends a put operation
     * @param key Object key
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode PutEnd(const std::string& key) const;

    /**
     * @brief Revokes a put operation
     * @param key Object key
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode PutRevoke(const std::string& key) const;

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode Remove(const std::string& key) const;

    /**
     * @brief Registers a segment to master for allocation
     * @param segment_name hostname:port of the segment
     * @param buffer Buffer address of the segment
     * @param size Size of the segment in bytes
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode MountSegment(const std::string& segment_name,
                                         const void* buffer, size_t size) const;

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_name Name which is used to register the segment
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode UnmountSegment(
        const std::string& segment_name) const;

    /**
     * @brief Checks if an object exists
     * @param key Key to check
     * @return ErrorCode::OK if exists, ErrorCode::OBJECT_NOT_FOUND if not
     * exists, other ErrorCode for errors
     */
    [[nodiscard]] ErrorCode IsExist(const std::string& key) const;

   private:
    // Template method: Execute RPC call and handle results
    template <typename Req, typename Res, typename... Builders>
    [[nodiscard]] ErrorCode ExecuteRpc(
        const std::string& rpc_name,
        grpc::Status (mooncake_store::MasterService::Stub::*method)(
            grpc::ClientContext*, const Req&, Res*),
        Res& response, Builders&&... builders) const;

    std::unique_ptr<mooncake_store::MasterService::Stub> master_stub_{nullptr};
};

}  // namespace mooncake
