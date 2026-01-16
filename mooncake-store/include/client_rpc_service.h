#pragma once

#include <memory>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>
#include "client_rpc_types.h"
#include "data_manager.h"
#include "types.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake {

/**
 * @class ClientRpcService
 * @brief RPC service for handling remote data read/write requests from peer
 * clients
 *
 * This service receives RPC requests from other clients (e.g., Client A) to
 * read/write data stored locally (on Client B). It uses DataManager to access
 * TieredBackend and TransferEngine to perform zero-copy RDMA transfers.
 */
class ClientRpcService {
   public:
    /**
     * @brief Constructor
     * @param data_manager Reference to DataManager instance (must outlive this
     * object)
     */
    explicit ClientRpcService(DataManager& data_manager);

    /**
     * @brief Read remote data: Client A requests Client B to read data and
     * transfer to A
     * @param request RemoteReadRequest containing key and destination buffers
     * @return ErrorCode indicating success or failure
     *
     * Flow:
     * 1. DataManager.ReadRemoteData(key, dest_buffers)
     * 2. TieredBackend.Get(key) → handle
     * 3. TransferEngine.submitTransfer(WRITE) to transfer data from B to A
     */
    tl::expected<void, ErrorCode> ReadRemoteData(
        const RemoteReadRequest& request);

    /**
     * @brief Write remote data: Client A requests Client B to write data from A
     * @param request RemoteWriteRequest containing key, source buffers, and
     * target_tier_id
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> WriteRemoteData(
        const RemoteWriteRequest& request);

    /**
     * @brief Batch read remote data for multiple keys
     * @param request BatchRemoteReadRequest containing multiple keys and their
     * destination buffers
     * @return Vector of expected results for each key
     */
    std::vector<tl::expected<void, ErrorCode>> BatchReadRemoteData(
        const BatchRemoteReadRequest& request);

    /**
     * @brief Batch write remote data for multiple keys
     * @param request BatchRemoteWriteRequest containing multiple keys, source
     * buffers, and tier_ids
     * @return Vector of expected results for each key
     */
    std::vector<tl::expected<void, ErrorCode>> BatchWriteRemoteData(
        const BatchRemoteWriteRequest& request);

   private:
    DataManager& data_manager_;  // Reference: owned by Client, same lifetime
};

/**
 * @brief Register ClientRpcService methods with coro_rpc_server
 * @param server coro_rpc_server instance
 * @param service ClientRpcService instance
 */
void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              ClientRpcService& service);

}  // namespace mooncake
