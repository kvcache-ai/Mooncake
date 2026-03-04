#pragma once

#include "master_client.h"
#include "p2p_rpc_types.h"

namespace mooncake {

/**
 * @brief Client for interacting with the mooncake P2P master service
 */
class P2PMasterClient final : public MasterClient {
   public:
    P2PMasterClient(const UUID& client_id,
                    MasterClientMetric* metrics = nullptr)
        : MasterClient(client_id, metrics) {}

    P2PMasterClient(const P2PMasterClient&) = delete;
    P2PMasterClient& operator=(const P2PMasterClient&) = delete;

    /**
     * @brief Gets write candidate route for a segment
     */
    [[nodiscard]] tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);

    /**
     * @brief Adds a replica to master
     */
    [[nodiscard]] tl::expected<void, ErrorCode> AddReplica(
        const AddReplicaRequest& req);

    /**
     * @brief Removes a replica from master
     */
    [[nodiscard]] tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);
};

}  // namespace mooncake
