#pragma once

#include "p2p_master_service.h"
#include "rpc_service.h"

#include "p2p_rpc_types.h"

namespace mooncake {

class WrappedP2PMasterService final : public WrappedMasterService {
   public:
    WrappedP2PMasterService(const WrappedMasterServiceConfig& config);

    ~WrappedP2PMasterService() override = default;

    MasterService& GetMasterService() override { return master_service_; }

    tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);

    tl::expected<void, ErrorCode> AddReplica(const AddReplicaRequest& req);

    tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);

   private:
    P2PMasterService master_service_;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service);

}  // namespace mooncake
