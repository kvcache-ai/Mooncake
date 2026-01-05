#pragma once

#include "rpc_service.h"

namespace mooncake {

// TODO: wanyue-wy
class WrappedP2PMasterService final : public WrappedMasterService {
   public:
    WrappedP2PMasterService(const WrappedMasterServiceConfig& config);

    ~WrappedP2PMasterService();

    //    private:
    //     P2PMasterService master_service_;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service);

}  // namespace mooncake
