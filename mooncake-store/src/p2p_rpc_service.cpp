#include "p2p_rpc_service.h"
#include "rpc_helper.h"

namespace mooncake {

WrappedP2PMasterService::WrappedP2PMasterService(
    const WrappedMasterServiceConfig& config)
    : WrappedMasterService(config) {}

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service) {
    RegisterRpcService(server, wrapped_master_service);
}

}  // namespace mooncake
