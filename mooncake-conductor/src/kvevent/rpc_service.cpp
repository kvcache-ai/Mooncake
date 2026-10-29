#include "conductor/kvevent/rpc_service.h"

#include "conductor/kvevent/conductor_service.h"

namespace mooncake::conductor::kvevent {

void RegisterConductorRpcService(coro_rpc::coro_rpc_server& server,
                                 ConductorService& service) {
    server.register_handler<&ConductorService::Query>(&service);
    server.register_handler<&ConductorService::Register>(&service);
    server.register_handler<&ConductorService::Unregister>(&service);
    server.register_handler<&ConductorService::GetGlobalView>(&service);
    server.register_handler<&ConductorService::ListServices>(&service);
}

}  // namespace mooncake::conductor::kvevent
