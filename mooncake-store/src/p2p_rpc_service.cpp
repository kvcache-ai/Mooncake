#include "p2p_rpc_service.h"
#include "rpc_helper.h"

namespace mooncake {

WrappedP2PMasterService::WrappedP2PMasterService(
    const WrappedMasterServiceConfig& config)
    : WrappedMasterService(config), master_service_(config) {}

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service) {
    RegisterRpcService(server, wrapped_master_service);
    server.register_handler<&mooncake::WrappedP2PMasterService::GetWriteRoute>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedP2PMasterService::AddReplica>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedP2PMasterService::RemoveReplica>(
        &wrapped_master_service);
}

tl::expected<WriteRouteResponse, ErrorCode>
WrappedP2PMasterService::GetWriteRoute(const WriteRouteRequest& req) {
    return execute_rpc(
        "GetWriteRoute", [&] { return master_service_.GetWriteRoute(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] { MasterMetricManager::instance().inc_get_write_route_requests(); },
        [] { MasterMetricManager::instance().inc_get_write_route_failures(); });
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::AddReplica(
    const AddReplicaRequest& req) {
    return execute_rpc(
        "AddReplica", [&] { return master_service_.AddReplica(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] { MasterMetricManager::instance().inc_add_replica_requests(); },
        [] { MasterMetricManager::instance().inc_add_replica_failures(); });
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::RemoveReplica(
    const RemoveReplicaRequest& req) {
    return execute_rpc(
        "RemoveReplica", [&] { return master_service_.RemoveReplica(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] { MasterMetricManager::instance().inc_remove_replica_requests(); },
        [] { MasterMetricManager::instance().inc_remove_replica_failures(); });
}

}  // namespace mooncake
