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
    server.register_handler<
        &mooncake::WrappedP2PMasterService::BatchMutateReplica>(
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

std::vector<tl::expected<void, ErrorCode>>
WrappedP2PMasterService::BatchMutateReplica(
    const BatchReplicaMutationRequest& req) {
    ScopedVLogTimer timer(1, "BatchMutateReplica");
    const size_t total_requests = req.mutations.size();
    timer.LogRequest("mutation_count=", total_requests);
    MasterMetricManager::instance().inc_batch_remove_replica_requests(
        total_requests);

    auto results = master_service_.BatchMutateReplica(req);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchMutateReplica failed for key '"
                       << req.mutations[i].key
                       << "', segment_id: " << req.mutations[i].segment_id
                       << ": " << toString(error);
        }
    }

    if (failure_count == total_requests && total_requests > 0) {
        MasterMetricManager::instance().inc_batch_remove_replica_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance()
            .inc_batch_remove_replica_partial_success(failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

}  // namespace mooncake
