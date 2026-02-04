#include "p2p_rpc_service.h"
#include "rpc_helper.h"
#include <csignal>

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
    server.register_handler<
        &mooncake::WrappedP2PMasterService::BatchGetWriteRoute>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedP2PMasterService::AddReplica>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedP2PMasterService::RemoveReplica>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedP2PMasterService::BatchRemoveReplica>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedP2PMasterService::BatchSyncReplica>(
            &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedP2PMasterService::SetSyncCompleted>(
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

BatchGetWriteRouteResponse WrappedP2PMasterService::BatchGetWriteRoute(
    const BatchGetWriteRouteRequest& req) {
    ScopedVLogTimer timer(1, "BatchGetWriteRoute");
    const size_t total = req.keys.size();
    timer.LogRequest("client_id=", req.client_id, ", key_count=", total);
    MasterMetricManager::instance().inc_batch_get_write_route_requests(total);

    auto response = master_service_.BatchGetWriteRoute(req);

    size_t failure_count = 0;
    for (size_t i = 0; i < response.error_codes.size(); ++i) {
        if (response.error_codes[i] != ErrorCode::OK) {
            failure_count++;
            LOG(ERROR) << "BatchGetWriteRoute failed for key '" << req.keys[i]
                       << "': " << toString(response.error_codes[i]);
        }
    }
    if (failure_count == total && total > 0) {
        MasterMetricManager::instance().inc_batch_get_write_route_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance()
            .inc_batch_get_write_route_partial_success(failure_count);
    }
    timer.LogResponse("total=", total, ", success=", total - failure_count,
                      ", failures=", failure_count);
    return response;
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
WrappedP2PMasterService::BatchRemoveReplica(
    const BatchRemoveReplicaRequest& req) {
    ScopedVLogTimer timer(1, "BatchRemoveReplica");
    const size_t total_requests = req.segment_ids.size();
    timer.LogRequest("key=", req.key, "segment_count=", total_requests);
    MasterMetricManager::instance().inc_batch_remove_replica_requests(
        total_requests);

    auto results = master_service_.BatchRemoveReplica(req);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchRemoveReplica failed for key '" << req.key
                       << "', segment_id: " << req.segment_ids[i] << ": "
                       << toString(error);
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

BatchSyncReplicaResponse WrappedP2PMasterService::BatchSyncReplica(
    const BatchSyncReplicaRequest& req) {
    ScopedVLogTimer timer(1, "BatchSyncReplica");
    timer.LogRequest("client_id=", req.client_id,
                     ", adds=", req.add_keys.size(),
                     ", removes=", req.remove_keys.size());

    auto response = master_service_.BatchSyncReplica(req);

    size_t add_failures = 0;
    for (auto ec : response.add_results) {
        if (ec != ErrorCode::OK) add_failures++;
    }
    size_t remove_failures = 0;
    for (auto ec : response.remove_results) {
        if (ec != ErrorCode::OK) remove_failures++;
    }

    MasterMetricManager::instance().inc_add_replica_requests(
        req.add_keys.size());
    MasterMetricManager::instance().inc_add_replica_failures(add_failures);
    MasterMetricManager::instance().inc_remove_replica_requests(
        req.remove_keys.size());
    MasterMetricManager::instance().inc_remove_replica_failures(
        remove_failures);
    timer.LogResponse("add_failures=", add_failures,
                      ", remove_failures=", remove_failures);
    return response;
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::SetSyncCompleted(
    UUID client_id) {
    ScopedVLogTimer timer(1, "SetSyncCompleted");
    timer.LogRequest("client_id=", client_id);

    auto result = master_service_.SetSyncCompleted(client_id);
    if (!result) {
        LOG(ERROR) << "SetSyncCompleted failed: " << toString(result.error());
    }
    return result;
}

}  // namespace mooncake
