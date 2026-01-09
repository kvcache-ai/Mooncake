#include "client_rpc_service.h"

#include <glog/logging.h>
#include "types.h"

namespace mooncake {

ClientRpcService::ClientRpcService(DataManager& data_manager)
    : data_manager_(data_manager) {
    // Constructor: store reference to DataManager
}

tl::expected<void, ErrorCode> ClientRpcService::ReadRemoteData(
    const RemoteReadRequest& request) {
    // TODO: Implement actual RPC handler logic
    // This should call data_manager_.ReadData(request.key, request.dest_buffers)
    return data_manager_.ReadData(request.key, request.dest_buffers);
}

tl::expected<void, ErrorCode> ClientRpcService::WriteRemoteData(
    const RemoteWriteRequest& request) {
    // TODO: Implement actual RPC handler logic
    // This should call data_manager_.WriteData(request.key, request.src_buffers, request.target_tier_id)
    return data_manager_.WriteData(request.key, request.src_buffers, request.target_tier_id);
}

std::vector<tl::expected<void, ErrorCode>> ClientRpcService::BatchReadRemoteData(
    const BatchRemoteReadRequest& request) {
    // TODO: Implement actual batch RPC handler logic
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(request.keys.size());
    
    for (size_t i = 0; i < request.keys.size(); ++i) {
        if (i < request.dest_buffers_list.size()) {
            auto result = data_manager_.ReadData(request.keys[i],
                                                 request.dest_buffers_list[i]);
            results.push_back(result);
        } else {
            results.push_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> ClientRpcService::BatchWriteRemoteData(
    const BatchRemoteWriteRequest& request) {
    // TODO: Implement actual batch RPC handler logic
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(request.keys.size());
    
    for (size_t i = 0; i < request.keys.size(); ++i) {
        if (i < request.src_buffers_list.size() && i < request.target_tier_ids.size()) {
            auto result = data_manager_.WriteData(request.keys[i],
                                                   request.src_buffers_list[i],
                                                   request.target_tier_ids[i]);
            results.push_back(result);
        } else {
            results.push_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
    }
    return results;
}

void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              ClientRpcService& service) {
    // TODO: Register RPC handlers with coro_rpc_server
    server.register_handler<&ClientRpcService::ReadRemoteData>(&service);
    server.register_handler<&ClientRpcService::WriteRemoteData>(&service);
    server.register_handler<&ClientRpcService::BatchReadRemoteData>(&service);
    server.register_handler<&ClientRpcService::BatchWriteRemoteData>(&service);
}

}  // namespace mooncake
