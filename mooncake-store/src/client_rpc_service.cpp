#include "client_rpc_service.h"
#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

ClientRpcService::ClientRpcService(DataManager& data_manager)
    : data_manager_(data_manager) {}

tl::expected<void, ErrorCode> ClientRpcService::ReadRemoteData(
    const RemoteReadRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::ReadRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.dest_buffers.size());

    if (request.key.empty()) {
        LOG(ERROR) << "ReadRemoteData: empty key";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (request.dest_buffers.empty()) {
        LOG(ERROR) << "ReadRemoteData: empty destination buffers";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers (segment name validation is done in DataManager)
    for (const auto& buffer_desc : request.dest_buffers) {
        if (buffer_desc.size == 0 || buffer_desc.addr == 0) {
            LOG(ERROR)
                << "ReadRemoteData: invalid buffer (zero size or null address)";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    // Delegate to DataManager
    auto result =
        data_manager_.ReadRemoteData(request.key, request.dest_buffers);

    if (!result.has_value()) {
        LOG(ERROR) << "ReadRemoteData failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        return result;
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<void, ErrorCode> ClientRpcService::WriteRemoteData(
    const RemoteWriteRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::WriteRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.src_buffers.size());

    if (request.key.empty()) {
        LOG(ERROR) << "WriteRemoteData: empty key";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (request.src_buffers.empty()) {
        LOG(ERROR) << "WriteRemoteData: empty source buffers";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate buffers (segment name validation is done in DataManager)
    for (const auto& buffer_desc : request.src_buffers) {
        if (buffer_desc.size == 0 || buffer_desc.addr == 0) {
            LOG(ERROR) << "WriteRemoteData: invalid buffer (zero size or null "
                          "address)";
            timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    // Delegate to DataManager
    auto result = data_manager_.WriteRemoteData(
        request.key, request.src_buffers, request.target_tier_id);

    if (!result.has_value()) {
        LOG(ERROR) << "WriteRemoteData failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        return result;
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
ClientRpcService::BatchReadRemoteData(const BatchRemoteReadRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::BatchReadRemoteData");
    timer.LogRequest("key_count=", request.keys.size());

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(request.keys.size());

    if (request.keys.size() != request.dest_buffers_list.size()) {
        LOG(ERROR) << "BatchReadRemoteData: key count (" << request.keys.size()
                   << ") != buffer list count ("
                   << request.dest_buffers_list.size() << ")";
        for (size_t i = 0; i < request.keys.size(); ++i) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Process each key sequentially
    for (size_t i = 0; i < request.keys.size(); ++i) {
        RemoteReadRequest single_request;
        single_request.key = request.keys[i];
        single_request.dest_buffers = request.dest_buffers_list[i];

        auto result = ReadRemoteData(single_request);
        if (!result.has_value()) {
            LOG(ERROR) << "BatchReadRemoteData failed for key: "
                       << single_request.key
                       << ", error: " << toString(result.error());
        }
        results.push_back(result);
    }

    timer.LogResponse("processed_count=", request.keys.size());
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
ClientRpcService::BatchWriteRemoteData(const BatchRemoteWriteRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::BatchWriteRemoteData");
    timer.LogRequest("key_count=", request.keys.size());

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(request.keys.size());

    if (request.keys.size() != request.src_buffers_list.size()) {
        LOG(ERROR) << "BatchWriteRemoteData: key count (" << request.keys.size()
                   << ") != buffer list count ("
                   << request.src_buffers_list.size() << ")";
        for (size_t i = 0; i < request.keys.size(); ++i) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    if (request.keys.size() != request.target_tier_ids.size()) {
        LOG(ERROR) << "BatchWriteRemoteData: key count (" << request.keys.size()
                   << ") != tier_id count (" << request.target_tier_ids.size()
                   << ")";
        for (size_t i = 0; i < request.keys.size(); ++i) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Process each key sequentially
    for (size_t i = 0; i < request.keys.size(); ++i) {
        RemoteWriteRequest single_request;
        single_request.key = request.keys[i];
        single_request.src_buffers = request.src_buffers_list[i];
        single_request.target_tier_id = request.target_tier_ids[i];

        auto result = WriteRemoteData(single_request);
        if (!result.has_value()) {
            LOG(ERROR) << "BatchWriteRemoteData failed for key: "
                       << single_request.key
                       << ", error: " << toString(result.error());
        }
        results.push_back(result);
    }

    timer.LogResponse("processed_count=", request.keys.size());
    return results;
}

void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              ClientRpcService& service) {
    server.register_handler<&ClientRpcService::ReadRemoteData>(&service);
    server.register_handler<&ClientRpcService::WriteRemoteData>(&service);
    server.register_handler<&ClientRpcService::BatchReadRemoteData>(&service);
    server.register_handler<&ClientRpcService::BatchWriteRemoteData>(&service);
}

}  // namespace mooncake
