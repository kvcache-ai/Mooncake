#include "master_client.h"

#include <glog/logging.h>

#include <chrono>
#include <cstdint>

namespace mooncake {

// Template member method for executing RPC calls and handling results
template <typename Req, typename Res, typename... Builders>
ErrorCode MasterClient::ExecuteRpc(
    const std::string& rpc_name,
    grpc::Status (mooncake_store::MasterService::Stub::*method)(
        grpc::ClientContext*, const Req&, Res*),
    Res& response, Builders&&... builders) const {
    // Build the request object
    Req request;
    (builders(request), ...);  // Use fold expression to apply all builders

    // Log request start
    auto start_time = std::chrono::steady_clock::now();
    VLOG(1) << "[MasterClient] operation=RPC_START method=" << rpc_name
            << " request=" << request.ShortDebugString();

    // Execute RPC call
    grpc::ClientContext context;
    auto status = (master_stub_.get()->*method)(&context, request, &response);

    // Calculate elapsed time
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                        end_time - start_time)
                        .count();

    // Handle errors
    if (!status.ok()) {
        LOG(ERROR) << "[MasterClient] operation=RPC_FAIL method=" << rpc_name
                   << " grpc_code=" << status.error_code()
                   << " latency_us=" << duration << " error_message=\""
                   << status.error_message() << "\"";
        return ErrorCode::RPC_FAIL;
    }

    // Log successful response
    VLOG(1) << "[MasterClient] operation=RPC_DONE method=" << rpc_name
            << " latency_us=" << duration
            << " status_code=" << response.status_code();

    // Convert and return error code
    return fromInt(response.status_code());
}

MasterClient::MasterClient() : master_stub_(nullptr) {}

MasterClient::~MasterClient() = default;

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    auto channel =
        grpc::CreateChannel(master_addr, grpc::InsecureChannelCredentials());
    master_stub_ = mooncake_store::MasterService::NewStub(channel);
    if (!master_stub_) {
        LOG(ERROR) << "[MasterClient] operation=CONNECT status=FAILED "
                      "error_message=\"Failed to create master service stub\"";
        return ErrorCode::RPC_FAIL;
    }
    LOG(INFO)
        << "[MasterClient] operation=CONNECT status=SUCCESS master_addr=\""
        << master_addr << "\"";
    return ErrorCode::OK;
}

ErrorCode MasterClient::GetReplicaList(
    const std::string& object_key,
    mooncake_store::GetReplicaListResponse& object_info) const {
    auto err = ExecuteRpc(
        "GetReplicaList", &mooncake_store::MasterService::Stub::GetReplicaList,
        object_info, [&](auto& req) { req.set_key(object_key); });

    if (err != ErrorCode::OK) {
        return err;
    }

    CHECK(object_info.replica_list().empty() == false);

    return ErrorCode::OK;
}

ErrorCode MasterClient::PutStart(
    const std::string& key, const std::vector<size_t>& slice_lengths,
    size_t value_length, const ReplicateConfig& config,
    mooncake_store::PutStartResponse& start_response) const {
    return ExecuteRpc("PutStart",
                      &mooncake_store::MasterService::Stub::PutStart,
                      start_response, [&](auto& req) {
                          req.set_key(key);
                          req.set_value_length(value_length);
                          for (const auto& length : slice_lengths) {
                              req.add_slice_lengths(length);
                          }
                          auto* replica_config = req.mutable_config();
                          replica_config->set_replica_num(config.replica_num);
                      });
}

ErrorCode MasterClient::PutEnd(const std::string& key) const {
    mooncake_store::PutEndResponse response;

    return ExecuteRpc("PutEnd", &mooncake_store::MasterService::Stub::PutEnd,
                      response, [&](auto& req) { req.set_key(key); });
}

ErrorCode MasterClient::PutRevoke(const std::string& key) const {
    mooncake_store::PutRevokeResponse response;

    return ExecuteRpc("PutRevoke",
                      &mooncake_store::MasterService::Stub::PutRevoke, response,
                      [&](auto& req) { req.set_key(key); });
}

ErrorCode MasterClient::Remove(const std::string& key) const {
    mooncake_store::RemoveResponse response;

    return ExecuteRpc("Remove", &mooncake_store::MasterService::Stub::Remove,
                      response, [&](auto& req) { req.set_key(key); });
}

ErrorCode MasterClient::MountSegment(const std::string& segment_name,
                                     const void* buffer, size_t size) const {
    mooncake_store::MountSegmentResponse response;

    return ExecuteRpc("MountSegment",
                      &mooncake_store::MasterService::Stub::MountSegment,
                      response, [&](auto& req) {
                          req.set_segment_name(segment_name);
                          req.set_buffer(reinterpret_cast<uintptr_t>(buffer));
                          req.set_size(size);
                      });
}

ErrorCode MasterClient::UnmountSegment(const std::string& segment_name) const {
    mooncake_store::UnmountSegmentResponse response;

    return ExecuteRpc(
        "UnmountSegment", &mooncake_store::MasterService::Stub::UnmountSegment,
        response, [&](auto& req) { req.set_segment_name(segment_name); });
}

ErrorCode MasterClient::IsExist(const std::string& key) const {
    mooncake_store::GetReplicaListResponse object_info;
    return GetReplicaList(key, object_info);
}

}  // namespace mooncake
