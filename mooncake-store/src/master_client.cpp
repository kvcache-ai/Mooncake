#include "master_client.h"

#include <string>
#include <vector>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

template <>
struct RpcNameTraits<&WrappedMasterService::ExistKey> {
    static constexpr const char* value = "ExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchExistKey> {
    static constexpr const char* value = "BatchExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CalcCacheStats> {
    static constexpr const char* value = "CalcCacheStats";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchQueryIp> {
    static constexpr const char* value = "BatchQueryIp";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaListByRegex> {
    static constexpr const char* value = "GetReplicaListByRegex";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaList> {
    static constexpr const char* value = "GetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchGetReplicaList> {
    static constexpr const char* value = "BatchGetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedMasterService::Remove> {
    static constexpr const char* value = "Remove";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RemoveByRegex> {
    static constexpr const char* value = "RemoveByRegex";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RemoveAll> {
    static constexpr const char* value = "RemoveAll";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MountSegment> {
    static constexpr const char* value = "MountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UnmountSegment> {
    static constexpr const char* value = "UnmountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::Heartbeat> {
    static constexpr const char* value = "Heartbeat";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RegisterClient> {
    static constexpr const char* value = "RegisterClient";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ServiceReady> {
    static constexpr const char* value = "ServiceReady";
};

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "MasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    if (client_addr_param_ != master_addr) {
        // WARNING: The existing client pool cannot be erased. So if there are a
        // lot of different addresses, there will be resource leak problems.
        auto client_pool = client_pools_->at(master_addr);
        client_accessor_.SetClientPool(client_pool);
        client_addr_param_ = master_addr;
    }
    auto pool = client_accessor_.GetClientPool();
    // The client pool does not have native connection check method, so we need
    // to use custom ServiceReady API.
    auto result =
        invoke_rpc<&WrappedMasterService::ServiceReady, std::string>();
    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        return result.error();
    }
    // Check if server version matches client version
    std::string server_version = result.value();
    std::string client_version = GetMooncakeStoreVersion();
    if (server_version != client_version) {
        LOG(ERROR) << "Version mismatch: server=" << server_version
                   << " client=" << client_version;
        timer.LogResponse("error_code=", ErrorCode::INVALID_VERSION);
        return ErrorCode::INVALID_VERSION;
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return ErrorCode::OK;
}

tl::expected<bool, ErrorCode> MasterClient::ExistKey(
    const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedMasterService::ExistKey, bool>(object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> MasterClient::BatchExistKey(
    const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchExistKey, bool>(
        object_keys.size(), object_keys);
    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<GetReplicaListResponse, ErrorCode> MasterClient::GetReplicaList(
    const std::string& key, const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", key);

    auto result = invoke_rpc<&WrappedMasterService::GetReplicaList,
                             GetReplicaListResponse>(key, config);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string>& keys,
                                  const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("requests_count=", keys.size());

    if (keys.empty()) {
        return {};
    }

    auto result = invoke_rpc<
        &WrappedMasterService::BatchGetReplicaList,
        std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>>(keys,
                                                                      config);
    if (result.has_value()) {
        timer.LogResponse("result=", result.value().size(), " requests");
    }
    return result.value();
}

tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
MasterClient::CalcCacheStats() {
    return invoke_rpc<&WrappedMasterService::CalcCacheStats,
                      MasterMetricManager::CacheHitStatDict>();
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
MasterClient::BatchQueryIp(const std::vector<UUID>& client_ids) {
    ScopedVLogTimer timer(1, "MasterClient::BatchQueryIp");
    timer.LogRequest("client_ids_count=", client_ids.size());

    auto result = invoke_rpc<
        &WrappedMasterService::BatchQueryIp,
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>>(
        client_ids);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
MasterClient::GetReplicaListByRegex(const std::string& str) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaListByRegex");
    timer.LogRequest("Regex=", str);

    auto result = invoke_rpc<
        &WrappedMasterService::GetReplicaListByRegex,
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>>(str);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::Remove(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key);

    auto result = invoke_rpc<&WrappedMasterService::Remove, void>(key);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveByRegex(
    const std::string& str) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveByRegex");
    timer.LogRequest("key=", str);

    auto result = invoke_rpc<&WrappedMasterService::RemoveByRegex, long>(str);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveAll() {
    ScopedVLogTimer timer(1, "MasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects");

    auto result = invoke_rpc<&WrappedMasterService::RemoveAll, long>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountSegment(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::UnmountSegment, void>(
        segment_id, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<HeartbeatResponse, ErrorCode> MasterClient::Heartbeat(
    const HeartbeatRequest& req) {
    ScopedVLogTimer timer(1, "MasterClient::Heartbeat");
    timer.LogRequest("client_id=", client_id_);

    auto result =
        invoke_rpc<&WrappedMasterService::Heartbeat, HeartbeatResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountSegment(
    const Segment& segment) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("segment_name=", segment.name, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::MountSegment, void>(
        segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<RegisterClientResponse, ErrorCode> MasterClient::RegisterClient(
    const std::vector<Segment>& segments) {
    ScopedVLogTimer timer(1, "MasterClient::RegisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", segments_count=", segments.size());

    RegisterClientRequest req;
    req.client_id = client_id_;
    req.segments = segments;

    auto result = invoke_rpc<&WrappedMasterService::RegisterClient,
                             RegisterClientResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake