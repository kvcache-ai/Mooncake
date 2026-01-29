#include "centralized_master_client.h"
#include "centralized_rpc_service.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::GetReplicaList> {
    static constexpr const char* value = "GetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::BatchGetReplicaList> {
    static constexpr const char* value = "BatchGetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::PutStart> {
    static constexpr const char* value = "PutStart";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::BatchPutStart> {
    static constexpr const char* value = "BatchPutStart";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::PutEnd> {
    static constexpr const char* value = "PutEnd";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::BatchPutEnd> {
    static constexpr const char* value = "BatchPutEnd";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::PutRevoke> {
    static constexpr const char* value = "PutRevoke";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::BatchPutRevoke> {
    static constexpr const char* value = "BatchPutRevoke";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::MountSegment> {
    static constexpr const char* value = "MountSegment";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::ReMountSegment> {
    static constexpr const char* value = "ReMountSegment";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::GetFsdir> {
    static constexpr const char* value = "GetFsdir";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::GetStorageConfig> {
    static constexpr const char* value = "GetStorageConfig";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::MountLocalDiskSegment> {
    static constexpr const char* value = "MountLocalDiskSegment";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::OffloadObjectHeartbeat> {
    static constexpr const char* value = "OffloadObjectHeartbeat";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::NotifyOffloadSuccess> {
    static constexpr const char* value = "NotifyOffloadSuccess";
};

tl::expected<GetReplicaListResponse, ErrorCode>
CentralizedMasterClient::GetReplicaList(const std::string& object_key) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::GetReplicaList");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedCentralizedMasterService::GetReplicaList,
                             GetReplicaListResponse>(object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
CentralizedMasterClient::BatchGetReplicaList(
    const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::BatchGetReplicaList");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result =
        invoke_batch_rpc<&WrappedCentralizedMasterService::BatchGetReplicaList,
                         GetReplicaListResponse>(object_keys.size(),
                                                 object_keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
CentralizedMasterClient::PutStart(const std::string& key,
                                  const std::vector<size_t>& slice_lengths,
                                  const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::PutStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

    uint64_t total_slice_length = 0;
    for (const auto& slice_length : slice_lengths) {
        total_slice_length += slice_length;
    }

    auto result = invoke_rpc<&WrappedCentralizedMasterService::PutStart,
                             std::vector<Replica::Descriptor>>(
        client_id_, key, total_slice_length, config);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
CentralizedMasterClient::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& batch_slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::BatchPutStart");
    timer.LogRequest("keys_count=", keys.size());

    std::vector<uint64_t> total_slice_lengths;
    total_slice_lengths.reserve(batch_slice_lengths.size());
    for (const auto& slice_lengths : batch_slice_lengths) {
        uint64_t total_slice_length = 0;
        for (const auto& slice_length : slice_lengths) {
            total_slice_length += slice_length;
        }
        total_slice_lengths.emplace_back(total_slice_length);
    }

    auto result =
        invoke_batch_rpc<&WrappedCentralizedMasterService::BatchPutStart,
                         std::vector<Replica::Descriptor>>(
            keys.size(), client_id_, keys, total_slice_lengths, config);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> CentralizedMasterClient::PutEnd(
    const std::string& key, ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::PutEnd");
    timer.LogRequest("key=", key);

    auto result = invoke_rpc<&WrappedCentralizedMasterService::PutEnd, void>(
        client_id_, key, replica_type);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> CentralizedMasterClient::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", keys.size());

    auto result =
        invoke_batch_rpc<&WrappedCentralizedMasterService::BatchPutEnd, void>(
            keys.size(), client_id_, keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> CentralizedMasterClient::PutRevoke(
    const std::string& key, ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::PutRevoke");
    timer.LogRequest("key=", key);

    auto result = invoke_rpc<&WrappedCentralizedMasterService::PutRevoke, void>(
        client_id_, key, replica_type);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>>
CentralizedMasterClient::BatchPutRevoke(const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

    auto result =
        invoke_batch_rpc<&WrappedCentralizedMasterService::BatchPutRevoke,
                         void>(keys.size(), client_id_, keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> CentralizedMasterClient::MountSegment(
    const Segment& segment) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::MountSegment");
    timer.LogRequest("base=", segment.base, ", size=", segment.size,
                     ", name=", segment.name, ", id=", segment.id,
                     ", client_id=", client_id_);

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::MountSegment, void>(
            segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> CentralizedMasterClient::ReMountSegment(
    const std::vector<Segment>& segments) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::ReMountSegment");
    timer.LogRequest("segments_num=", segments.size(),
                     ", client_id=", client_id_);

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::ReMountSegment, void>(
            segments, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> CentralizedMasterClient::GetFsdir() {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::GetFsdir, std::string>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
CentralizedMasterClient::GetStorageConfig() {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::GetStorageConfig");
    timer.LogRequest("action=get_storage_config");

    auto result = invoke_rpc<&WrappedCentralizedMasterService::GetStorageConfig,
                             GetStorageConfigResponse>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> CentralizedMasterClient::MountLocalDiskSegment(
    const UUID& client_id, bool enable_offloading) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::MountLocalDiskSegment");
    timer.LogRequest("client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::MountLocalDiskSegment,
                   void>(client_id, enable_offloading);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
CentralizedMasterClient::OffloadObjectHeartbeat(const UUID& client_id,
                                                bool enable_offloading) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::OffloadObjectHeartbeat");
    timer.LogRequest("client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::OffloadObjectHeartbeat,
                   std::unordered_map<std::string, int64_t>>(client_id,
                                                             enable_offloading);
    return result;
}

tl::expected<void, ErrorCode> CentralizedMasterClient::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::NotifyOffloadSuccess");
    timer.LogRequest("client_id=", client_id, ", keys_count=", keys.size(),
                     ", metadatas_count=", metadatas.size());

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::NotifyOffloadSuccess,
                   void>(client_id, keys, metadatas);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
