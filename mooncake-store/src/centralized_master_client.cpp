#include "centralized_master_client.h"
#include "centralized_rpc_service.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

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
struct RpcNameTraits<&WrappedCentralizedMasterService::BatchReplicaClear> {
    static constexpr const char* value = "BatchReplicaClear";
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

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::CreateCopyTask> {
    static constexpr const char* value = "CreateCopyTask";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::CreateMoveTask> {
    static constexpr const char* value = "CreateMoveTask";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::QueryTask> {
    static constexpr const char* value = "QueryTask";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::FetchTasks> {
    static constexpr const char* value = "FetchTasks";
};

template <>
struct RpcNameTraits<&WrappedCentralizedMasterService::MarkTaskToComplete> {
    static constexpr const char* value = "MarkTaskToComplete";
};

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

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedMasterClient::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::BatchReplicaClear");
    timer.LogRequest("object_keys_count=", object_keys.size(),
                     ", client_id=", client_id,
                     ", segment_name=", segment_name);
    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::BatchReplicaClear,
                   std::vector<std::string>>(object_keys, client_id,
                                             segment_name);
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

tl::expected<UUID, ErrorCode> CentralizedMasterClient::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::CreateCopyTask");
    timer.LogRequest("key=", key, ", targets_size=", targets.size());

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::CreateCopyTask, UUID>(
            key, targets);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UUID, ErrorCode> CentralizedMasterClient::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::CreateMoveTask");
    timer.LogRequest("key=", key, ", source=", source, ", target=", target);

    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::CreateMoveTask, UUID>(
            key, source, target);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<QueryTaskResponse, ErrorCode> CentralizedMasterClient::QueryTask(
    const UUID& task_id) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::QueryTask");
    timer.LogRequest("task_id=", task_id);

    auto result = invoke_rpc<&WrappedCentralizedMasterService::QueryTask,
                             QueryTaskResponse>(task_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<TaskAssignment>, ErrorCode>
CentralizedMasterClient::FetchTasks(size_t batch_size) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::FetchTasks");
    timer.LogRequest("client_id=", client_id_, ", batch_size=", batch_size);
    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::FetchTasks,
                   std::vector<TaskAssignment>>(client_id_, batch_size);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> CentralizedMasterClient::MarkTaskToComplete(
    const TaskCompleteRequest& task_update) {
    ScopedVLogTimer timer(1, "CentralizedMasterClient::MarkTaskToComplete");
    timer.LogRequest("client_id=", client_id_, ", task_id=", task_update.id);
    auto result =
        invoke_rpc<&WrappedCentralizedMasterService::MarkTaskToComplete, void>(
            client_id_, task_update);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
