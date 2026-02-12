#include "master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <csignal>
#include <string>
#include <vector>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "master_metric_manager.h"
#include "version.h"

namespace mooncake {

template <auto Method>
struct RpcNameTraits;

template <>
struct RpcNameTraits<&WrappedMasterService::ExistKey> {
    static constexpr const char* value = "ExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchExistKey> {
    static constexpr const char* value = "BatchExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaList> {
    static constexpr const char* value = "GetReplicaList";
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
struct RpcNameTraits<&WrappedMasterService::BatchReplicaClear> {
    static constexpr const char* value = "BatchReplicaClear";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaListByRegex> {
    static constexpr const char* value = "GetReplicaListByRegex";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchGetReplicaList> {
    static constexpr const char* value = "BatchGetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PutStart> {
    static constexpr const char* value = "PutStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchPutStart> {
    static constexpr const char* value = "BatchPutStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PutEnd> {
    static constexpr const char* value = "PutEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchPutEnd> {
    static constexpr const char* value = "BatchPutEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::PutRevoke> {
    static constexpr const char* value = "PutRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchPutRevoke> {
    static constexpr const char* value = "BatchPutRevoke";
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
struct RpcNameTraits<&WrappedMasterService::ReMountSegment> {
    static constexpr const char* value = "ReMountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UnmountSegment> {
    static constexpr const char* value = "UnmountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::Ping> {
    static constexpr const char* value = "Ping";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetFsdir> {
    static constexpr const char* value = "GetFsdir";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetStorageConfig> {
    static constexpr const char* value = "GetStorageConfig";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ServiceReady> {
    static constexpr const char* value = "ServiceReady";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MountLocalDiskSegment> {
    static constexpr const char* value = "MountLocalDiskSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::OffloadObjectHeartbeat> {
    static constexpr const char* value = "OffloadObjectHeartbeat";
};

template <>
struct RpcNameTraits<&WrappedMasterService::NotifyOffloadSuccess> {
    static constexpr const char* value = "NotifyOffloadSuccess";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CopyStart> {
    static constexpr const char* value = "CopyStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CopyEnd> {
    static constexpr const char* value = "CopyEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CopyRevoke> {
    static constexpr const char* value = "CopyRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CacheOnGet> {
    static constexpr const char* value = "CacheOnGet";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MoveStart> {
    static constexpr const char* value = "MoveStart";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MoveEnd> {
    static constexpr const char* value = "MoveEnd";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MoveRevoke> {
    static constexpr const char* value = "MoveRevoke";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CreateCopyTask> {
    static constexpr const char* value = "CreateCopyTask";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CreateMoveTask> {
    static constexpr const char* value = "CreateMoveTask";
};

template <>
struct RpcNameTraits<&WrappedMasterService::QueryTask> {
    static constexpr const char* value = "QueryTask";
};

template <>
struct RpcNameTraits<&WrappedMasterService::FetchTasks> {
    static constexpr const char* value = "FetchTasks";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MarkTaskToComplete> {
    static constexpr const char* value = "MarkTaskToComplete";
};

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::invoke_rpc(Args&&... args) {
    auto pool = client_accessor_.GetClientPool();

    // Increment RPC counter
    if (metrics_) {
        metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
    }

    auto start_time = std::chrono::steady_clock::now();
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Client not available";
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "RPC call failed: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            if (metrics_) {
                auto end_time = std::chrono::steady_clock::now();
                auto latency =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        end_time - start_time);
                metrics_->rpc_latency.observe(
                    {RpcNameTraits<ServiceMethod>::value}, latency.count());
            }
            co_return result->result();
        }());
}

template <auto ServiceMethod, typename ResultType, typename... Args>
std::vector<tl::expected<ResultType, ErrorCode>> MasterClient::invoke_batch_rpc(
    size_t input_size, Args&&... args) {
    auto pool = client_accessor_.GetClientPool();

    // Increment RPC counter
    if (metrics_) {
        metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
    }

    auto start_time = std::chrono::steady_clock::now();
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<
                  std::vector<tl::expected<ResultType, ErrorCode>>> {
            auto ret = co_await pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Client not available";
                co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                    input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "Batch RPC call failed: " << result.error().msg;
                std::vector<tl::expected<ResultType, ErrorCode>> error_results;
                error_results.reserve(input_size);
                for (size_t i = 0; i < input_size; ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            if (metrics_) {
                auto end_time = std::chrono::steady_clock::now();
                auto latency =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        end_time - start_time);
                metrics_->rpc_latency.observe(
                    {RpcNameTraits<ServiceMethod>::value}, latency.count());
            }
            co_return result->result();
        }());
}

MasterClient::~MasterClient() = default;

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

tl::expected<std::vector<std::string>, ErrorCode>
MasterClient::BatchReplicaClear(const std::vector<std::string>& object_keys,
                                const UUID& client_id,
                                const std::string& segment_name) {
    ScopedVLogTimer timer(1, "MasterClient::BatchReplicaClear");
    timer.LogRequest("object_keys_count=", object_keys.size(),
                     ", client_id=", client_id,
                     ", segment_name=", segment_name);
    auto result = invoke_rpc<&WrappedMasterService::BatchReplicaClear,
                             std::vector<std::string>>(object_keys, client_id,
                                                       segment_name);
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

tl::expected<GetReplicaListResponse, ErrorCode> MasterClient::GetReplicaList(
    const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedMasterService::GetReplicaList,
                             GetReplicaListResponse>(object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchGetReplicaList,
                                   GetReplicaListResponse>(object_keys.size(),
                                                           object_keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::PutStart(const std::string& key,
                       const std::vector<size_t>& slice_lengths,
                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::PutStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

    uint64_t total_slice_length = 0;
    for (const auto& slice_length : slice_lengths) {
        total_slice_length += slice_length;
    }

    auto result = invoke_rpc<&WrappedMasterService::PutStart,
                             std::vector<Replica::Descriptor>>(
        client_id_, key, total_slice_length, config);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterClient::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutStart");
    timer.LogRequest("keys_count=", keys.size());

    std::vector<uint64_t> total_slice_lengths;
    total_slice_lengths.reserve(slice_lengths.size());
    for (const auto& slice_lengths : slice_lengths) {
        uint64_t total_slice_length = 0;
        for (const auto& slice_length : slice_lengths) {
            total_slice_length += slice_length;
        }
        total_slice_lengths.emplace_back(total_slice_length);
    }

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchPutStart,
                                   std::vector<Replica::Descriptor>>(
        keys.size(), client_id_, keys, total_slice_lengths, config);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutEnd(const std::string& key,
                                                   ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::PutEnd");
    timer.LogRequest("key=", key);

    auto result = invoke_rpc<&WrappedMasterService::PutEnd, void>(
        client_id_, key, replica_type);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchPutEnd, void>(
        keys.size(), client_id_, keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutRevoke(
    const std::string& key, ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::PutRevoke");
    timer.LogRequest("key=", key);

    auto result = invoke_rpc<&WrappedMasterService::PutRevoke, void>(
        client_id_, key, replica_type);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchPutRevoke, void>(
        keys.size(), client_id_, keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::Remove(const std::string& key,
                                                   bool force) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key, ", force=", force);

    auto result = invoke_rpc<&WrappedMasterService::Remove, void>(key, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveByRegex(
    const std::string& str, bool force) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveByRegex");
    timer.LogRequest("key=", str, ", force=", force);

    auto result =
        invoke_rpc<&WrappedMasterService::RemoveByRegex, long>(str, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveAll(bool force) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);

    auto result = invoke_rpc<&WrappedMasterService::RemoveAll, long>(force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountSegment(
    const Segment& segment) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("base=", segment.base, ", size=", segment.size,
                     ", name=", segment.name, ", id=", segment.id,
                     ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::MountSegment, void>(
        segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::ReMountSegment(
    const std::vector<Segment>& segments) {
    ScopedVLogTimer timer(1, "MasterClient::ReMountSegment");
    timer.LogRequest("segments_num=", segments.size(),
                     ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::ReMountSegment, void>(
        segments, client_id_);
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

tl::expected<PingResponse, ErrorCode> MasterClient::Ping() {
    ScopedVLogTimer timer(1, "MasterClient::Ping");
    timer.LogRequest("client_id=", client_id_);

    auto result =
        invoke_rpc<&WrappedMasterService::Ping, PingResponse>(client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> MasterClient::GetFsdir() {
    ScopedVLogTimer timer(1, "MasterClient::GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto result = invoke_rpc<&WrappedMasterService::GetFsdir, std::string>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
MasterClient::GetStorageConfig() {
    ScopedVLogTimer timer(1, "MasterClient::GetStorageConfig");
    timer.LogRequest("action=get_storage_config");

    auto result = invoke_rpc<&WrappedMasterService::GetStorageConfig,
                             GetStorageConfigResponse>();
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountLocalDiskSegment(
    const UUID& client_id, bool enable_offloading) {
    ScopedVLogTimer timer(1, "MasterClient::MountLocalDiskSegment");
    timer.LogRequest("client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result =
        invoke_rpc<&WrappedMasterService::MountLocalDiskSegment, void>(
            client_id, enable_offloading);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UUID, ErrorCode> MasterClient::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    ScopedVLogTimer timer(1, "MasterClient::CreateCopyTask");
    timer.LogRequest("key=", key, ", targets_size=", targets.size());

    auto result =
        invoke_rpc<&WrappedMasterService::CreateCopyTask, UUID>(key, targets);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UUID, ErrorCode> MasterClient::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    ScopedVLogTimer timer(1, "MasterClient::CreateMoveTask");
    timer.LogRequest("key=", key, ", source=", source, ", target=", target);

    auto result = invoke_rpc<&WrappedMasterService::CreateMoveTask, UUID>(
        key, source, target);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
MasterClient::OffloadObjectHeartbeat(const UUID& client_id,
                                     bool enable_offloading) {
    ScopedVLogTimer timer(1, "MasterClient::OffloadObjectHeartbeat");
    timer.LogRequest("client_id=", client_id,
                     ", enable_offloading=", enable_offloading);

    auto result = invoke_rpc<&WrappedMasterService::OffloadObjectHeartbeat,
                             std::unordered_map<std::string, int64_t>>(
        client_id, enable_offloading);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    ScopedVLogTimer timer(1, "MasterClient::NotifyOffloadSuccess");
    timer.LogRequest("client_id=", client_id, ", keys_count=", keys.size(),
                     ", metadatas_count=", metadatas.size());

    auto result = invoke_rpc<&WrappedMasterService::NotifyOffloadSuccess, void>(
        client_id, keys, metadatas);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<CopyStartResponse, ErrorCode> MasterClient::CopyStart(
    const std::string& key, const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    ScopedVLogTimer timer(1, "MasterClient::CopyStart");
    timer.LogRequest("key=", key, ", src_segment=", src_segment,
                     ", tgt_segments_count=", tgt_segments.size());

    auto result =
        invoke_rpc<&WrappedMasterService::CopyStart, CopyStartResponse>(
            client_id_, key, src_segment, tgt_segments);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<QueryTaskResponse, ErrorCode> MasterClient::QueryTask(
    const UUID& task_id) {
    ScopedVLogTimer timer(1, "MasterClient::QueryTask");
    timer.LogRequest("task_id=", task_id);

    auto result =
        invoke_rpc<&WrappedMasterService::QueryTask, QueryTaskResponse>(
            task_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::CopyEnd(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::CopyEnd");
    timer.LogRequest("key=", key);

    auto result =
        invoke_rpc<&WrappedMasterService::CopyEnd, void>(client_id_, key);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> MasterClient::FetchTasks(
    size_t batch_size) {
    ScopedVLogTimer timer(1, "MasterClient::FetchTasks");
    timer.LogRequest("client_id=", client_id_, ", batch_size=", batch_size);
    auto result =
        invoke_rpc<&WrappedMasterService::FetchTasks,
                   std::vector<TaskAssignment>>(client_id_, batch_size);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::CopyRevoke(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::CopyRevoke");
    timer.LogRequest("key=", key);

    auto result =
        invoke_rpc<&WrappedMasterService::CopyRevoke, void>(client_id_, key);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<CacheOnGetResponse, ErrorCode> MasterClient::CacheOnGet(
    const std::string& key, const std::string& local_segment) {
    ScopedVLogTimer timer(1, "MasterClient::CacheOnGet");
    timer.LogRequest("key=", key, ", local_segment=", local_segment);

    auto result =
        invoke_rpc<&WrappedMasterService::CacheOnGet, CacheOnGetResponse>(
            client_id_, key, local_segment);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<MoveStartResponse, ErrorCode> MasterClient::MoveStart(
    const std::string& key, const std::string& src_segment,
    const std::string& tgt_segment) {
    ScopedVLogTimer timer(1, "MasterClient::MoveStart");
    timer.LogRequest("key=", key, ", src_segment=", src_segment,
                     ", tgt_segment=", tgt_segment);

    auto result =
        invoke_rpc<&WrappedMasterService::MoveStart, MoveStartResponse>(
            client_id_, key, src_segment, tgt_segment);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MoveEnd(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::MoveEnd");
    timer.LogRequest("key=", key);

    auto result =
        invoke_rpc<&WrappedMasterService::MoveEnd, void>(client_id_, key);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MoveRevoke(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::MoveRevoke");
    timer.LogRequest("key=", key);

    auto result =
        invoke_rpc<&WrappedMasterService::MoveRevoke, void>(client_id_, key);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MarkTaskToComplete(
    const TaskCompleteRequest& task_update) {
    ScopedVLogTimer timer(1, "MasterClient::MarkTaskToComplete");
    timer.LogRequest("client_id=", client_id_, ", task_id=", task_update.id);
    auto result = invoke_rpc<&WrappedMasterService::MarkTaskToComplete, void>(
        client_id_, task_update);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
