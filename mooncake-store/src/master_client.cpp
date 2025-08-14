#include "master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <string>
#include <vector>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"

#include <source_location>

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

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> MasterClient::invoke_rpc(Args&&... args) {
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    // Increment RPC counter
    if (metrics_) {
        metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
    }

    auto start_time = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<ServiceMethod>(std::forward<Args>(args)...);

    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto result = co_await co_await request_result;
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
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        return std::vector<tl::expected<ResultType, ErrorCode>>(
            input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
    }

    // Increment RPC counter
    if (metrics_) {
        metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
    }

    auto start_time = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<ServiceMethod>(std::forward<Args>(args)...);
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<
                  std::vector<tl::expected<ResultType, ErrorCode>>> {
            auto result = co_await co_await request_result;
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

    auto location = std::source_location::current();
    auto name = location.function_name();
    LOG(INFO) << "Connecting to master at " << master_addr << " from " << name;

    MutexLocker lock(&connect_mutex_);
    if (client_addr_param_ == master_addr) {
        auto client = client_accessor_.GetClient();
        auto result =
            async_simple::coro::syncAwait(client->connect(master_addr));
        if (result.val() != 0) {
            LOG(ERROR) << "Failed to connect to master: " << result.message();
            timer.LogResponse("error_code=", ErrorCode::RPC_FAIL);
            return ErrorCode::RPC_FAIL;
        }
        timer.LogResponse("error_code=", ErrorCode::OK);
        return ErrorCode::OK;
    } else {
        // Once connected to address A, the coro_rpc_client does not support
        // connect to a new address B. So we need to create a new
        // coro_rpc_client if the address is different from the current one.
        auto client = std::make_shared<coro_rpc::coro_rpc_client>();
        auto result =
            async_simple::coro::syncAwait(client->connect(master_addr));
        if (result.val() != 0) {
            LOG(ERROR) << "Failed to connect to master: " << result.message();
            timer.LogResponse("error_code=", ErrorCode::RPC_FAIL);
            return ErrorCode::RPC_FAIL;
        }
        // Set the client to the accessor and update the address parameter
        client_accessor_.SetClient(client);
        client_addr_param_ = master_addr;
        timer.LogResponse("error_code=", ErrorCode::OK);
        return ErrorCode::OK;
    }
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

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::GetReplicaList(const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedMasterService::GetReplicaList,
                             std::vector<Replica::Descriptor>>(object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchGetReplicaList,
                                   std::vector<Replica::Descriptor>>(
        object_keys.size(), object_keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::PutStart(const std::string& key,
                       const std::vector<size_t>& slice_lengths,
                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::PutStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

    // Convert size_t to uint64_t for RPC
    std::vector<uint64_t> rpc_slice_lengths;
    rpc_slice_lengths.reserve(slice_lengths.size());
    for (const auto& length : slice_lengths) {
        rpc_slice_lengths.push_back(length);
    }

    auto result = invoke_rpc<&WrappedMasterService::PutStart,
                             std::vector<Replica::Descriptor>>(
        key, rpc_slice_lengths, config);
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

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchPutStart,
                                   std::vector<Replica::Descriptor>>(
        keys.size(), keys, slice_lengths, config);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutEnd(const std::string& key,
                                                   ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::PutEnd");
    timer.LogRequest("key=", key);

    auto result =
        invoke_rpc<&WrappedMasterService::PutEnd, void>(key, replica_type);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchPutEnd, void>(
        keys.size(), keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutRevoke(
    const std::string& key, ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "MasterClient::PutRevoke");
    timer.LogRequest("key=", key);

    auto result =
        invoke_rpc<&WrappedMasterService::PutRevoke, void>(key, replica_type);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchPutRevoke, void>(
        keys.size(), keys);
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::Remove(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key);

    auto result = invoke_rpc<&WrappedMasterService::Remove, void>(key);
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

tl::expected<void, ErrorCode> MasterClient::MountSegment(
    const Segment& segment, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("base=", segment.base, ", size=", segment.size,
                     ", name=", segment.name, ", id=", segment.id,
                     ", client_id=", client_id);

    auto result = invoke_rpc<&WrappedMasterService::MountSegment, void>(
        segment, client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::ReMountSegment");
    timer.LogRequest("segments_num=", segments.size(),
                     ", client_id=", client_id);

    auto result = invoke_rpc<&WrappedMasterService::ReMountSegment, void>(
        segments, client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id);

    auto result = invoke_rpc<&WrappedMasterService::UnmountSegment, void>(
        segment_id, client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<PingResponse, ErrorCode> MasterClient::Ping(
    const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::Ping");
    timer.LogRequest("client_id=", client_id);

    auto result =
        invoke_rpc<&WrappedMasterService::Ping, PingResponse>(client_id);
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

}  // namespace mooncake
