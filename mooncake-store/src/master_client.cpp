#include "master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <string>
#include <vector>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>

#include "rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

using namespace coro_rpc;
using namespace async_simple::coro;

MasterClient::MasterClient() = default;
MasterClient::~MasterClient() = default;

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "MasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    auto result = coro::syncAwait(client_.connect(master_addr));
    if (result.val() != 0) {
        LOG(ERROR) << "Failed to connect to master: " << result.message();
        return ErrorCode::INTERNAL_ERROR;
    }
    timer.LogResponse("error_code=", ErrorCode::OK);   
    return ErrorCode::OK;
}

ExistKeyResponse MasterClient::ExistKey(const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);

    auto request_result =
        client_.send_request<&WrappedMasterService::ExistKey>(object_key);
    std::optional<ExistKeyResponse> result = coro::syncAwait(
        [&]() -> coro::Lazy<std::optional<ExistKeyResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to check key existence: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());

    if (!result) {
        auto response = ExistKeyResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }

    timer.LogResponseJson(result.value());
    return result.value();
}

GetReplicaListResponse MasterClient::GetReplicaList(
    const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", object_key);

    auto request_result =
        client_.send_request<&WrappedMasterService::GetReplicaList>(object_key);
    std::optional<GetReplicaListResponse> result = coro::syncAwait(
        [&]() -> coro::Lazy<std::optional<GetReplicaListResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get replica list: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = GetReplicaListResponse{{}, ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

BatchGetReplicaListResponse MasterClient::BatchGetReplicaList(
    const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("action=get_batch_replica_list");

    auto request_result =
        client_.send_request<&WrappedMasterService::BatchGetReplicaList>(
            object_keys);
    std::optional<BatchGetReplicaListResponse> result = coro::syncAwait(
        [&]() -> coro::Lazy<std::optional<BatchGetReplicaListResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get batch replica list: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = BatchGetReplicaListResponse{{}, ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

PutStartResponse MasterClient::PutStart(
    const std::string& key, const std::vector<size_t>& slice_lengths,
    size_t value_length, const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::PutStart");
    timer.LogRequest("key=", key, ", value_length=", value_length,
                     ", slice_count=", slice_lengths.size());

    // Convert size_t to uint64_t for RPC
    std::vector<uint64_t> rpc_slice_lengths;
    rpc_slice_lengths.reserve(slice_lengths.size());
    for (const auto& length : slice_lengths) {
        rpc_slice_lengths.push_back(length);
    }

    auto request_result = client_.send_request<&WrappedMasterService::PutStart>(
        key, value_length, rpc_slice_lengths, config);
    std::optional<PutStartResponse> result =
        coro::syncAwait([&]() -> coro::Lazy<std::optional<PutStartResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to start put operation: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = PutStartResponse{{}, ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

BatchPutStartResponse MasterClient::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::unordered_map<std::string, uint64_t>& value_lengths,
    const std::unordered_map<std::string, std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutStart");
    timer.LogRequest("keys_count=", keys.size());

    auto request_result =
        client_.send_request<&WrappedMasterService::BatchPutStart>(
            keys, value_lengths, slice_lengths, config);
    std::optional<BatchPutStartResponse> result = coro::syncAwait(
        [&]() -> coro::Lazy<std::optional<BatchPutStartResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to start batch put operation: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = BatchPutStartResponse{{}, ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

PutEndResponse MasterClient::PutEnd(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutEnd");
    timer.LogRequest("key=", key);

    auto request_result =
        client_.send_request<&WrappedMasterService::PutEnd>(key);
    std::optional<PutEndResponse> result =
        coro::syncAwait([&]() -> coro::Lazy<std::optional<PutEndResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to end put operation: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = PutEndResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

BatchPutEndResponse MasterClient::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", keys.size());

    auto request_result =
        client_.send_request<&WrappedMasterService::BatchPutEnd>(keys);
    std::optional<BatchPutEndResponse> result = coro::syncAwait(
        [&]() -> coro::Lazy<std::optional<BatchPutEndResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to end batch put operation: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = BatchPutEndResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

PutRevokeResponse MasterClient::PutRevoke(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutRevoke");
    timer.LogRequest("key=", key);

    auto request_result =
        client_.send_request<&WrappedMasterService::PutRevoke>(key);
    std::optional<PutRevokeResponse> result =
        coro::syncAwait([&]() -> coro::Lazy<std::optional<PutRevokeResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to revoke put operation: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = PutRevokeResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

BatchPutRevokeResponse MasterClient::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

    auto request_result =
        client_.send_request<&WrappedMasterService::BatchPutRevoke>(keys);
    std::optional<BatchPutRevokeResponse> result = coro::syncAwait(
        [&]() -> coro::Lazy<std::optional<BatchPutRevokeResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to revoke batch put operation: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = BatchPutRevokeResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

RemoveResponse MasterClient::Remove(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key);

    auto request_result =
        client_.send_request<&WrappedMasterService::Remove>(key);
    std::optional<RemoveResponse> result =
        coro::syncAwait([&]() -> coro::Lazy<std::optional<RemoveResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to remove object: " << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = RemoveResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

RemoveAllResponse MasterClient::RemoveAll() {
    ScopedVLogTimer timer(1, "MasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects");

    auto request_result =
        client_.send_request<&WrappedMasterService::RemoveAll>();
    std::optional<RemoveAllResponse> result =
        coro::syncAwait([&]() -> coro::Lazy<std::optional<RemoveAllResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to remove all objects: "
                          << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());

    if (!result) {
        auto response = RemoveAllResponse{toInt(ErrorCode::RPC_FAIL)};
        timer.LogResponseJson(response);
        return response;
    }

    timer.LogResponseJson(result.value());
    return result.value();
}

MountSegmentResponse MasterClient::MountSegment(const std::string& segment_name,
                                                const void* buffer,
                                                size_t size) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("segment_name=", segment_name, ", buffer=", buffer,
                     ", size=", size);

    std::optional<MountSegmentResponse> result =
        syncAwait([&]() -> coro::Lazy<std::optional<MountSegmentResponse>> {
            Lazy<async_rpc_result<MountSegmentResponse>> handler =
                co_await client_
                    .send_request<&WrappedMasterService::MountSegment>(
                        reinterpret_cast<uint64_t>(buffer),
                        static_cast<uint64_t>(size), segment_name);
            async_rpc_result<MountSegmentResponse> result = co_await handler;
            if (!result) {
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        LOG(ERROR) << "Failed to mount segment due to rpc error";
        auto response = MountSegmentResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

UnmountSegmentResponse MasterClient::UnmountSegment(
    const std::string& segment_name) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegment");
    timer.LogRequest("segment_name=", segment_name);

    auto request_result =
        client_.send_request<&WrappedMasterService::UnmountSegment>(
            segment_name);
    std::optional<UnmountSegmentResponse> result = coro::syncAwait(
        [&]() -> coro::Lazy<std::optional<UnmountSegmentResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to unmount segment: "
                           << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());
    if (!result) {
        auto response = UnmountSegmentResponse{ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }
    timer.LogResponseJson(result.value());
    return result.value();
}

GetSessionIdResponse MasterClient::GetSessionId() {
    ScopedVLogTimer timer(1, "MasterClient::GetSessionId");
    timer.LogRequest("action=get_session_id");

    auto request_result =
        client_.send_request<&WrappedMasterService::GetSessionId>();
    std::optional<GetSessionIdResponse> result =
        coro::syncAwait([&]() -> coro::Lazy<std::optional<GetSessionIdResponse>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get session id: "
                          << result.error().msg;
                co_return std::nullopt;
            }
            co_return result->result();
        }());

    if (!result) {
        auto response = GetSessionIdResponse{{}, ErrorCode::RPC_FAIL};
        timer.LogResponseJson(response);
        return response;
    }

    timer.LogResponseJson(result.value());
    return result.value();
}

}  // namespace mooncake
