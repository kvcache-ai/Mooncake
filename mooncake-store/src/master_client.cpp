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

namespace mooncake {

using namespace coro_rpc;
using namespace async_simple::coro;

MasterClient::MasterClient() = default;
MasterClient::~MasterClient() = default;

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "MasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    if (client_addr_param_ == master_addr) {
        auto client = client_accessor_.GetClient();
        auto result = coro::syncAwait(client->connect(master_addr));
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
        auto client = std::make_shared<coro_rpc_client>();
        auto result = coro::syncAwait(client->connect(master_addr));
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

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::ExistKey>(object_key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<bool, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to check key existence: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> MasterClient::BatchExistKey(
    const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return std::vector<tl::expected<bool, ErrorCode>>(
            object_keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
    }
    auto request_result =
        client->send_request<&WrappedMasterService::BatchExistKey>(object_keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<tl::expected<bool, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to check batch key existence: "
                           << result.error().msg;
                std::vector<tl::expected<bool, ErrorCode>> error_results;
                error_results.reserve(object_keys.size());
                for (size_t i = 0; i < object_keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());

    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::GetReplicaList(const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", object_key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::GetReplicaList>(object_key);

    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get replica list: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("keys_count=", object_keys.size());

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            error_results;
        error_results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            error_results.emplace_back(
                tl::make_unexpected(ErrorCode::RPC_FAIL));
        }
        return error_results;
    }

    auto request_result =
        client->send_request<&WrappedMasterService::BatchGetReplicaList>(
            object_keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get batch replica list: "
                           << result.error().msg;
                std::vector<
                    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
                    error_results;
                error_results.reserve(object_keys.size());
                for (size_t i = 0; i < object_keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());

    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::PutStart(const std::string& key,
                       const std::vector<size_t>& slice_lengths,
                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::PutStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    // Convert size_t to uint64_t for RPC
    std::vector<uint64_t> rpc_slice_lengths;
    rpc_slice_lengths.reserve(slice_lengths.size());
    for (const auto& length : slice_lengths) {
        rpc_slice_lengths.push_back(length);
    }

    auto request_result = client->send_request<&WrappedMasterService::PutStart>(
        key, rpc_slice_lengths, config);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to start put operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
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

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            error_results(keys.size(),
                          tl::make_unexpected(ErrorCode::RPC_FAIL));
        return error_results;
    }

    auto request_result =
        client->send_request<&WrappedMasterService::BatchPutStart>(
            keys, slice_lengths, config);

    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                // create a vector full of error
                std::vector<
                    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
                    error_results(keys.size(),
                                  tl::make_unexpected(ErrorCode::RPC_FAIL));
                LOG(ERROR) << "Failed to start batch put operation, error"
                           << result.error().msg;
                co_return error_results;
            }
            co_return result->result();
        }());

    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutEnd(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutEnd");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::PutEnd>(key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to end put operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", keys.size());

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<void, ErrorCode>> error_results;
        error_results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            error_results.emplace_back(
                tl::make_unexpected(ErrorCode::RPC_FAIL));
        }
        return error_results;
    }

    auto request_result =
        client->send_request<&WrappedMasterService::BatchPutEnd>(keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<tl::expected<void, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to end batch put operation: "
                           << result.error().msg;
                std::vector<tl::expected<void, ErrorCode>> error_results;
                error_results.reserve(keys.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutRevoke(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutRevoke");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::PutRevoke>(key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to revoke put operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<void, ErrorCode>> error_results;
        error_results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            error_results.emplace_back(
                tl::make_unexpected(ErrorCode::RPC_FAIL));
        }
        return error_results;
    }

    auto request_result =
        client->send_request<&WrappedMasterService::BatchPutRevoke>(keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<tl::expected<void, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to revoke batch put operation: "
                           << result.error().msg;
                std::vector<tl::expected<void, ErrorCode>> error_results;
                error_results.reserve(keys.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::Remove(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::Remove>(key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to remove object: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveAll() {
    ScopedVLogTimer timer(1, "MasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects");

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::RemoveAll>();
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<long, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to remove all objects: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountSegment(
    const Segment& segment, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("base=", segment.base, ", size=", segment.size,
                     ", name=", segment.name, ", id=", segment.id,
                     ", client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
        Lazy<async_rpc_result<tl::expected<void, ErrorCode>>> handler =
            co_await client->send_request<&WrappedMasterService::MountSegment>(
                segment, client_id);
        async_rpc_result<tl::expected<void, ErrorCode>> result =
            co_await handler;
        if (!result) {
            LOG(ERROR) << "Failed to mount segment due to rpc error";
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        co_return result->result();
    }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::ReMountSegment");
    timer.LogRequest("segments_num=", segments.size(),
                     ", client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
        Lazy<async_rpc_result<tl::expected<void, ErrorCode>>> handler =
            co_await client
                ->send_request<&WrappedMasterService::ReMountSegment>(
                    segments, client_id);
        async_rpc_result<tl::expected<void, ErrorCode>> result =
            co_await handler;
        if (!result) {
            LOG(ERROR) << "Failed to remount segment due to rpc error";
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        co_return result->result();
    }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::UnmountSegment>(segment_id,
                                                                    client_id);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to unmount segment: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::pair<ViewVersionId, ClientStatus>, ErrorCode>
MasterClient::Ping(const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::Ping");
    timer.LogRequest("client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::Ping>(client_id);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<tl::expected<std::pair<ViewVersionId, ClientStatus>,
                                         ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to ping master: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> MasterClient::GetFsdir() {
    ScopedVLogTimer timer(1, "MasterClient::GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::GetFsdir>();
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<tl::expected<std::string, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get fsdir: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
