#include "client_rpc_service.h"

#include <utility>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

namespace {

size_t CalculateBufferSize(const std::vector<RemoteBufferDesc>& buffers) {
    size_t total = 0;
    for (const auto& buf : buffers) total += buf.size;
    return total;
}

bool IsValidRequest(const RemoteReadRequest& request) {
    if (request.key.empty()) {
        LOG(ERROR) << "RemoteReadRequest: empty key";
        return false;
    }
    if (request.dest_buffers.empty()) {
        LOG(ERROR) << "RemoteReadRequest: empty buffers";
        return false;
    }
    for (const auto& buf : request.dest_buffers) {
        if (buf.size == 0 || buf.addr == 0) {
            LOG(ERROR) << "RemoteReadRequest: invalid buffer (zero size or "
                          "null address)";
            return false;
        }
    }
    return true;
}

bool IsValidRequest(const RemoteWriteRequest& request) {
    if (request.key.empty()) {
        LOG(ERROR) << "RemoteWriteRequest: empty key";
        return false;
    }
    if (request.src_buffers.empty()) {
        LOG(ERROR) << "RemoteWriteRequest: empty buffers";
        return false;
    }
    for (const auto& buf : request.src_buffers) {
        if (buf.size == 0 || buf.addr == 0) {
            LOG(ERROR) << "RemoteWriteRequest: invalid buffer (zero size or "
                          "null address)";
            return false;
        }
    }
    return true;
}

bool IsValidRequest(const PreWriteRequest& request) {
    if (request.key.empty() || request.size_bytes == 0) {
        LOG(ERROR) << "PreWriteRequest: invalid key or size";
        return false;
    }
    return true;
}

bool IsValidRequest(const WriteCommitRequest& request) {
    if (request.key.empty() || IsZeroUuid(request.pending_write_token)) {
        LOG(ERROR) << "WriteCommitRequest: invalid key or token";
        return false;
    }
    return true;
}

bool IsValidRequest(const PinKeyRequest& request) {
    if (request.key.empty()) {
        LOG(ERROR) << "PinKeyRequest: empty key";
        return false;
    }
    return true;
}

bool IsValidRequest(const UnPinKeyRequest& request) {
    if (request.key.empty() || IsZeroUuid(request.pin_token)) {
        LOG(ERROR) << "UnPinKeyRequest: invalid key or token";
        return false;
    }
    return true;
}

}  // anonymous namespace

ClientRpcService::ClientRpcService(DataManager& data_manager,
                                   P2PClientMetric* metrics)
    : data_manager_(data_manager), metrics_(metrics) {}

tl::expected<void, ErrorCode> ClientRpcService::ReadRemoteData(
    const RemoteReadRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::ReadRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.dest_buffers.size());

    if (metrics_) {
        metrics_->peer_request.get_requests.inc();
    }
    Stopwatch sw;

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request.get_failures.inc();
            metrics_->peer_request.get_latency_failure.observe(sw.elapsed_us());
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Delegate to DataManager
    auto result =
        data_manager_.ReadRemoteData(request.key, request.dest_buffers);

    if (!result.has_value()) {
        LOG(ERROR) << "ReadRemoteData failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
            data_manager_.RectifyReadRoute(request.key);
        }
        if (metrics_) {
            if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
                metrics_->peer_request.get_misses.inc();
            } else {
                metrics_->peer_request.get_failures.inc();
            }
            metrics_->peer_request.get_latency_failure.observe(sw.elapsed_us());
        }
        return result;
    }

    // Record successful get: hits + bytes + latency
    if (metrics_) {
        metrics_->peer_request.get_hits.inc();
        metrics_->peer_request.get_bytes.inc(
            CalculateBufferSize(request.dest_buffers));
        metrics_->peer_request.get_latency_success.observe(sw.elapsed_us());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<UUID, ErrorCode> ClientRpcService::WriteRemoteData(
    const RemoteWriteRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::WriteRemoteData");
    timer.LogRequest("key=", request.key,
                     "buffer_count=", request.src_buffers.size());

    if (metrics_) {
        metrics_->peer_request.put_requests.inc();
    }
    Stopwatch sw;

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request.put_failures.inc();
            metrics_->peer_request.put_latency_failure.observe(sw.elapsed_us());
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Delegate to DataManager
    auto result = data_manager_.WriteRemoteData(
        request.key, request.src_buffers, request.target_tier_id);

    if (!result.has_value()) {
        LOG(ERROR) << "WriteRemoteData failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        if (metrics_) {
            metrics_->peer_request.put_failures.inc();
            metrics_->peer_request.put_latency_failure.observe(sw.elapsed_us());
        }
        return result;
    }

    // Record successful put: bytes + latency
    if (metrics_) {
        metrics_->peer_request.put_bytes.inc(
            CalculateBufferSize(request.src_buffers));
        metrics_->peer_request.put_latency_success.observe(sw.elapsed_us());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return result;
}

tl::expected<PreWriteResponse, ErrorCode> ClientRpcService::PreWrite(
    const PreWriteRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::PreWrite");
    timer.LogRequest("key=", request.key, "size_bytes=", request.size_bytes);

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = data_manager_.PreWrite(request.key, request.size_bytes,
                                         request.target_tier_id);
    if (!result) {
        LOG(ERROR) << "PreWrite failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        return tl::make_unexpected(result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return std::move(*result);
}

tl::expected<void, ErrorCode> ClientRpcService::WriteCommit(
    const WriteCommitRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::WriteCommit");
    timer.LogRequest("key=", request.key);

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result =
        data_manager_.WriteCommit(request.key, request.pending_write_token);
    if (!result) {
        LOG(ERROR) << "WriteCommit failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        return result;
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<PinKeyResponse, ErrorCode> ClientRpcService::PinKey(
    const PinKeyRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::PinKey");
    timer.LogRequest("key=", request.key);

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = data_manager_.PinKey(request.key, request.target_tier_id);
    if (!result) {
        LOG(ERROR) << "PinKey failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        return tl::make_unexpected(result.error());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return std::move(*result);
}

tl::expected<void, ErrorCode> ClientRpcService::UnPinKey(
    const UnPinKeyRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::UnPinKey");
    timer.LogRequest("key=", request.key);

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = data_manager_.UnPinKey(request.key, request.pin_token);
    if (!result) {
        LOG(ERROR) << "UnPinKey failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        return result;
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              ClientRpcService& service) {
    server.register_handler<&ClientRpcService::ReadRemoteData>(&service);
    server.register_handler<&ClientRpcService::WriteRemoteData>(&service);
    server.register_handler<&ClientRpcService::PreWrite>(&service);
    server.register_handler<&ClientRpcService::WriteCommit>(&service);
    server.register_handler<&ClientRpcService::PinKey>(&service);
    server.register_handler<&ClientRpcService::UnPinKey>(&service);
}

}  // namespace mooncake
