#include "client_rpc_service.h"

#include <utility>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include "utils.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

namespace {

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
        metrics_->peer_request_metrics.read_remote_data.requests.inc();
    }
    Stopwatch sw;

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request_metrics.read_remote_data.failures.inc();
            metrics_->peer_request_metrics.read_remote_data.latency_failure.observe(
                sw.elapsed_us());
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
                metrics_->peer_request_metrics.read_remote_data.misses.inc();
            } else {
                metrics_->peer_request_metrics.read_remote_data.failures.inc();
            }
            metrics_->peer_request_metrics.read_remote_data.latency_failure.observe(
                sw.elapsed_us());
        }
        return result;
    }

    // Record successful get: latency
    if (metrics_) {
        metrics_->peer_request_metrics.read_remote_data.hits.inc();
        metrics_->peer_request_metrics.read_remote_data.latency_success.observe(
            sw.elapsed_us());
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
        metrics_->peer_request_metrics.write_remote_data.requests.inc();
    }
    Stopwatch sw;

    if (!IsValidRequest(request)) {
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request_metrics.write_remote_data.failures.inc();
            metrics_->peer_request_metrics.write_remote_data.latency_failure.observe(
                sw.elapsed_us());
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
            metrics_->peer_request_metrics.write_remote_data.failures.inc();
            metrics_->peer_request_metrics.write_remote_data.latency_failure.observe(
                sw.elapsed_us());
        }
        return result;
    }

    if (metrics_) {
        metrics_->peer_request_metrics.write_remote_data.latency_success.observe(
            sw.elapsed_us());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return result;
}

tl::expected<PreWriteResponse, ErrorCode> ClientRpcService::PreWrite(
    const PreWriteRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::PreWrite");
    timer.LogRequest("key=", request.key, "size_bytes=", request.size_bytes);

    if (metrics_) {
        metrics_->peer_request_metrics.prewrite.requests.inc();
    }
    Stopwatch sw;

    if (request.key.empty() || request.size_bytes == 0) {
        LOG(ERROR) << "PreWriteRequest: invalid key or size";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request_metrics.prewrite.failures.inc();
            metrics_->peer_request_metrics.prewrite.latency_failure.observe(
                sw.elapsed_us());
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = data_manager_.PreWrite(request.key, request.size_bytes,
                                         request.target_tier_id);
    if (!result) {
        LOG(ERROR) << "PreWrite failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        if (metrics_) {
            metrics_->peer_request_metrics.prewrite.failures.inc();
            metrics_->peer_request_metrics.prewrite.latency_failure.observe(
                sw.elapsed_us());
        }
        return tl::make_unexpected(result.error());
    }

    if (metrics_) {
        metrics_->peer_request_metrics.prewrite.latency_success.observe(
            sw.elapsed_us());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return std::move(*result);
}

tl::expected<void, ErrorCode> ClientRpcService::WriteCommit(
    const WriteCommitRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::WriteCommit");
    timer.LogRequest("key=", request.key);

    if (metrics_) {
        metrics_->peer_request_metrics.write_commit.requests.inc();
    }
    Stopwatch sw;

    if (request.key.empty() || IsZeroUUID(request.write_operation_id)) {
        LOG(ERROR) << "WriteCommitRequest: invalid key or token";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request_metrics.write_commit.failures.inc();
            metrics_->peer_request_metrics.write_commit.latency_failure.observe(
                sw.elapsed_us());
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result =
        data_manager_.WriteCommit(request.key, request.write_operation_id);
    if (!result) {
        LOG(ERROR) << "WriteCommit failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        if (metrics_) {
            metrics_->peer_request_metrics.write_commit.failures.inc();
            metrics_->peer_request_metrics.write_commit.latency_failure.observe(
                sw.elapsed_us());
        }
        return result;
    }

    if (metrics_) {
        metrics_->peer_request_metrics.write_commit.latency_success.observe(
            sw.elapsed_us());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<void, ErrorCode> ClientRpcService::WriteRevoke(
    const WriteRevokeRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::WriteRevoke");
    timer.LogRequest("key=", request.key);

    if (metrics_) {
        metrics_->peer_request_metrics.write_revoke.requests.inc();
    }
    Stopwatch sw;

    if (request.key.empty() || IsZeroUUID(request.write_operation_id)) {
        LOG(ERROR) << "WriteRevokeRequest: invalid key or token";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request_metrics.write_revoke.failures.inc();
            metrics_->peer_request_metrics.write_revoke.latency_failure.observe(
                sw.elapsed_us());
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result =
        data_manager_.WriteRevoke(request.key, request.write_operation_id);
    if (!result) {
        LOG(ERROR) << "WriteRevoke failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        if (metrics_) {
            metrics_->peer_request_metrics.write_revoke.failures.inc();
            metrics_->peer_request_metrics.write_revoke.latency_failure.observe(
                sw.elapsed_us());
        }
        return result;
    }

    if (metrics_) {
        metrics_->peer_request_metrics.write_revoke.latency_success.observe(
            sw.elapsed_us());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<PinKeyResponse, ErrorCode> ClientRpcService::PinKey(
    const PinKeyRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::PinKey");
    timer.LogRequest("key=", request.key);

    if (metrics_) {
        metrics_->peer_request_metrics.pin_key.requests.inc();
    }
    Stopwatch sw;

    if (request.key.empty()) {
        LOG(ERROR) << "PinKeyRequest: empty key";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request_metrics.pin_key.failures.inc();
            metrics_->peer_request_metrics.pin_key.latency_failure.observe(
                sw.elapsed_us());
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result = data_manager_.PinKey(request.key, request.target_tier_id);
    if (!result) {
        LOG(ERROR) << "PinKey failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        if (metrics_) {
            if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
                metrics_->peer_request_metrics.pin_key.misses.inc();
            } else {
                metrics_->peer_request_metrics.pin_key.failures.inc();
            }
            metrics_->peer_request_metrics.pin_key.latency_failure.observe(
                sw.elapsed_us());
        }
        return tl::make_unexpected(result.error());
    }

    if (metrics_) {
        metrics_->peer_request_metrics.pin_key.hits.inc();
        metrics_->peer_request_metrics.pin_key.latency_success.observe(
            sw.elapsed_us());
    }

    timer.LogResponse("error_code=", ErrorCode::OK);
    return std::move(*result);
}

tl::expected<void, ErrorCode> ClientRpcService::UnPinKey(
    const UnPinKeyRequest& request) {
    ScopedVLogTimer timer(1, "ClientRpcService::UnPinKey");
    timer.LogRequest("key=", request.key);

    if (metrics_) {
        metrics_->peer_request_metrics.unpin_key.requests.inc();
    }
    Stopwatch sw;

    if (request.key.empty() || IsZeroUUID(request.read_operation_id)) {
        LOG(ERROR) << "UnPinKeyRequest: invalid key or token";
        timer.LogResponse("error_code=", ErrorCode::INVALID_PARAMS);
        if (metrics_) {
            metrics_->peer_request_metrics.unpin_key.failures.inc();
            metrics_->peer_request_metrics.unpin_key.latency_failure.observe(
                sw.elapsed_us());
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto result =
        data_manager_.UnPinKey(request.key, request.read_operation_id);
    if (!result) {
        LOG(ERROR) << "UnPinKey failed for key: " << request.key
                   << ", error: " << toString(result.error());
        timer.LogResponse("error_code=", result.error());
        if (metrics_) {
            metrics_->peer_request_metrics.unpin_key.failures.inc();
            metrics_->peer_request_metrics.unpin_key.latency_failure.observe(
                sw.elapsed_us());
        }
        return result;
    }

    if (metrics_) {
        metrics_->peer_request_metrics.unpin_key.latency_success.observe(
            sw.elapsed_us());
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
    server.register_handler<&ClientRpcService::WriteRevoke>(&service);
    server.register_handler<&ClientRpcService::PinKey>(&service);
    server.register_handler<&ClientRpcService::UnPinKey>(&service);
}

}  // namespace mooncake
