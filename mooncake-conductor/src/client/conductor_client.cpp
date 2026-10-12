#include "conductor/client/conductor_client.h"

// <csignal> must precede the ylt headers: coro_io.hpp relies on a transitive
// include that disappears under ASIO_SEPARATE_COMPILATION (top-level build).
#include <csignal>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>

#include <chrono>
#include <mutex>
#include <utility>

#include "conductor/kvevent/conductor_service.h"

namespace mooncake::conductor {

struct ConductorClient::Impl {
    coro_rpc::coro_rpc_client client;
    std::mutex mu;  // serializes calls; see header's thread-safety note
    int64_t request_timeout_ms = 3000;
    bool connected = false;
};

ConductorClient::ConductorClient() : impl_(std::make_unique<Impl>()) {}
ConductorClient::~ConductorClient() { Close(); }

tl::expected<void, ErrorCode> ConductorClient::Setup(
    const ConductorClientConfig& config) {
    std::lock_guard lock(impl_->mu);
    impl_->request_timeout_ms = config.request_timeout_ms;
    // Split "host:port" for coro_rpc_client::connect(host, port).
    const auto colon = config.conductor_addr.rfind(':');
    if (colon == std::string::npos) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const std::string host = config.conductor_addr.substr(0, colon);
    const std::string port = config.conductor_addr.substr(colon + 1);
    auto ec = async_simple::coro::syncAwait(impl_->client.connect(
        host, port, std::chrono::milliseconds(config.connect_timeout_ms)));
    if (ec) {
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    impl_->connected = true;
    return {};
}

void ConductorClient::Close() {
    std::lock_guard lock(impl_->mu);
    impl_->connected = false;
    // ylt 0.5.7 client.close() uses close_socket_async: a CAS keeps it
    // idempotent, the socket closes asynchronously without blocking, and
    // connect_impl resets has_closed_, so Setup() after Close() reconnects.
    impl_->client.close();
}

bool ConductorClient::HealthCheck() {
    auto result = ListServices();
    return result.has_value();
}

namespace {
// Shared call-and-error-mapping template, following the master_client.cpp
// convention: timed_out -> RPC_TIMEOUT, other transport errors -> RPC_FAIL.
// ImplT is deduced so the private nested type ConductorClient::Impl never has
// to be named outside the class.
template <auto Method, typename Ret, typename ImplT, typename... Args>
tl::expected<Ret, ErrorCode> Invoke(ImplT& impl, Args&&... args) {
    std::lock_guard lock(impl.mu);
    if (!impl.connected) {
        return tl::make_unexpected(ErrorCode::CONDUCTOR_UNAVAILABLE);
    }
    auto rpc =
        async_simple::coro::syncAwait(impl.client.template call_for<Method>(
            std::chrono::milliseconds(impl.request_timeout_ms),
            std::forward<Args>(args)...));
    if (!rpc) {
        return tl::make_unexpected(rpc.error().code == coro_rpc::errc::timed_out
                                       ? ErrorCode::RPC_TIMEOUT
                                       : ErrorCode::RPC_FAIL);
    }
    return std::move(rpc.value());
}
}  // namespace

tl::expected<QueryResult, ErrorCode> ConductorClient::Query(
    const QueryRequest& request) {
    return Invoke<&kvevent::ConductorService::Query, QueryResult>(*impl_,
                                                                  request);
}

tl::expected<RegisterResult, ErrorCode> ConductorClient::Register(
    const common::ServiceConfig& config) {
    return Invoke<&kvevent::ConductorService::Register, RegisterResult>(*impl_,
                                                                        config);
}

tl::expected<UnregisterResult, ErrorCode> ConductorClient::Unregister(
    const std::string& instance_id, const std::string& tenant_id, int dp_rank) {
    return Invoke<&kvevent::ConductorService::Unregister, UnregisterResult>(
        *impl_, instance_id, tenant_id, dp_rank);
}

tl::expected<prefixindex::GlobalView, ErrorCode>
ConductorClient::GetGlobalView() {
    return Invoke<&kvevent::ConductorService::GetGlobalView,
                  prefixindex::GlobalView>(*impl_);
}

tl::expected<std::vector<common::ServiceConfig>, ErrorCode>
ConductorClient::ListServices() {
    return Invoke<&kvevent::ConductorService::ListServices,
                  std::vector<common::ServiceConfig>>(*impl_);
}

}  // namespace mooncake::conductor
