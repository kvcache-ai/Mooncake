#include "control_plane/rpc_runtime.h"

#include <glog/logging.h>

namespace mooncake {

RpcServer::RpcServer(uint16_t port, unsigned thread_num)
    : port_(port), thread_num_(thread_num) {
    server_ = std::make_unique<coro_rpc::coro_rpc_server>(thread_num_, port_);
}

bool RpcServer::start() {
    if (!server_) return false;
    // Use async_start() instead of start() to avoid blocking the calling
    // thread.
    auto fut = server_->async_start();
    if (fut.hasResult()) {
        // Listen failed - the future resolved immediately.
        auto ec = std::move(fut).get();
        LOG(ERROR) << "RpcServer: failed to start: " << ec.message();
        return false;
    }
    return true;
}

uint16_t RpcServer::getPort() const { return server_ ? server_->port() : 0; }

std::string RpcServer::getListenAddr(const std::string& host_ip) const {
    if (!server_) return "";
    return host_ip + ":" + std::to_string(server_->port());
}

void RpcServer::shutdown() {
    if (server_) {
        server_->stop();
        server_.reset();
    }
}

async_simple::coro::Lazy<std::shared_ptr<coro_rpc::coro_rpc_client>>
RpcClient::getOrCreateClient(std::shared_ptr<SharedState> state,
                             const std::string& addr) {
    // Fast path: lookup under lock.
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        auto it = state->clients.find(addr);
        if (it != state->clients.end()) co_return it->second;
    }

    // Slow path: create + connect outside the lock so that a slow TCP
    // handshake suspends only this coroutine, not other peers'.
    coro_rpc::coro_rpc_client::config config;
    config.connect_timeout_duration = std::chrono::seconds(3);
    config.request_timeout_duration = std::chrono::seconds(5);

    auto client = std::make_shared<coro_rpc::coro_rpc_client>(
        coro_io::get_global_executor(), config);

    auto ec = co_await client->connect(addr);
    if (ec) {
        LOG(ERROR) << "RpcClient: connect failed to " << addr << ": "
                   << ec.message();
        co_return nullptr;
    }

    // Double-check under lock.
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        auto it = state->clients.find(addr);
        if (it != state->clients.end()) co_return it->second;

        state->clients[addr] = client;
        co_return client;
    }
}

void RpcClient::spawn(async_simple::coro::Lazy<void> task) {
    auto executor = coro_io::get_global_executor();
    std::move(task).via(executor).start([](auto&&) {});
}

std::unique_ptr<coro_rpc::coro_rpc_client> RpcClient::createSyncClient() {
    coro_rpc::coro_rpc_client::config config;
    config.connect_timeout_duration = std::chrono::seconds(3);
    return std::make_unique<coro_rpc::coro_rpc_client>(
        coro_io::get_global_executor(), config);
}

bool RpcClient::isConnected(const std::string& addr) const {
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->clients.find(addr) != state_->clients.end();
}

bool RpcClient::tryReconnect(const std::string& addr) {
    // Evict the old entry under the lock.  In-flight coroutines may still
    // hold shared_ptr copies of the old client, so it stays alive until
    // they complete - no use-after-free.
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->clients.erase(addr);
    }

    coro_rpc::coro_rpc_client::config config;
    config.connect_timeout_duration = std::chrono::seconds(3);

    auto client = std::make_shared<coro_rpc::coro_rpc_client>(
        coro_io::get_global_executor(), config);

    auto ec = async_simple::coro::syncAwait(client->connect(addr));
    if (ec) {
        LOG(ERROR) << "RpcClient: reconnect failed to " << addr << ": "
                   << ec.message();
        return false;
    }

    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->clients[addr] = client;
    return true;
}

}  // namespace mooncake
