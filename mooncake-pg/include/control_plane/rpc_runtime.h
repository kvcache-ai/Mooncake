#ifndef MOONCAKE_PG_RPC_RUNTIME_H
#define MOONCAKE_PG_RPC_RUNTIME_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

#include <glog/logging.h>

#include <csignal>

#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <async_simple/coro/SyncAwait.h>
#include <async_simple/coro/Lazy.h>

namespace mooncake {

// RpcServer  - thin wrapper around coro_rpc::coro_rpc_server.

class RpcServer {
   public:
    explicit RpcServer(uint16_t port = 0, unsigned thread_num = 2);

    // Register coro_rpc service handler(s).  coro_rpc expects non-type
    // template parameters (function pointers) and a raw pointer to the
    // service instance.  Caller must keep *impl alive for the server
    // lifetime.
    template <auto First, auto... Rest>
    void registerHandler(util::class_type_t<decltype(First)>* impl) {
        server_->register_handler<First, Rest...>(impl);
    }

    bool start();
    uint16_t getPort() const;
    std::string getListenAddr(const std::string& host_ip) const;
    void shutdown();

   private:
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    uint16_t port_;
    unsigned thread_num_;
};

// RpcClient helpers  - free function coroutines.

namespace rpc_detail {

// Shared state for connection caching (forward declaration).
struct RpcSharedState {
    std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<coro_rpc::coro_rpc_client>>
        clients;
    std::atomic<bool> shutdown{false};
};

// Coroutine-based connect + cache lookup.
async_simple::coro::Lazy<std::shared_ptr<coro_rpc::coro_rpc_client>>
getOrCreateClientAsync(std::shared_ptr<RpcSharedState> state,
                       const std::string& addr);

// Spawn a Lazy<void> coroutine on the global I/O executor.
void spawnOnExecutor(async_simple::coro::Lazy<void> task);

// Create a coro_rpc_client with a local io_context (for sync RPC).
std::unique_ptr<coro_rpc::coro_rpc_client> createSyncClient();

// Fire-and-forget coroutine: connect, send_request, discard result.
template <auto Func, typename Req>
async_simple::coro::Lazy<void> sendCoroutine(
    std::shared_ptr<RpcSharedState> state, const std::string& addr, Req req) {
    if (state->shutdown.load(std::memory_order_acquire)) co_return;
    auto client = co_await getOrCreateClientAsync(state, addr);
    if (!client) co_return;
    try {
        auto send_lazy =
            co_await client->template send_request<Func, Req>(std::move(req));
        co_await std::move(send_lazy);
    } catch (const std::exception& e) {
        if (!state->shutdown.load(std::memory_order_acquire)) {
            LOG(ERROR) << "RpcClient: fire-and-forget RPC to " << addr
                       << " failed: " << e.what();
        }
    }
}

// Async call with callback coroutine: connect, send_request, invoke callback.
// Callback is templated (not std::function) so the caller's lambda is moved
// rather than copied.
template <auto Func, typename Req, typename ResponseType, typename Callback>
async_simple::coro::Lazy<void> callAsyncCoroutine(
    std::shared_ptr<RpcSharedState> state, const std::string& addr, Req req,
    Callback cb) {
    if (state->shutdown.load(std::memory_order_acquire)) co_return;
    auto client = co_await getOrCreateClientAsync(state, addr);
    if (!client) {
        if (!state->shutdown.load(std::memory_order_acquire)) {
            LOG(ERROR) << "callAsyncCoroutine: failed to connect to " << addr;
        }
        cb(ResponseType{});
        co_return;
    }
    try {
        auto send_lazy =
            co_await client->template send_request<Func, Req>(std::move(req));
        auto res = co_await std::move(send_lazy);
        if (res) {
            cb(std::move(res.value().result()));
        } else {
            if (!state->shutdown.load(std::memory_order_acquire)) {
                LOG(ERROR) << "RpcClient: async rpc to " << addr
                           << " failed: " << res.error().msg;
            }
            cb(ResponseType{});
        }
    } catch (const std::exception& e) {
        if (!state->shutdown.load(std::memory_order_acquire)) {
            LOG(ERROR) << "RpcClient: async rpc caught exception: " << e.what();
        }
        cb(ResponseType{});
    }
}

}  // namespace rpc_detail

// RpcClient  - unified outbound RPC client.
//
//   call<&Service::method>(addr, req)             - sync, blocks caller
//   callAsync<&Service::method>(addr, req, cb)    - async, callback on
//                                                    global executor thread
//   send<&Service::method>(addr, req)             - fire-and-forget

class RpcClient {
   public:
    RpcClient() = default;
    ~RpcClient() = default;

    template <auto Func, typename Req>
    auto call(const std::string& addr, Req req,
              std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        using ResponseType = decltype(coro_rpc::get_return_type<Func>());

        auto client = rpc_detail::createSyncClient();

        auto ec = async_simple::coro::syncAwait(client->connect(addr));
        if (ec) {
            LOG(ERROR) << "RpcClient: call connect failed to " << addr << ": "
                       << ec.message();
            return ResponseType{};
        }
        auto result =
            async_simple::coro::syncAwait(client->call_for<Func>(timeout, req));
        if (!result) {
            LOG(ERROR) << "RpcClient: call RPC failed: " << result.error().msg;
            return ResponseType{};
        }
        return std::move(result.value());
    }

    template <auto Func, typename Req, typename Callback>
    void callAsync(const std::string& addr, Req req, Callback cb) {
        using ResponseType = decltype(coro_rpc::get_return_type<Func>());

        auto task =
            rpc_detail::callAsyncCoroutine<Func, Req, ResponseType, Callback>(
                state_, addr, std::move(req), std::move(cb));

        rpc_detail::spawnOnExecutor(std::move(task));
    }

    template <auto Func, typename Req>
    void send(const std::string& addr, Req req) {
        auto task =
            rpc_detail::sendCoroutine<Func, Req>(state_, addr, std::move(req));

        rpc_detail::spawnOnExecutor(std::move(task));
    }

    bool isConnected(const std::string& addr) const;
    bool tryReconnect(const std::string& addr);

    // Mark the client as shutting down.  In-flight async coroutines will
    // drop silently instead of logging errors or invoking callbacks that may
    // access a destroyed AgentHost.
    void shutdown() { state_->shutdown.store(true, std::memory_order_release); }

   private:
    std::shared_ptr<rpc_detail::RpcSharedState> state_ =
        std::make_shared<rpc_detail::RpcSharedState>();
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_RPC_RUNTIME_H
