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

class RpcServer {
   public:
    explicit RpcServer(uint16_t port = 0, unsigned thread_num = 2);

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

class RpcClient {
   public:
    RpcClient() = default;
    ~RpcClient() = default;

   private:
    template <typename T>
    static void setRpcError(T&, ...) {}  // fallback: do nothing

    template <typename T>
    static auto setRpcError(T& resp, const std::string& msg)
        -> decltype(resp.reject_reason, void()) {
        resp.reject_reason = msg;
    }

    template <typename T>
    static auto setRpcError(T& resp, const std::string& msg)
        -> decltype(resp.error_msg, void()) {
        resp.error_msg = msg;
    }

   public:
    // Synchronous call.
    template <auto Func, typename Req>
    auto call(const std::string& addr, Req req,
              std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        using ResponseType = decltype(coro_rpc::get_return_type<Func>());

        auto client = createSyncClient();

        auto ec = async_simple::coro::syncAwait(client->connect(addr));
        if (ec) {
            LOG(ERROR) << "RpcClient: call connect failed to " << addr << ": "
                       << ec.message();
            ResponseType resp{};
            setRpcError(resp,
                        "RPC connect failed: " + std::string(ec.message()));
            return resp;
        }
        auto result =
            async_simple::coro::syncAwait(client->call_for<Func>(timeout, req));
        if (!result) {
            LOG(ERROR) << "RpcClient: call RPC failed: " << result.error().msg;
            ResponseType resp{};
            setRpcError(resp,
                        "RPC call failed: " + std::string(result.error().msg));
            return resp;
        }
        return std::move(result.value());
    }

    // Async call with callback.
    template <auto Func, typename Req, typename Callback>
    void callAsync(const std::string& addr, Req req, Callback cb) {
        using ResponseType = decltype(coro_rpc::get_return_type<Func>());
        auto task = callAsyncCoroutine<Func, Req, ResponseType, Callback>(
            state_, addr, std::move(req), std::move(cb));
        spawn(std::move(task));
    }

    // Fire-and-forget.
    template <auto Func, typename Req>
    void send(const std::string& addr, Req req) {
        auto task = sendCoroutine<Func, Req>(state_, addr, std::move(req));
        spawn(std::move(task));
    }

    bool isConnected(const std::string& addr) const;
    bool tryReconnect(const std::string& addr);

    void shutdown() { state_->shutdown.store(true, std::memory_order_release); }

   private:
    struct SharedState {
        std::mutex mutex;
        std::unordered_map<std::string,
                           std::shared_ptr<coro_rpc::coro_rpc_client>>
            clients;
        std::atomic<bool> shutdown{false};
    };

    // Coroutine-based connect + cache lookup.
    static async_simple::coro::Lazy<std::shared_ptr<coro_rpc::coro_rpc_client>>
    getOrCreateClient(std::shared_ptr<SharedState> state,
                      const std::string& addr);

    // Spawn a coroutine on the global I/O executor.
    static void spawn(async_simple::coro::Lazy<void> task);

    // Create a coro_rpc_client with local io_context (for sync call()).
    static std::unique_ptr<coro_rpc::coro_rpc_client> createSyncClient();

    // Fire-and-forget coroutine: connect, send_request, discard result.
    template <auto Func, typename Req>
    static async_simple::coro::Lazy<void> sendCoroutine(
        std::shared_ptr<SharedState> state, const std::string& addr, Req req) {
        if (state->shutdown.load(std::memory_order_acquire)) co_return;
        auto client = co_await getOrCreateClient(state, addr);
        if (!client) co_return;
        try {
            auto send_lazy = co_await client->template send_request<Func, Req>(
                std::move(req));
            co_await std::move(send_lazy);
        } catch (const std::exception& e) {
            if (!state->shutdown.load(std::memory_order_acquire)) {
                VLOG(1) << "RpcClient: fire-and-forget RPC to " << addr
                        << " failed: " << e.what();
            }
        }
    }

    // Async call coroutine: connect, send_request, invoke callback.
    template <auto Func, typename Req, typename ResponseType, typename Callback>
    static async_simple::coro::Lazy<void> callAsyncCoroutine(
        std::shared_ptr<SharedState> state, const std::string& addr, Req req,
        Callback cb) {
        if (state->shutdown.load(std::memory_order_acquire)) co_return;
        auto client = co_await getOrCreateClient(state, addr);
        if (!client) {
            if (!state->shutdown.load(std::memory_order_acquire)) {
                VLOG(1) << "RpcClient: callAsync failed to connect to " << addr;
            }
            cb(ResponseType{});
            co_return;
        }
        try {
            auto send_lazy = co_await client->template send_request<Func, Req>(
                std::move(req));
            auto res = co_await std::move(send_lazy);
            if (res) {
                cb(std::move(res.value().result()));
            } else {
                if (!state->shutdown.load(std::memory_order_acquire)) {
                    VLOG(1) << "RpcClient: async rpc to " << addr
                            << " failed: " << res.error().msg;
                }
                cb(ResponseType{});
            }
        } catch (const std::exception& e) {
            if (!state->shutdown.load(std::memory_order_acquire)) {
                VLOG(1) << "RpcClient: async rpc caught exception: "
                        << e.what();
            }
            cb(ResponseType{});
        }
    }

    std::shared_ptr<SharedState> state_ = std::make_shared<SharedState>();
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_RPC_RUNTIME_H
