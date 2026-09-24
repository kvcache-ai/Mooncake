#ifndef MOONCAKE_PG_RPC_RUNTIME_H
#define MOONCAKE_PG_RPC_RUNTIME_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

#include <csignal>

#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <async_simple/coro/SyncAwait.h>
#include <async_simple/coro/Lazy.h>

#include "error_types.h"

namespace mooncake {

class RpcServer {
   public:
    explicit RpcServer(uint16_t port = 0, unsigned thread_num = 2);

    template <auto First, auto... Rest>
    void registerHandler(util::class_type_t<decltype(First)>* impl) {
        server_->register_handler<First, Rest...>(impl);
    }

    bool start();
    std::string getListenAddr(const std::string& host_ip) const;
    void shutdown();

   private:
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    uint16_t port_;
    unsigned thread_num_;
};

class RpcClient {
   private:
    template <auto Func>
    using ResponseT = decltype(coro_rpc::get_return_type<Func>());

    template <auto Func>
    static PGResult<ResponseT<Func>> makeRpcFailure(std::string_view message) {
        return makePGError(PGErrorCode::RpcError,
                           std::string(coro_rpc::get_func_name<Func>()) +
                               " RPC failed: " + std::string(message));
    }

    static std::string describeException(std::exception_ptr exception) {
        try {
            std::rethrow_exception(std::move(exception));
        } catch (const std::exception& e) {
            return e.what();
        } catch (...) {
            return "unknown transport exception";
        }
    }

   public:
    static constexpr auto kConnectTimeout = std::chrono::seconds(3);
    static constexpr auto kDefaultRequestTimeout =
        std::chrono::milliseconds(30000);

    explicit RpcClient(
        std::chrono::milliseconds request_timeout = kDefaultRequestTimeout,
        std::chrono::milliseconds connect_timeout = kConnectTimeout);
    ~RpcClient() = default;

    template <auto Func, typename Req>
    PGResult<ResponseT<Func>> call(
        const std::string& addr, Req req,
        std::optional<std::chrono::milliseconds> timeout = std::nullopt) {
        auto client = createSyncClient();
        auto request_timeout = timeout.value_or(state_->request_timeout);

        auto ec = async_simple::coro::syncAwait(client->connect(addr));
        if (ec) {
            return makeRpcFailure<Func>(ec.message());
        }
        auto rpc_result = async_simple::coro::syncAwait(
            client->call_for<Func>(request_timeout, std::move(req)));
        if (!rpc_result) {
            return makeRpcFailure<Func>(rpc_result.error().msg);
        }

        if constexpr (std::is_void_v<ResponseT<Func>>) {
            return {};
        } else {
            return std::move(rpc_result).value();
        }
    }

    // Async call with transport failures and the response delivered through
    // one PGResult. The callback is not invoked after shutdown begins.
    template <auto Func, typename Req, typename Callback>
    void callAsync(const std::string& addr, Req req, Callback cb) {
        static_assert(
            std::is_invocable_v<Callback, PGResult<ResponseT<Func>>>,
            "RpcClient::callAsync callback must accept PGResult<Response>");
        auto task = callAsyncCoroutine<Func>(state_, addr, std::move(req),
                                             std::move(cb));
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
        SharedState(std::chrono::milliseconds request_timeout,
                    std::chrono::milliseconds connect_timeout)
            : request_timeout(request_timeout),
              connect_timeout(connect_timeout) {}

        std::mutex mutex;
        std::unordered_map<std::string,
                           std::shared_ptr<coro_rpc::coro_rpc_client>>
            clients;
        std::atomic<bool> shutdown{false};
        const std::chrono::milliseconds request_timeout;
        const std::chrono::milliseconds connect_timeout;
    };

    // Coroutine-based connect + cache lookup.
    static async_simple::coro::Lazy<std::shared_ptr<coro_rpc::coro_rpc_client>>
    getOrCreateClient(std::shared_ptr<SharedState> state,
                      const std::string& addr);

    // Spawn a coroutine on the global I/O executor.
    static void spawn(async_simple::coro::Lazy<void> task);

    // Create a coro_rpc_client with local io_context (for sync call()).
    std::unique_ptr<coro_rpc::coro_rpc_client> createSyncClient();

    // Fire-and-forget coroutine: connect, send_request, discard result.
    template <auto Func, typename Req>
    static async_simple::coro::Lazy<void> sendCoroutine(
        std::shared_ptr<SharedState> state, const std::string& addr, Req req) {
        if (state->shutdown.load(std::memory_order_acquire)) co_return;
        auto client = co_await getOrCreateClient(state, addr);
        if (!client) co_return;
        try {
            coro_rpc::request_config_t config;
            config.request_timeout_duration = state->request_timeout;
            auto send_lazy = co_await client->template send_request<Func>(
                std::move(config), std::move(req));
            co_await std::move(send_lazy);
        } catch (const std::exception& e) {
            if (!state->shutdown.load(std::memory_order_acquire)) {
                VLOG(1) << "RpcClient: fire-and-forget RPC to " << addr
                        << " failed: " << e.what();
            }
        }
    }

    // Async call coroutine: connect, send_request, invoke callback.
    template <auto Func, typename Req, typename Callback>
    static async_simple::coro::Lazy<void> callAsyncCoroutine(
        std::shared_ptr<SharedState> state, const std::string& addr, Req req,
        Callback cb) {
        if (state->shutdown.load(std::memory_order_acquire)) co_return;

        using Response = ResponseT<Func>;
        auto complete = [&](PGResult<Response> result) {
            if (!state->shutdown.load(std::memory_order_acquire)) {
                cb(std::move(result));
            }
        };

        auto client_try = co_await getOrCreateClient(state, addr).coAwaitTry();
        if (client_try.hasError()) {
            complete(makeRpcFailure<Func>(
                describeException(client_try.getException())));
            co_return;
        }
        auto client = std::move(client_try).value();
        if (!client) {
            complete(makeRpcFailure<Func>("failed to connect to " + addr));
            co_return;
        }
        if (state->shutdown.load(std::memory_order_acquire)) co_return;

        coro_rpc::request_config_t config;
        config.request_timeout_duration = state->request_timeout;
        auto send_operation = client->template send_request<Func>(
            std::move(config), std::move(req));
        auto send_try = co_await std::move(send_operation).coAwaitTry();
        if (send_try.hasError()) {
            complete(makeRpcFailure<Func>(
                describeException(send_try.getException())));
            co_return;
        }

        auto receive_operation = std::move(send_try).value();
        auto receive_try = co_await std::move(receive_operation).coAwaitTry();
        if (receive_try.hasError()) {
            complete(makeRpcFailure<Func>(
                describeException(receive_try.getException())));
            co_return;
        }

        auto rpc_result = std::move(receive_try).value();
        if (!rpc_result) {
            complete(makeRpcFailure<Func>(rpc_result.error().msg));
        } else if constexpr (std::is_void_v<Response>) {
            complete(PGResult<void>{});
        } else {
            complete(
                PGResult<Response>{std::move(rpc_result.value().result())});
        }
    }

    std::shared_ptr<SharedState> state_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_RPC_RUNTIME_H
