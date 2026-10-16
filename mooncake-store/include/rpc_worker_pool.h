#pragma once

#include <cstddef>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <asio/thread_pool.hpp>
#include <async_simple/coro/Lazy.h>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/type_traits.h>
#include <ylt/util/utils.hpp>

namespace mooncake {

namespace rpc_worker_pool_detail {

// ylt reports the parameters of a function that takes none as void.
template <auto Method>
using HandlerParams = std::conditional_t<
    std::is_void_v<::util::function_parameters_t<decltype(Method)>>,
    std::tuple<>, ::util::function_parameters_t<decltype(Method)>>;

// A coroutine handler that runs Method on the pool's worker threads. Its
// parameters are the decayed parameters of Method, which is what coro_rpc
// deserializes the request into, so the coroutine frame owns them.
template <auto Method, typename Service,
          typename Params = HandlerParams<Method>>
class PooledHandler;

template <auto Method, typename Service, typename... Params>
class PooledHandler<Method, Service, std::tuple<Params...>> {
    using Result = ::util::function_return_type_t<decltype(Method)>;

   public:
    PooledHandler(Service& service, asio::thread_pool::executor_type executor)
        : service_(service), executor_(executor) {}

    async_simple::coro::Lazy<Result> Handle(Params... params) {
        auto result = co_await coro_io::post(
            [&]() -> Result {
                return (service_.*Method)(std::move(params)...);
            },
            executor_);
        co_return std::move(result).value();
    }

   private:
    Service& service_;
    asio::thread_pool::executor_type executor_;
};

}  // namespace rpc_worker_pool_detail

// Runs RPC handlers on worker threads, apart from the server's io threads.
// coro_rpc binds every connection to one io thread and runs a plain handler
// inline on it, so a slow handler stops that thread from reading any other
// connection bound to it -- the Ping of a healthy client included. A handler
// registered here only queues the call on a worker thread, and the io thread
// goes back to reading.
class RpcWorkerPool {
   public:
    explicit RpcWorkerPool(size_t thread_count) : threads_(thread_count) {}
    // Waits for the calls already queued. Destroy it after the server has
    // stopped and before the service the handlers call into.
    ~RpcWorkerPool() { threads_.join(); }

    RpcWorkerPool(const RpcWorkerPool&) = delete;
    RpcWorkerPool& operator=(const RpcWorkerPool&) = delete;

    // Registers Method under the route key coro_rpc derives from its name,
    // so clients call it exactly as before.
    template <auto Method, typename Service>
    void Register(coro_rpc::coro_rpc_server& server, Service& service) {
        using Handler = rpc_worker_pool_detail::PooledHandler<Method, Service>;
        auto handler =
            std::make_shared<Handler>(service, threads_.get_executor());
        server.register_handler<&Handler::Handle>(handler.get(),
                                                  coro_rpc::func_id<Method>());
        handlers_.push_back(std::move(handler));
    }

   private:
    asio::thread_pool threads_;
    std::vector<std::shared_ptr<void>> handlers_;
};

}  // namespace mooncake
