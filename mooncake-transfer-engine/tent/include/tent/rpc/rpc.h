// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_YLT_RPC_H
#define TENT_YLT_RPC_H

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <async_simple/coro/Lazy.h>

#include "tent/common/status.h"
#include "ylt/coro_io/coro_io.hpp"

namespace mooncake {
namespace tent {

enum RpcFuncID {
    GetSegmentDesc = 1,
    BootstrapRdma,
    SendData,
    RecvData,
    Notify,
    Probe,
    Delegate,
    Pin,
    Unpin,
    SubscribeSegmentUpdate,
    NotifySegmentUpdated,
    // Appended to preserve the numeric values of the existing RPCs.
    BootstrapUb,
};

class ClientPool;

class CoroRpcAgent {
   public:
    CoroRpcAgent();

    virtual ~CoroRpcAgent();

    CoroRpcAgent(const CoroRpcAgent &) = delete;
    CoroRpcAgent &operator=(const CoroRpcAgent &) = delete;

   public:
    using Function = std::function<void(const std::string_view & /* request */,
                                        std::string & /* response */)>;

    // Handlers run on the server io_context thread pool (the threads
    // argument of start(), 1 by default), so a blocking one stalls every
    // connection sharing its thread, not just its own.
    //
    // offload=false (default): inline, no thread hop. Right for anything that
    // returns promptly.
    // offload=true: runs on ylt's blocking executor while the connection
    // coroutine suspends, keeping the io_context free. Callers see the same
    // request/response, but ordering within a connection is no longer
    // guaranteed.
    Status registerFunction(int func_id, const Function &func,
                            bool offload = false);

    // threads: number of io_context worker threads (default 1, the
    // historical RDMA-only behavior). TCP SendData/RecvData copies are
    // offloaded off this pool, but attachments are still read here; when
    // TCP is enabled the engine defaults rpc_server_threads higher so
    // concurrent bulk transfers are not serialized on one io_context.
    Status start(uint16_t &port, bool ipv6 = false, size_t threads = 1);

    Status stop();

    // Drops pooled client connections that have been idle for longer than
    // idle_timeout, and forgets the pools that are then empty and unused.
    // A pool is only ever grown, and a peer that has left is never called
    // again, so without this its idle sockets sit in CLOSE-WAIT (and its
    // epoll/timer/event fds stay allocated) until the process exits.
    // Swept from getOrCreatePool(), rate-limited to a few seconds apart.
    // Reclaiming on the call path keeps this free of a timer thread, which a
    // per-thread client agent cannot afford; the price is that a process that
    // makes no RPC at all never sweeps - but it is not accumulating pools
    // either. Returns the number of connections dropped.
    size_t evictIdleClients(std::chrono::nanoseconds idle_timeout);

    // Test-only hooks: idle reclamation is time-based, so a test runs a
    // sweep with its own threshold instead of waiting out the production one.
    size_t idleClientCountForTest(const std::string &server_addr);
    size_t poolCountForTest();

    Status call(const std::string &server_addr, int func_id,
                const std::string_view &request, std::string &response);

    // Same as call(), but moves the request into the coroutine instead of
    // copying it, for large payloads.
    Status callOwned(const std::string &server_addr, int func_id,
                     std::string request, std::string &response);

    using AsyncCallback = std::function<void(Status, std::string)>;
    void callAsync(const std::string &server_addr, int func_id,
                   const std::string &request, AsyncCallback callback);
    // Same as callAsync(), but retries once on a transport-level RPC failure.
    // Only for idempotent RPCs: a failure may arrive after the peer already ran
    // the handler, so a retry can run it twice.
    void callAsyncRetryOnce(const std::string &server_addr, int func_id,
                            const std::string &request, AsyncCallback callback);

    async_simple::coro::Lazy<std::pair<Status, std::string>> callCoroutine(
        std::string server_addr, int func_id, std::string request,
        bool retry_rpc_error_once = false);

   private:
    async_simple::coro::Lazy<void> process(int func_id);

    std::shared_ptr<ClientPool> getOrCreatePool(const std::string &server_addr);

   private:
    struct Handler {
        Function func;
        bool offload = false;
    };

    coro_rpc::coro_rpc_server *server_ = nullptr;

    std::mutex pools_mutex_;
    std::unordered_map<std::string, std::shared_ptr<ClientPool>> pools_;
    // Guarded by pools_mutex_. Bounds how often getOrCreatePool() walks
    // every pool.
    std::chrono::steady_clock::time_point last_idle_sweep_{};

    std::mutex func_map_mutex_;
    std::unordered_map<int, Handler> func_map_;

    std::atomic<bool> running_{false};
};

}  // namespace tent
}  // namespace mooncake
#endif  // TENT_YLT_RPC_H
