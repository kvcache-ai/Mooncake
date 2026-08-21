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
    BootstrapRdma = 2,
    SendData = 3,
    RecvData = 4,
    Notify = 5,
    Probe = 6,
    Delegate = 7,
    Pin = 8,
    Unpin = 9,
    SubscribeSegmentUpdate = 10,
    NotifySegmentUpdated = 11,
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
    // historical behavior). The TCP data-path handlers do full-payload
    // blocking copies inline, so a single thread caps TCP throughput;
    // sourced from the rpc_server_threads config key.
    Status start(uint16_t &port, bool ipv6 = false, size_t threads = 1);

    Status stop();

    Status call(const std::string &server_addr, int func_id,
                const std::string_view &request, std::string &response);

    // Same as call(), but moves the request into the coroutine instead of
    // copying it, for large payloads.
    Status callOwned(const std::string &server_addr, int func_id,
                     std::string request, std::string &response);

    using AsyncCallback = std::function<void(Status, std::string)>;
    void callAsync(const std::string &server_addr, int func_id,
                   const std::string &request, AsyncCallback callback);

    async_simple::coro::Lazy<std::pair<Status, std::string>> callCoroutine(
        std::string server_addr, int func_id, std::string request);

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

    std::mutex func_map_mutex_;
    std::unordered_map<int, Handler> func_map_;

    std::atomic<bool> running_{false};
};

}  // namespace tent
}  // namespace mooncake
#endif  // TENT_YLT_RPC_H
