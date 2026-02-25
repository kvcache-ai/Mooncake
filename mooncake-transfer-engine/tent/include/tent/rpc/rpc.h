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
#include <csignal>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

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
    Delegate,
    Pin,
    Unpin
};

class CoroRpcAgent {
   public:
    CoroRpcAgent();

    virtual ~CoroRpcAgent();

    CoroRpcAgent(const CoroRpcAgent &) = delete;
    CoroRpcAgent &operator=(const CoroRpcAgent &) = delete;

   public:
    using Function = std::function<void(const std::string_view & /* request */,
                                        std::string & /* response */)>;
    Status registerFunction(int func_id, const Function &func);

    Status start(uint16_t &port, bool ipv6 = false);

    Status stop();

    Status call(const std::string &server_addr, int func_id,
                const std::string_view &request, std::string &response);

   private:
    void process(int func_id);

   private:
    coro_rpc::coro_rpc_server *server_ = nullptr;

    std::mutex sessions_mutex_;
    std::unordered_map<std::string, coro_rpc::coro_rpc_client *> sessions_;
    std::atomic<int> uid_{0};

    std::mutex func_map_mutex_;
    std::unordered_map<int, Function> func_map_;

    std::atomic<bool> running_{false};
    const static size_t kRpcThreads = 1;
};

}  // namespace tent
}  // namespace mooncake
#endif  // TENT_YLT_RPC_H
