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

#ifndef RPC_STUB_H
#define RPC_STUB_H

#include <jsoncpp/json/json.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "v1/common.h"
#include "ylt/coro_io/coro_io.hpp"

namespace mooncake {
namespace v1 {

// AsioRpcServer and AsioRpcClient replaces the naive Socket-based
// handshake service in older versions. This provides higher reliablity
// and more features (such as the last-resort method of data transfer)

using RpcRawData = std::vector<char>;
enum RpcErrorCode { OK = 0, ErrIncompleted, ErrInvalidFunc, ErrConnectFailed };
enum RpcFuncID {
    GetSegmentDesc = 1,
    BootstrapRdma,
    SendData,
    RecvData,
    Notify
};

class AsioRpcServer {
   public:
    AsioRpcServer();

    ~AsioRpcServer();

    AsioRpcServer(const AsioRpcServer &) = delete;
    AsioRpcServer &operator=(const AsioRpcServer &) = delete;

   public:
    using Function = std::function<void(const RpcRawData & /* request */,
                                        RpcRawData & /* response */)>;
    Status registerFunction(int func_id, const Function &func);

    Status start(uint16_t &port, bool ipv6 = false);

    Status stop();

    int process(int func_id, const RpcRawData &request, RpcRawData &response);

   private:
    void doAccept();

   private:
    std::atomic<bool> running_;
    asio::io_context io_context_;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
    std::mutex func_map_mutex_;
    std::unordered_map<int, Function> func_map_;
    std::thread worker_;
};

class AsioRpcClient {
   public:
    static Status Call(const std::string &server_addr, int func_id,
                       const RpcRawData &request, RpcRawData &response);

   public:
    static RpcErrorCode do_call(const std::string &server_addr, int func_id,
                                const RpcRawData &request,
                                RpcRawData &response);
};

}  // namespace v1
}  // namespace mooncake
#endif  // RPC_STUB_H