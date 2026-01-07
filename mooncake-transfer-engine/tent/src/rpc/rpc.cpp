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

// TODO the implementation needs to be updated
// - Caching sockets, and supporting multiple calls
// - Keep it lightweighted and tidy

#include "tent/rpc/rpc.h"

#include <glog/logging.h>

#include "tent/common/utils/ip.h"
#include "tent/common/utils/random.h"

namespace mooncake {
namespace tent {

using namespace coro_rpc;
using namespace async_simple::coro;

CoroRpcAgent::CoroRpcAgent() {}

CoroRpcAgent::~CoroRpcAgent() { stop(); }

Status CoroRpcAgent::registerFunction(int func_id, const Function& func) {
    func_map_mutex_.lock();
    func_map_[func_id] = func;
    func_map_mutex_.unlock();
    return Status::OK();
}

Status CoroRpcAgent::start(uint16_t& port, bool ipv6) {
    const static uint16_t kStartPort = 15000;
    const static uint16_t kPortRange = 2000;
    const static int kMaxRetry = 10;
    if (running_)
        return Status::InvalidArgument("RPC server already started" LOC_MARK);
    easylog::set_min_severity(easylog::Severity::FATAL);
    for (int retry = 0; retry < kMaxRetry; ++retry) {
        try {
            if (port == 0)
                port = kStartPort + SimpleRandom::Get().next(kPortRange);
            server_ = new coro_rpc::coro_rpc_server(kRpcThreads, port);
            server_->register_handler<&CoroRpcAgent::process>(this);
            server_->async_start();
            running_ = true;
            return Status::OK();
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to start RPC server on port " << port
                         << ": " << e.what();
            port = 0;
        }
    }
    return Status::RpcServiceError("Failed to bind any RPC port" LOC_MARK);
}

Status CoroRpcAgent::stop() {
    if (server_) {
        server_->stop();
        delete server_;
        server_ = nullptr;
    }
    for (auto& entry : sessions_) {
        delete entry.second;
    }
    sessions_.clear();
    return Status::OK();
}

void CoroRpcAgent::process(int func_id) {
    auto ctx = coro_rpc::get_context();
    if (func_map_.count(func_id)) {
        auto request = ctx->get_request_attachment();
        std::string response;
        auto func = func_map_.at(func_id);
        func(request, response);
        ctx->set_response_attachment(response);
    }
}

Status CoroRpcAgent::call(const std::string& server_addr, int func_id,
                          const std::string_view& request,
                          std::string& response) {
    coro_rpc::coro_rpc_client* client = nullptr;
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        auto it = sessions_.find(server_addr);
        if (it == sessions_.end()) {
            coro_rpc_client* client = new coro_rpc_client();
            auto conn_result =
                async_simple::coro::syncAwait(client->connect(server_addr));
            if (conn_result.val() != 0) {
                LOG(ERROR) << "Failed to connect RPC server. "
                           << "server " << server_addr << ", "
                           << "func_id " << func_id << ", "
                           << "message " << conn_result.message();
                return Status::RpcServiceError(
                    "Failed to connect RPC server" LOC_MARK);
            }
            it = sessions_.emplace(server_addr, client).first;
        }
        client = it->second;
    }
    client->set_req_attachment(request);
    auto call_result = async_simple::coro::syncAwait(
        client->call<&CoroRpcAgent::process>(func_id));
    if (!call_result.has_value()) {
        // LOG(ERROR) << "Failed to call RPC function."
        //            << "server " << server_addr << ", "
        //            << "func_id " << func_id;
        return Status::RpcServiceError("Failed to call RPC function" LOC_MARK);
    }
    response = client->get_resp_attachment();
    client->release_resp_attachment();
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake