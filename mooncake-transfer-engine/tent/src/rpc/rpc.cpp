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
#include "tent/rpc/rpc.h"

#include <glog/logging.h>
#include <async_simple/executors/SimpleExecutor.h>

#include "tent/common/utils/ip.h"
#include "tent/common/utils/random.h"

namespace mooncake {
namespace tent {

using namespace coro_rpc;
using namespace async_simple::coro;

namespace {
thread_local bool tl_inside_rpc_handler = false;

struct RpcHandlerScope {
    explicit RpcHandlerScope(bool& flag) : flag_(flag), prev_(flag) {
        flag_ = true;
    }
    ~RpcHandlerScope() { flag_ = prev_; }

   private:
    bool& flag_;
    bool prev_;
};
}  // namespace

class ClientPool {
   public:
    std::unique_ptr<coro_rpc_client> acquire() {
        std::lock_guard<std::mutex> guard(lock_);
        if (!idle_clients_.empty()) {
            auto client = std::move(idle_clients_.back());
            idle_clients_.pop_back();
            return client;
        }
        return nullptr;
    }

    void release(std::unique_ptr<coro_rpc_client> client) {
        if (!client) return;
        std::lock_guard<std::mutex> guard(lock_);
        idle_clients_.push_back(std::move(client));
    }

    void clear() {
        std::lock_guard<std::mutex> guard(lock_);
        idle_clients_.clear();
    }

   private:
    std::mutex lock_;
    std::vector<std::unique_ptr<coro_rpc_client>> idle_clients_;
};

struct ClientLease {
    std::unique_ptr<coro_rpc_client> client;
    std::shared_ptr<ClientPool> pool;
    bool broken = false;

    ~ClientLease() {
        if (client && !broken) {
            pool->release(std::move(client));
        }
    }

    coro_rpc_client* operator->() const { return client.get(); }
};

CoroRpcAgent::CoroRpcAgent() = default;

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
            server_ = new coro_rpc::coro_rpc_server(kRpcThreads, port,
                                                    ipv6 ? "::" : "0.0.0.0");
            server_->register_handler<&CoroRpcAgent::process>(this);
            server_->async_start();
            const auto err = server_->get_errc();
            if (err) {
                LOG(WARNING)
                    << "Failed to start RPC server(async_start) on port "
                    << port << ": " << err.message();
                delete server_;
                server_ = nullptr;
                port = 0;
                continue;
            }
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

    std::lock_guard<std::mutex> lock(pools_mutex_);
    for (auto& entry : pools_) {
        entry.second->clear();
    }
    pools_.clear();
    return Status::OK();
}

void CoroRpcAgent::process(int func_id) {
    RpcHandlerScope handler_scope(tl_inside_rpc_handler);
    auto ctx = coro_rpc::get_context();
    if (func_map_.count(func_id)) {
        auto request = ctx->get_request_attachment();
        std::string response;
        auto func = func_map_.at(func_id);
        func(request, response);
        ctx->set_response_attachment(response);
    }
}

std::shared_ptr<ClientPool> CoroRpcAgent::getOrCreatePool(
    const std::string& server_addr) {
    std::lock_guard<std::mutex> lock(pools_mutex_);
    auto it = pools_.find(server_addr);
    if (it == pools_.end()) {
        it = pools_.emplace(server_addr, std::make_shared<ClientPool>()).first;
    }
    return it->second;
}

Lazy<std::pair<Status, std::string>> CoroRpcAgent::callCoroutine(
    std::string server_addr, int func_id, std::string request) {
    if (tl_inside_rpc_handler) {
        co_return std::make_pair(
            Status::InvalidArgument(
                "RPC call from RPC handler is forbidden" LOC_MARK),
            "");
    }

    auto pool = getOrCreatePool(server_addr);
    ClientLease lease{pool->acquire(), pool, false};

    if (!lease.client) {
        lease.client = std::make_unique<coro_rpc_client>();
        auto conn_result = co_await lease.client->connect(server_addr);
        if (conn_result.val() != 0) {
            lease.broken = true;
            auto msg = "Failed to connect RPC server. server: " + server_addr +
                       ", func_id: " + std::to_string(func_id) +
                       ", message: " + std::string{conn_result.message()};
            co_return std::make_pair(Status::RpcServiceError(msg + LOC_MARK),
                                     "");
        }
    }

    lease->set_req_attachment(request);

    auto call_result = co_await lease->call<&CoroRpcAgent::process>(func_id);

    if (!call_result.has_value()) {
        lease.broken = true;
        auto msg = "Failed to call RPC function. server: " + server_addr +
                   ", func_id: " + std::to_string(func_id) +
                   ", message: " + std::string{call_result.error().msg};
        co_return std::make_pair(Status::RpcServiceError(msg + LOC_MARK), "");
    }

    std::string response{lease->get_resp_attachment()};
    lease->release_resp_attachment();

    co_return std::make_pair(Status::OK(), std::move(response));
}

Status CoroRpcAgent::call(const std::string& server_addr, int func_id,
                          const std::string_view& request,
                          std::string& response) {
    auto [status, resp] = async_simple::coro::syncAwait(
        callCoroutine(server_addr, func_id, std::string(request)));

    if (status.ok()) {
        response = std::move(resp);
    }
    return status;
}

void CoroRpcAgent::callAsync(const std::string& server_addr, int func_id,
                             const std::string& request,
                             AsyncCallback callback) {
    callCoroutine(server_addr, func_id, request)
        .start([cb = std::move(callback)](auto&& try_result) {
            if (try_result.hasError()) {
                cb(Status::RpcServiceError("Async RPC exception" LOC_MARK), "");
            } else {
                auto& val = try_result.value();
                cb(val.first, std::move(val.second));
            }
        });
}
}  // namespace tent
}  // namespace mooncake
