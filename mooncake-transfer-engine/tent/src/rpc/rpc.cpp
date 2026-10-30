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

#include <stdexcept>

#include "transfer_engine_rpc_client_io_context.h"
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
    using Clock = std::chrono::steady_clock;

    struct Lease {
        std::unique_ptr<coro_rpc_client> client;
        // Which generation the client was drawn from. Flushing is keyed on
        // this: see clearIfCurrent().
        uint64_t generation = 0;
    };

    Lease acquire() {
        std::lock_guard<std::mutex> guard(lock_);
        last_activity_ = Clock::now();
        if (!idle_clients_.empty()) {
            // Take the most recently parked client. The ones at the front
            // keep aging, so evictIdle() below reclaims exactly the
            // connections that no burst has needed since.
            auto client = std::move(idle_clients_.back().client);
            idle_clients_.pop_back();
            return Lease{std::move(client), generation_};
        }
        return Lease{nullptr, generation_};
    }

    // A client is only released after a call succeeded on it, which proves it
    // is alive, so it goes back regardless of which generation it came from.
    void release(std::unique_ptr<coro_rpc_client> client) {
        if (!client) return;
        std::lock_guard<std::mutex> guard(lock_);
        last_activity_ = Clock::now();
        idle_clients_.push_back(IdleClient{std::move(client), last_activity_});
    }

    // Detaches every client parked before `deadline` and hands it to the
    // caller, which destroys it outside lock_. Dropping an idle client is
    // transparent: the next call for this address simply reconnects, the same
    // as after clearIfCurrent().
    void evictIdle(Clock::time_point deadline,
                   std::vector<std::unique_ptr<coro_rpc_client>>& dropped) {
        std::lock_guard<std::mutex> guard(lock_);
        auto it = idle_clients_.begin();
        while (it != idle_clients_.end() && it->parked_at < deadline) {
            dropped.push_back(std::move(it->client));
            ++it;
        }
        idle_clients_.erase(idle_clients_.begin(), it);
    }

    // True when nothing is pooled and nothing has used this pool since
    // `deadline`, so the map entry can go too. A call in flight has stamped
    // last_activity_ at acquire time, which keeps its pool alive.
    bool unusedSince(Clock::time_point deadline) {
        std::lock_guard<std::mutex> guard(lock_);
        return idle_clients_.empty() && last_activity_ < deadline;
    }

    size_t idleCount() {
        std::lock_guard<std::mutex> guard(lock_);
        return idle_clients_.size();
    }

    // Drop every idle client, but only on behalf of a caller that is still
    // looking at the generation it drew from. Several callers notice a peer
    // restart at once; without this, the second one to fail would throw away
    // the connections the first one has already re-established, and the pool
    // would churn for as long as stale clients keep surfacing.
    void clearIfCurrent(uint64_t generation) {
        std::lock_guard<std::mutex> guard(lock_);
        if (generation != generation_) return;
        ++generation_;
        idle_clients_.clear();
    }

    void clear() {
        std::lock_guard<std::mutex> guard(lock_);
        ++generation_;
        idle_clients_.clear();
    }

   private:
    struct IdleClient {
        std::unique_ptr<coro_rpc_client> client;
        Clock::time_point parked_at;
    };

    std::mutex lock_;
    uint64_t generation_ = 0;
    // Oldest first: acquire() takes from the back and release() appends, so
    // the vector stays ordered by parked_at.
    std::vector<IdleClient> idle_clients_;
    Clock::time_point last_activity_ = Clock::now();
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

Status CoroRpcAgent::registerFunction(int func_id, const Function& func,
                                      bool offload) {
    std::lock_guard<std::mutex> guard(func_map_mutex_);
    func_map_[func_id] = Handler{func, offload};
    return Status::OK();
}

Status CoroRpcAgent::start(uint16_t& port, bool ipv6, size_t threads) {
    const static uint16_t kStartPort = 15000;
    const static uint16_t kPortRange = 2000;
    const static int kMaxRetry = 10;
    if (running_)
        return Status::InvalidArgument("RPC server already started" LOC_MARK);
    easylog::set_min_severity(easylog::Severity::FATAL);
    if (threads > 1)
        LOG(INFO) << "CoroRpcAgent: RPC server threads set to " << threads;
    for (int retry = 0; retry < kMaxRetry; ++retry) {
        try {
            if (port == 0)
                port = kStartPort + SimpleRandom::Get().next(kPortRange);
            server_ = new coro_rpc::coro_rpc_server(threads, port,
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

Lazy<void> CoroRpcAgent::process(int func_id) {
    auto* ctx = co_await coro_rpc::get_context_in_coro();
    // registerFunction() may run while the server is serving, and a rehash
    // under a bare find() is a data race. The handler is copied out and the
    // lock released before the coroutine can suspend below.
    Handler handler;
    {
        std::lock_guard<std::mutex> guard(func_map_mutex_);
        auto it = func_map_.find(func_id);
        if (it == func_map_.end()) {
            throw std::runtime_error("Unknown TENT RPC function: " +
                                     std::to_string(func_id));
        }
        handler = it->second;
    }

    auto request = ctx->get_request_attachment();
    std::string response;
    if (handler.offload) {
        // Suspends this coroutine, freeing the io_context thread. The request
        // buffer belongs to the context_info this coroutine keeps alive, so
        // the view survives the hop. tl_inside_rpc_handler is deliberately
        // not set: it guards against deadlocking the RPC thread, which an
        // offloaded handler no longer occupies.
        //
        // post() reports a throwing handler through the Try instead of
        // unwinding, so rethrow here: the router turns that into the same
        // rpc_throw_exception an inline handler would produce. Dropping it
        // would answer a malformed request with an empty success.
        auto result =
            co_await coro_io::post([&] { handler.func(request, response); });
        result.value();
    } else {
        RpcHandlerScope handler_scope(tl_inside_rpc_handler);
        handler.func(request, response);
    }
    ctx->set_response_attachment(std::move(response));
}

namespace {
// How long a pooled connection may sit unused before it is dropped. Long
// enough that a peer which is merely between bursts keeps its connections,
// short enough that a peer which has left stops holding descriptors.
constexpr auto kClientIdleTimeout = std::chrono::minutes(5);
// Bounds how often the sweep walks every pool; it runs under pools_mutex_,
// which the call path takes anyway.
constexpr auto kIdleSweepInterval = std::chrono::seconds(10);
}  // namespace

size_t CoroRpcAgent::evictIdleClients(std::chrono::nanoseconds idle_timeout) {
    const auto deadline = ClientPool::Clock::now() - idle_timeout;
    // Both lists outlive the critical section below on purpose: closing a
    // connection runs on the client io_context, and this sweep is itself
    // reached from a coroutine on that pool, so no destructor may run while
    // pools_mutex_ is held.
    std::vector<std::unique_ptr<coro_rpc_client>> dropped;
    std::vector<std::shared_ptr<ClientPool>> drained;
    {
        std::lock_guard<std::mutex> lock(pools_mutex_);
        last_idle_sweep_ = ClientPool::Clock::now();
        for (auto it = pools_.begin(); it != pools_.end();) {
            it->second->evictIdle(deadline, dropped);
            if (it->second->unusedSince(deadline)) {
                // A concurrent caller may still hold this shared_ptr, so the
                // erase below is not necessarily the last reference; keeping
                // one here makes the release point explicit either way.
                drained.push_back(it->second);
                it = pools_.erase(it);
            } else {
                ++it;
            }
        }
    }
    const size_t count = dropped.size();
    dropped.clear();
    drained.clear();
    return count;
}

size_t CoroRpcAgent::idleClientCountForTest(const std::string& server_addr) {
    std::lock_guard<std::mutex> lock(pools_mutex_);
    auto it = pools_.find(server_addr);
    return it == pools_.end() ? 0 : it->second->idleCount();
}

size_t CoroRpcAgent::poolCountForTest() {
    std::lock_guard<std::mutex> lock(pools_mutex_);
    return pools_.size();
}

std::shared_ptr<ClientPool> CoroRpcAgent::getOrCreatePool(
    const std::string& server_addr) {
    {
        std::lock_guard<std::mutex> lock(pools_mutex_);
        auto it = pools_.find(server_addr);
        if (it != pools_.end() &&
            ClientPool::Clock::now() - last_idle_sweep_ <= kIdleSweepInterval) {
            return it->second;
        }
    }
    // Sweep on the two events worth paying for: the first call of each sweep
    // interval, and any call to an address with no pool yet - in a deployment
    // where peers keep being replaced, a new address is the event that
    // recurs. Run outside pools_mutex_ so the dropped clients' destructors do
    // not execute under it.
    evictIdleClients(kClientIdleTimeout);
    std::lock_guard<std::mutex> lock(pools_mutex_);
    auto it = pools_.find(server_addr);
    if (it == pools_.end()) {
        it = pools_.emplace(server_addr, std::make_shared<ClientPool>()).first;
    }
    return it->second;
}

namespace {

// True when the failure came back from the peer as a reply, which means the
// connection carried a full round trip and is still usable. Every other
// failure leaves the connection in an unknown state.
bool peerAnswered(coro_rpc::errc ec) {
    switch (ec) {
        case coro_rpc::errc::rpc_throw_exception:
        case coro_rpc::errc::function_not_registered:
        case coro_rpc::errc::invalid_rpc_arguments:
        case coro_rpc::errc::invalid_rpc_result:
        case coro_rpc::errc::message_too_large:
            return true;
        default:
            return false;
    }
}

}  // namespace

Lazy<std::pair<Status, std::string>> CoroRpcAgent::callCoroutine(
    std::string server_addr, int func_id, std::string request,
    bool retry_rpc_error_once) {
    if (tl_inside_rpc_handler) {
        co_return std::make_pair(
            Status::InvalidArgument(
                "RPC call from RPC handler is forbidden" LOC_MARK),
            "");
    }

    const int max_attempts = retry_rpc_error_once ? 2 : 1;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        auto pool = getOrCreatePool(server_addr);
        auto acquired = pool->acquire();
        const uint64_t generation = acquired.generation;
        ClientLease lease{std::move(acquired.client), pool, false};
        const bool from_pool = lease.client != nullptr;

        if (!lease.client) {
            lease.client = std::make_unique<coro_rpc_client>(
                GetTransferEngineRpcClientIoContextPool().get_executor());
            auto conn_result = co_await lease.client->connect(server_addr);
            if (conn_result.val() != 0) {
                lease.broken = true;
                auto msg =
                    "Failed to connect RPC server. server: " + server_addr +
                    ", func_id: " + std::to_string(func_id) +
                    ", message: " + std::string{conn_result.message()};
                if (attempt + 1 < max_attempts) continue;
                co_return std::make_pair(
                    Status::RpcConnectionError(msg + LOC_MARK), "");
            }
        }

        lease->set_req_attachment(request);

        auto call_result =
            co_await lease->call<&CoroRpcAgent::process>(func_id);

        if (!call_result.has_value()) {
            lease.broken = true;
            // An idle pooled connection only learns that its peer restarted
            // when it is next used. Drop the whole pool so a retry starts
            // from a fresh connection.
            if (from_pool && !peerAnswered(call_result.error().code)) {
                pool->clearIfCurrent(generation);
            }
            auto msg = "Failed to call RPC function. server: " + server_addr +
                       ", func_id: " + std::to_string(func_id) +
                       ", message: " + std::string{call_result.error().msg};
            if (attempt + 1 < max_attempts) continue;
            co_return std::make_pair(Status::RpcServiceError(msg + LOC_MARK),
                                     "");
        }

        // The internal buffer is padded for small attachments; trim the moved
        // string to the real attachment length from the view.
        const size_t len = lease->get_resp_attachment().size();
        std::string response = lease->release_resp_attachment();
        response.resize(len);

        co_return std::make_pair(Status::OK(), std::move(response));
    }

    co_return std::make_pair(
        Status::InternalError("RPC retry loop exhausted" LOC_MARK), "");
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

Status CoroRpcAgent::callOwned(const std::string& server_addr, int func_id,
                               std::string request, std::string& response) {
    auto [status, resp] = async_simple::coro::syncAwait(
        callCoroutine(server_addr, func_id, std::move(request)));

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

void CoroRpcAgent::callAsyncRetryOnce(const std::string& server_addr,
                                      int func_id, const std::string& request,
                                      AsyncCallback callback) {
    callCoroutine(server_addr, func_id, request, true)
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
