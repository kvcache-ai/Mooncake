// Copyright 2026 KVCache.AI
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

#include "engine.h"

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_io/server_acceptor.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>
#include <ylt/easylog.hpp>

#include "error.h"

namespace mooncake {
namespace {

enum class PayloadDecision { kOk, kTooLarge, kOverBudget };

class HandshakeAdmission {
   public:
    static HandshakeAdmission &Get() {
        static HandshakeAdmission admission;
        return admission;
    }

    void Configure(uint64_t max_bytes, uint64_t max_frame) {
        std::lock_guard<std::mutex> lock(mu_);
        max_bytes_ = max_bytes;
        max_frame_ = max_frame;
    }

    PayloadDecision Reserve(uint64_t body, uint64_t attach) {
        const uint64_t sum = body + attach;
        std::lock_guard<std::mutex> lock(mu_);
        if (body > max_frame_ || attach > max_frame_ ||
            sum > max_frame_ + kHandshakeEnvelopeBytes) {
            return PayloadDecision::kTooLarge;
        }
        if (bytes_ + sum > max_bytes_) return PayloadDecision::kOverBudget;
        bytes_ += sum;
        return PayloadDecision::kOk;
    }

    void Release(uint64_t n) {
        std::lock_guard<std::mutex> lock(mu_);
        if (bytes_ >= n)
            bytes_ -= n;
        else
            bytes_ = 0;
    }

   private:
    std::mutex mu_;
    uint64_t max_bytes_ = 512ull << 20;
    uint64_t max_frame_ = 1ull << 20;
    uint64_t bytes_ = 0;
};

struct BudgetTicket {
    uint64_t bytes = 0;
    ~BudgetTicket() {
        if (bytes != 0) HandshakeAdmission::Get().Release(bytes);
    }
};

thread_local std::unique_ptr<BudgetTicket> pending_budget;

std::unique_ptr<BudgetTicket> TakePendingBudget() {
    return std::move(pending_budget);
}

struct HandshakeBudgetRouter;

struct HandshakeRpcProtocol : coro_rpc::protocol::coro_rpc_protocol {
    using router = HandshakeBudgetRouter;

    template <typename Socket>
    static async_simple::coro::Lazy<std::error_code> read_payload(
        Socket &socket, req_header &req_head, std::string &buffer,
        coro_io::heterogeneous_buffer &attachment) {
        const uint64_t body = req_head.length;
        const uint64_t attach = req_head.attach_length;
        const PayloadDecision decision =
            HandshakeAdmission::Get().Reserve(body, attach);
        if (decision == PayloadDecision::kTooLarge) {
            co_return std::make_error_code(std::errc::message_size);
        }
        if (decision == PayloadDecision::kOverBudget) {
            co_return std::make_error_code(std::errc::not_enough_memory);
        }
        auto ec = co_await coro_rpc_protocol::read_payload(socket, req_head,
                                                           buffer, attachment);
        if (ec) {
            HandshakeAdmission::Get().Release(body + attach);
            co_return ec;
        }
        auto ticket = std::make_unique<BudgetTicket>();
        ticket->bytes = body + attach;
        pending_budget = std::move(ticket);
        co_return std::error_code{};
    }
};

struct HandshakeBudgetRouter
    : coro_rpc::protocol::router<HandshakeRpcProtocol> {
    using base_router = coro_rpc::protocol::router<HandshakeRpcProtocol>;

    template <typename Handler, typename Protocols>
    async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>
    route_coro(uint64_t conn_id, uint64_t req_id, Handler handler,
               std::string_view data, Protocols protocols,
               const HandshakeRpcProtocol::route_key_t &route_key) {
        std::unique_ptr<BudgetTicket> ticket = TakePendingBudget();
        auto result = co_await base_router::route_coro(
            conn_id, req_id, handler, data, std::move(protocols), route_key);
        co_return result;
    }
};

struct HandshakeServerConfig : coro_rpc::config_t {
    using rpc_protocol = HandshakeRpcProtocol;
};

using HandshakeServer = coro_rpc::coro_rpc_server_base<HandshakeServerConfig>;

class BoundFdAcceptor : public coro_io::server_acceptor_base {
   public:
    BoundFdAcceptor(int fd, std::string address, uint16_t port)
        : server_acceptor_base(std::move(address), port), fd_(fd) {}

    ~BoundFdAcceptor() override = default;

    bool consumed() const { return consumed_; }

    coro_io::listen_errc listen() override {
        if (pool_ == nullptr || fd_ < 0)
            return coro_io::listen_errc::open_error;
        executor_ = pool_->get_executor();
        acceptor_.emplace(executor_->get_asio_executor());
        sockaddr_storage storage{};
        socklen_t length = sizeof(storage);
        if (getsockname(fd_, reinterpret_cast<sockaddr *>(&storage), &length) !=
            0) {
            return coro_io::listen_errc::bad_address;
        }
        const auto protocol = storage.ss_family == AF_INET6
                                  ? asio::ip::tcp::v6()
                                  : asio::ip::tcp::v4();
        asio::error_code ec;
        acceptor_->assign(protocol, fd_, ec);
        if (ec) return coro_io::listen_errc::listen_error;
        consumed_ = true;
        fd_ = -1;
        acceptor_->listen(asio::socket_base::max_listen_connections, ec);
        if (ec) return coro_io::listen_errc::listen_error;
        const auto endpoint = acceptor_->local_endpoint(ec);
        if (ec) return coro_io::listen_errc::listen_error;
        port_ = endpoint.port();
        return coro_io::listen_errc::ok;
    }

    async_simple::coro::Lazy<
        ylt::expected<coro_io::socket_wrapper_t, std::error_code>>
    accept() override {
        accept_started_.store(true, std::memory_order_release);
        auto socket_executor = pool_->get_executor();
        asio::ip::tcp::socket socket(socket_executor->get_asio_executor());
        co_await coro_io::dispatch(executor_->get_asio_executor());
        auto error = co_await coro_io::async_accept(*acceptor_, socket);
        if (error) {
            if (error == asio::error::operation_aborted ||
                error == asio::error::bad_descriptor) {
                acceptor_close_waiter_.set_value();
            }
            co_return ylt::unexpected<std::error_code>{error};
        }
        co_return coro_io::socket_wrapper_t{std::move(socket), socket_executor};
    }

    void close_now() override {
        if (!acceptor_) return;
        if (!accept_started_.load(std::memory_order_acquire)) {
            coro_io::detail::close_acceptor_now(*acceptor_);
            return;
        }
        asio::dispatch(executor_->get_asio_executor(), [this]() {
            if (acceptor_) coro_io::detail::close_acceptor_now(*acceptor_);
        });
    }

    void close() override {
        close_now();
        if (accept_started_.load(std::memory_order_acquire)) {
            acceptor_close_future_.wait();
        }
    }

   private:
    int fd_ = -1;
    bool consumed_ = false;
    std::optional<asio::ip::tcp::acceptor> acceptor_;
    coro_io::ExecutorWrapper<> *executor_ = nullptr;
    std::promise<void> acceptor_close_waiter_;
    std::future<void> acceptor_close_future_ =
        acceptor_close_waiter_.get_future();
    std::atomic<bool> accept_started_{false};
};

}  // namespace

struct HandshakeRpc::ListenState {
    HandshakeServer *server = nullptr;
};

// Connection and Metadata callbacks are blocking user code. Keep them off the
// RPC IO threads, on a pool this object owns. coro_io::post's default executor
// is a process-wide pool with one thread per CPU and is not used here.
struct HandshakeRpc::OffloadPool {
    static constexpr unsigned kThreads = 2;

    OffloadPool() {
        pool = std::make_shared<coro_io::io_context_pool>(kThreads);
        runner = std::thread([pool = pool] { pool->run(); });
    }

    ~OffloadPool() {
        if (pool) pool->stop();
        if (runner.joinable()) runner.join();
    }

    OffloadPool(const OffloadPool &) = delete;
    OffloadPool &operator=(const OffloadPool &) = delete;

    std::shared_ptr<coro_io::io_context_pool> pool;
    std::thread runner;
};

bool ParseHandshakeJson(std::string_view text, Json::Value &value,
                        std::string &error) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    return reader->parse(text.data(), text.data() + text.size(), &value,
                         &error);
}

std::string WriteHandshakeJson(const Json::Value &value) {
    return Json::FastWriter{}.write(value);
}

HandshakeRpc::HandshakeRpc()
    : limits_(LoadHandshakeLimits()),
      pool_(std::make_unique<HandshakeClientPool>(limits_)) {}

HandshakeRpc::~HandshakeRpc() { Stop(); }

int HandshakeRpc::Start(uint16_t port, int sockfd, bool *fd_consumed) {
    if (fd_consumed != nullptr) *fd_consumed = false;
    limits_ = LoadHandshakeLimits();
    pool_->UpdateLimits(limits_);
    HandshakeAdmission::Get().Configure(limits_.max_bytes, limits_.max_frame);
    static std::once_flag log_once;
    std::call_once(log_once,
                   [] { easylog::set_min_severity(easylog::Severity::ERROR); });

    if (!offload_) offload_ = std::make_unique<OffloadPool>();

    HandshakeServerConfig config;
    config.thread_num = static_cast<unsigned>(limits_.threads);
    config.port = port;
    config.address = limits_.ipv6 ? "::" : "0.0.0.0";
    config.is_enable_tcp_no_delay = true;
    config.conn_timeout_duration =
        std::chrono::seconds(limits_.timeout_seconds);

    std::vector<std::unique_ptr<coro_io::server_acceptor_base>> acceptors;
    BoundFdAcceptor *bound = nullptr;
    if (sockfd >= 0) {
        auto owned =
            std::make_unique<BoundFdAcceptor>(sockfd, config.address, port);
        bound = owned.get();
        acceptors.push_back(std::move(owned));
    }
    auto listen = std::make_unique<ListenState>();
    if (acceptors.empty()) {
        listen->server = new HandshakeServer(config);
    } else {
        listen->server = new HandshakeServer(config, std::move(acceptors));
    }
    listen->server->register_handler<&HandshakeRpc::process>(this);
    listen->server->async_start();
    const auto err = listen->server->get_errc();
    if (bound != nullptr && bound->consumed() && fd_consumed != nullptr) {
        *fd_consumed = true;
    }
    if (err) {
        LOG(ERROR) << "coro handshake listen failed: " << err.message();
        delete listen->server;
        offload_.reset();
        return ERR_SOCKET;
    }
    listen_ = std::move(listen);
    port_ = port;
    running_ = true;
    LOG(INFO) << "coro handshake listening on port " << port_
              << " threads=" << limits_.threads
              << " offload_threads=" << OffloadPool::kThreads
              << " max_inflight_bytes=" << limits_.max_bytes
              << " max_frame=" << limits_.max_frame;
    return 0;
}

void HandshakeRpc::Stop() {
    {
        std::lock_guard<std::mutex> lock(lifecycle_mu_);
        stopping_ = true;
    }
    if (pool_) pool_->StopSweeper();
    if (listen_ != nullptr && listen_->server != nullptr) {
        for (auto &acceptor : listen_->server->get_acceptors())
            acceptor->close();
    }
    {
        std::unique_lock<std::mutex> lock(lifecycle_mu_);
        while (inflight_ > 0) {
            LOG(WARNING) << "Waiting for " << inflight_
                         << " handshake handlers to finish";
            lifecycle_cv_.wait_for(lock, std::chrono::seconds(1));
        }
    }
    if (listen_ != nullptr && listen_->server != nullptr) {
        listen_->server->stop();
        delete listen_->server;
        listen_->server = nullptr;
    }
    listen_.reset();
    running_ = false;
    // Handlers have finished, including callbacks sitting on this pool.
    offload_.reset();
    if (pool_) pool_->Clear();
}

void HandshakeRpc::SetCallback(int func_id,
                               HandShakePlugin::OnReceiveCallBack callback) {
    std::lock_guard<std::mutex> lock(callback_mu_);
    callbacks_[func_id] = std::move(callback);
}

bool HandshakeRpc::EnterHandler() {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (stopping_) return false;
    ++inflight_;
    return true;
}

void HandshakeRpc::LeaveHandler() {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (inflight_ > 0) --inflight_;
    if (inflight_ == 0) lifecycle_cv_.notify_all();
}

bool HandshakeRpc::TryPermit() {
    int current = inflight_offload_.load(std::memory_order_relaxed);
    while (current < limits_.max_concurrency) {
        if (inflight_offload_.compare_exchange_weak(
                current, current + 1, std::memory_order_acq_rel)) {
            return true;
        }
    }
    return false;
}

void HandshakeRpc::ReleasePermit() {
    inflight_offload_.fetch_sub(1, std::memory_order_acq_rel);
}

async_simple::coro::Lazy<int32_t> HandshakeRpc::process(int func_id) {
    auto entered = EnterHandler();
    if (!entered)
        co_return static_cast<int32_t>(HandshakeStatus::kHandlerFailed);
    InflightGuard guard(this);

    auto *ctx = co_await coro_rpc::get_context_in_coro<HandshakeRpcProtocol>();
    const std::string_view attachment = ctx->get_request_attachment();

    std::optional<Permit> permit;
    const bool offload =
        func_id == kHandshakeConnection || func_id == kHandshakeMetadata;
    if (offload) {
        if (!TryPermit())
            co_return static_cast<int32_t>(HandshakeStatus::kBusy);
        permit.emplace(this);
    }

    HandShakePlugin::OnReceiveCallBack callback;
    {
        std::lock_guard<std::mutex> lock(callback_mu_);
        auto it = callbacks_.find(func_id);
        if (it != callbacks_.end()) callback = it->second;
    }
    if (func_id < kHandshakeConnection || func_id > kHandshakeProbe) {
        co_return static_cast<int32_t>(HandshakeStatus::kBadJson);
    }
    if (!callback) {
        co_return static_cast<int32_t>(HandshakeStatus::kCallbackMissing);
    }

    Json::Value peer;
    Json::Value local(Json::objectValue);
    if (func_id != kHandshakeProbe || !attachment.empty()) {
        std::string error;
        if (!attachment.empty() &&
            !ParseHandshakeJson(attachment, peer, error)) {
            LOG(ERROR) << "handshake JSON rejected: " << error;
            co_return static_cast<int32_t>(HandshakeStatus::kBadJson);
        }
    }
    int rc = 0;
    try {
        if (offload) {
            auto posted =
                co_await coro_io::post([&] { rc = callback(peer, local); },
                                       offload_->pool->get_executor());
            posted.value();
        } else {
            rc = callback(peer, local);
        }
    } catch (const std::exception &ex) {
        LOG(ERROR) << "handshake callback failed: " << ex.what();
        co_return static_cast<int32_t>(HandshakeStatus::kHandlerFailed);
    }
    if (rc != 0 && func_id != kHandshakeConnection) {
        co_return static_cast<int32_t>(HandshakeStatus::kHandlerFailed);
    }
    std::string encoded = WriteHandshakeJson(local);
    if (encoded.size() > limits_.max_frame) {
        LOG(ERROR) << "handshake response exceeds MC_HANDSHAKE_MAX_LENGTH";
        co_return static_cast<int32_t>(HandshakeStatus::kResponseTooLarge);
    }
    ctx->set_response_attachment(std::move(encoded));
    co_return static_cast<int32_t>(HandshakeStatus::kOk);
}

}  // namespace mooncake
