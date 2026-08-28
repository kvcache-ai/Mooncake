// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_server.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <future>
#include <utility>

#include <glog/logging.h>

#include "tent/transport/tcp/high_performance_tcp_protocol.h"

namespace mooncake::tent {

class HighPerformanceTcpServer::Session
    : public std::enable_shared_from_this<HighPerformanceTcpServer::Session> {
   public:
    Session(HighPerformanceTcpServer* parent, size_t worker_id,
            std::shared_ptr<asio::ip::tcp::socket> socket, Config config,
            HighPerformanceTcpBufferRegistry* registry)
        : parent_(parent),
          worker_id_(worker_id),
          socket_(std::move(socket)),
          config_(std::move(config)),
          registry_(registry),
          timer_(socket_->get_executor()) {}

    void start() { readHeader(); }

    void cancel() {
        forced_close_ = true;
        cancelTimer();
        std::error_code ignored;
        socket_->cancel(ignored);
        socket_->shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        socket_->close(ignored);
        // Every live session always has exactly one async socket operation
        // outstanding. Its callback performs the final lease release and
        // registry removal, so cancel() never publishes closure early.
    }

   private:
    void readHeader() {
        if (forced_close_) {
            finishClosed();
            return;
        }
        lease_.reset();
        body_offset_ = 0;
        ++request_epoch_;
        armProgressTimer(request_epoch_);
        auto self = shared_from_this();
        asio::async_read(
            *socket_, asio::buffer(request_bytes_),
            [self](const std::error_code& error, size_t bytes) {
                if (self->forced_close_) {
                    self->finishClosed();
                    return;
                }
                if (error || bytes != kHighPerformanceTcpRequestSize) {
                    self->finishClosed();
                    return;
                }
                self->handleHeader();
            });
    }

    void handleHeader() {
        HighPerformanceTcpStatus wire_error =
            HighPerformanceTcpStatus::kInternalError;
        const Status decoded = DecodeHighPerformanceTcpRequest(
            request_bytes_.data(), request_bytes_.size(), &request_,
            &wire_error);
        if (!decoded.ok()) {
            sendErrorAndClose(wire_error);
            return;
        }
        if (request_.length > config_.max_transfer_bytes) {
            sendResponse(HighPerformanceTcpStatus::kBadLength, 0, true);
            return;
        }

        HighPerformanceTcpBufferRegistry::AcquireFailure failure =
            HighPerformanceTcpBufferRegistry::AcquireFailure::kNone;
        const Status lease_status = registry_->acquireRemoteLease(
            request_.remote_addr, request_.length, request_.registration_id,
            request_.opcode, &lease_, &failure);
        if (!lease_status.ok()) {
            sendResponse(HighPerformanceTcpWireStatusForAcquireFailure(failure),
                         0, false);
            return;
        }

        armProgressTimer(request_epoch_);
        if (request_.opcode == HighPerformanceTcpOpcode::kRead) {
            // The response says the entire requested range is valid. The
            // client still completes only after all payload bytes arrive.
            sendResponse(HighPerformanceTcpStatus::kOk, request_.length, false,
                         [self = shared_from_this()] {
                             self->body_offset_ = 0;
                             self->writeReadBodyChunk();
                         });
        } else {
            body_offset_ = 0;
            readWriteBodyChunk();
        }
    }

    uint8_t* remoteDataAt(uint64_t offset) {
        return static_cast<uint8_t*>(lease_.data()) +
               (request_.remote_addr - lease_.base()) + offset;
    }

    void writeReadBodyChunk() {
        if (body_offset_ == request_.length) {
            lease_.reset();
            cancelTimer();
            readHeader();
            return;
        }
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(
            config_.chunk_size, request_.length - body_offset_));
        auto self = shared_from_this();
        asio::async_write(
            *socket_, asio::buffer(remoteDataAt(body_offset_), chunk),
            [self, chunk](const std::error_code& error, size_t bytes) {
                if (self->forced_close_) {
                    self->finishClosed();
                    return;
                }
                if (error || bytes != chunk) {
                    self->finishClosed();
                    return;
                }
                self->body_offset_ += bytes;
                self->armProgressTimer(self->request_epoch_);
                self->writeReadBodyChunk();
            });
    }

    void readWriteBodyChunk() {
        if (body_offset_ == request_.length) {
            // The final async_read callback has returned: every byte is now in
            // destination DRAM and no socket callback can touch the range.
            lease_.reset();
            armProgressTimer(request_epoch_);
            sendResponse(HighPerformanceTcpStatus::kOk, request_.length, false,
                         [self = shared_from_this()] {
                             self->cancelTimer();
                             self->readHeader();
                         });
            return;
        }
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(
            config_.chunk_size, request_.length - body_offset_));
        auto self = shared_from_this();
        asio::async_read(
            *socket_, asio::buffer(remoteDataAt(body_offset_), chunk),
            [self, chunk](const std::error_code& error, size_t bytes) {
                if (self->forced_close_) {
                    self->finishClosed();
                    return;
                }
                if (error || bytes != chunk) {
                    self->finishClosed();
                    return;
                }
                self->body_offset_ += bytes;
                self->armProgressTimer(self->request_epoch_);
                self->readWriteBodyChunk();
            });
    }

    void sendErrorAndClose(HighPerformanceTcpStatus status) {
        request_.request_id = 0;
        sendResponse(status, 0, true);
    }

    void sendResponse(HighPerformanceTcpStatus status, uint64_t committed,
                      bool close_after,
                      std::function<void()> continuation = {}) {
        response_bytes_ = EncodeHighPerformanceTcpResponse(
            {status, request_.request_id, committed});
        // Error responses are I/O too: arm a deadline even when request
        // validation failed before a lease was acquired.
        armProgressTimer(request_epoch_);
        auto self = shared_from_this();
        asio::async_write(
            *socket_, asio::buffer(response_bytes_),
            [self, close_after, continuation = std::move(continuation)](
                const std::error_code& error, size_t bytes) mutable {
                if (self->forced_close_) {
                    self->finishClosed();
                    return;
                }
                if (error || bytes != kHighPerformanceTcpResponseSize) {
                    self->finishClosed();
                    return;
                }
                if (close_after) {
                    self->finishClosed();
                    return;
                }
                self->armProgressTimer(self->request_epoch_);
                if (continuation) {
                    continuation();
                } else {
                    self->cancelTimer();
                    self->readHeader();
                }
            });
    }

    void armProgressTimer(uint64_t epoch) {
        const uint64_t generation = ++timer_generation_;
        timer_.expires_after(
            std::chrono::milliseconds(config_.progress_timeout_ms));
        auto self = shared_from_this();
        timer_.async_wait(
            [self, epoch, generation](const std::error_code& error) {
                if (error == asio::error::operation_aborted) return;
                if (error || epoch != self->request_epoch_ ||
                    generation != self->timer_generation_) {
                    return;
                }
                self->forced_close_ = true;
                std::error_code ignored;
                self->socket_->cancel(ignored);
                self->socket_->shutdown(asio::ip::tcp::socket::shutdown_both,
                                        ignored);
                self->socket_->close(ignored);
                // Finalization waits for the canceled body/response callback.
            });
    }

    void cancelTimer() {
        ++timer_generation_;
        std::error_code ignored;
        timer_.cancel(ignored);
    }

    void finishClosed() {
        if (closed_) return;
        closed_ = true;
        cancelTimer();
        std::error_code ignored;
        socket_->cancel(ignored);
        socket_->shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        socket_->close(ignored);
        lease_.reset();
        parent_->onSessionClosed(worker_id_, shared_from_this());
    }

    HighPerformanceTcpServer* parent_;
    size_t worker_id_;
    std::shared_ptr<asio::ip::tcp::socket> socket_;
    Config config_;
    HighPerformanceTcpBufferRegistry* registry_;
    asio::steady_timer timer_;

    std::array<uint8_t, kHighPerformanceTcpRequestSize> request_bytes_{};
    std::array<uint8_t, kHighPerformanceTcpResponseSize> response_bytes_{};
    HighPerformanceTcpRequestFrame request_;
    HighPerformanceTcpBufferRegistry::Lease lease_;
    uint64_t body_offset_{0};
    uint64_t request_epoch_{0};
    uint64_t timer_generation_{0};
    bool forced_close_{false};
    bool closed_{false};
};

HighPerformanceTcpServer::HighPerformanceTcpServer(
    Config config, HighPerformanceTcpBufferRegistry* registry,
    HighPerformanceTcpWorkers* workers)
    : config_(std::move(config)), registry_(registry), workers_(workers) {
    if (workers_ != nullptr) sessions_.resize(workers_->workerCount());
}

HighPerformanceTcpServer::~HighPerformanceTcpServer() {
    (void)stop();
    DCHECK(workers_ == nullptr ||
           active_sessions_.load(std::memory_order_acquire) == 0)
        << "HP TCP server destroyed with active sessions";
}

bool HighPerformanceTcpServer::reserveConnection() {
    size_t current = active_sessions_.load(std::memory_order_acquire);
    while (current < config_.max_connections) {
        if (active_sessions_.compare_exchange_weak(current, current + 1,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
            return true;
        }
    }
    return false;
}

Status HighPerformanceTcpServer::start(uint16_t* bound_port) {
    if (bound_port == nullptr || registry_ == nullptr || workers_ == nullptr ||
        workers_->workerCount() == 0 || config_.chunk_size == 0 ||
        config_.max_transfer_bytes == 0 || config_.progress_timeout_ms == 0 ||
        config_.max_connections == 0) {
        return Status::InvalidArgument(
            "invalid HP TCP server configuration" LOC_MARK);
    }
    if (started_.exchange(true, std::memory_order_acq_rel)) {
        return Status::InvalidArgument(
            "HP TCP server already started" LOC_MARK);
    }

    try {
        asio::ip::tcp::endpoint endpoint;
        if (config_.bind_address.empty()) {
            endpoint =
                asio::ip::tcp::endpoint(asio::ip::tcp::v4(), config_.port);
        } else {
            endpoint = asio::ip::tcp::endpoint(
                asio::ip::make_address(config_.bind_address), config_.port);
        }
        acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(accept_io_);
        acceptor_->open(endpoint.protocol());
        acceptor_->set_option(asio::socket_base::reuse_address(true));
        acceptor_->bind(endpoint);
        acceptor_->listen(asio::socket_base::max_listen_connections);
        *bound_port = acceptor_->local_endpoint().port();
        accept_guard_.emplace(asio::make_work_guard(accept_io_));
        stopping_.store(false, std::memory_order_release);
        Status accept_status = startAccept();
        if (!accept_status.ok()) {
            std::error_code ignored;
            acceptor_->close(ignored);
            accept_guard_->reset();
            started_.store(false, std::memory_order_release);
            acceptor_.reset();
            accept_guard_.reset();
            return accept_status;
        }
        accept_thread_ = std::thread([this] {
            try {
                accept_io_.run();
            } catch (const std::exception& error) {
                LOG(ERROR) << "HP TCP accept loop failed: " << error.what();
                stopping_.store(true, std::memory_order_release);
            } catch (...) {
                LOG(ERROR) << "HP TCP accept loop failed";
                stopping_.store(true, std::memory_order_release);
            }
        });
        return Status::OK();
    } catch (const std::exception& error) {
        started_.store(false, std::memory_order_release);
        acceptor_.reset();
        accept_guard_.reset();
        return Status::InternalError(
            std::string("HP TCP listener start failed: ") + error.what() +
            LOC_MARK);
    }
}

Status HighPerformanceTcpServer::startAccept() {
    if (stopping_.load(std::memory_order_acquire) || !acceptor_ ||
        !acceptor_->is_open()) {
        return Status::OK();
    }
    try {
        const size_t worker_id =
            next_worker_.fetch_add(1, std::memory_order_relaxed) %
            workers_->workerCount();
        auto socket = std::make_shared<asio::ip::tcp::socket>(
            workers_->ioContext(worker_id));
        acceptor_->async_accept(*socket, [this, worker_id, socket](
                                             const std::error_code& error) {
            if (!error && !stopping_.load(std::memory_order_acquire)) {
                if (reserveConnection()) {
                    try {
                        asio::post(workers_->ioContext(worker_id),
                                   [this, worker_id, socket] {
                                       installAcceptedSocket(worker_id, socket);
                                   });
                    } catch (const std::exception& post_error) {
                        LOG(ERROR) << "HP TCP accepted-socket dispatch failed: "
                                   << post_error.what();
                        if (active_sessions_.fetch_sub(
                                1, std::memory_order_acq_rel) == 1) {
                            sessions_wait_cv_.notify_all();
                        }
                        std::error_code ignored;
                        socket->close(ignored);
                    } catch (...) {
                        LOG(ERROR) << "HP TCP accepted-socket dispatch failed";
                        if (active_sessions_.fetch_sub(
                                1, std::memory_order_acq_rel) == 1) {
                            sessions_wait_cv_.notify_all();
                        }
                        std::error_code ignored;
                        socket->close(ignored);
                    }
                } else {
                    std::error_code ignored;
                    socket->close(ignored);
                }
            }
            if (!stopping_.load(std::memory_order_acquire)) {
                Status next = startAccept();
                if (!next.ok()) {
                    LOG(ERROR)
                        << "HP TCP accept re-arm failed: " << next.ToString();
                    stopping_.store(true, std::memory_order_release);
                    std::error_code ignored;
                    if (acceptor_) acceptor_->close(ignored);
                    if (accept_guard_) accept_guard_->reset();
                }
            }
        });
        return Status::OK();
    } catch (const std::exception& error) {
        return Status::InternalError(
            std::string("HP TCP accept initiation failed: ") + error.what() +
            LOC_MARK);
    } catch (...) {
        return Status::InternalError(
            "HP TCP accept initiation failed" LOC_MARK);
    }
}

void HighPerformanceTcpServer::installAcceptedSocket(
    size_t worker_id, std::shared_ptr<asio::ip::tcp::socket> socket) {
    if (stopping_.load(std::memory_order_acquire)) {
        std::error_code ignored;
        socket->close(ignored);
        if (active_sessions_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            sessions_wait_cv_.notify_all();
        }
        return;
    }
    std::error_code error;
    socket->set_option(asio::ip::tcp::no_delay(true), error);
    if (error) {
        socket->close(error);
        if (active_sessions_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            sessions_wait_cv_.notify_all();
        }
        return;
    }

    std::shared_ptr<Session> session;
    try {
        session = std::make_shared<Session>(this, worker_id, socket, config_,
                                            registry_);
        sessions_[worker_id].insert(session);
        session->start();
    } catch (const std::exception& start_error) {
        LOG(ERROR) << "HP TCP server session start failed: "
                   << start_error.what();
        if (session) sessions_[worker_id].erase(session);
        std::error_code ignored;
        socket->cancel(ignored);
        socket->close(ignored);
        if (active_sessions_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            sessions_wait_cv_.notify_all();
        }
    } catch (...) {
        LOG(ERROR) << "HP TCP server session start failed";
        if (session) sessions_[worker_id].erase(session);
        std::error_code ignored;
        socket->cancel(ignored);
        socket->close(ignored);
        if (active_sessions_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            sessions_wait_cv_.notify_all();
        }
    }
}

void HighPerformanceTcpServer::onSessionClosed(
    size_t worker_id, const std::shared_ptr<Session>& session) {
    if (worker_id < sessions_.size()) sessions_[worker_id].erase(session);
    if (active_sessions_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        std::lock_guard<std::mutex> lock(sessions_wait_mutex_);
        sessions_wait_cv_.notify_all();
    }
}

Status HighPerformanceTcpServer::stopAccepting() {
    if (!started_.load(std::memory_order_acquire)) return Status::OK();
    if (stopping_.exchange(true, std::memory_order_acq_rel)) {
        if (accept_thread_.joinable()) accept_thread_.join();
        return Status::OK();
    }

    if (accept_thread_.joinable()) {
        auto done = std::make_shared<std::promise<void>>();
        auto future = done->get_future();
        try {
            asio::post(accept_io_, [this, done] {
                std::error_code ignored;
                if (acceptor_) {
                    acceptor_->cancel(ignored);
                    acceptor_->close(ignored);
                }
                if (accept_guard_) accept_guard_->reset();
                done->set_value();
            });
            future.wait();
        } catch (...) {
            std::error_code ignored;
            if (acceptor_) acceptor_->close(ignored);
            if (accept_guard_) accept_guard_->reset();
        }
        accept_thread_.join();
    }
    return Status::OK();
}

void HighPerformanceTcpServer::cancelWorkerSessions(size_t worker_id) {
    if (worker_id >= sessions_.size()) return;
    // Copy because Session::finishClosed erases from this set later.
    std::vector<std::shared_ptr<Session>> copy(sessions_[worker_id].begin(),
                                               sessions_[worker_id].end());
    for (const auto& session : copy) session->cancel();
}

Status HighPerformanceTcpServer::cancelAll() {
    if (workers_ == nullptr || active_sessions_.load() == 0) {
        return Status::OK();
    }
    if (workers_->onWorkerThread()) {
        return Status::InvalidArgument(
            "HP TCP server cancelAll cannot block a worker" LOC_MARK);
    }
    if (!workers_->controlContextAvailable()) {
        return active_sessions_.load() == 0
                   ? Status::OK()
                   : Status::InternalError(
                         "HP TCP worker contexts unavailable with live "
                         "sessions" LOC_MARK);
    }

    try {
        for (size_t i = 0; i < workers_->workerCount(); ++i) {
            asio::post(workers_->ioContext(i),
                       [this, i] { cancelWorkerSessions(i); });
        }
    } catch (const std::exception& error) {
        return Status::InternalError(
            std::string("HP TCP server cancellation post failed: ") +
            error.what() + LOC_MARK);
    }
    CHECK_STATUS(workers_->barrier());

    std::unique_lock<std::mutex> lock(sessions_wait_mutex_);
    sessions_wait_cv_.wait(lock, [&] {
        return active_sessions_.load(std::memory_order_acquire) == 0;
    });
    return Status::OK();
}

Status HighPerformanceTcpServer::stop() {
    Status first = stopAccepting();
    Status canceled = cancelAll();
    acceptor_.reset();
    accept_guard_.reset();
    started_.store(false, std::memory_order_release);
    if (!first.ok()) return first;
    return canceled;
}

}  // namespace mooncake::tent
