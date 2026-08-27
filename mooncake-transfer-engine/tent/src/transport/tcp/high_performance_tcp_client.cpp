// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_client.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <deque>
#include <limits>
#include <optional>
#include <utility>

#include <glog/logging.h>

namespace mooncake::tent {
class HighPerformanceTcpClient::Lane
    : public std::enable_shared_from_this<HighPerformanceTcpClient::Lane> {
   public:
    Lane(HighPerformanceTcpClient* parent, asio::io_context& io, Config config,
         LaneKey key)
        : parent_(parent),
          config_(std::move(config)),
          key_(std::move(key)),
          resolver_(io),
          socket_(io),
          timer_(io) {}

    void enqueue(Operation operation) {
        try {
            queue_.push_back(std::move(operation));
        } catch (const std::exception& error) {
            LOG(ERROR) << "HP TCP lane enqueue failed: " << error.what();
            completeStandalone(std::move(operation), FAILED, 0);
            return;
        } catch (...) {
            LOG(ERROR) << "HP TCP lane enqueue failed";
            completeStandalone(std::move(operation), FAILED, 0);
            return;
        }
        if (!current_) startNext();
    }

    void cancelAll(TransferStatusEnum terminal) {
        while (!queue_.empty()) {
            Operation operation = std::move(queue_.front());
            queue_.pop_front();
            completeStandalone(std::move(operation), terminal, 0);
        }
        if (!current_) {
            closeDirty();
            return;
        }
        forced_terminal_ = terminal;
        ++timer_generation_;
        std::error_code ignored;
        timer_.cancel(ignored);
        resolver_.cancel();
        closeDirty();
        // The outstanding async callback owns the final completion. Releasing
        // the operation (and its memory lease in the adapter) here would be a
        // use-after-free risk because Asio still owns the user buffer.
    }

    bool cancelRequest(uint64_t request_id) {
        for (auto it = queue_.begin(); it != queue_.end(); ++it) {
            if (it->request_id != request_id) continue;
            Operation operation = std::move(*it);
            queue_.erase(it);
            completeStandalone(std::move(operation), CANCELED, 0);
            return true;
        }
        if (!current_ || current_->request_id != request_id) return false;
        forced_terminal_ = CANCELED;
        cancelTimer();
        resolver_.cancel();
        closeDirty();
        // The live Asio callback owns final completion and buffer retirement.
        return true;
    }

   private:
    void startNext() {
        if (current_ || queue_.empty()) return;
        current_.emplace(std::move(queue_.front()));
        queue_.pop_front();
        ++operation_epoch_;
        forced_terminal_.reset();
        body_offset_ = 0;
        request_bytes_ = EncodeHighPerformanceTcpRequest(
            {current_->opcode, current_->request_id, current_->registration_id,
             current_->remote_addr, current_->length});

        try {
            if (connected_ && socket_.is_open()) {
                writeHeader(operation_epoch_);
            } else {
                resolve(operation_epoch_);
            }
        } catch (const std::exception& error) {
            LOG(ERROR) << "HP TCP async initiation failed: " << error.what();
            closeDirty();
            finishCurrent(FAILED, 0, false);
        } catch (...) {
            LOG(ERROR) << "HP TCP async initiation failed";
            closeDirty();
            finishCurrent(FAILED, 0, false);
        }
    }

    void resolve(uint64_t epoch) {
        armTimer(config_.connect_timeout_ms, epoch);
        auto self = shared_from_this();
        resolver_.async_resolve(
            key_.host, std::to_string(key_.port),
            [self, epoch](const std::error_code& error,
                          asio::ip::tcp::resolver::results_type results) {
                if (!self->matches(epoch)) return;
                if (self->finishForcedIfAny()) return;
                if (error) {
                    self->finishIoError(error);
                    return;
                }
                self->connect(epoch, std::move(results));
            });
    }

    void connect(uint64_t epoch,
                 asio::ip::tcp::resolver::results_type results) {
        std::error_code ignored;
        socket_.close(ignored);
        auto self = shared_from_this();
        asio::async_connect(socket_, results,
                            [self, epoch](const std::error_code& error,
                                          const asio::ip::tcp::endpoint&) {
                                if (!self->matches(epoch)) return;
                                if (self->finishForcedIfAny()) return;
                                if (error) {
                                    self->finishIoError(error);
                                    return;
                                }
                                self->cancelTimer();
                                self->connected_ = true;
                                self->parent_->connections_created_.fetch_add(
                                    1, std::memory_order_relaxed);
                                self->writeHeader(epoch);
                            });
    }

    void writeHeader(uint64_t epoch) {
        armTimer(config_.progress_timeout_ms, epoch);
        auto self = shared_from_this();
        asio::async_write(
            socket_, asio::buffer(request_bytes_),
            [self, epoch](const std::error_code& error, size_t bytes) {
                if (!self->matches(epoch)) return;
                if (self->finishForcedIfAny()) return;
                if (error || bytes != kHighPerformanceTcpRequestSize) {
                    self->finishIoError(error ? error
                                              : asio::error::operation_aborted);
                    return;
                }
                self->armTimer(self->config_.progress_timeout_ms, epoch);
                if (self->current_->opcode ==
                    HighPerformanceTcpOpcode::kWrite) {
                    self->body_offset_ = 0;
                    self->writeBodyChunk(epoch);
                } else {
                    self->readResponse(epoch);
                }
            });
    }

    void writeBodyChunk(uint64_t epoch) {
        if (body_offset_ == current_->length) {
            readResponse(epoch);
            return;
        }
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(
            config_.chunk_size, current_->length - body_offset_));
        auto* data = static_cast<uint8_t*>(current_->local_addr) + body_offset_;
        auto self = shared_from_this();
        asio::async_write(
            socket_, asio::buffer(data, chunk),
            [self, epoch, chunk](const std::error_code& error, size_t bytes) {
                if (!self->matches(epoch)) return;
                if (self->finishForcedIfAny()) return;
                if (error || bytes != chunk) {
                    self->finishIoError(error ? error
                                              : asio::error::operation_aborted);
                    return;
                }
                self->body_offset_ += bytes;
                self->armTimer(self->config_.progress_timeout_ms, epoch);
                self->writeBodyChunk(epoch);
            });
    }

    void readResponse(uint64_t epoch) {
        auto self = shared_from_this();
        asio::async_read(
            socket_, asio::buffer(response_bytes_),
            [self, epoch](const std::error_code& error, size_t bytes) {
                if (!self->matches(epoch)) return;
                if (self->finishForcedIfAny()) return;
                if (error || bytes != kHighPerformanceTcpResponseSize) {
                    self->finishIoError(error ? error
                                              : asio::error::operation_aborted);
                    return;
                }
                self->armTimer(self->config_.progress_timeout_ms, epoch);
                HighPerformanceTcpResponseFrame response;
                const Status decoded = DecodeHighPerformanceTcpResponse(
                    self->response_bytes_.data(), self->response_bytes_.size(),
                    &response);
                if (!decoded.ok()) {
                    self->finishProtocolError();
                    return;
                }
                if (response.request_id != self->current_->request_id) {
                    self->finishProtocolError();
                    return;
                }
                if (response.status != HighPerformanceTcpStatus::kOk) {
                    self->finishRemoteError(response.status);
                    return;
                }
                if (response.committed_bytes != self->current_->length) {
                    self->finishProtocolError();
                    return;
                }
                if (self->current_->opcode ==
                    HighPerformanceTcpOpcode::kWrite) {
                    self->finishClean();
                } else {
                    self->body_offset_ = 0;
                    self->readBodyChunk(epoch);
                }
            });
    }

    void readBodyChunk(uint64_t epoch) {
        if (body_offset_ == current_->length) {
            finishClean();
            return;
        }
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(
            config_.chunk_size, current_->length - body_offset_));
        auto* data = static_cast<uint8_t*>(current_->local_addr) + body_offset_;
        auto self = shared_from_this();
        asio::async_read(
            socket_, asio::buffer(data, chunk),
            [self, epoch, chunk](const std::error_code& error, size_t bytes) {
                if (!self->matches(epoch)) return;
                if (self->finishForcedIfAny()) return;
                if (error || bytes != chunk) {
                    self->finishIoError(error ? error
                                              : asio::error::operation_aborted);
                    return;
                }
                self->body_offset_ += bytes;
                self->armTimer(self->config_.progress_timeout_ms, epoch);
                self->readBodyChunk(epoch);
            });
    }

    void armTimer(uint64_t timeout_ms, uint64_t epoch) {
        const uint64_t generation = ++timer_generation_;
        timer_.expires_after(std::chrono::milliseconds(timeout_ms));
        auto self = shared_from_this();
        timer_.async_wait(
            [self, epoch, generation](const std::error_code& error) {
                if (error == asio::error::operation_aborted) return;
                if (error || !self->matches(epoch) ||
                    generation != self->timer_generation_) {
                    return;
                }
                self->forced_terminal_ = TIMEOUT;
                self->resolver_.cancel();
                self->closeDirty();
                // The active resolve/socket callback completes the operation.
                // This makes timeout sticky without releasing the caller buffer
                // before the canceled I/O handler has quiesced.
            });
    }

    void cancelTimer() {
        ++timer_generation_;
        std::error_code ignored;
        timer_.cancel(ignored);
    }

    bool matches(uint64_t epoch) const {
        return current_.has_value() && epoch == operation_epoch_;
    }

    bool finishForcedIfAny() {
        if (!forced_terminal_.has_value()) return false;
        const TransferStatusEnum terminal = *forced_terminal_;
        closeDirty();
        finishCurrent(terminal, 0, false);
        return true;
    }

    void closeDirty() {
        connected_ = false;
        std::error_code ignored;
        if (socket_.is_open()) {
            socket_.cancel(ignored);
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
            socket_.close(ignored);
        }
    }

    void finishProtocolError() {
        closeDirty();
        finishCurrent(FAILED, 0, false);
    }

    void finishRemoteError(HighPerformanceTcpStatus status) {
        closeDirty();
        finishCurrent(FAILED, 0, false, status);
    }

    void finishIoError(const std::error_code&) {
        const TransferStatusEnum terminal = forced_terminal_.value_or(FAILED);
        closeDirty();
        finishCurrent(terminal, 0, false);
    }

    void finishClean() {
        cancelTimer();
        finishCurrent(COMPLETED, current_->length, true);
    }

    void finishCurrent(
        TransferStatusEnum terminal, size_t bytes, bool keep_stream,
        std::optional<HighPerformanceTcpStatus> remote_status = std::nullopt) {
        cancelTimer();
        if (!keep_stream) closeDirty();
        Operation operation = std::move(*current_);
        current_.reset();
        ++operation_epoch_;  // invalidate late timer/cancel callbacks
        forced_terminal_.reset();
        completeStandalone(std::move(operation), terminal, bytes,
                           remote_status);
        startNext();
    }

    void completeStandalone(
        Operation operation, TransferStatusEnum terminal, size_t bytes,
        std::optional<HighPerformanceTcpStatus> remote_status = std::nullopt) {
        try {
            if (operation.complete)
                operation.complete(terminal, bytes, remote_status);
        } catch (const std::exception& error) {
            LOG(ERROR) << "HP TCP completion callback threw: " << error.what();
        } catch (...) {
            LOG(ERROR) << "HP TCP completion callback threw";
        }
        parent_->operationFinished();
    }

    HighPerformanceTcpClient* parent_;
    Config config_;
    LaneKey key_;
    asio::ip::tcp::resolver resolver_;
    asio::ip::tcp::socket socket_;
    asio::steady_timer timer_;
    std::deque<Operation> queue_;
    std::optional<Operation> current_;
    std::array<uint8_t, kHighPerformanceTcpRequestSize> request_bytes_{};
    std::array<uint8_t, kHighPerformanceTcpResponseSize> response_bytes_{};
    uint64_t operation_epoch_{0};
    uint64_t timer_generation_{0};
    uint64_t body_offset_{0};
    bool connected_{false};
    std::optional<TransferStatusEnum> forced_terminal_;
};

bool HighPerformanceTcpClient::LaneKey::operator==(const LaneKey& other) const {
    return peer_id == other.peer_id && incarnation == other.incarnation &&
           host == other.host && port == other.port && lane_id == other.lane_id;
}

size_t HighPerformanceTcpClient::LaneKeyHash::operator()(
    const LaneKey& key) const {
    size_t hash = std::hash<uint64_t>{}(key.peer_id);
    const auto mix = [&hash](size_t value) {
        hash ^= value + static_cast<size_t>(0x9e3779b97f4a7c15ULL) +
                (hash << 6U) + (hash >> 2U);
    };
    mix(std::hash<std::string>{}(key.incarnation));
    mix(std::hash<std::string>{}(key.host));
    mix(std::hash<uint16_t>{}(key.port));
    mix(std::hash<uint32_t>{}(key.lane_id));
    return hash;
}

HighPerformanceTcpClient::HighPerformanceTcpClient(
    Config config, HighPerformanceTcpWorkers* workers)
    : config_(std::move(config)), workers_(workers) {
    if (workers_ != nullptr) worker_states_.resize(workers_->workerCount());
}

HighPerformanceTcpClient::~HighPerformanceTcpClient() {
    if (workers_ != nullptr && workers_->running() &&
        !workers_->onWorkerThread()) {
        (void)cancelAll(CANCELED);
    }
}

void HighPerformanceTcpClient::operationStarted() {
    active_operations_.fetch_add(1, std::memory_order_acq_rel);
}

void HighPerformanceTcpClient::operationFinished() {
    const uint64_t previous =
        active_operations_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 1) {
        std::lock_guard<std::mutex> lock(active_mutex_);
        active_cv_.notify_all();
    }
}

void HighPerformanceTcpClient::enqueueOnOwner(size_t owner_worker,
                                              Operation operation) {
    if (workers_ == nullptr || owner_worker >= worker_states_.size() ||
        !operation.complete) {
        if (operation.complete) {
            try {
                operation.complete(FAILED, 0, std::nullopt);
            } catch (...) {
                LOG(ERROR) << "HP TCP rejected-operation callback threw";
            }
        }
        return;
    }

    operationStarted();
    try {
        if (stopping_.load(std::memory_order_acquire) ||
            operation.local_addr == nullptr || operation.length == 0 ||
            operation.length > config_.max_transfer_bytes ||
            operation.host.empty() || operation.port == 0 ||
            operation.lane_id >= config_.connections_per_peer) {
            try {
                operation.complete(CANCELED, 0, std::nullopt);
            } catch (...) {
                LOG(ERROR) << "HP TCP rejected-operation callback threw";
            }
            operationFinished();
            return;
        }

        auto& lanes = worker_states_[owner_worker].lanes;
        for (auto it = lanes.begin(); it != lanes.end();) {
            if (it->first.peer_id == operation.peer_id &&
                it->first.incarnation != operation.incarnation) {
                it->second->cancelAll(CANCELED);
                it = lanes.erase(it);
            } else {
                ++it;
            }
        }

        LaneKey key{operation.peer_id, operation.incarnation, operation.host,
                    operation.port, operation.lane_id};
        auto it = lanes.find(key);
        if (it == lanes.end()) {
            auto lane = std::make_shared<Lane>(
                this, workers_->ioContext(owner_worker), config_, key);
            it = lanes.emplace(std::move(key), std::move(lane)).first;
        }
        // Lane::enqueue owns completion + active-operation retirement from
        // this point forward, including allocation/initiation failures.
        it->second->enqueue(std::move(operation));
    } catch (const std::exception& error) {
        LOG(ERROR) << "HP TCP lane setup failed: " << error.what();
        try {
            operation.complete(FAILED, 0, std::nullopt);
        } catch (...) {
            LOG(ERROR) << "HP TCP failed-operation callback threw";
        }
        operationFinished();
    } catch (...) {
        LOG(ERROR) << "HP TCP lane setup failed";
        try {
            operation.complete(FAILED, 0, std::nullopt);
        } catch (...) {
            LOG(ERROR) << "HP TCP failed-operation callback threw";
        }
        operationFinished();
    }
}

void HighPerformanceTcpClient::cancelWorker(size_t worker_id,
                                            TransferStatusEnum terminal) {
    if (worker_id >= worker_states_.size()) return;
    auto& lanes = worker_states_[worker_id].lanes;
    for (auto& [key, lane] : lanes) {
        (void)key;
        lane->cancelAll(terminal);
    }
    lanes.clear();
}

void HighPerformanceTcpClient::cancelRequestOnWorker(size_t worker_id,
                                                     uint64_t request_id) {
    if (worker_id >= worker_states_.size()) return;
    for (auto& [key, lane] : worker_states_[worker_id].lanes) {
        (void)key;
        if (lane->cancelRequest(request_id)) return;
    }
}

Status HighPerformanceTcpClient::cancelRequest(size_t owner_worker,
                                               uint64_t request_id) {
    if (workers_ == nullptr || owner_worker >= worker_states_.size() ||
        request_id == 0) {
        return Status::InvalidArgument(
            "invalid HP TCP cancellation request" LOC_MARK);
    }
    if (!workers_->running()) {
        return Status::InternalError(
            "HP TCP worker contexts are unavailable" LOC_MARK);
    }
    try {
        asio::post(workers_->ioContext(owner_worker),
                   [this, owner_worker, request_id] {
                       cancelRequestOnWorker(owner_worker, request_id);
                   });
    } catch (const std::exception& error) {
        return Status::InternalError(
            std::string("HP TCP cancellation post failed: ") + error.what() +
            LOC_MARK);
    }
    return Status::OK();
}

Status HighPerformanceTcpClient::cancelAll(TransferStatusEnum terminal) {
    if (workers_ == nullptr) return Status::OK();
    if (workers_->onWorkerThread()) {
        return Status::InvalidArgument(
            "HP TCP client cancelAll cannot block a worker" LOC_MARK);
    }
    stopping_.store(true, std::memory_order_release);

    if (workers_->running()) {
        try {
            // Post cancellation to every owner before waiting for active I/O.
            for (size_t i = 0; i < workers_->workerCount(); ++i) {
                asio::post(workers_->ioContext(i),
                           [this, i, terminal] { cancelWorker(i, terminal); });
            }
        } catch (const std::exception& error) {
            return Status::InternalError(
                std::string("HP TCP client cancellation post failed: ") +
                error.what() + LOC_MARK);
        }
        CHECK_STATUS(workers_->barrier());
    }

    std::unique_lock<std::mutex> lock(active_mutex_);
    active_cv_.wait(lock, [&] {
        return active_operations_.load(std::memory_order_acquire) == 0;
    });
    return Status::OK();
}

}  // namespace mooncake::tent
