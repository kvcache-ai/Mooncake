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

#include "tent/transport/tcp_hp/tcp_hp_transport.h"

#include <glog/logging.h>

#include <chrono>
#include <random>
#include <thread>

#include <asio.hpp>
#include <asio/ip/v6_only.hpp>

#include "tent/common/status.h"
#include "tent/common/utils/ip.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"
#include "tent/transport/tcp_hp/hp_session.h"
#include "tent/transport/tcp_hp/protocol.h"

namespace mooncake {
namespace tent {

using tcpsocket = asio::ip::tcp::socket;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static std::string ExtractHost(const std::string& endpoint) {
    if (!endpoint.empty() && endpoint.front() == '[') {
        auto end = endpoint.find(']');
        if (end != std::string::npos) return endpoint.substr(1, end - 1);
    }
    auto first_colon = endpoint.find(':');
    if (first_colon != std::string::npos) {
        if (endpoint.find(':', first_colon + 1) != std::string::npos)
            return endpoint;  // IPv6 literal without port
        return endpoint.substr(0, first_colon);
    }
    return endpoint;
}

static bool IsLoopbackEndpoint(const std::string& endpoint) {
    const std::string host = ExtractHost(endpoint);
    return host.compare(0, 4, "127.") == 0 || host == "localhost" ||
           host == "::1";
}

// ===========================================================================
// TcpHpTransport
// ===========================================================================

TcpHpTransport::TcpHpTransport() {}

TcpHpTransport::~TcpHpTransport() { uninstall(); }

Status TcpHpTransport::startDataServer() {
    auto& ctx = io_pool_->getContext(0);

    const int kStartPort = 10000;
    const int kPortRange = 50000;
    const int kMaxRetries = 32;
    std::mt19937 rng(std::random_device{}());

    for (int i = 0; i < kMaxRetries; ++i) {
        uint16_t port = kStartPort + static_cast<uint16_t>(rng() % kPortRange);
        try {
            // Try IPv6 dual-stack first.
            asio::ip::tcp::endpoint ep(asio::ip::tcp::v6(), port);
            auto acc = std::make_unique<asio::ip::tcp::acceptor>(ctx);
            std::error_code ec;
            acc->open(ep.protocol(), ec);
            if (!ec) {
                acc->set_option(asio::ip::v6_only(false), ec);
                if (!ec) {
                    acc->set_option(
                        asio::ip::tcp::acceptor::reuse_address(true));
                    acc->bind(ep, ec);
                    if (!ec) {
                        acc->listen(asio::socket_base::max_listen_connections);
                        acceptor_ = std::move(acc);
                        data_port_ = port;
                        return Status::OK();
                    }
                }
                acc->close();
            }
            // Fallback: IPv4.
            asio::ip::tcp::endpoint ep4(asio::ip::tcp::v4(), port);
            acc = std::make_unique<asio::ip::tcp::acceptor>(ctx);
            acc->open(ep4.protocol(), ec);
            if (ec) continue;
            acc->set_option(asio::ip::tcp::acceptor::reuse_address(true));
            acc->bind(ep4, ec);
            if (ec) continue;
            acc->listen(asio::socket_base::max_listen_connections);
            acceptor_ = std::move(acc);
            data_port_ = port;
            return Status::OK();
        } catch (const std::exception& e) {
            LOG(WARNING) << "TCP_HP data server: port " << port
                         << " failed: " << e.what();
        }
    }
    return Status::InternalError(
        "TCP_HP data server: unable to find available port");
}

void TcpHpTransport::doAccept() {
    acceptor_->async_accept([this](asio::error_code ec, tcpsocket socket) {
        // Immediately re-arm acceptor so we don't block on handshake.
        if (running_.load(std::memory_order_relaxed)) {
            doAccept();
        }
        if (!ec) {
            conn_mgr_->configureSocket(socket);
            // Async server-side handshake — does not block the io_context.
            conn_mgr_->asyncServerHandshake(
                std::move(socket),
                [this](Status hs_status, tcpsocket hs_socket) {
                    if (hs_status.ok()) {
                        auto session = std::make_shared<tcp_hp::HpSession>(
                            std::move(hs_socket), chunk_size_,
                            bounce_pool_.get(), transfer_timeout_s_);
                        session->onAccept(&metadata_->segmentManager());
                    } else {
                        LOG(WARNING) << "TCP_HP handshake rejected: "
                                     << hs_status.message();
                        asio::error_code close_ec;
                        hs_socket.close(close_ec);
                    }
                });
        }
    });
}

Status TcpHpTransport::install(std::string& local_segment_name,
                               std::shared_ptr<ControlService> metadata,
                               std::shared_ptr<Topology> local_topology,
                               std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "TCP_HP transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    installed_ = true;

    metadata_->setNotifyCallback([&](const Notification& message) -> int {
        RWSpinlock::WriteGuard guard(notify_lock_);
        notify_list_.push_back(message);
        return 0;
    });

    // Read configuration.
    size_t io_threads = 8;
    size_t max_inflight = 128;
    size_t bounce_initial = 32;
    size_t bounce_max = 256;
    tcp_hp::ConnOptions conn_opts;
    if (conf) {
        chunk_size_ = conf->get("transports/tcp_hp/chunk_size",
                                static_cast<int>(chunk_size_));
        io_threads = conf->get("transports/tcp_hp/io_threads",
                               static_cast<int>(io_threads));
        max_inflight = conf->get("transports/tcp_hp/max_inflight",
                                 static_cast<int>(max_inflight));
        transfer_timeout_s_ = conf->get("transports/tcp_hp/transfer_timeout_s",
                                        static_cast<int>(transfer_timeout_s_));
        bounce_initial =
            conf->get("transports/tcp_hp/bounce_buffer/initial_count",
                      static_cast<int>(bounce_initial));
        bounce_max = conf->get("transports/tcp_hp/bounce_buffer/max_count",
                               static_cast<int>(bounce_max));
        conn_opts.max_pool_size =
            conf->get("transports/tcp_hp/max_pool_size",
                      static_cast<int>(conn_opts.max_pool_size));
        conn_opts.connect_timeout_ms =
            conf->get("transports/tcp_hp/connect_timeout_ms",
                      conn_opts.connect_timeout_ms);
        conn_opts.connect_retries = conf->get(
            "transports/tcp_hp/connect_retries", conn_opts.connect_retries);
        conn_opts.sndbuf =
            conf->get("transports/tcp_hp/socket/sndbuf", conn_opts.sndbuf);
        conn_opts.rcvbuf =
            conf->get("transports/tcp_hp/socket/rcvbuf", conn_opts.rcvbuf);
        conn_opts.keepalive_idle_s =
            conf->get("transports/tcp_hp/socket/keepalive_idle_s",
                      conn_opts.keepalive_idle_s);
        conn_opts.keepalive_interval_s =
            conf->get("transports/tcp_hp/socket/keepalive_interval_s",
                      conn_opts.keepalive_interval_s);
        conn_opts.keepalive_count =
            conf->get("transports/tcp_hp/socket/keepalive_count",
                      conn_opts.keepalive_count);
    }

    // Create I/O pool and connection manager.
    io_pool_ = std::make_unique<tcp_hp::IoContextPool>(io_threads);
    conn_mgr_ =
        std::make_unique<tcp_hp::ConnManager>(*io_pool_, std::move(conn_opts));

    // Phase 2: bounce buffer pool (only for GPU platforms).
    if (Platform::getLoader().type() == "cuda" ||
        Platform::getLoader().type() == "cann") {
        bounce_pool_ = std::make_unique<tcp_hp::BounceBufferPool>(
            chunk_size_, bounce_initial, bounce_max);
    }

    // Phase 2: inflight controller.
    inflight_ctrl_ = std::make_unique<tcp_hp::InflightController>(max_inflight);

    // Thread pool for blocking connect operations (so io_context threads
    // are never blocked by synchronous connect + retry + sleep).
    connect_pool_ = std::make_unique<asio::thread_pool>(
        std::max<size_t>(2, io_threads / 2));

    // Start the TCP data server.
    CHECK_STATUS(startDataServer());
    running_.store(true, std::memory_order_release);

    io_pool_->start();
    doAccept();

    LOG(INFO) << "TCP_HP data server listening on port " << data_port_
              << " (io_threads=" << io_threads << ", chunk_size=" << chunk_size_
              << ", max_inflight=" << max_inflight
              << ", bounce_pool=" << (bounce_pool_ ? "yes" : "no")
              << ", timeout=" << transfer_timeout_s_ << "s)";

    // Publish TCP_HP data port in segment's transport_attrs.
    auto& manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    auto& detail = std::get<MemorySegmentDesc>(segment->detail);
    detail.transport_attrs[static_cast<int>(TransportType::TCP_HP)] =
        std::to_string(data_port_);

    caps.dram_to_dram = true;
    if (Platform::getLoader().type() == "cuda" ||
        Platform::getLoader().type() == "cann") {
        caps.dram_to_gpu = true;
        caps.gpu_to_dram = true;
        caps.gpu_to_gpu = true;
    }
    return Status::OK();
}

Status TcpHpTransport::uninstall() {
    if (installed_) {
        running_.store(false, std::memory_order_release);

        // Close acceptor first to stop new incoming connections.
        if (acceptor_) {
            asio::error_code ec;
            acceptor_->close(ec);
        }

        // Wait for inflight transfers to drain (best-effort with timeout).
        if (inflight_ctrl_ && inflight_ctrl_->current() > 0) {
            constexpr int kDrainTimeoutMs = 5000;
            constexpr int kPollIntervalMs = 10;
            int waited = 0;
            while (inflight_ctrl_->current() > 0 &&
                   waited < kDrainTimeoutMs) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(kPollIntervalMs));
                waited += kPollIntervalMs;
            }
            if (inflight_ctrl_->current() > 0) {
                LOG(WARNING) << "TCP_HP uninstall: " << inflight_ctrl_->current()
                             << " transfers still inflight after "
                             << kDrainTimeoutMs << "ms, forcing shutdown";
            }
        }

        if (connect_pool_) connect_pool_->join();
        if (io_pool_) io_pool_->stop();
        inflight_ctrl_.reset();
        connect_pool_.reset();
        conn_mgr_.reset();
        acceptor_.reset();
        bounce_pool_.reset();
        io_pool_.reset();
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status TcpHpTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    auto tcp_batch = Slab<TcpHpSubBatch>::Get().allocate();
    if (!tcp_batch)
        return Status::InternalError("Unable to allocate TCP_HP sub-batch");
    batch = tcp_batch;
    tcp_batch->task_list.reserve(max_size);
    tcp_batch->max_size = max_size;
    return Status::OK();
}

Status TcpHpTransport::freeSubBatch(SubBatchRef& batch) {
    auto tcp_batch = dynamic_cast<TcpHpSubBatch*>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP_HP sub-batch" LOC_MARK);
    size_t inflight = tcp_batch->outstanding.load(std::memory_order_acquire);
    if (inflight > 0) {
        return Status::InternalError(
            "Cannot free TCP_HP sub-batch with " + std::to_string(inflight) +
            " inflight transfers" LOC_MARK);
    }
    Slab<TcpHpSubBatch>::Get().deallocate(tcp_batch);
    batch = nullptr;
    return Status::OK();
}

Status TcpHpTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto tcp_batch = dynamic_cast<TcpHpSubBatch*>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP_HP sub-batch" LOC_MARK);
    if (request_list.size() + tcp_batch->task_list.size() > tcp_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    size_t base = tcp_batch->task_list.size();
    for (auto& request : request_list) {
        tcp_batch->task_list.push_back(TcpHpTask{});
        auto& task = tcp_batch->task_list.back();
        task.target_addr = request.target_offset;
        task.request = request;
        task.status_word.store(TransferStatusEnum::PENDING,
                              std::memory_order_release);
        task.transferred_bytes.store(0, std::memory_order_release);
    }

    size_t end = tcp_batch->task_list.size();
    for (size_t i = base; i < end; ++i) {
        tcp_batch->outstanding.fetch_add(1, std::memory_order_relaxed);
        startTransfer(tcp_batch, &tcp_batch->task_list[i]);
    }
    return Status::OK();
}

Status TcpHpTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                         TransferStatus& status) {
    auto tcp_batch = dynamic_cast<TcpHpSubBatch*>(batch);
    if (task_id < 0 || task_id >= static_cast<int>(tcp_batch->task_list.size()))
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    auto& task = tcp_batch->task_list[task_id];
    status = TransferStatus{task.status_word.load(std::memory_order_acquire),
                            task.transferred_bytes.load(std::memory_order_acquire)};
    return Status::OK();
}

Status TcpHpTransport::addMemoryBuffer(BufferDesc& desc,
                                       const MemoryOptions& options) {
    desc.transports.push_back(TransportType::TCP_HP);
    return Status::OK();
}

Status TcpHpTransport::removeMemoryBuffer(BufferDesc& desc) {
    return Status::OK();
}

// ---------------------------------------------------------------------------
// startTransfer
// ---------------------------------------------------------------------------
void TcpHpTransport::startTransfer(TcpHpSubBatch* batch, TcpHpTask* task) {
    if (task->request.target_id == LOCAL_SEGMENT_ID &&
        IsLoopbackEndpoint(local_segment_name_)) {
        LOG_FIRST_N(WARNING, 1)
            << "TCP_HP transfer targets LOCAL_SEGMENT_ID on loopback "
            << local_segment_name_;
    }

    auto do_transfer = [this, batch, task]() {
        std::string host;
        uint16_t remote_data_port = 0;
        auto status = findRemoteDataEndpoint(
            task->request.target_offset, task->request.length,
            task->request.target_id, host, remote_data_port);
        if (!status.ok()) {
            task->status_word.store(TransferStatusEnum::FAILED,
                                   std::memory_order_release);
            batch->outstanding.fetch_sub(1, std::memory_order_release);
            if (inflight_ctrl_) inflight_ctrl_->release();
            return;
        }

        asio::ip::tcp::socket socket(io_pool_->getNextContext());
        status = conn_mgr_->acquire(host, remote_data_port, socket);
        if (!status.ok()) {
            LOG(ERROR) << "TCP_HP connect failed to " << host << ":"
                       << remote_data_port << ": " << status.message();
            task->status_word.store(TransferStatusEnum::FAILED,
                                   std::memory_order_release);
            batch->outstanding.fetch_sub(1, std::memory_order_release);
            if (inflight_ctrl_) inflight_ctrl_->release();
            return;
        }

        auto session = std::make_shared<tcp_hp::HpSession>(
            std::move(socket), chunk_size_, bounce_pool_.get(),
            transfer_timeout_s_);

        // Release inflight slot when transfer completes.
        auto* ctrl = inflight_ctrl_.get();
        session->on_complete = [batch, task, ctrl](TransferStatusEnum s) {
            if (s == TransferStatusEnum::COMPLETED)
                task->transferred_bytes.store(task->request.length,
                                             std::memory_order_release);
            task->status_word.store(s, std::memory_order_release);
            batch->outstanding.fetch_sub(1, std::memory_order_release);
            if (ctrl) ctrl->release();
        };

        auto* mgr = conn_mgr_.get();
        session->return_socket =
            [mgr, host, remote_data_port](asio::ip::tcp::socket s) {
                mgr->release(host, remote_data_port, std::move(s));
            };

        session->initiateClient(
            task->request.source, task->request.target_offset,
            task->request.length,
            static_cast<uint8_t>(task->request.opcode));
    };

    // Wrap do_transfer to always run on connect_pool_ so that blocking
    // connect + retry never stalls an io_context thread.
    auto posted_transfer = [this, xfer = std::move(do_transfer)]() {
        asio::post(*connect_pool_, xfer);
    };

    // Inflight flow control: queue if at limit.
    if (inflight_ctrl_ && !inflight_ctrl_->tryAcquire()) {
        inflight_ctrl_->enqueue(std::move(posted_transfer));
    } else {
        posted_transfer();
    }
}

Status TcpHpTransport::findRemoteDataEndpoint(uint64_t dest_addr,
                                              uint64_t length,
                                              uint64_t target_id,
                                              std::string& host,
                                              uint16_t& data_port) {
    SegmentDesc* desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;
    auto buffer = desc->findBuffer(dest_addr, length);
    const auto& mem = desc->getMemory();
    if (!buffer || mem.rpc_server_addr.empty())
        return Status::InvalidArgument(
            "Requested address is not in registered buffer" LOC_MARK);

    auto parsed = parseHostNameWithPort(mem.rpc_server_addr, 0);
    host = parsed.first;

    auto* port_str =
        mem.getTransportAttrs(static_cast<int>(TransportType::TCP_HP));
    if (!port_str || port_str->empty())
        return Status::InvalidArgument(
            "Remote segment has no TCP_HP data port published" LOC_MARK);

    int port_val = std::atoi(port_str->c_str());
    if (port_val <= 0 || port_val > 65535)
        return Status::InvalidArgument(
            "Remote segment has invalid TCP_HP data port" LOC_MARK);
    data_port = static_cast<uint16_t>(port_val);
    return Status::OK();
}

Status TcpHpTransport::sendNotification(SegmentID target_id,
                                        const Notification& message) {
    SegmentDesc* desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;
    std::string rpc_server_addr = desc->getMemory().rpc_server_addr;
    if (rpc_server_addr.empty())
        return Status::InvalidArgument("Requested segment type error" LOC_MARK);
    return ControlClient::notify(rpc_server_addr, message);
}

Status TcpHpTransport::receiveNotification(
    std::vector<Notification>& notify_list) {
    RWSpinlock::WriteGuard guard(notify_lock_);
    notify_list.clear();
    notify_list.swap(notify_list_);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
