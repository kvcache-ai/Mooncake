// Copyright 2024 KVCache.AI
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

#include "tent/transport/tcp/tcp_transport.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <random>

#include <asio.hpp>
#include <asio/ip/v6_only.hpp>

#include "tent/common/status.h"
#include "tent/common/utils/ip.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/slab.h"
#include "tent/runtime/control_plane.h"

namespace mooncake {
namespace tent {

using tcpsocket = asio::ip::tcp::socket;

// Wire protocol header for the data plane.
// Kept minimal: opcode + dest addr + length (17 bytes).
struct DataHeader {
    uint8_t opcode;   // Request::WRITE or Request::READ
    uint64_t addr;    // destination address (little-endian)
    uint64_t length;  // transfer length    (little-endian)
};

static const size_t kChunkSize = 65536;

// ---------------------------------------------------------------------------
// Helper: extract host part from "host:port" or "[host]:port"
// ---------------------------------------------------------------------------
static std::string ExtractHost(const std::string &endpoint) {
    if (!endpoint.empty() && endpoint.front() == '[') {
        auto end = endpoint.find(']');
        if (end != std::string::npos) {
            return endpoint.substr(1, end - 1);
        }
    }
    auto first_colon = endpoint.find(':');
    if (first_colon != std::string::npos) {
        if (endpoint.find(':', first_colon + 1) != std::string::npos) {
            return endpoint;  // Likely an IPv6 literal without port.
        }
        return endpoint.substr(0, first_colon);
    }
    return endpoint;
}

static bool IsLoopbackEndpoint(const std::string &endpoint) {
    const std::string host = ExtractHost(endpoint);
    static const char kLoopbackV4Prefix[] = "127.";
    return host.compare(0, sizeof(kLoopbackV4Prefix) - 1, kLoopbackV4Prefix) ==
               0 ||
           host == "localhost" || host == "::1";
}

// ===========================================================================
// Session: handles one data transfer over a TCP connection.
//
// Client side (initiator): creates socket, connects, sends header, then
//   WRITE: sends body data  ->  done
//   READ:  receives body data -> done
//
// Server side (acceptor): accepts socket, reads header, then
//   WRITE (from client's perspective): reads body and copies to local memory
//   READ  (from client's perspective): sends body from local memory
// ===========================================================================
struct Session : public std::enable_shared_from_this<Session> {
    explicit Session(tcpsocket socket) : socket_(std::move(socket)) {}

    tcpsocket socket_;
    DataHeader header_{};
    uint64_t total_transferred_ = 0;
    char *local_buffer_ = nullptr;
    std::function<void(TransferStatusEnum)> on_complete_;
    // Callback to return socket to pool on successful client transfer.
    std::function<void(asio::ip::tcp::socket)> return_socket_;

    // ---------- Client-side initiation ----------
    void initiateClient(void *buffer, uint64_t dest_addr, size_t size,
                        uint8_t opcode) {
        local_buffer_ = static_cast<char *>(buffer);
        header_.opcode = opcode;
        header_.addr = htole64(dest_addr);
        header_.length = htole64(size);
        total_transferred_ = 0;
        writeHeader();
    }

    // ---------- Server-side acceptance ----------
    void onAccept(SegmentManager *seg_mgr) {
        total_transferred_ = 0;
        seg_mgr_ = seg_mgr;
        readHeader();
    }

   private:
    SegmentManager *seg_mgr_ = nullptr;

    void writeHeader() {
        auto self(shared_from_this());
        asio::async_write(
            socket_, asio::buffer(&header_, sizeof(DataHeader)),
            [this, self](const asio::error_code &ec, std::size_t len) {
                if (ec || len != sizeof(DataHeader)) {
                    LOG(ERROR)
                        << "Session::writeHeader failed: " << ec.message();
                    finish(TransferStatusEnum::FAILED);
                    return;
                }
                uint8_t op = header_.opcode;
                if (op == static_cast<uint8_t>(Request::WRITE))
                    sendBody();
                else
                    recvBody();
            });
    }

    void readHeader() {
        auto self(shared_from_this());
        asio::async_read(
            socket_, asio::buffer(&header_, sizeof(DataHeader)),
            [this, self](const asio::error_code &ec, std::size_t len) {
                if (ec || len != sizeof(DataHeader)) {
                    if (ec != asio::error::eof)
                        LOG(ERROR)
                            << "Session::readHeader failed: " << ec.message();
                    finish(TransferStatusEnum::FAILED);
                    return;
                }
                uint64_t addr = le64toh(header_.addr);
                uint64_t length = le64toh(header_.length);

                // Validate buffer ownership on the server side.
                auto *local_desc = seg_mgr_->getLocal().get();
                if (!local_desc->findBuffer(addr, length)) {
                    LOG(ERROR) << "Session: target address 0x" << std::hex
                               << addr << " length " << std::dec << length
                               << " not in registered buffer";
                    finish(TransferStatusEnum::FAILED);
                    return;
                }
                local_buffer_ = reinterpret_cast<char *>(addr);

                // From the client's perspective WRITE means client sends data
                // to us (server reads body). READ means client wants data from
                // us (server sends data).
                if (header_.opcode == static_cast<uint8_t>(Request::WRITE))
                    recvBody();  // server reads data from client
                else
                    sendBody();  // server sends data to client
            });
    }

    static bool needBounceBuffer() {
        auto &loader = Platform::getLoader();
        return loader.type() == "cuda" || loader.type() == "cann";
    }

    // Send body in kChunkSize chunks.
    void sendBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.length);
        size_t remaining = size - total_transferred_;
        if (remaining == 0) {
            finish(TransferStatusEnum::COMPLETED);
            return;
        }

        size_t chunk = std::min(kChunkSize, remaining);
        char *src = local_buffer_ + total_transferred_;

        // For GPU memory we stage through a DRAM bounce buffer.
        // Must allocate per-chunk because async_write holds the buffer
        // reference until the callback fires, while other sessions may
        // interleave on the same event loop thread.
        char *send_buf = src;
        std::shared_ptr<char[]> bounce;
        if (needBounceBuffer()) {
            bounce.reset(new char[chunk]);
            Platform::getLoader().copy(bounce.get(), src, chunk);
            send_buf = bounce.get();
        }

        asio::async_write(socket_, asio::buffer(send_buf, chunk),
                          [this, self, bounce](const asio::error_code &ec,
                                               std::size_t transferred) {
                              // bounce kept alive by shared_ptr capture until
                              // callback.
                              if (ec) {
                                  LOG(ERROR) << "Session::sendBody failed: "
                                             << ec.message();
                                  finish(TransferStatusEnum::FAILED);
                                  return;
                              }
                              total_transferred_ += transferred;
                              sendBody();
                          });
    }

    // Receive body in kChunkSize chunks.
    void recvBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.length);
        size_t remaining = size - total_transferred_;
        if (remaining == 0) {
            finish(TransferStatusEnum::COMPLETED);
            return;
        }

        size_t chunk = std::min(kChunkSize, remaining);
        char *dst = local_buffer_ + total_transferred_;

        char *recv_buf = dst;
        std::shared_ptr<char[]> bounce;
        if (needBounceBuffer()) {
            bounce.reset(new char[chunk]);
            recv_buf = bounce.get();
        }

        asio::async_read(
            socket_, asio::buffer(recv_buf, chunk),
            [this, self, dst, bounce](const asio::error_code &ec,
                                      std::size_t transferred) {
                if (ec) {
                    LOG(ERROR) << "Session::recvBody failed: " << ec.message();
                    finish(TransferStatusEnum::FAILED);
                    return;
                }
                if (bounce) {
                    Platform::getLoader().copy(dst, bounce.get(), transferred);
                }
                total_transferred_ += transferred;
                recvBody();
            });
    }

    void finish(TransferStatusEnum status) {
        if (status == TransferStatusEnum::COMPLETED && seg_mgr_) {
            // Server side: keep connection alive, wait for next transfer.
            total_transferred_ = 0;
            local_buffer_ = nullptr;
            readHeader();
            return;
        }

        if (status == TransferStatusEnum::COMPLETED && return_socket_) {
            // Client side success: return socket to connection pool.
            auto cb = std::move(return_socket_);
            cb(std::move(socket_));
        } else {
            // Failure: close socket.
            asio::error_code ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
            socket_.close(ec);
        }
        if (on_complete_) on_complete_(status);
    }
};

// ===========================================================================
// TcpTransport implementation
// ===========================================================================

TcpTransport::TcpTransport() {}

TcpTransport::~TcpTransport() { uninstall(); }

Status TcpTransport::startDataServer() {
    io_context_ = std::make_unique<asio::io_context>();

    // Try a few random ports, similar to CoroRpcAgent::start().
    const int kStartPort = 10000;
    const int kPortRange = 50000;
    const int kMaxRetries = 32;
    std::mt19937 rng(std::random_device{}());

    for (int i = 0; i < kMaxRetries; ++i) {
        uint16_t port = kStartPort + static_cast<uint16_t>(rng() % kPortRange);
        try {
            // Try IPv6 dual-stack first, fall back to IPv4.
            asio::ip::tcp::endpoint ep(asio::ip::tcp::v6(), port);
            auto acc = std::make_unique<asio::ip::tcp::acceptor>(*io_context_);
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
            // Fallback: IPv4
            asio::ip::tcp::endpoint ep4(asio::ip::tcp::v4(), port);
            acc = std::make_unique<asio::ip::tcp::acceptor>(*io_context_);
            acc->open(ep4.protocol(), ec);
            if (ec) continue;
            acc->set_option(asio::ip::tcp::acceptor::reuse_address(true));
            acc->bind(ep4, ec);
            if (ec) continue;
            acc->listen(asio::socket_base::max_listen_connections);
            acceptor_ = std::move(acc);
            data_port_ = port;
            return Status::OK();
        } catch (const std::exception &e) {
            LOG(WARNING) << "TCP data server: port " << port
                         << " failed: " << e.what();
        }
    }
    return Status::InternalError(
        "TCP data server: unable to find available port");
}

void TcpTransport::doAccept() {
    acceptor_->async_accept([this](asio::error_code ec, tcpsocket socket) {
        if (!ec) {
            socket.set_option(asio::ip::tcp::no_delay(true), ec);
            auto session = std::make_shared<Session>(std::move(socket));
            session->onAccept(&metadata_->segmentManager());
        }
        if (running_.load(std::memory_order_relaxed)) {
            doAccept();
        }
    });
}

Status TcpTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "TCP transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    installed_ = true;
    metadata_->setNotifyCallback([&](const Notification &message) -> int {
        RWSpinlock::WriteGuard guard(notify_lock_);
        notify_list_.push_back(message);
        return 0;
    });

    // Start the ASIO-based TCP data server.
    CHECK_STATUS(startDataServer());
    running_.store(true, std::memory_order_release);

    // Keep io_context alive even when there's no pending work, so worker
    // threads don't exit prematurely.
    work_guard_ = std::make_unique<
        asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_->get_executor());

    doAccept();

    worker_thread_ = std::thread([this]() {
        try {
            io_context_->run();
        } catch (const std::exception &e) {
            LOG(ERROR) << "TCP data server worker exception: " << e.what();
        }
    });

    LOG(INFO) << "TCP data server listening on port " << data_port_;

    // Publish the TCP data port in the segment's transport_attrs so that
    // remote peers can discover it.
    auto &manager = metadata_->segmentManager();
    auto segment = manager.getLocal();
    auto &detail = std::get<MemorySegmentDesc>(segment->detail);
    detail.transport_attrs[static_cast<int>(TransportType::TCP)] =
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

Status TcpTransport::uninstall() {
    if (installed_) {
        running_.store(false, std::memory_order_release);
        work_guard_.reset();  // Allow io_context::run() to return.
        if (io_context_) {
            io_context_->stop();
        }
        if (worker_thread_.joinable()) worker_thread_.join();
        // Drain connection pool before destroying io_context.
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            conn_pool_.clear();
        }
        acceptor_.reset();
        io_context_.reset();
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status TcpTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto tcp_batch = Slab<TcpSubBatch>::Get().allocate();
    if (!tcp_batch)
        return Status::InternalError("Unable to allocate TCP sub-batch");
    batch = tcp_batch;
    tcp_batch->task_list.reserve(max_size);
    tcp_batch->max_size = max_size;
    return Status::OK();
}

Status TcpTransport::freeSubBatch(SubBatchRef &batch) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    Slab<TcpSubBatch>::Get().deallocate(tcp_batch);
    batch = nullptr;
    return Status::OK();
}

Status TcpTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    if (request_list.size() + tcp_batch->task_list.size() > tcp_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    // First, add all tasks to the vector. This must be done before
    // dispatching any transfers, because push_back could reallocate the
    // vector and invalidate pointers to earlier elements.
    size_t base = tcp_batch->task_list.size();
    for (auto &request : request_list) {
        tcp_batch->task_list.push_back(TcpTask{});
        auto &task = tcp_batch->task_list.back();
        task.target_addr = request.target_offset;
        task.request = request;
        task.status_word = TransferStatusEnum::PENDING;
        task.transferred_bytes = 0;
    }

    // Now dispatch all transfers. The vector won't reallocate since all
    // elements are already added, so task pointers are stable.
    size_t end = tcp_batch->task_list.size();
    for (size_t i = base; i < end; ++i) {
        startTransfer(&tcp_batch->task_list[i]);
    }
    return Status::OK();
}

Status TcpTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)tcp_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = tcp_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

Status TcpTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    desc.transports.push_back(TransportType::TCP);
    return Status::OK();
}

Status TcpTransport::removeMemoryBuffer(BufferDesc &desc) {
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Connection pool: reuse TCP connections to avoid per-transfer connect cost.
// ---------------------------------------------------------------------------
asio::ip::tcp::socket TcpTransport::acquireConnection(const std::string &host,
                                                      uint16_t port) {
    std::string key = host + ":" + std::to_string(port);
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        auto it = conn_pool_.find(key);
        if (it != conn_pool_.end() && !it->second.empty()) {
            auto socket = std::move(it->second.back());
            it->second.pop_back();
            return socket;
        }
    }
    // No pooled connection — create a new one.
    asio::ip::tcp::resolver resolver(*io_context_);
    auto endpoints = resolver.resolve(host, std::to_string(port));
    asio::ip::tcp::socket socket(*io_context_);
    asio::connect(socket, endpoints);
    socket.set_option(asio::ip::tcp::no_delay(true));
    return socket;
}

void TcpTransport::releaseConnection(const std::string &key,
                                     asio::ip::tcp::socket socket) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    conn_pool_[key].push_back(std::move(socket));
}

// ---------------------------------------------------------------------------
// startTransfer: resolve remote endpoint, connect, and dispatch async I/O.
// ---------------------------------------------------------------------------
void TcpTransport::startTransfer(TcpTask *task) {
    if (task->request.target_id == LOCAL_SEGMENT_ID &&
        IsLoopbackEndpoint(local_segment_name_)) {
        LOG_FIRST_N(WARNING, 1)
            << "TCP transfer targets LOCAL_SEGMENT_ID on loopback endpoint "
            << local_segment_name_
            << ". When running multiple store instances on the same host with "
               "MC_STORE_MEMCPY=0, TCP local transfers will fail. Enable "
               "MC_STORE_MEMCPY or SHM, or use a non-loopback address.";
    }

    std::string host;
    uint16_t remote_data_port = 0;
    auto status = findRemoteDataEndpoint(
        task->request.target_offset, task->request.length,
        task->request.target_id, host, remote_data_port);
    if (!status.ok()) {
        task->status_word = TransferStatusEnum::FAILED;
        return;
    }

    try {
        auto socket = acquireConnection(host, remote_data_port);
        std::string pool_key = host + ":" + std::to_string(remote_data_port);

        auto session = std::make_shared<Session>(std::move(socket));
        session->on_complete_ = [task](TransferStatusEnum s) {
            if (s == TransferStatusEnum::COMPLETED) {
                task->transferred_bytes = task->request.length;
            }
            task->status_word = s;
        };
        session->return_socket_ = [this, pool_key](asio::ip::tcp::socket s) {
            releaseConnection(pool_key, std::move(s));
        };
        session->initiateClient(
            task->request.source, task->request.target_offset,
            task->request.length, static_cast<uint8_t>(task->request.opcode));
    } catch (const std::exception &e) {
        LOG(ERROR) << "TCP startTransfer connect failed to " << host << ":"
                   << remote_data_port << " : " << e.what();
        task->status_word = TransferStatusEnum::FAILED;
    }
}

// ---------------------------------------------------------------------------
// findRemoteDataEndpoint: look up the remote segment, extract host from
// rpc_server_addr, and get TCP data port from transport_attrs[TCP].
// ---------------------------------------------------------------------------
Status TcpTransport::findRemoteDataEndpoint(uint64_t dest_addr, uint64_t length,
                                            uint64_t target_id,
                                            std::string &host,
                                            uint16_t &data_port) {
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;
    auto buffer = desc->findBuffer(dest_addr, length);
    const auto &mem = desc->getMemory();
    if (!buffer || mem.rpc_server_addr.empty())
        return Status::InvalidArgument(
            "Requested address is not in registered buffer" LOC_MARK);

    // Extract host from the rpc_server_addr (e.g. "192.168.1.1:12345").
    auto parsed = parseHostNameWithPort(mem.rpc_server_addr, 0);
    host = parsed.first;

    // Get TCP data port from transport_attrs.
    auto *port_str =
        mem.getTransportAttrs(static_cast<int>(TransportType::TCP));
    if (!port_str || port_str->empty())
        return Status::InvalidArgument(
            "Remote segment has no TCP data port published" LOC_MARK);

    int port_val = std::atoi(port_str->c_str());
    if (port_val <= 0 || port_val > 65535)
        return Status::InvalidArgument(
            "Remote segment has invalid TCP data port" LOC_MARK);
    data_port = static_cast<uint16_t>(port_val);
    return Status::OK();
}

Status TcpTransport::sendNotification(SegmentID target_id,
                                      const Notification &message) {
    std::string rpc_server_addr;
    SegmentDesc *desc = nullptr;
    auto status = metadata_->segmentManager().getRemoteCached(desc, target_id);
    if (!status.ok()) return status;
    rpc_server_addr = desc->getMemory().rpc_server_addr;
    if (rpc_server_addr.empty())
        return Status::InvalidArgument("Requested segment type error" LOC_MARK);
    return ControlClient::notify(rpc_server_addr, message);
}

Status TcpTransport::receiveNotification(
    std::vector<Notification> &notify_list) {
    RWSpinlock::ReadGuard guard(notify_lock_);
    notify_list.clear();
    notify_list.swap(notify_list_);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
