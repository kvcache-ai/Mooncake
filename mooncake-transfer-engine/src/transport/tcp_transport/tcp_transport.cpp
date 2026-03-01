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

#include "transport/tcp_transport/tcp_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>
#include <asio/ip/v6_only.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

#include "cuda_alike.h"

namespace mooncake {
using tcpsocket = asio::ip::tcp::socket;
const static size_t kDefaultBufferSize = 65536;

struct SessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t opcode;
};

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
static bool isCudaMemory(void* addr) {
    cudaPointerAttributes attributes;
    auto status = cudaPointerGetAttributes(&attributes, addr);
    if (status != cudaSuccess) return false;
    if (attributes.type == cudaMemoryTypeDevice) return true;
    return false;
}
#endif

// Forward declaration
class TcpTransport;

// Server-side session: handles one transfer request on a persistent connection
struct ServerSession : public std::enable_shared_from_this<ServerSession> {
    explicit ServerSession(std::shared_ptr<tcpsocket> socket)
        : socket_(std::move(socket)) {}

    std::shared_ptr<tcpsocket> socket_;
    SessionHeader header_;
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    std::function<void(TransferStatusEnum)> on_finalize_;
    std::mutex session_mutex_;

    void start() {
        session_mutex_.lock();
        total_transferred_bytes_ = 0;
        readHeader();
    }

   private:
    void readHeader() {
        auto self(shared_from_this());
        asio::async_read(
            *socket_, asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    // If client closed connection (EOF), this is normal - don't
                    // log
                    if (ec.value() != asio::error::eof) {
                        LOG(WARNING)
                            << "ServerSession::readHeader failed. Error: "
                            << ec.message() << " (value: " << ec.value() << ")"
                            << ", bytes read: " << len;
                    }
                    session_mutex_.unlock();
                    return;  // Don't continue, socket will be closed
                }

                local_buffer_ = (char*)(le64toh(header_.addr));
                if (header_.opcode == (uint8_t)TransferRequest::WRITE)
                    readBody();
                else
                    writeBody();
            });
    }

    void writeBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(kDefaultBufferSize, size - total_transferred_bytes_);
        if (buffer_size == 0) {
            session_mutex_.unlock();
            // Transfer complete, wait for next request on this connection
            start();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
        if (isCudaMemory(addr)) {
            dram_buffer = new char[buffer_size];
            cudaError_t cuda_status =
                cudaMemcpy(dram_buffer, addr + total_transferred_bytes_,
                           buffer_size, cudaMemcpyDefault);
            if (cuda_status != cudaSuccess) {
                LOG(ERROR) << "ServerSession::writeBody failed to copy from "
                              "CUDA memory. "
                           << "Error: " << cudaGetErrorString(cuda_status);
                session_mutex_.unlock();
                delete[] dram_buffer;
                return;  // Connection will be closed
            }
        }
#endif

        asio::async_write(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, self](const asio::error_code& ec,
                                            std::size_t transferred_bytes) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
                if (isCudaMemory(addr)) {
                    delete[] dram_buffer;
                }
#endif
                if (ec) {
                    LOG(ERROR)
                        << "ServerSession::writeBody failed. "
                        << "Attempt to write data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    session_mutex_.unlock();
                    return;  // Connection will be closed
                }
                total_transferred_bytes_ += transferred_bytes;
                writeBody();
            });
    }

    void readBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(kDefaultBufferSize, size - total_transferred_bytes_);
        if (buffer_size == 0) {
            session_mutex_.unlock();
            // Transfer complete, wait for next request on this connection
            start();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
        bool is_cuda_memory = isCudaMemory(addr);
        if (is_cuda_memory) {
            dram_buffer = new char[buffer_size];
        }
#else
        bool is_cuda_memory = false;
#endif

        asio::async_read(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, is_cuda_memory, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                if (ec) {
                    // If client closed connection (EOF), this is normal - don't
                    // log
                    if (ec.value() != asio::error::eof) {
                        LOG(WARNING)
                            << "ServerSession::readBody failed. "
                            << "Attempt to read data "
                            << static_cast<void*>(addr) << " using buffer "
                            << static_cast<void*>(dram_buffer)
                            << ". Error: " << ec.message()
                            << " (value: " << ec.value() << ")";
                    }
                    session_mutex_.unlock();
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
                    if (is_cuda_memory) delete[] dram_buffer;
#endif
                    return;  // Connection will be closed
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
                if (is_cuda_memory) {
                    cudaError_t cuda_status =
                        cudaMemcpy(addr + total_transferred_bytes_, dram_buffer,
                                   transferred_bytes, cudaMemcpyDefault);
                    if (cuda_status != cudaSuccess) {
                        LOG(ERROR)
                            << "ServerSession::readBody failed to copy to CUDA "
                               "memory. "
                            << "Error: " << cudaGetErrorString(cuda_status);
                        delete[] dram_buffer;
                        session_mutex_.unlock();
                        return;  // Connection will be closed
                    }
                    delete[] dram_buffer;
                }
#endif
                total_transferred_bytes_ += transferred_bytes;
                readBody();
            });
    }
};

// Client-side session: initiates one transfer request
struct ClientSession : public std::enable_shared_from_this<ClientSession> {
    explicit ClientSession(std::shared_ptr<tcpsocket> socket,
                           std::function<void()> on_complete = nullptr)
        : socket_(std::move(socket)), on_complete_(std::move(on_complete)) {}

    std::shared_ptr<tcpsocket> socket_;
    SessionHeader header_;
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    std::function<void(TransferStatusEnum)> on_finalize_;
    std::function<void()> on_complete_;  // Callback when transfer completes
    std::mutex session_mutex_;

    void initiate(void* buffer, uint64_t dest_addr, size_t size,
                  TransferRequest::OpCode opcode) {
        session_mutex_.lock();
        local_buffer_ = (char*)buffer;
        header_.addr = htole64(dest_addr);
        header_.size = htole64(size);
        header_.opcode = (uint8_t)opcode;
        total_transferred_bytes_ = 0;
        writeHeader();
    }

   private:
    void writeHeader() {
        auto self(shared_from_this());
        asio::async_write(
            *socket_, asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    LOG(ERROR) << "ClientSession::writeHeader failed. Error: "
                               << ec.message() << " (value: " << ec.value()
                               << ")" << ", bytes written: " << len;
                    if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                    session_mutex_.unlock();
                    if (on_complete_) on_complete_();
                    return;
                }
                if (header_.opcode == (uint8_t)TransferRequest::WRITE)
                    writeBody();
                else
                    readBody();
            });
    }

    void readBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(kDefaultBufferSize, size - total_transferred_bytes_);
        if (buffer_size == 0) {
            if (on_finalize_) on_finalize_(TransferStatusEnum::COMPLETED);
            session_mutex_.unlock();
            if (on_complete_) on_complete_();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
        bool is_cuda_memory = isCudaMemory(addr);
        if (is_cuda_memory) {
            dram_buffer = new char[buffer_size];
        }
#else
        bool is_cuda_memory = false;
#endif

        asio::async_read(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, is_cuda_memory, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                if (ec) {
                    LOG(ERROR)
                        << "ClientSession::readBody failed. "
                        << "Attempt to read data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                    if (on_complete_) on_complete_();
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
                    if (is_cuda_memory) delete[] dram_buffer;
#endif
                    session_mutex_.unlock();
                    return;
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
                if (is_cuda_memory) {
                    cudaError_t cuda_status =
                        cudaMemcpy(addr + total_transferred_bytes_, dram_buffer,
                                   transferred_bytes, cudaMemcpyDefault);
                    if (cuda_status != cudaSuccess) {
                        LOG(ERROR)
                            << "ClientSession::readBody failed to copy to CUDA "
                               "memory. "
                            << "Error: " << cudaGetErrorString(cuda_status);
                        if (on_finalize_)
                            on_finalize_(TransferStatusEnum::FAILED);
                        if (on_complete_) on_complete_();
                        delete[] dram_buffer;
                        session_mutex_.unlock();
                        return;
                    }
                    delete[] dram_buffer;
                }
#endif
                total_transferred_bytes_ += transferred_bytes;
                readBody();
            });
    }

    void writeBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(kDefaultBufferSize, size - total_transferred_bytes_);
        if (buffer_size == 0) {
            if (on_finalize_) on_finalize_(TransferStatusEnum::COMPLETED);
            session_mutex_.unlock();
            if (on_complete_) on_complete_();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
        if (isCudaMemory(addr)) {
            dram_buffer = new char[buffer_size];
            cudaError_t cuda_status =
                cudaMemcpy(dram_buffer, addr + total_transferred_bytes_,
                           buffer_size, cudaMemcpyDefault);
            if (cuda_status != cudaSuccess) {
                LOG(ERROR) << "ClientSession::writeBody failed to copy from "
                              "CUDA memory. "
                           << "Error: " << cudaGetErrorString(cuda_status);
                if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                if (on_complete_) on_complete_();
                session_mutex_.unlock();
                delete[] dram_buffer;
                return;
            }
        }
#endif

        asio::async_write(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, self](const asio::error_code& ec,
                                            std::size_t transferred_bytes) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP)
                if (isCudaMemory(addr)) {
                    delete[] dram_buffer;
                }
#endif
                if (ec) {
                    LOG(ERROR)
                        << "ClientSession::writeBody failed. "
                        << "Attempt to write data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                    if (on_complete_) on_complete_();
                    session_mutex_.unlock();
                    return;
                }
                total_transferred_bytes_ += transferred_bytes;
                writeBody();
            });
    }
};

struct TcpContext {
    TcpContext(short port) : acceptor(io_context) {
        std::error_code ec;
        asio::ip::tcp::endpoint endpoint(asio::ip::tcp::v6(), port);

        acceptor.open(endpoint.protocol(), ec);
        if (!ec) {
            acceptor.set_option(asio::ip::v6_only(false), ec);
            if (!ec) {
                acceptor.set_option(
                    asio::ip::tcp::acceptor::reuse_address(true));
                acceptor.bind(endpoint, ec);
                if (!ec) {
                    acceptor.listen();
                    return;
                }
            }
            acceptor.close();
        }
        LOG(ERROR) << "Failed to set up IPv6 dual-stack listener: "
                   << ec.message() << " (error code: " << ec.value() << ")";
        asio::ip::tcp::endpoint endpoint_v4(asio::ip::tcp::v4(), port);
        acceptor.open(endpoint_v4.protocol());
        acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor.bind(endpoint_v4);
        acceptor.listen();
    }

    void doAccept() {
        acceptor.async_accept([this](asio::error_code ec, tcpsocket socket) {
            if (!ec) {
                auto socket_ptr =
                    std::make_shared<tcpsocket>(std::move(socket));
                auto session = std::make_shared<ServerSession>(socket_ptr);
                session->start();  // Start processing requests on this
                                   // persistent connection
            }
            doAccept();
        });
    }

    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
};

TcpTransport::TcpTransport() : context_(nullptr), running_(false) {
    if (getenv("MC_TCP_ENABLE_CONNECTION_POOL") != nullptr) {
        std::string val(getenv("MC_TCP_ENABLE_CONNECTION_POOL"));
        std::transform(val.begin(), val.end(), val.begin(), ::tolower);
        if (val == "0" || val == "false" || val == "no") {
            enable_connection_pool_ = false;
        } else {
            enable_connection_pool_ = true;
        }
    }
}

TcpTransport::~TcpTransport() {
    if (running_) {
        running_ = false;
        context_->io_context.stop();
        thread_.join();
    }

    // Clear connection pool BEFORE deleting context
    // because sockets in the pool reference io_context
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        connection_pool_.clear();
    }

    if (context_) {
        delete context_;
        context_ = nullptr;
    }

    metadata_->removeSegmentDesc(local_server_name_);
}

int TcpTransport::startHandshakeDaemon() {
    return metadata_->startHandshakeDaemon(nullptr,
                                           metadata_->localRpcMeta().rpc_port,
                                           metadata_->localRpcMeta().sockfd);
}

int TcpTransport::install(std::string& local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;
    int sockfd = -1;
    int tcp_port = findAvailableTcpPort(sockfd);
    if (tcp_port == 0) {
        LOG(ERROR) << "TcpTransport: unable to find available tcp port for "
                      "data transmission";
        return -1;
    }

    int ret = allocateLocalSegmentID(tcp_port);
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot allocate local segment";
        return -1;
    }

    ret = startHandshakeDaemon();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot start handshake daemon";
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return -1;
    }

    close(sockfd);  // the above function has opened a socket
    LOG(INFO) << "TcpTransport: listen on port " << tcp_port;
    context_ = new TcpContext(tcp_port);
    running_ = true;
    thread_ = std::thread(&TcpTransport::worker, this);
    return 0;
}

int TcpTransport::allocateLocalSegmentID(int tcp_data_port) {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "tcp";
    desc->tcp_data_port = tcp_data_port;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int TcpTransport::registerLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = local_server_name_;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;
    return metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
}

int TcpTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int TcpTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int TcpTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    for (auto& addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

Status TcpTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "TcpTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status TcpTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "TcpTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "TcpTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto& request : entries) {
        TransferTask& task = batch_desc.task_list[task_id];
        ++task_id;
        task.total_bytes = request.length;
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->tcp.dest_addr = request.target_offset;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        startTransfer(slice);
    }

    return Status::OK();
}

Status TcpTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        assert(task.request);
        auto& request = *task.request;
        task.total_bytes = request.length;
        Slice* slice = getSliceCache().allocate();
        slice->source_addr = (char*)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->tcp.dest_addr = request.target_offset;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        startTransfer(slice);
    }
    return Status::OK();
}

void TcpTransport::worker() {
    while (running_) {
        try {
            context_->doAccept();
            context_->io_context.run();
        } catch (std::exception& e) {
            LOG(ERROR) << "TcpTransport::worker encountered an exception "
                          "during doAccept/run: "
                       << e.what();
        }
    }
}

std::shared_ptr<asio::ip::tcp::socket> TcpTransport::getConnection(
    const std::string& host, uint16_t port) {
    // If connection pool is disabled, always create a new connection
    if (!enable_connection_pool_) {
        try {
            asio::ip::tcp::resolver resolver(context_->io_context);
            auto endpoint_iterator =
                resolver.resolve(host, std::to_string(port));
            auto socket_ptr =
                std::make_shared<asio::ip::tcp::socket>(context_->io_context);
            asio::connect(*socket_ptr, endpoint_iterator);
            return socket_ptr;
        } catch (std::exception& e) {
            LOG(ERROR)
                << "TcpTransport::getConnection failed to create connection to "
                << host << ":" << port << ". Error: " << e.what();
            return nullptr;
        }
    }

    ConnectionKey key{host, port};

    // First phase: search for available connection while holding the lock
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);

        // Cleanup idle and dead connections
        cleanupIdleConnections();

        auto it = connection_pool_.find(key);
        if (it != connection_pool_.end()) {
            auto& queue = it->second;

            // Find an available connection
            for (auto queue_it = queue.begin(); queue_it != queue.end();) {
                auto& entry = *queue_it;
                if (!entry->in_use) {
                    // Check if connection is still alive
                    if (entry->socket->is_open()) {
                        entry->in_use = true;
                        entry->last_used = std::chrono::steady_clock::now();
                        return entry->socket;
                    } else {
                        // Remove dead connection immediately
                        queue_it = queue.erase(queue_it);
                        continue;
                    }
                }
                ++queue_it;
            }
        }
    }

    // No available connection, create a new one (pool grows dynamically)
    // Release lock before creating new connection to avoid blocking other
    // threads during slow DNS resolution and TCP handshake
    std::shared_ptr<asio::ip::tcp::socket> new_socket;
    try {
        asio::ip::tcp::resolver resolver(context_->io_context);
        auto endpoint_iterator = resolver.resolve(host, std::to_string(port));
        new_socket =
            std::make_shared<asio::ip::tcp::socket>(context_->io_context);
        asio::connect(*new_socket, endpoint_iterator);
    } catch (std::exception& e) {
        LOG(ERROR)
            << "TcpTransport::getConnection failed to create connection to "
            << host << ":" << port << ". Error: " << e.what();
        return nullptr;
    }

    // Re-acquire lock to add the new connection to the pool
    std::shared_ptr<PooledConnection> entry;
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        // Re-check if another thread already added a connection while we were
        // creating this one
        auto& queue = connection_pool_[key];
        for (auto it = queue.begin(); it != queue.end(); ++it) {
            auto& existing_entry = *it;
            if (!existing_entry->in_use && existing_entry->socket->is_open()) {
                // Another thread added an available connection, use that
                // instead and close the one we just created
                if (new_socket && new_socket->is_open()) {
                    asio::error_code ec;
                    new_socket->close(ec);
                }
                existing_entry->in_use = true;
                existing_entry->last_used = std::chrono::steady_clock::now();
                return existing_entry->socket;
            }
        }

        // No other connection available, add the one we created to the pool
        entry = std::make_shared<PooledConnection>(new_socket, host, port);
        queue.push_back(entry);
    }

    return entry->socket;
}

void TcpTransport::returnConnection(
    const std::string& host, uint16_t port,
    std::shared_ptr<asio::ip::tcp::socket> socket) {
    ConnectionKey key{host, port};

    std::lock_guard<std::mutex> lock(pool_mutex_);

    auto it = connection_pool_.find(key);
    if (it != connection_pool_.end()) {
        for (auto entry_it = it->second.begin(); entry_it != it->second.end();
             ++entry_it) {
            if ((*entry_it)->socket == socket) {
                if (socket->is_open()) {
                    (*entry_it)->in_use = false;
                    (*entry_it)->last_used = std::chrono::steady_clock::now();
                } else {
                    // Connection is dead, remove from pool
                    it->second.erase(entry_it);
                }
                return;
            }
        }
    }

    // Connection not found in pool (might be temporary), close it
    if (socket && socket->is_open()) {
        asio::error_code ec;
        socket->close(ec);
    }
}

void TcpTransport::cleanupIdleConnections() {
    auto now = std::chrono::steady_clock::now();

    for (auto it = connection_pool_.begin(); it != connection_pool_.end();) {
        auto& queue = it->second;

        // Remove idle connections that exceed timeout
        while (!queue.empty()) {
            auto& entry = queue.back();
            if (!entry->in_use) {
                auto idle_duration =
                    std::chrono::duration_cast<std::chrono::seconds>(
                        now - entry->last_used)
                        .count();
                if (idle_duration > kConnectionIdleTimeout.count()) {
                    if (entry->socket && entry->socket->is_open()) {
                        asio::error_code ec;
                        entry->socket->close(ec);
                    }
                    queue.pop_back();
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        // Remove empty endpoint queues
        if (queue.empty()) {
            it = connection_pool_.erase(it);
        } else {
            ++it;
        }
    }
}

void TcpTransport::startTransfer(Slice* slice) {
    auto desc = metadata_->getSegmentDescByID(slice->target_id);
    if (!desc) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get segment "
                      "description for target_id: "
                   << slice->target_id;
        slice->markFailed();
        return;
    }

    TransferMetadata::RpcMetaDesc meta_entry;
    if (metadata_->getRpcMetaEntry(desc->name, meta_entry)) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get RPC meta "
                      "entry for segment name: "
                   << desc->name;
        slice->markFailed();
        return;
    }

    // Get connection from pool
    auto socket =
        getConnection(meta_entry.ip_or_host_name, desc->tcp_data_port);
    if (!socket) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get connection to "
                   << meta_entry.ip_or_host_name << ":" << desc->tcp_data_port;
        slice->markFailed();
        return;
    }

    try {
        auto session = std::make_shared<ClientSession>(socket);

        session->on_finalize_ = [slice](TransferStatusEnum status) {
            if (status == TransferStatusEnum::COMPLETED)
                slice->markSuccess();
            else
                slice->markFailed();
        };

        // Return connection to pool when transfer completes, or close if
        // disabled
        if (enable_connection_pool_) {
            session->on_complete_ = [this, host = meta_entry.ip_or_host_name,
                                     port = desc->tcp_data_port, socket]() {
                returnConnection(host, port, socket);
            };
        } else {
            session->on_complete_ = [socket]() {
                // Close connection immediately after transfer
                if (socket && socket->is_open()) {
                    asio::error_code ec;
                    socket->close(ec);
                }
            };
        }

        session->initiate(slice->source_addr, slice->tcp.dest_addr,
                          slice->length, slice->opcode);
    } catch (std::exception& e) {
        LOG(ERROR) << "TcpTransport::startTransfer encountered an exception. "
                      "Slice details - source_addr: "
                   << slice->source_addr << ", length: " << slice->length
                   << ", opcode: " << (int)slice->opcode
                   << ", target_id: " << slice->target_id
                   << ". Exception: " << e.what();
        // On exception, always close the socket and remove from pool if present
        // Don't return it to the pool as it may be in an inconsistent state
        if (socket && socket->is_open()) {
            asio::error_code ec;
            socket->close(ec);
        }
        if (enable_connection_pool_) {
            // Remove the connection from pool if it was pooled
            ConnectionKey key{meta_entry.ip_or_host_name, desc->tcp_data_port};
            std::lock_guard<std::mutex> lock(pool_mutex_);
            auto it = connection_pool_.find(key);
            if (it != connection_pool_.end()) {
                auto& queue = it->second;
                for (auto queue_it = queue.begin(); queue_it != queue.end();
                     ++queue_it) {
                    if ((*queue_it)->socket == socket) {
                        queue.erase(queue_it);
                        break;
                    }
                }
                if (queue.empty()) {
                    connection_pool_.erase(it);
                }
            }
        }
        slice->markFailed();
    }
}

}  // namespace mooncake
