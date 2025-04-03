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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
using tcpsocket = boost::asio::ip::tcp::socket;
const static size_t kDefaultBufferSize = 65536;

struct SessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t opcode;
};

struct Session : public std::enable_shared_from_this<Session> {
    explicit Session(tcpsocket socket) : socket_(std::move(socket)) {}

    tcpsocket socket_;
    SessionHeader header_;
    uint64_t total_transferred_bytes_;
    char *local_buffer_;
    std::function<void(TransferStatusEnum)> on_finalize_;
    std::mutex session_mutex_;

    void initiate(void *buffer, uint64_t dest_addr, size_t size,
                  TransferRequest::OpCode opcode) {
        session_mutex_.lock();
        local_buffer_ = (char *)buffer;
        header_.addr = htole64(dest_addr);
        header_.size = htole64(size);
        header_.opcode = (uint8_t)opcode;
        total_transferred_bytes_ = 0;
        writeHeader();
    }

    void onAccept() {
        session_mutex_.lock();
        total_transferred_bytes_ = 0;
        readHeader();
    }

   private:
    void writeHeader() {
        // LOG(INFO) << "writeHeader";
        auto self(shared_from_this());
        boost::asio::async_write(
            socket_, boost::asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const boost::system::error_code &ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                    session_mutex_.unlock();
                    return;
                }
                if (header_.opcode == (uint8_t)TransferRequest::WRITE)
                    writeBody();
                else
                    readBody();
            });
    }

    void readHeader() {
        // LOG(INFO) << "readHeader";
        auto self(shared_from_this());
        boost::asio::async_read(
            socket_, boost::asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const boost::system::error_code &ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                    session_mutex_.unlock();
                    return;
                }

                local_buffer_ = (char *)(le64toh(header_.addr));
                if (header_.opcode == (uint8_t)TransferRequest::WRITE)
                    readBody();
                else
                    writeBody();
            });
    }

    void writeBody() {
        // LOG(INFO) << "writeBody";
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char *addr = local_buffer_;

        size_t buffer_size =
            std::min(kDefaultBufferSize, size - total_transferred_bytes_);
        if (buffer_size == 0) {
            if (on_finalize_) on_finalize_(TransferStatusEnum::COMPLETED);
            session_mutex_.unlock();
            return;
        }

        boost::asio::async_write(
            socket_,
            boost::asio::buffer(addr + total_transferred_bytes_, buffer_size),
            [this, addr, self](const boost::system::error_code &ec,
                               std::size_t transferred_bytes) {
                if (ec) {
                    if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                    session_mutex_.unlock();
                    return;
                }
                total_transferred_bytes_ += transferred_bytes;
                writeBody();
            });
    }

    void readBody() {
        // LOG(INFO) << "readBody";
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char *addr = local_buffer_;

        size_t buffer_size =
            std::min(kDefaultBufferSize, size - total_transferred_bytes_);
        if (buffer_size == 0) {
            if (on_finalize_) on_finalize_(TransferStatusEnum::COMPLETED);
            session_mutex_.unlock();
            return;
        }

        boost::asio::async_read(
            socket_,
            boost::asio::buffer(addr + total_transferred_bytes_, buffer_size),
            [this, addr, self](const boost::system::error_code &ec,
                               std::size_t transferred_bytes) {
                if (ec) {
                    if (on_finalize_) on_finalize_(TransferStatusEnum::FAILED);
                    session_mutex_.unlock();
                    return;
                }
                total_transferred_bytes_ += transferred_bytes;
                readBody();
            });
    }
};

struct TcpContext {
    TcpContext(short port)
        : acceptor(io_context, boost::asio::ip::tcp::endpoint(
                                   boost::asio::ip::tcp::v4(), port)) {}

    void doAccept() {
        acceptor.async_accept(
            [this](boost::system::error_code ec, tcpsocket socket) {
                if (!ec)
                    std::make_shared<Session>(std::move(socket))->onAccept();
                doAccept();
            });
    }

    boost::asio::io_context io_context;
    boost::asio::ip::tcp::acceptor acceptor;
};

TcpTransport::TcpTransport() : context_(nullptr), running_(false) {
    // TODO
}

TcpTransport::~TcpTransport() {
    if (running_) {
        running_ = false;
        context_->io_context.stop();
        thread_.join();
    }

    if (context_) {
        delete context_;
        context_ = nullptr;
    }

    metadata_->removeSegmentDesc(local_server_name_);
}

int TcpTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;

    int ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot allocate local segment";
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "TcpTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return -1;
    }

    context_ = new TcpContext(meta->localRpcMeta().rpc_port);
    running_ = true;
    thread_ = std::thread(&TcpTransport::worker, this);
    return 0;
}

int TcpTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "tcp";
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int TcpTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = local_server_name_;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;
    return metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
}

int TcpTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int TcpTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int TcpTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

Status TcpTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "TcpTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count ==
        task.slice_count) {
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

Status TcpTransport::submitTransfer(BatchID batch_id,
                                 const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "TcpTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "TcpTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        task.total_bytes = request.length;
        auto slice = new Slice();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->tcp.dest_addr = request.target_offset;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_count += 1;
        startTransfer(slice);
    }

    return Status::OK();
}

Status TcpTransport::submitTransferTask(
    const std::vector<TransferRequest *> &request_list,
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < request_list.size(); ++index) {
        auto &request = *request_list[index];
        auto &task = *task_list[index];
        task.total_bytes = request.length;
        auto slice = new Slice();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->tcp.dest_addr = request.target_offset;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_count += 1;
        startTransfer(slice);
    }
    return Status::OK();
}

void TcpTransport::worker() {
    while (running_) {
        try {
            context_->doAccept();
            context_->io_context.run();
        } catch (std::exception &e) {
            LOG(ERROR) << "TcpTransport: exception: " << e.what();
        }
    }
}

void TcpTransport::startTransfer(Slice *slice) {
    try {
        boost::asio::ip::tcp::resolver resolver(context_->io_context);
        boost::asio::ip::tcp::socket socket(context_->io_context);
        auto desc = metadata_->getSegmentDescByID(slice->target_id);
        if (!desc) {
            slice->markFailed();
            return;
        }

        TransferMetadata::RpcMetaDesc meta_entry;
        if (metadata_->getRpcMetaEntry(desc->name, meta_entry)) {
            slice->markFailed();
            return;
        }
        if (meta_entry.sockfd) {
            close(meta_entry.rpc_port);
            meta_entry.rpc_port = -1;
        }
        auto endpoint_iterator = resolver.resolve(
            boost::asio::ip::tcp::v4(), meta_entry.ip_or_host_name,
            std::to_string(meta_entry.rpc_port));
        boost::asio::connect(socket, endpoint_iterator);
        auto session = std::make_shared<Session>(std::move(socket));
        session->on_finalize_ = [slice](TransferStatusEnum status) {
            if (status == TransferStatusEnum::COMPLETED)
                slice->markSuccess();
            else
                slice->markFailed();
        };
        session->initiate(slice->source_addr, slice->tcp.dest_addr,
                          slice->length, slice->opcode);
    } catch (std::exception &e) {
        LOG(ERROR) << "TcpTransport: ASIO exception: " << e.what();
        slice->markFailed();
    }
}
}  // namespace mooncake