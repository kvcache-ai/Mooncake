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

#include "tent/transport/tcp_hp/hp_session.h"

#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <memory>

#include <asio.hpp>

#include "tent/runtime/platform.h"

namespace mooncake {
namespace tent {
namespace tcp_hp {

HpSession::HpSession(asio::ip::tcp::socket socket, size_t chunk_size,
                     BounceBufferPool* bounce_pool, unsigned timeout_s)
    : socket_(std::move(socket)),
      chunk_size_(chunk_size),
      bounce_pool_(bounce_pool),
      timeout_s_(timeout_s),
      deadline_(socket_.get_executor()) {}

// Check actual memory type of local_buffer_, not the platform type.
// This avoids the CUDA-build pitfall where DRAM<->DRAM transfers
// would incorrectly go through cudaMemcpy bounce path.
bool HpSession::needBounceBuffer() const {
    if (!local_buffer_) return false;
    auto mtype = Platform::getLoader().getMemoryType(local_buffer_);
    return mtype == MTYPE_CUDA;
}

// ========== Deadline timer ==========

void HpSession::startDeadline() {
    if (timeout_s_ == 0) return;
    auto self(shared_from_this());
    deadline_.expires_after(std::chrono::seconds(timeout_s_));
    deadline_.async_wait([this, self](const asio::error_code& ec) {
        if (!ec && !finished_) {
            LOG(ERROR) << "HpSession: transfer timed out after "
                       << timeout_s_ << "s";
            asio::error_code close_ec;
            socket_.close(close_ec);  // cancels all pending async ops
        }
    });
}

void HpSession::cancelDeadline() {
    if (timeout_s_ == 0) return;
    asio::error_code ec;
    deadline_.cancel(ec);
}

// ========== Client-side ==========

void HpSession::initiateClient(void* buffer, uint64_t dest_addr, size_t size,
                                uint8_t opcode) {
    local_buffer_ = static_cast<char*>(buffer);
    header_.encode(opcode, dest_addr, size);
    total_transferred_ = 0;
    startDeadline();

    bool is_gpu = needBounceBuffer();

    if (opcode == static_cast<uint8_t>(Request::WRITE) && !is_gpu) {
        // CPU WRITE: scatter-gather header+body in one writev.
        sendBodyZeroCopy();
        return;
    }

    if (opcode == static_cast<uint8_t>(Request::READ) && !is_gpu) {
        // CPU READ: send header, then receive full body in one async_read.
        // writeHeader will dispatch to recvBodyZeroCopy via the opcode check.
    }

    writeHeader();
}

void HpSession::writeHeader() {
    auto self(shared_from_this());
    asio::async_write(
        socket_, asio::buffer(&header_, sizeof(DataHeader)),
        [this, self](const asio::error_code& ec, std::size_t len) {
            if (ec || len != sizeof(DataHeader)) {
                LOG(ERROR) << "HpSession::writeHeader failed: "
                           << ec.message();
                finish(TransferStatusEnum::FAILED);
                return;
            }
            bool is_gpu = needBounceBuffer();
            if (header_.opcode == static_cast<uint8_t>(Request::WRITE)) {
                sendBodyChunked();
            } else {
                // READ path
                if (!is_gpu) {
                    recvBodyZeroCopy();
                } else {
                    recvBodyChunked();
                }
            }
        });
}

// ========== Server-side ==========

void HpSession::onAccept(SegmentManager* seg_mgr) {
    total_transferred_ = 0;
    seg_mgr_ = seg_mgr;
    readHeader();
}

void HpSession::readHeader() {
    auto self(shared_from_this());
    asio::async_read(
        socket_, asio::buffer(&header_, sizeof(DataHeader)),
        [this, self](const asio::error_code& ec, std::size_t len) {
            if (ec || len != sizeof(DataHeader)) {
                if (ec != asio::error::eof)
                    LOG(ERROR) << "HpSession::readHeader failed: "
                               << ec.message();
                finish(TransferStatusEnum::FAILED);
                return;
            }
            uint64_t addr = header_.decodedAddr();
            uint64_t length = header_.decodedLength();

            auto* local_desc = seg_mgr_->getLocal().get();
            if (!local_desc->findBuffer(addr, length)) {
                LOG(ERROR) << "HpSession: target address 0x" << std::hex
                           << addr << " length " << std::dec << length
                           << " not in registered buffer";
                finish(TransferStatusEnum::FAILED);
                return;
            }
            local_buffer_ = reinterpret_cast<char*>(addr);

            bool is_gpu = needBounceBuffer();
            if (header_.opcode == static_cast<uint8_t>(Request::WRITE)) {
                // Client sends data -> server receives.
                if (!is_gpu) {
                    // CPU: receive full body directly into local_buffer_.
                    recvBodyZeroCopy();
                } else {
                    recvBodyChunked();
                }
            } else {
                // Client reads data -> server sends.
                if (!is_gpu) {
                    // CPU: send full body directly from local_buffer_.
                    // Note: server-side send can't do scatter-gather with
                    // header (header already sent by client), so we just
                    // do a single large async_write.
                    sendBodyZeroCopyNoHeader();
                } else {
                    sendBodyChunked();
                }
            }
        });
}

// ========== Zero-copy paths (CPU memory only) ==========

void HpSession::sendBodyZeroCopy() {
    auto self(shared_from_this());
    uint64_t size = header_.decodedLength();

    // Client-side WRITE: gather [DataHeader] [entire body] in one writev.
    std::array<asio::const_buffer, 2> bufs = {
        asio::buffer(&header_, sizeof(DataHeader)),
        asio::buffer(local_buffer_, size)};

    asio::async_write(
        socket_, bufs,
        [this, self, size](const asio::error_code& ec, std::size_t /*n*/) {
            if (ec) {
                LOG(ERROR) << "HpSession::sendBodyZeroCopy failed: "
                           << ec.message();
                finish(TransferStatusEnum::FAILED);
                return;
            }
            total_transferred_ = size;
            finish(TransferStatusEnum::COMPLETED);
        });
}

void HpSession::recvBodyZeroCopy() {
    auto self(shared_from_this());
    uint64_t size = header_.decodedLength();

    // Receive entire body in one async_read directly into local_buffer_.
    asio::async_read(
        socket_, asio::buffer(local_buffer_, size),
        [this, self, size](const asio::error_code& ec, std::size_t /*n*/) {
            if (ec) {
                LOG(ERROR) << "HpSession::recvBodyZeroCopy failed: "
                           << ec.message();
                finish(TransferStatusEnum::FAILED);
                return;
            }
            total_transferred_ = size;
            finish(TransferStatusEnum::COMPLETED);
        });
}

void HpSession::sendBodyZeroCopyNoHeader() {
    auto self(shared_from_this());
    uint64_t size = header_.decodedLength();

    // Server-side READ response: send full body without DataHeader prefix.
    asio::async_write(
        socket_, asio::buffer(local_buffer_, size),
        [this, self, size](const asio::error_code& ec, std::size_t /*n*/) {
            if (ec) {
                LOG(ERROR) << "HpSession::sendBodyZeroCopyNoHeader failed: "
                           << ec.message();
                finish(TransferStatusEnum::FAILED);
                return;
            }
            total_transferred_ = size;
            finish(TransferStatusEnum::COMPLETED);
        });
}

// ========== Chunked data transfer (GPU memory) ==========

void HpSession::sendBodyChunked() {
    auto self(shared_from_this());
    uint64_t size = header_.decodedLength();
    size_t remaining = size - total_transferred_;
    if (remaining == 0) {
        finish(TransferStatusEnum::COMPLETED);
        return;
    }

    size_t chunk = std::min(chunk_size_, remaining);
    char* src = local_buffer_ + total_transferred_;

    char* send_buf = src;
    char* bounce = nullptr;
    bool bounce_from_pool = false;
    if (needBounceBuffer()) {
        if (bounce_pool_) bounce = bounce_pool_->acquire();
        if (bounce) {
            bounce_from_pool = true;
        } else {
            bounce = new char[chunk];
        }
        Platform::getLoader().copy(bounce, src, chunk);
        send_buf = bounce;
    }

    auto* pool = bounce_pool_;
    asio::async_write(
        socket_, asio::buffer(send_buf, chunk),
        [this, self, bounce, bounce_from_pool, pool](
            const asio::error_code& ec, std::size_t transferred) {
            if (bounce) {
                if (bounce_from_pool) pool->release(bounce);
                else delete[] bounce;
            }
            if (ec) {
                LOG(ERROR) << "HpSession::sendBodyChunked failed: "
                           << ec.message();
                finish(TransferStatusEnum::FAILED);
                return;
            }
            total_transferred_ += transferred;
            sendBodyChunked();
        });
}

void HpSession::recvBodyChunked() {
    auto self(shared_from_this());
    uint64_t size = header_.decodedLength();
    size_t remaining = size - total_transferred_;
    if (remaining == 0) {
        finish(TransferStatusEnum::COMPLETED);
        return;
    }

    size_t chunk = std::min(chunk_size_, remaining);
    char* dst = local_buffer_ + total_transferred_;

    char* recv_buf = dst;
    char* bounce = nullptr;
    bool bounce_from_pool = false;
    if (needBounceBuffer()) {
        if (bounce_pool_) bounce = bounce_pool_->acquire();
        if (bounce) {
            bounce_from_pool = true;
        } else {
            bounce = new char[chunk];
        }
        recv_buf = bounce;
    }

    auto* pool = bounce_pool_;
    asio::async_read(
        socket_, asio::buffer(recv_buf, chunk),
        [this, self, dst, bounce, bounce_from_pool, pool](
            const asio::error_code& ec, std::size_t transferred) {
            if (ec) {
                if (bounce) {
                    if (bounce_from_pool) pool->release(bounce);
                    else delete[] bounce;
                }
                LOG(ERROR) << "HpSession::recvBodyChunked failed: "
                           << ec.message();
                finish(TransferStatusEnum::FAILED);
                return;
            }
            if (bounce) {
                Platform::getLoader().copy(dst, bounce, transferred);
                if (bounce_from_pool) pool->release(bounce);
                else delete[] bounce;
            }
            total_transferred_ += transferred;
            recvBodyChunked();
        });
}

// ========== Finish ==========

void HpSession::finish(TransferStatusEnum status) {
    // Guard against double-finish (e.g. timeout fires after I/O error).
    if (finished_) return;
    finished_ = true;
    cancelDeadline();

    if (status == TransferStatusEnum::COMPLETED && seg_mgr_) {
        // Server side: keep connection alive, wait for next transfer.
        finished_ = false;  // reset for next transfer on this connection
        total_transferred_ = 0;
        local_buffer_ = nullptr;
        readHeader();
        return;
    }

    if (status == TransferStatusEnum::COMPLETED && return_socket) {
        auto cb = std::move(return_socket);
        cb(std::move(socket_));
    } else {
        asio::error_code ec;
        socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
        socket_.close(ec);
    }
    if (on_complete) on_complete(status);
}

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake
