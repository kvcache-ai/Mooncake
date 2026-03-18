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

#ifndef TCP_HP_SESSION_H_
#define TCP_HP_SESSION_H_

#include <chrono>
#include <functional>
#include <memory>

#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>

#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/tcp_hp/bounce_buffer_pool.h"
#include "tent/transport/tcp_hp/protocol.h"

namespace mooncake {
namespace tent {
namespace tcp_hp {

// Handles one data transfer over a TCP connection.
//
// Client side: sends DataHeader, then WRITE sends body / READ receives body.
// Server side: reads DataHeader, validates buffer, then mirrors the operation.
//
// Phase 2 enhancements:
// - BounceBufferPool: reuses pre-allocated DRAM buffers for GPU transfers
// - Scatter-gather: zero-copy send for CPU memory (header+body in one writev)
class HpSession : public std::enable_shared_from_this<HpSession> {
   public:
    // timeout_s: per-transfer timeout in seconds (0 = no timeout)
    explicit HpSession(asio::ip::tcp::socket socket, size_t chunk_size,
                       BounceBufferPool* bounce_pool = nullptr,
                       unsigned timeout_s = 0);

    // --- Client-side initiation ---
    void initiateClient(void* buffer, uint64_t dest_addr, size_t size,
                        uint8_t opcode);

    // --- Server-side: start reading next transfer header ---
    void onAccept(SegmentManager* seg_mgr);

    // Completion callback: called with final status.
    std::function<void(TransferStatusEnum)> on_complete;
    // Socket return callback: called on success to return socket to pool.
    std::function<void(asio::ip::tcp::socket)> return_socket;
    // Server session close callback: notifies transport when a server-side
    // session ends (error/EOF), so it can be removed from active tracking.
    std::function<void()> on_close;

    // Close socket externally (used during shutdown to cancel pending I/O).
    void closeSocket();

   private:
    void writeHeader();
    void readHeader();

    // Chunked send/recv (used for GPU memory or server-side).
    void sendBodyChunked();
    void recvBodyChunked();

    // Scatter-gather zero-copy: header + full body in one async_write.
    // Used for client-side WRITE with CPU memory.
    void sendBodyZeroCopy();

    // Full-buffer zero-copy: header then one large async_read.
    // Used for client-side READ with CPU memory.
    void recvBodyZeroCopy();

    // Zero-copy send without header prefix (server-side READ response).
    void sendBodyZeroCopyNoHeader();

    void finish(TransferStatusEnum status);

    // Check if local_buffer_ is GPU memory and needs bounce buffer.
    bool needBounceBuffer() const;

    // Start / cancel deadline timer for the current transfer.
    void startDeadline();
    void cancelDeadline();

    asio::ip::tcp::socket socket_;
    DataHeader header_{};
    char* local_buffer_ = nullptr;
    uint64_t total_transferred_ = 0;
    SegmentManager* seg_mgr_ = nullptr;
    size_t chunk_size_;
    BounceBufferPool* bounce_pool_;  // nullable; owned by TcpHpTransport
    unsigned timeout_s_ = 0;
    asio::steady_timer deadline_;
    bool finished_ = false;  // guard against double-finish from timeout + I/O
};

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_SESSION_H_
