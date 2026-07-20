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
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <type_traits>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

#include "cuda_alike.h"

namespace mooncake {
using tcpsocket = asio::ip::tcp::socket;

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
namespace {
using LaneConnectHandlerHook = void (*)() noexcept;
using LaneRetryHandlerHook = void (*)() noexcept;
using LaneObserverHook = void (*)(int, size_t, uint64_t, size_t, bool) noexcept;
using LaneFailureReasonHook = void (*)(int) noexcept;

std::mutex lane_test_hook_mutex;
LaneConnectHandlerHook lane_connect_handler_hook = nullptr;
LaneRetryHandlerHook lane_retry_handler_hook = nullptr;
LaneObserverHook lane_observer_hook = nullptr;
LaneFailureReasonHook lane_failure_reason_hook = nullptr;

enum LaneTestEvent {
    kLaneQueueAdmitted = 1,
    kLaneQueueRejected = 2,
    kLaneConnecting = 3,
    kLaneBusy = 4,
    kLaneTerminal = 5,
    kLaneShutdownClean = 6,
    kLaneLateHandler = 7,
    kLaneRetryArmed = 8,
    kLaneRetryFired = 9,
    kLaneRetryLate = 10,
    kLaneCooldownStarted = 11,
};

void invokeLaneConnectHandlerHook() noexcept {
    LaneConnectHandlerHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_connect_handler_hook;
    }
    if (hook) hook();
}

void invokeLaneRetryHandlerHook() noexcept {
    LaneRetryHandlerHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_retry_handler_hook;
    }
    if (hook) hook();
}

void invokeLaneObserverHook(int event, size_t queue_depth,
                            uint64_t queued_bytes, size_t active_sockets,
                            bool lane_has_current) noexcept {
    LaneObserverHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_observer_hook;
    }
    if (hook)
        hook(event, queue_depth, queued_bytes, active_sockets,
             lane_has_current);
}

void invokeLaneFailureReasonHook(int reason) noexcept {
    LaneFailureReasonHook hook;
    {
        std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
        hook = lane_failure_reason_hook;
    }
    if (hook) hook(reason);
}
}  // namespace

void tcpTransportSetLaneConnectHandlerHookForTest(
    LaneConnectHandlerHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_connect_handler_hook = hook;
}

void tcpTransportSetLaneObserverHookForTest(LaneObserverHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_observer_hook = hook;
}

void tcpTransportSetLaneRetryHandlerHookForTest(
    LaneRetryHandlerHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_retry_handler_hook = hook;
}

void tcpTransportSetLaneFailureReasonHookForTest(
    LaneFailureReasonHook hook) noexcept {
    std::lock_guard<std::mutex> lock(lane_test_hook_mutex);
    lane_failure_reason_hook = hook;
}

bool tcpTransportLaneTypesAreMoveOnlyForTest() noexcept {
    return std::is_move_constructible<TcpTransport::TcpWorkItem>::value &&
           !std::is_copy_constructible<TcpTransport::TcpWorkItem>::value &&
           !std::is_copy_assignable<TcpTransport::TcpWorkItem>::value &&
           std::is_move_constructible<TcpTransport::TerminalAction>::value &&
           !std::is_copy_constructible<TcpTransport::TerminalAction>::value &&
           !std::is_copy_assignable<TcpTransport::TerminalAction>::value;
}
#endif

static size_t getChunkSize() {
    static const size_t val = [] {
        const char* env = std::getenv("MC_TCP_SLICE_SIZE");
        if (env) {
            try {
                size_t v = std::stoull(env);
                if (v > 0) return v;
                LOG(WARNING)
                    << "Ignore non-positive MC_TCP_SLICE_SIZE value: " << env
                    << ", using default 65536";
            } catch (const std::exception& e) {
                // A non-numeric or out-of-range value makes std::stoull throw;
                // fall through to the default instead of letting the exception
                // propagate out of this static initializer and abort the
                // transfer that first reads the chunk size.
                LOG(WARNING)
                    << "Invalid MC_TCP_SLICE_SIZE value: " << env
                    << ". Error: " << e.what() << ", using default 65536";
            }
        }
        return size_t(65536);  // 64KB default
    }();
    return val;
}

struct SessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t opcode;
};

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
static bool isCudaMemory(void* addr) {
    cudaPointerAttributes attributes;
    auto status = cudaPointerGetAttributes(&attributes, addr);
    if (status != cudaSuccess) return false;
    return attributes.type == cudaMemoryTypeDevice;
}

// Returns the CUDA device ordinal if addr is device memory, or -1 otherwise.
// Callers must call cudaSetDevice before any cudaMemcpy to avoid implicit
// GPU 0 context creation.
static int getCudaDeviceId(void* addr) {
    cudaPointerAttributes attributes;
    auto status = cudaPointerGetAttributes(&attributes, addr);
    if (status != cudaSuccess) return -1;
    if (attributes.type == cudaMemoryTypeDevice) return attributes.device;
    return -1;
}

#ifdef USE_MACA
static cudaError_t copyTcpCudaMemory(void* dst, const void* src, size_t size) {
    cudaStream_t stream;
    cudaError_t status =
        cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
    if (status != cudaSuccess) return status;

    status = cudaMemcpyAsync(dst, src, size, cudaMemcpyDefault, stream);
    if (status == cudaSuccess) {
        status = cudaStreamSynchronize(stream);
    }

    cudaError_t destroy_status = cudaStreamDestroy(stream);
    return status == cudaSuccess ? destroy_status : status;
}
#endif
#endif

// Forward declaration
class TcpTransport;

using ValidateAddrFn = std::function<bool(uint64_t, uint64_t)>;

// --- Acknowledged framing (protocol v2, #2086) ------------------------------
// v1 framing gives the initiator no channel to learn whether the receiver
// applied (or even accepted) a WRITE: COMPLETED fires when the final chunk
// enters the initiator's kernel socket buffer, while megabytes may still be
// in flight toward destination memory, and a rejected request is silently
// "successful". v2 requests set the high bit of the opcode; the server then
// (a) prefixes every READ response with an 8-byte status frame and (b) sends
// an 8-byte status frame for WRITE only after the final chunk has been
// applied to destination memory. Initiators enable v2 only when the target
// segment advertises tcp_proto_version >= 2, so old servers never see
// flagged opcodes and old initiators keep receiving v1 framing.
static constexpr uint8_t kOpcodeV2Flag = 0x80;
// Status frames carry a magic in the high 32 bits so that a v2 initiator
// which reaches a v1 server through a stale descriptor (v1 treats unknown
// opcodes as READ and immediately streams payload bytes) fails fast on the
// first frame instead of misinterpreting the stream. Residual risk: payload
// bytes that happen to equal a valid frame (2^-64 per request, data
// dependent) are indistinguishable in-band; eliminating that would need a
// nonce/checksum handshake, which this deliberately avoids.
static constexpr uint64_t kStatusMagic = 0x4D435456ull << 32;  // "MCTV"
static constexpr uint64_t kStatusOk = kStatusMagic | 0;
static constexpr uint64_t kStatusAddrRejected = kStatusMagic | 1;
static inline bool statusFrameValid(uint64_t frame) {
    return (frame & 0xFFFFFFFF00000000ull) == kStatusMagic;
}

// Operational escape hatch: MC_TCP_PROTO=1 forces initiators to speak the
// legacy unacknowledged framing even to v2-capable servers. Also used by
// tests to cover the mixed-version matrix in one process.
static bool forceLegacyTcpProto() {
    // Read per call (startTransfer already does metadata lookups; getenv is
    // noise) so tests can cover both protocol modes in one process.
    const char* env = std::getenv("MC_TCP_PROTO");
    return env && env[0] == '1' && env[1] == '\0';
}

// Server-side session: handles transfer requests on a persistent connection.
// The session owns the socket; ending the callback chain without rearming
// (start()/next handler) drops the last reference and closes the connection.
struct ServerSession : public std::enable_shared_from_this<ServerSession> {
    explicit ServerSession(std::shared_ptr<tcpsocket> socket,
                           ValidateAddrFn validate_addr)
        : socket_(std::move(socket)),
          validate_addr_(std::move(validate_addr)) {}

    std::shared_ptr<tcpsocket> socket_;
    ValidateAddrFn validate_addr_;
    SessionHeader header_;
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    bool v2_ = false;
    uint64_t status_frame_;

    void start() {
        total_transferred_bytes_ = 0;
        readHeader();
    }

   private:
    // Send an 8-byte status frame, then run `next` (or end the session —
    // closing the connection — when `next` is empty or the send fails).
    void sendStatus(uint64_t status, std::function<void()> next) {
        status_frame_ = htole64(status);
        auto self(shared_from_this());
        asio::async_write(*socket_,
                          asio::buffer(&status_frame_, sizeof(status_frame_)),
                          [this, self, next = std::move(next)](
                              const asio::error_code& ec, std::size_t) {
                              if (ec)
                                  return;  // connection closes with the session
                              if (next) next();
                          });
    }

    void readHeader() {
        auto self(shared_from_this());
        asio::async_read(
            *socket_, asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    if (ec.value() != asio::error::eof) {
                        LOG(WARNING)
                            << "ServerSession::readHeader failed. Error: "
                            << ec.message() << " (value: " << ec.value() << ")"
                            << ", bytes read: " << len;
                    }
                    return;
                }

                v2_ = (header_.opcode & kOpcodeV2Flag) != 0;
                const uint8_t opcode = header_.opcode & ~kOpcodeV2Flag;
                local_buffer_ = (char*)(le64toh(header_.addr));
                uint64_t size = le64toh(header_.size);
                if (validate_addr_ &&
                    !validate_addr_((uint64_t)local_buffer_, size)) {
                    LOG(ERROR) << "ServerSession: remote-supplied address 0x"
                               << std::hex << (uint64_t)local_buffer_
                               << std::dec << " with size " << size
                               << " is not within any registered buffer";
                    // v2 initiators learn of the rejection; v1 initiators
                    // only see the connection close (and, for small WRITEs,
                    // may have already reported success — the defect v2
                    // exists to fix).
                    if (v2_) sendStatus(kStatusAddrRejected, nullptr);
                    return;
                }
                if (opcode == (uint8_t)TransferRequest::WRITE) {
                    readBody();
                } else if (v2_) {
                    // READ, v2: status frame precedes the data.
                    sendStatus(kStatusOk, [this] { writeBody(); });
                } else {
                    writeBody();
                }
            });
    }

    void writeBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (buffer_size == 0) {
            // Transfer complete, wait for next request on this connection
            start();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
            cudaSetDevice(cuda_device);
#ifdef USE_MACA
            cudaError_t cuda_status = copyTcpCudaMemory(
                dram_buffer, addr + total_transferred_bytes_, buffer_size);
#else
            cudaError_t cuda_status =
                cudaMemcpy(dram_buffer, addr + total_transferred_bytes_,
                           buffer_size, cudaMemcpyDefault);
#endif
            if (cuda_status != cudaSuccess) {
                LOG(ERROR) << "ServerSession::writeBody failed to copy from "
                              "CUDA memory. "
                           << "Error: " << cudaGetErrorString(cuda_status);
                delete[] dram_buffer;
                return;  // Connection will be closed
            }
        }
#endif

        asio::async_write(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                if (cuda_device >= 0) {
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
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (buffer_size == 0) {
            // Destination memory now holds the complete payload. Under v2,
            // acknowledge before accepting the next request — this is what
            // makes the initiator's COMPLETED mean "applied at the
            // destination" rather than "left my socket buffer".
            if (v2_) {
                sendStatus(kStatusOk, [this] { start(); });
            } else {
                start();
            }
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
        }
#endif

        asio::async_read(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
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
                    if (cuda_device >= 0) delete[] dram_buffer;
                    return;  // Connection will be closed
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                if (cuda_device >= 0) {
                    cudaSetDevice(cuda_device);
#ifdef USE_MACA
                    cudaError_t cuda_status =
                        copyTcpCudaMemory(addr + total_transferred_bytes_,
                                          dram_buffer, transferred_bytes);
#else
                    cudaError_t cuda_status =
                        cudaMemcpy(addr + total_transferred_bytes_, dram_buffer,
                                   transferred_bytes, cudaMemcpyDefault);
#endif
                    if (cuda_status != cudaSuccess) {
                        LOG(ERROR)
                            << "ServerSession::readBody failed to copy to CUDA "
                               "memory. "
                            << "Error: " << cudaGetErrorString(cuda_status);
                        delete[] dram_buffer;
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
    using OnTerminal =
        std::function<void(TransferStatusEnum, bool connection_clean)>;

    explicit ClientSession(std::shared_ptr<tcpsocket> socket, bool use_v2,
                           OnTerminal on_terminal)
        : socket_(std::move(socket)),
          v2_(use_v2),
          on_terminal_(std::move(on_terminal)) {}

    std::shared_ptr<tcpsocket> socket_;
    SessionHeader header_;
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    bool v2_;
    uint64_t status_frame_;
    // v2 WRITE runs the body stream and the ack read concurrently (one
    // async op per direction; handlers serialize on the io thread). The
    // concurrent read lets a rejection — or a v1 server's bogus payload —
    // abort a large in-flight WRITE instead of deadlocking on mutually
    // full socket buffers, and delivers rejection frames before the close.
    bool write_body_done_ = false;
    bool write_acked_ok_ = false;
    // An early negative/malformed ack can arrive while asio::async_write still
    // owns a buffer pointing into the caller's source memory. Do not publish a
    // terminal status until that body operation has completed or been
    // cancelled: callers are allowed to release the source buffer as soon as
    // the transfer becomes terminal.
    bool write_body_in_flight_ = false;
    bool write_abort_requested_ = false;
    // A v2 status frame is prompt by construction: a READ status precedes
    // any payload, and a WRITE ack follows at most one chunk's apply after
    // the body is done. The only peer that never sends one is a legacy
    // server reached through a stale v2 descriptor — and for requests
    // shorter than a frame it also keeps the connection open (it streamed
    // size < 8 bytes of "READ payload" and is waiting for our next header),
    // so without a deadline both sides wait forever. Bound that wait; the
    // default is generous so no healthy slow path can trip it, since it only
    // covers the frame itself, never payload streaming.
    static int statusFrameTimeoutSec() {
        const char* env = std::getenv("MC_TCP_STATUS_TIMEOUT_SEC");
        if (env) {
            int v = std::atoi(env);
            if (v > 0) return v;
        }
        return 30;
    }
    std::optional<asio::steady_timer> status_timer_;
    bool status_deadline_disarmed_ = false;
    bool terminal_reported_ = false;
    OnTerminal on_terminal_;

    void initiate(void* buffer, uint64_t dest_addr, size_t size,
                  TransferRequest::OpCode opcode) {
        local_buffer_ = (char*)buffer;
        header_.addr = htole64(dest_addr);
        header_.size = htole64(size);
        header_.opcode = (uint8_t)opcode | (v2_ ? kOpcodeV2Flag : 0);
        total_transferred_bytes_ = 0;
        writeHeader();
    }

    void cancel() noexcept {
        cancelStatusDeadline();
        if (!socket_) return;
        asio::error_code cancel_ec;
        socket_->cancel(cancel_ec);
        asio::error_code close_ec;
        socket_->close(close_ec);
    }

   private:
    // All handlers run on the transport's single io thread, so arm/cancel
    // and the expiry handler never race. Expiry only closes the socket: the
    // pending status read then completes with an error and its handler owns
    // the failure path (including source-buffer quiescence for WRITE).
    void armStatusDeadline() {
        auto self(shared_from_this());
        status_deadline_disarmed_ = false;
        status_timer_.emplace(socket_->get_executor());
        status_timer_->expires_after(
            std::chrono::seconds(statusFrameTimeoutSec()));
        status_timer_->async_wait([this, self](const asio::error_code& ec) {
            // The disarmed flag also covers an expiry that was already
            // queued when cancel() ran (cancel cannot revoke those, and by
            // then the socket may have been re-pooled).
            if (ec == asio::error::operation_aborted ||
                status_deadline_disarmed_) {
                return;
            }
            LOG(ERROR) << "ClientSession: no status frame within "
                       << statusFrameTimeoutSec()
                       << "s (peer likely speaks the legacy protocol); "
                          "dropping connection";
            if (socket_ && socket_->is_open()) {
                asio::error_code cec;
                socket_->close(cec);
            }
        });
    }

    void cancelStatusDeadline() {
        status_deadline_disarmed_ = true;
        if (status_timer_) {
            asio::error_code ec;
            status_timer_->cancel(ec);
        }
    }

    // Single terminal path. The invoking Asio operation has already released
    // its buffer before entering its completion handler. The lane posts any
    // follow-up pump, so a clean socket cannot be reused inline here.
    void finalize(TransferStatusEnum status, bool clean) {
        if (terminal_reported_) return;
        terminal_reported_ = true;
        cancelStatusDeadline();
        auto on_terminal = std::move(on_terminal_);
        if (on_terminal) on_terminal(status, clean);
    }

    // Abort a v2 WRITE and cancel any body operation. If asio still owns the
    // current source buffer, its completion handler is responsible for
    // finalizing after the buffer is quiescent.
    void abortWrite() {
        write_abort_requested_ = true;
        if (socket_ && socket_->is_open()) {
            asio::error_code ec;
            socket_->close(ec);
        }
        if (!write_body_in_flight_) finalize(TransferStatusEnum::FAILED, false);
    }

    void writeHeader() {
        auto self(shared_from_this());
        asio::async_write(
            *socket_, asio::buffer(&header_, sizeof(SessionHeader)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                if (ec || len != sizeof(SessionHeader)) {
                    LOG(ERROR)
                        << "ClientSession::writeHeader failed. Error: "
                        << ec.message() << " (value: " << ec.value() << ")"
                        << ", bytes written: " << len;
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                if ((header_.opcode & ~kOpcodeV2Flag) ==
                    (uint8_t)TransferRequest::WRITE) {
                    if (v2_) readWriteAck();  // concurrent with the body
                    writeBody();
                } else if (v2_) {
                    readReadStatus();
                } else {
                    readBody();
                }
            });
    }

    // v2 READ: the server prefixes the data with a status frame.
    void readReadStatus() {
        auto self(shared_from_this());
        armStatusDeadline();
        asio::async_read(
            *socket_, asio::buffer(&status_frame_, sizeof(status_frame_)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                cancelStatusDeadline();
                if (ec || len != sizeof(status_frame_)) {
                    LOG(ERROR)
                        << "ClientSession: failed to read READ status "
                           "frame. Error: "
                        << ec.message() << " (value: " << ec.value() << ")";
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                uint64_t frame = le64toh(status_frame_);
                if (!statusFrameValid(frame)) {
                    LOG(ERROR) << "ClientSession: malformed READ status "
                                  "frame (peer likely speaks the legacy "
                                  "protocol); dropping connection";
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                if (frame != kStatusOk) {
                    LOG(ERROR) << "ClientSession: READ rejected by server, "
                                  "status "
                               << (frame & 0xFFFFFFFFull);
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                readBody();
            });
    }

    // v2 WRITE: completion is the server's acknowledgment that the payload
    // has been applied to destination memory. Armed concurrently with the
    // body stream; a well-behaved v2 server only sends the frame after the
    // final chunk, so a frame arriving before the body is done is either a
    // rejection or a legacy peer's payload — both close the socket immediately
    // and publish failure only after the outstanding body write is quiescent.
    void readWriteAck() {
        auto self(shared_from_this());
        asio::async_read(
            *socket_, asio::buffer(&status_frame_, sizeof(status_frame_)),
            [this, self](const asio::error_code& ec, std::size_t len) {
                cancelStatusDeadline();
                if (ec || len != sizeof(status_frame_)) {
                    // The body path may have already finalized a failure and
                    // closed the socket; finalize() is idempotent (moved-from
                    // callbacks are null-checked).
                    if (ec != asio::error::operation_aborted) {
                        LOG(ERROR)
                            << "ClientSession: failed to read WRITE "
                               "ack frame. Error: "
                            << ec.message() << " (value: " << ec.value() << ")";
                    }
                    abortWrite();
                    return;
                }
                uint64_t frame = le64toh(status_frame_);
                if (!statusFrameValid(frame)) {
                    LOG(ERROR) << "ClientSession: malformed WRITE ack frame "
                                  "(peer likely speaks the legacy protocol); "
                                  "dropping connection";
                    abortWrite();
                    return;
                }
                if (frame != kStatusOk) {
                    LOG(ERROR) << "ClientSession: WRITE rejected by server, "
                                  "status "
                               << (frame & 0xFFFFFFFFull);
                    abortWrite();
                    return;
                }
                if (!write_body_done_) {
                    // The server's ack can legitimately overtake the final
                    // local write-completion handler (both become ready
                    // together for small writes; the io thread may run this
                    // handler first). Record it; the body path finalizes.
                    write_acked_ok_ = true;
                    return;
                }
                finalize(TransferStatusEnum::COMPLETED, true);
            });
    }

    void readBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (buffer_size == 0) {
            finalize(TransferStatusEnum::COMPLETED, true);
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
        }
#endif

        asio::async_read(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                if (ec) {
                    LOG(ERROR)
                        << "ClientSession::readBody failed. "
                        << "Attempt to read data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                    if (cuda_device >= 0) delete[] dram_buffer;
#endif
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
                if (cuda_device >= 0) {
                    cudaSetDevice(cuda_device);
#ifdef USE_MACA
                    cudaError_t cuda_status =
                        copyTcpCudaMemory(addr + total_transferred_bytes_,
                                          dram_buffer, transferred_bytes);
#else
                    cudaError_t cuda_status =
                        cudaMemcpy(addr + total_transferred_bytes_, dram_buffer,
                                   transferred_bytes, cudaMemcpyDefault);
#endif
                    if (cuda_status != cudaSuccess) {
                        LOG(ERROR)
                            << "ClientSession::readBody failed to copy to CUDA "
                               "memory. "
                            << "Error: " << cudaGetErrorString(cuda_status);
                        delete[] dram_buffer;
                        finalize(TransferStatusEnum::FAILED, false);
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
            std::min(getChunkSize(), size - total_transferred_bytes_);
        if (buffer_size == 0) {
            if (v2_) {
                if (write_abort_requested_) {
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                // Completion comes from the server's acknowledgment, whose
                // read is already in flight (armed in writeHeader) and may
                // have finished first.
                write_body_done_ = true;
                if (write_acked_ok_) {
                    finalize(TransferStatusEnum::COMPLETED, true);
                } else {
                    // From here a well-behaved server owes at most one
                    // chunk's apply plus the frame; a legacy peer behind a
                    // stale descriptor may owe nothing, ever.
                    armStatusDeadline();
                }
            } else {
                // v1: no acknowledgment exists in the protocol; this only
                // means the payload left the initiator (#2086).
                finalize(TransferStatusEnum::COMPLETED, true);
            }
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;
        int cuda_device = -1;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
        cuda_device = getCudaDeviceId(addr);
        if (cuda_device >= 0) {
            dram_buffer = new char[buffer_size];
            cudaSetDevice(cuda_device);
#ifdef USE_MACA
            cudaError_t cuda_status = copyTcpCudaMemory(
                dram_buffer, addr + total_transferred_bytes_, buffer_size);
#else
            cudaError_t cuda_status =
                cudaMemcpy(dram_buffer, addr + total_transferred_bytes_,
                           buffer_size, cudaMemcpyDefault);
#endif
            if (cuda_status != cudaSuccess) {
                LOG(ERROR) << "ClientSession::writeBody failed to copy from "
                              "CUDA memory. "
                           << "Error: " << cudaGetErrorString(cuda_status);
                delete[] dram_buffer;
                abortWrite();
                return;
            }
        }
#endif

        write_body_in_flight_ = true;
        asio::async_write(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [this, addr, dram_buffer, cuda_device, self](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                write_body_in_flight_ = false;
                if (cuda_device >= 0) {
                    delete[] dram_buffer;
                }
                if (ec) {
                    LOG(ERROR)
                        << "ClientSession::writeBody failed. "
                        << "Attempt to write data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    abortWrite();
                    return;
                }
                if (write_abort_requested_) {
                    // The early ack path closed the socket while this
                    // operation still owned the caller's source buffer. It is
                    // safe to publish failure now that the handler has run.
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                total_transferred_bytes_ += transferred_bytes;
                writeBody();
            });
    }
};

struct TcpContext {
    TcpContext(short port, ValidateAddrFn validate_addr)
        : acceptor(io_context), validate_addr_(std::move(validate_addr)) {
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
                asio::error_code nodelay_ec;
                socket.set_option(asio::ip::tcp::no_delay(true), nodelay_ec);
                auto socket_ptr =
                    std::make_shared<tcpsocket>(std::move(socket));
                auto session =
                    std::make_shared<ServerSession>(socket_ptr, validate_addr_);
                session->start();
            }
            doAccept();
        });
    }

    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
    ValidateAddrFn validate_addr_;
};

namespace {
constexpr size_t kMaxTcpLanesPerPeer = 16;

size_t parseBoundedTcpSetting(const char* name, const char* value,
                              size_t default_value, size_t minimum,
                              size_t maximum) {
    if (!value) return default_value;

    const std::string text(value);
    size_t parsed = 0;
    bool valid = !text.empty();
    for (char c : text) {
        if (c < '0' || c > '9') {
            valid = false;
            break;
        }
        const size_t digit = static_cast<size_t>(c - '0');
        if (parsed > (maximum - digit) / size_t(10)) {
            valid = false;
            break;
        }
        parsed = parsed * 10 + digit;
    }
    if (valid && parsed >= minimum && parsed <= maximum) return parsed;

    LOG(WARNING) << "Invalid " << name << " value: " << text
                 << ", using default " << default_value;
    return default_value;
}

bool validateTcpAddress(const std::shared_ptr<TransferMetadata>& metadata,
                        uint64_t addr, uint64_t size) {
    if (size == 0 || addr + size < addr) return false;

    auto desc = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!desc) return false;
    for (const auto& buffer : desc->buffers) {
        if (buffer.addr + buffer.length < buffer.addr) continue;
        if (buffer.addr <= addr && addr + size <= buffer.addr + buffer.length)
            return true;
    }
    return false;
}
}  // namespace

TcpTransport::TcpTransport()
    : context_(nullptr),
      running_(false),
      lane_state_(std::make_shared<ConnectionLaneState>()) {
    if (getenv("MC_TCP_ENABLE_CONNECTION_POOL") != nullptr) {
        std::string val(getenv("MC_TCP_ENABLE_CONNECTION_POOL"));
        std::transform(val.begin(), val.end(), val.begin(),
                       [](unsigned char c) -> char { return std::tolower(c); });
        if (val == "0" || val == "false" || val == "no") {
            enable_connection_pool_ = false;
        } else {
            enable_connection_pool_ = true;
        }
    }

    if (enable_connection_pool_) {
        constexpr size_t kDefaultLanesPerPeer = 4;
        constexpr size_t kDefaultQueuedTransfersPerPeer = 1024;
        constexpr size_t kMaxQueuedTransfersPerPeer = 65535;

        const char* lanes_env = getenv("MC_TCP_LANES_PER_PEER");
        const char* deprecated_env = getenv("MC_TCP_MAX_CONNECTIONS_PER_PEER");
        if (lanes_env) {
            lane_state_->lanes_per_peer = parseBoundedTcpSetting(
                "MC_TCP_LANES_PER_PEER", lanes_env, kDefaultLanesPerPeer, 1,
                kMaxTcpLanesPerPeer);
        } else if (deprecated_env) {
            LOG(WARNING) << "MC_TCP_MAX_CONNECTIONS_PER_PEER is deprecated; "
                            "use MC_TCP_LANES_PER_PEER";
            lane_state_->lanes_per_peer = parseBoundedTcpSetting(
                "MC_TCP_MAX_CONNECTIONS_PER_PEER", deprecated_env,
                kDefaultLanesPerPeer, 1, kMaxTcpLanesPerPeer);
        }

        lane_state_->max_queued_transfers_per_peer = parseBoundedTcpSetting(
            "MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER",
            getenv("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER"),
            kDefaultQueuedTransfersPerPeer, 1, kMaxQueuedTransfersPerPeer);
    }
}

TcpTransport::~TcpTransport() {
    shutdownConnectionLanes();

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
    auto metadata = metadata_;
    context_ = new TcpContext(tcp_port, [metadata = std::move(metadata)](
                                            uint64_t addr, uint64_t size) {
        return validateTcpAddress(metadata, addr, size);
    });
    if (enable_connection_pool_) {
        lane_runtime_ =
            std::make_shared<ConnectionLaneRuntime>(context_->io_context);
        lane_state_->runtime = lane_runtime_;
    }
    running_ = true;
    thread_ = std::thread(&TcpTransport::worker, this);
    return 0;
}

int TcpTransport::allocateLocalSegmentID(int tcp_data_port) {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (!desc->protocol.empty()) desc->protocol += ",";
    desc->protocol += "tcp";
#else
    desc->protocol = "tcp";
#endif
    desc->tcp_data_port = tcp_data_port;
    // Advertise acknowledged framing (#2086); initiators fall back to v1
    // against descriptors that do not carry the field.
    desc->tcp_proto_version = 2;
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
#ifdef ENABLE_MULTI_PROTOCOL
    buffer_desc.protocol = "tcp";
#endif
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
            context_->io_context.restart();
        }
    }
}

std::shared_ptr<asio::ip::tcp::socket> TcpTransport::getConnection(
    const std::string& host, uint16_t port) {
    // If connection pool is disabled, always create a new connection
    try {
        asio::ip::tcp::resolver resolver(context_->io_context);
        auto endpoint_iterator = resolver.resolve(host, std::to_string(port));
        auto socket_ptr =
            std::make_shared<asio::ip::tcp::socket>(context_->io_context);
        asio::connect(*socket_ptr, endpoint_iterator);
        socket_ptr->set_option(asio::ip::tcp::no_delay(true));
        return socket_ptr;
    } catch (std::exception& e) {
        LOG(ERROR)
            << "TcpTransport::getConnection failed to create connection to "
            << host << ":" << port << ". Error: " << e.what();
        return nullptr;
    }
}

namespace {
constexpr size_t kMaxConcurrentLaneProbes = 1;
// Conservative fixed policy for the first rate-limited implementation. The
// exact cooldown/backoff policy remains subject to maintainer review.
constexpr auto kReconnectRoundCooldown = std::chrono::seconds(1);
constexpr auto kShutdownCancellationWait = std::chrono::seconds(2);

bool shouldLogOccurrence(uint64_t occurrence) {
    return occurrence != 0 && (occurrence & (occurrence - 1)) == 0;
}

// Tracks only whether executor-posted cancellation actions ran. It is not an
// asynchronous-handler quiescence barrier; stop/join provides that boundary.
struct LaneCancellationPostTracker {
    std::mutex mutex;
    std::condition_variable cv;
    size_t pending = 0;

    void add() {
        std::lock_guard<std::mutex> lock(mutex);
        ++pending;
    }

    void done() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (pending != 0) --pending;
        }
        cv.notify_all();
    }

    void waitUntil(std::chrono::steady_clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_until(lock, deadline, [this] { return pending == 0; });
    }
};
}  // namespace

bool TcpTransport::hasUsableLaneLocked(const PeerConnectionGroup& group) {
    for (const auto& lane : group.lanes) {
        if ((lane->state == LaneState::IDLE || lane->state == LaneState::BUSY ||
             lane->state == LaneState::COMPLETING) &&
            lane->socket && lane->socket->is_open()) {
            return true;
        }
    }
    return false;
}

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
size_t TcpTransport::activeSocketCountLocked(const PeerConnectionGroup& group) {
    size_t count = 0;
    for (const auto& lane : group.lanes) {
        if (lane->resolver || lane->socket) ++count;
    }
    return count;
}
#endif

void TcpTransport::beginConnectRoundLocked(PeerConnectionGroup& group) {
    ++group.connect_round;
    if (group.connect_round == 0) group.connect_round = 1;
    group.connect_attempts_in_round = 0;
    group.connect_round_had_success = false;
    group.next_probe_not_before = {};
}

void TcpTransport::enterReconnectCooldownLocked(PeerConnectionGroup& group) {
    group.next_probe_not_before =
        std::chrono::steady_clock::now() + kReconnectRoundCooldown;
}

void TcpTransport::addQueuedBytesLocked(PeerConnectionGroup& group,
                                        uint64_t length) {
    if (group.queued_bytes_saturated) return;
    if (group.queued_bytes > std::numeric_limits<uint64_t>::max() - length) {
        group.queued_bytes = std::numeric_limits<uint64_t>::max();
        group.queued_bytes_saturated = true;
        return;
    }
    group.queued_bytes += length;
}

void TcpTransport::removeQueuedBytesLocked(PeerConnectionGroup& group,
                                           uint64_t length) {
    if (!group.queued_bytes_saturated) {
        group.queued_bytes =
            group.queued_bytes >= length ? group.queued_bytes - length : 0;
        return;
    }

    // Once saturated, UINT64_MAX is only an explicit lower-fidelity marker,
    // not an exact sum. Recompute after removal so exact accounting resumes as
    // soon as the remaining queue fits in uint64_t.
    group.queued_bytes = 0;
    group.queued_bytes_saturated = false;
    for (const auto& item : group.queue) {
        addQueuedBytesLocked(group, item.slice->length);
        if (group.queued_bytes_saturated) break;
    }
}

void TcpTransport::clearQueuedBytesLocked(PeerConnectionGroup& group) {
    group.queued_bytes = 0;
    group.queued_bytes_saturated = false;
}

bool TcpTransport::armRetryTimerLocked(
    const std::shared_ptr<PeerConnectionGroup>& group) {
    if (group->retry_timer || group->state != GroupState::OPEN ||
        group->queue.empty()) {
        return true;
    }

    try {
        auto timer = std::make_shared<asio::steady_timer>(group->executor);
        timer->expires_at(group->next_probe_not_before);
        ++group->retry_epoch;
        if (group->retry_epoch == 0) ++group->retry_epoch;
        const uint64_t retry_epoch = group->retry_epoch;
        group->retry_timer = timer;
        // A handler abandoned by io_context.stop() is harmless: it holds only
        // group/timer shared state and can only request a new connection round.
        timer->async_wait([group, timer, retry_epoch](asio::error_code ec) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            invokeLaneRetryHandlerHook();
#endif
            handleRetryTimer(group, timer, retry_epoch, ec);
        });
        return true;
    } catch (...) {
        group->retry_timer.reset();
        ++group->retry_epoch;
        if (group->retry_epoch == 0) ++group->retry_epoch;
        return false;
    }
}

void TcpTransport::handleRetryTimer(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<asio::steady_timer>& timer, uint64_t retry_epoch,
    asio::error_code ec) {
    uint64_t pump_epoch = 0;
    [[maybe_unused]] bool fired = false;
    [[maybe_unused]] bool late = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->retry_epoch != retry_epoch || group->retry_timer != timer) {
            late = true;
        } else {
            group->retry_timer.reset();
            if (ec || group->state != GroupState::OPEN) {
                late = group->state != GroupState::OPEN;
            } else if (!group->queue.empty()) {
                beginConnectRoundLocked(*group);
                pump_epoch = requestGroupPumpLocked(*group);
                fired = true;
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (late)
        invokeLaneObserverHook(kLaneRetryLate, 0, 0, 0, false);
    else if (fired)
        invokeLaneObserverHook(kLaneRetryFired, 0, 0, 0, false);
#endif
    if (pump_epoch != 0) postGroupPump(group, pump_epoch);
}

uint64_t TcpTransport::requestGroupPumpLocked(PeerConnectionGroup& group) {
    if (group.state != GroupState::OPEN || group.pump_scheduled ||
        group.queue.empty()) {
        return 0;
    }
    group.pump_scheduled = true;
    ++group.pump_epoch;
    if (group.pump_epoch == 0) ++group.pump_epoch;
    return group.pump_epoch;
}

void TcpTransport::enqueuePooledTransfer(const ConnectionKey& key,
                                         TcpWorkItem work) {
    const auto state = lane_state_;
    std::shared_ptr<PeerConnectionGroup> group;
    std::optional<TcpWorkItem> rejected;
    WorkFailureReason rejection_reason = WorkFailureReason::QUEUE_FULL;
    uint64_t pump_epoch = 0;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif

    try {
        std::lock_guard<std::mutex> state_lock(state->mutex);
        if (state->shutting_down) {
            rejected.emplace(std::move(work));
            rejection_reason = WorkFailureReason::SHUTDOWN;
        } else {
            auto runtime = state->runtime.lock();
            if (!runtime) {
                rejected.emplace(std::move(work));
                rejection_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
            } else {
                auto group_it = state->groups.find(key);
                if (group_it == state->groups.end()) {
                    group = std::make_shared<PeerConnectionGroup>(
                        key, runtime->executor, state->lanes_per_peer,
                        state->max_queued_transfers_per_peer,
                        state->failure_counters);
                    group->lanes.reserve(state->lanes_per_peer);
                    for (size_t i = 0; i < state->lanes_per_peer; ++i) {
                        group->lanes.push_back(
                            std::make_shared<ConnectionLane>(i, group));
                    }
                    auto [inserted_it, inserted] =
                        state->groups.emplace(key, group);
                    if (!inserted) group = inserted_it->second;
                } else {
                    group = group_it->second;
                }

                std::lock_guard<std::mutex> group_lock(group->mutex);
                if (group->state != GroupState::OPEN) {
                    rejected.emplace(std::move(work));
                    rejection_reason = WorkFailureReason::SHUTDOWN;
                } else if (group->queue.size() >= group->queue_capacity) {
                    rejected.emplace(std::move(work));
                } else {
                    // Submissions arriving during cooldown remain subject to
                    // the bounded queue and wait until the retry timer expires
                    // and a new round begins, so their added latency is bounded
                    // by the cooldown length.
                    work.admission_sequence = group->next_admission_sequence++;
                    work.enqueued_at = std::chrono::steady_clock::now();
                    group->queue.emplace_back(std::move(work));
                    addQueuedBytesLocked(*group,
                                         group->queue.back().slice->length);
                    pump_epoch = requestGroupPumpLocked(*group);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                    queue_depth = group->queue.size();
                    queued_bytes = group->queued_bytes;
                    active_sockets = activeSocketCountLocked(*group);
#endif
                }
            }
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to admit TCP work for " << key.host << ":"
                   << key.port << ". Error: " << e.what();
        if (work.slice) rejected.emplace(std::move(work));
        rejection_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
    } catch (...) {
        LOG(ERROR) << "Failed to admit TCP work for " << key.host << ":"
                   << key.port << ". Error: unknown exception";
        if (work.slice) rejected.emplace(std::move(work));
        rejection_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (rejected) {
        invokeLaneObserverHook(kLaneQueueRejected, queue_depth, queued_bytes,
                               active_sockets, false);
    } else {
        invokeLaneObserverHook(kLaneQueueAdmitted, queue_depth, queued_bytes,
                               active_sockets, false);
    }
#endif

    if (rejected)
        failWorkItem(std::move(*rejected), rejection_reason,
                     state->failure_counters);
    else if (pump_epoch != 0)
        postGroupPump(group, pump_epoch);
}

void TcpTransport::postGroupPump(
    const std::shared_ptr<PeerConnectionGroup>& group, uint64_t pump_epoch) {
    try {
        asio::post(group->executor,
                   [group, pump_epoch] { runGroupPump(group, pump_epoch); });
    } catch (const std::exception& e) {
        std::deque<TcpWorkItem> failed;
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            if (group->pump_scheduled && group->pump_epoch == pump_epoch) {
                group->pump_scheduled = false;
                failed.swap(group->queue);
                clearQueuedBytesLocked(*group);
            }
        }
        LOG(ERROR) << "Failed to schedule TCP lane pump for " << group->key.host
                   << ":" << group->key.port << ". Error: " << e.what();
        failWorkItems(std::move(failed), WorkFailureReason::RUNTIME_UNAVAILABLE,
                      group->failure_counters);
    } catch (...) {
        std::deque<TcpWorkItem> failed;
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            if (group->pump_scheduled && group->pump_epoch == pump_epoch) {
                group->pump_scheduled = false;
                failed.swap(group->queue);
                clearQueuedBytesLocked(*group);
            }
        }
        LOG(ERROR) << "Failed to schedule TCP lane pump for " << group->key.host
                   << ":" << group->key.port;
        failWorkItems(std::move(failed), WorkFailureReason::RUNTIME_UNAVAILABLE,
                      group->failure_counters);
    }
}

void TcpTransport::runGroupPump(
    const std::shared_ptr<PeerConnectionGroup>& group, uint64_t pump_epoch) {
    struct LaneStart {
        std::shared_ptr<ConnectionLane> lane;
        uint64_t epoch;
    };
    std::array<LaneStart, kMaxTcpLanesPerPeer> sessions;
    std::array<LaneStart, kMaxTcpLanesPerPeer> connects;
    size_t session_count = 0;
    size_t connect_count = 0;
    std::deque<TcpWorkItem> failed;
    WorkFailureReason failure_reason = WorkFailureReason::CONNECT_FAILED;
    [[maybe_unused]] bool retry_armed = false;
    [[maybe_unused]] bool cooldown_started = false;

    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (!group->pump_scheduled || group->pump_epoch != pump_epoch) return;
        group->pump_scheduled = false;
        if (group->state != GroupState::OPEN) return;

        for (const auto& lane : group->lanes) {
            if (group->queue.empty()) break;
            if (lane->state != LaneState::IDLE) continue;
            if (!lane->socket || !lane->socket->is_open()) {
                lane->socket.reset();
                lane->state = LaneState::DISCONNECTED;
                continue;
            }

            lane->current.emplace(std::move(group->queue.front()));
            const uint64_t length = lane->current->slice->length;
            group->queue.pop_front();
            removeQueuedBytesLocked(*group, length);
            if (group->queue.empty()) clearQueuedBytesLocked(*group);
            lane->state = LaneState::BUSY;
            ++lane->operation_epoch;
            if (lane->operation_epoch == 0) ++lane->operation_epoch;
            sessions[session_count++] = {lane, lane->operation_epoch};
        }

        bool waiting_for_cooldown = false;
        if (!group->queue.empty() && !hasUsableLaneLocked(*group) &&
            group->probes_in_flight == 0 &&
            group->connect_attempts_in_round >= group->lanes.size()) {
            if (std::chrono::steady_clock::now() <
                group->next_probe_not_before) {
                const bool timer_was_armed = group->retry_timer != nullptr;
                if (armRetryTimerLocked(group)) {
                    waiting_for_cooldown = true;
                    retry_armed = !timer_was_armed && group->retry_timer;
                } else {
                    failed.swap(group->queue);
                    clearQueuedBytesLocked(*group);
                    failure_reason = WorkFailureReason::RUNTIME_UNAVAILABLE;
                }
            } else {
                beginConnectRoundLocked(*group);
            }
        }

        if (!waiting_for_cooldown && failed.empty()) {
            const size_t probe_limit =
                std::min(group->lane_count, kMaxConcurrentLaneProbes);
            while (!group->queue.empty() &&
                   group->probes_in_flight < probe_limit) {
                auto lane_it = std::find_if(
                    group->lanes.begin(), group->lanes.end(),
                    [&group](const auto& lane) {
                        return lane->state == LaneState::DISCONNECTED &&
                               lane->last_connect_round != group->connect_round;
                    });
                if (lane_it == group->lanes.end()) break;

                auto lane = *lane_it;
                lane->state = LaneState::CONNECTING;
                lane->connect_stage = LaneConnectStage::NONE;
                lane->last_connect_round = group->connect_round;
                ++lane->operation_epoch;
                if (lane->operation_epoch == 0) ++lane->operation_epoch;
                ++group->probes_in_flight;
                ++group->connect_attempts_in_round;
                connects[connect_count++] = {lane, lane->operation_epoch};
            }

            if (!group->queue.empty() && !hasUsableLaneLocked(*group) &&
                group->probes_in_flight == 0 &&
                group->connect_attempts_in_round >= group->lanes.size()) {
                failed.swap(group->queue);
                clearQueuedBytesLocked(*group);
                enterReconnectCooldownLocked(*group);
                cooldown_started = true;
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (retry_armed) invokeLaneObserverHook(kLaneRetryArmed, 0, 0, 0, false);
    if (cooldown_started)
        invokeLaneObserverHook(kLaneCooldownStarted, 0, 0, 0, false);
#endif
    for (size_t i = 0; i < connect_count; ++i)
        startLaneConnect(group, connects[i].lane, connects[i].epoch);
    for (size_t i = 0; i < session_count; ++i)
        startLaneSession(group, sessions[i].lane, sessions[i].epoch);
    failWorkItems(std::move(failed), failure_reason, group->failure_counters);
}

void TcpTransport::startLaneConnect(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch) {
    std::string initiation_error;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::CONNECTING ||
            lane->operation_epoch != epoch) {
            return;
        }
        try {
            lane->resolver =
                std::make_shared<asio::ip::tcp::resolver>(group->executor);
            lane->socket =
                std::make_shared<asio::ip::tcp::socket>(group->executor);
            lane->connect_stage = LaneConnectStage::RESOLVING;
            lane->resolver->async_resolve(
                group->key.host, std::to_string(group->key.port),
                [group, lane, epoch](
                    asio::error_code ec,
                    asio::ip::tcp::resolver::results_type results) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                    invokeLaneConnectHandlerHook();
#endif
                    handleLaneResolved(group, lane, epoch, ec,
                                       std::move(results));
                });
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            queue_depth = group->queue.size();
            queued_bytes = group->queued_bytes;
            active_sockets = activeSocketCountLocked(*group);
#endif
        } catch (const std::exception& e) {
            initiation_error = e.what();
        } catch (...) {
            initiation_error = "unknown exception";
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (initiation_error.empty()) {
        invokeLaneObserverHook(kLaneConnecting, queue_depth, queued_bytes,
                               active_sockets, false);
    }
#endif
    if (!initiation_error.empty())
        handleLaneConnectFailure(group, lane, epoch, initiation_error);
}

void TcpTransport::handleLaneResolved(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    asio::error_code ec, asio::ip::tcp::resolver::results_type results) {
    if (ec) {
        handleLaneConnectFailure(group, lane, epoch, ec.message());
        return;
    }

    std::string initiation_error;
    [[maybe_unused]] bool stale = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::CONNECTING ||
            lane->connect_stage != LaneConnectStage::RESOLVING ||
            lane->operation_epoch != epoch || !lane->socket) {
            stale = true;
        } else {
            lane->connect_stage = LaneConnectStage::CONNECTING;
            try {
                asio::async_connect(
                    *lane->socket, results,
                    [group, lane, epoch](asio::error_code connect_ec,
                                         const asio::ip::tcp::endpoint&) {
                        handleLaneConnected(group, lane, epoch, connect_ec);
                    });
            } catch (const std::exception& e) {
                initiation_error = e.what();
            } catch (...) {
                initiation_error = "unknown exception";
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
#endif
    if (!initiation_error.empty())
        handleLaneConnectFailure(group, lane, epoch, initiation_error);
}

void TcpTransport::handleLaneConnected(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    asio::error_code ec) {
    if (ec) {
        handleLaneConnectFailure(group, lane, epoch, ec.message());
        return;
    }

    uint64_t pump_epoch = 0;
    [[maybe_unused]] bool stale = false;
    std::string option_error;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::CONNECTING ||
            lane->connect_stage != LaneConnectStage::CONNECTING ||
            lane->operation_epoch != epoch || !lane->socket) {
            stale = true;
        } else {
            asio::error_code option_ec;
            lane->socket->set_option(asio::ip::tcp::no_delay(true), option_ec);
            if (option_ec) {
                option_error = option_ec.message();
            } else {
                if (group->probes_in_flight != 0) --group->probes_in_flight;
                group->connect_round_had_success = true;
                lane->resolver.reset();
                lane->connect_stage = LaneConnectStage::NONE;
                lane->state = LaneState::IDLE;
                pump_epoch = requestGroupPumpLocked(*group);
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
#endif
    if (!option_error.empty()) {
        handleLaneConnectFailure(group, lane, epoch, option_error);
    } else if (pump_epoch != 0) {
        postGroupPump(group, pump_epoch);
    }
}

void TcpTransport::handleLaneConnectFailure(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    const std::string& error) {
    std::shared_ptr<asio::ip::tcp::resolver> resolver;
    std::shared_ptr<asio::ip::tcp::socket> socket;
    std::deque<TcpWorkItem> failed;
    uint64_t pump_epoch = 0;
    bool stale = false;
    [[maybe_unused]] bool cooldown_started = false;
    uint64_t connect_failure_log_count = 0;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (lane->state != LaneState::CONNECTING ||
            lane->operation_epoch != epoch) {
            stale = true;
        } else {
            connect_failure_log_count = ++group->connect_failure_log_count;
            if (group->probes_in_flight != 0) --group->probes_in_flight;
            resolver = std::move(lane->resolver);
            socket = std::move(lane->socket);
            lane->connect_stage = LaneConnectStage::NONE;
            lane->state = group->state == GroupState::OPEN
                              ? LaneState::DISCONNECTED
                              : LaneState::CLOSING;

            if (group->state == GroupState::OPEN && !group->queue.empty() &&
                !hasUsableLaneLocked(*group) && group->probes_in_flight == 0 &&
                group->connect_attempts_in_round >= group->lanes.size()) {
                failed.swap(group->queue);
                clearQueuedBytesLocked(*group);
                enterReconnectCooldownLocked(*group);
                cooldown_started = true;
            } else {
                pump_epoch = requestGroupPumpLocked(*group);
            }
        }
    }

    if (resolver) {
        try {
            resolver->cancel();
        } catch (...) {
        }
    }
    closeSocketNoThrow(socket);
    if (!stale && shouldLogOccurrence(connect_failure_log_count)) {
        LOG(ERROR) << "TCP lane connection to " << group->key.host << ":"
                   << group->key.port << " failed: " << error
                   << " (attempt failure " << connect_failure_log_count << ")";
    }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
    if (cooldown_started)
        invokeLaneObserverHook(kLaneCooldownStarted, 0, 0, 0, false);
#endif
    failWorkItems(std::move(failed), WorkFailureReason::CONNECT_FAILED,
                  group->failure_counters);
    if (pump_epoch != 0) postGroupPump(group, pump_epoch);
}

void TcpTransport::startLaneSession(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch) {
    std::string initiation_error;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->state != LaneState::BUSY || lane->operation_epoch != epoch ||
            !lane->current) {
            return;
        }
        if (!lane->socket || !lane->socket->is_open()) {
            initiation_error = "lane socket is not open";
        } else {
            // This function runs on the TCP executor. Keep construction and
            // initial Asio initiation under the group lock so shutdown cannot
            // invalidate the checked epoch between validation and initiation.
            // Asio initiating functions do not invoke their completion handler
            // inline, so this cannot call lane terminal completion under the
            // mutex.
            try {
                std::weak_ptr<PeerConnectionGroup> weak_group(group);
                std::weak_ptr<ConnectionLane> weak_lane(lane);
                auto session = std::make_shared<ClientSession>(
                    lane->socket, lane->current->use_v2,
                    [weak_group, weak_lane, epoch](TransferStatusEnum status,
                                                   bool clean) noexcept {
                        auto callback_group = weak_group.lock();
                        auto callback_lane = weak_lane.lock();
                        if (!callback_group || !callback_lane) return;
                        handleLaneTerminal(callback_group, callback_lane, epoch,
                                           status, clean);
                    });
                lane->session = session;
                session->initiate(lane->current->slice->source_addr,
                                  lane->current->slice->tcp.dest_addr,
                                  lane->current->slice->length,
                                  lane->current->slice->opcode);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                queue_depth = group->queue.size();
                queued_bytes = group->queued_bytes;
                active_sockets = activeSocketCountLocked(*group);
#endif
            } catch (const std::exception& e) {
                lane->session.reset();
                initiation_error = e.what();
            } catch (...) {
                lane->session.reset();
                initiation_error = "unknown exception";
            }
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (initiation_error.empty()) {
        invokeLaneObserverHook(kLaneBusy, queue_depth, queued_bytes,
                               active_sockets, true);
    }
#endif
    if (!initiation_error.empty()) {
        LOG(ERROR) << "Failed to start TCP lane session for " << group->key.host
                   << ":" << group->key.port << ". Error: " << initiation_error;
        handleLaneTerminal(group, lane, epoch, TransferStatusEnum::FAILED,
                           false);
    }
}

void TcpTransport::handleLaneTerminal(
    const std::shared_ptr<PeerConnectionGroup>& group,
    const std::shared_ptr<ConnectionLane>& lane, uint64_t epoch,
    TransferStatusEnum status, bool connection_clean) noexcept {
    std::optional<TerminalAction> action;
    std::shared_ptr<asio::ip::tcp::socket> socket_to_close;
    bool stale = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (lane->operation_epoch != epoch || lane->state != LaneState::BUSY ||
            !lane->current) {
            stale = true;
        } else {
            action.emplace(std::move(*lane->current), status, connection_clean);
            lane->current.reset();
            lane->session.reset();
            lane->state = LaneState::COMPLETING;
            if (!connection_clean || group->state != GroupState::OPEN)
                socket_to_close = std::move(lane->socket);
        }
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    if (stale) {
        invokeLaneObserverHook(kLaneLateHandler, 0, 0, 0, false);
    }
#endif
    if (stale) return;

    // A dirty protocol stream must be closed before terminal Slice status is
    // visible to the caller.
    closeSocketNoThrow(socket_to_close);
    completeTerminalAction(std::move(*action));

    uint64_t pump_epoch = 0;
    std::shared_ptr<asio::ip::tcp::socket> shutdown_socket;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    size_t queue_depth = 0;
    uint64_t queued_bytes = 0;
    size_t active_sockets = 0;
#endif
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->state != GroupState::OPEN ||
            lane->operation_epoch != epoch) {
            lane->state = LaneState::CLOSING;
            shutdown_socket = std::move(lane->socket);
        } else if (connection_clean && lane->socket &&
                   lane->socket->is_open()) {
            lane->state = LaneState::IDLE;
        } else {
            lane->socket.reset();
            lane->state = LaneState::DISCONNECTED;
            if (!group->queue.empty() && group->probes_in_flight == 0 &&
                !hasUsableLaneLocked(*group) &&
                group->connect_round_had_success) {
                beginConnectRoundLocked(*group);
            }
        }
        pump_epoch = requestGroupPumpLocked(*group);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        queue_depth = group->queue.size();
        queued_bytes = group->queued_bytes;
        active_sockets = activeSocketCountLocked(*group);
#endif
    }
    closeSocketNoThrow(shutdown_socket);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    invokeLaneObserverHook(kLaneTerminal, queue_depth, queued_bytes,
                           active_sockets, false);
#endif
    if (pump_epoch != 0) postGroupPump(group, pump_epoch);
}

void TcpTransport::completeTerminalAction(TerminalAction action) noexcept {
    try {
        if (action.status == TransferStatusEnum::COMPLETED)
            action.work.slice->markSuccess();
        else
            action.work.slice->markFailed();
    } catch (const std::exception& e) {
        LOG(ERROR) << "TCP Slice terminal completion threw: " << e.what();
    } catch (...) {
        LOG(ERROR) << "TCP Slice terminal completion threw";
    }
}

uint64_t TcpTransport::recordWorkFailure(
    WorkFailureReason reason,
    const std::shared_ptr<FailureCounters>& counters) noexcept {
    if (!counters) return 0;

    std::atomic<uint64_t>* counter = nullptr;
    switch (reason) {
        case WorkFailureReason::QUEUE_FULL:
            counter = &counters->queue_full;
            break;
        case WorkFailureReason::RUNTIME_UNAVAILABLE:
            counter = &counters->runtime_unavailable;
            break;
        case WorkFailureReason::CONNECT_FAILED:
            counter = &counters->connect_failed;
            break;
        case WorkFailureReason::SESSION_FAILED:
            counter = &counters->session_failed;
            break;
        case WorkFailureReason::SHUTDOWN:
            counter = &counters->shutdown;
            break;
    }
    if (!counter) return 0;

    const uint64_t occurrence =
        counter->fetch_add(1, std::memory_order_relaxed) + 1;
    if (reason == WorkFailureReason::QUEUE_FULL &&
        shouldLogOccurrence(occurrence)) {
        LOG(WARNING) << "TCP lane queue-full rejection count: " << occurrence;
    }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    invokeLaneFailureReasonHook(static_cast<int>(reason));
#endif
    return occurrence;
}

void TcpTransport::failWorkItem(
    TcpWorkItem work, WorkFailureReason reason,
    const std::shared_ptr<FailureCounters>& counters) noexcept {
    recordWorkFailure(reason, counters);
    completeTerminalAction(
        TerminalAction(std::move(work), TransferStatusEnum::FAILED, false));
}

void TcpTransport::failWorkItems(
    std::deque<TcpWorkItem> work, WorkFailureReason reason,
    const std::shared_ptr<FailureCounters>& counters) noexcept {
    while (!work.empty()) {
        auto item = std::move(work.front());
        work.pop_front();
        failWorkItem(std::move(item), reason, counters);
    }
}

void TcpTransport::closeSocketNoThrow(
    const std::shared_ptr<asio::ip::tcp::socket>& socket) noexcept {
    if (!socket) return;
    asio::error_code error;
    socket->cancel(error);
    socket->close(error);
}

void TcpTransport::shutdownConnectionLanes() {
    const auto state = lane_state_;
    std::vector<std::shared_ptr<PeerConnectionGroup>> groups;
    {
        std::lock_guard<std::mutex> state_lock(state->mutex);
        if (state->shutting_down) return;
        state->shutting_down = true;
        groups.reserve(state->groups.size());
        for (const auto& entry : state->groups) groups.push_back(entry.second);
    }

    for (const auto& group : groups) {
        std::deque<TcpWorkItem> queued;
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            group->state = GroupState::CLOSING;
            group->pump_scheduled = false;
            ++group->pump_epoch;
            queued.swap(group->queue);
            clearQueuedBytesLocked(*group);
            ++group->retry_epoch;
            if (group->retry_epoch == 0) ++group->retry_epoch;
            for (const auto& lane : group->lanes) {
                ++lane->operation_epoch;
                if (lane->operation_epoch == 0) ++lane->operation_epoch;
                if (lane->state != LaneState::CLOSED)
                    lane->state = LaneState::CLOSING;
            }
        }
        failWorkItems(std::move(queued), WorkFailureReason::SHUTDOWN,
                      state->failure_counters);
    }

    auto cancellation_posts = std::make_shared<LaneCancellationPostTracker>();
    if (context_ && running_) {
        for (const auto& group : groups) {
            cancellation_posts->add();
            try {
                asio::post(group->executor, [group, cancellation_posts] {
                    std::vector<std::shared_ptr<ClientSession>> sessions;
                    std::vector<std::shared_ptr<asio::ip::tcp::resolver>>
                        resolvers;
                    std::vector<std::shared_ptr<asio::ip::tcp::socket>> sockets;
                    std::shared_ptr<asio::steady_timer> retry_timer;
                    {
                        std::lock_guard<std::mutex> lock(group->mutex);
                        retry_timer = group->retry_timer;
                        for (const auto& lane : group->lanes) {
                            if (lane->session)
                                sessions.push_back(lane->session);
                            if (lane->resolver)
                                resolvers.push_back(lane->resolver);
                            if (lane->socket) sockets.push_back(lane->socket);
                        }
                    }
                    for (const auto& session : sessions)
                        if (session) session->cancel();
                    for (const auto& resolver : resolvers) {
                        if (!resolver) continue;
                        try {
                            resolver->cancel();
                        } catch (...) {
                        }
                    }
                    for (const auto& socket : sockets)
                        closeSocketNoThrow(socket);
                    if (retry_timer) {
                        asio::error_code timer_ec;
                        retry_timer->cancel(timer_ec);
                    }
                    cancellation_posts->done();
                });
            } catch (...) {
                cancellation_posts->done();
            }
        }
        cancellation_posts->waitUntil(std::chrono::steady_clock::now() +
                                      kShutdownCancellationWait);
    }

    running_ = false;
    if (context_) context_->io_context.stop();
    if (thread_.joinable()) thread_.join();

    std::deque<TcpWorkItem> deferred;
    std::vector<std::shared_ptr<ClientSession>> sessions;
    std::vector<std::shared_ptr<asio::ip::tcp::resolver>> resolvers;
    std::vector<std::shared_ptr<asio::ip::tcp::socket>> sockets;
    std::vector<std::shared_ptr<asio::steady_timer>> retry_timers;

    for (const auto& group : groups) {
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            if (group->retry_timer)
                retry_timers.push_back(std::move(group->retry_timer));
            for (const auto& lane : group->lanes) {
                if (lane->current) {
                    deferred.emplace_back(std::move(*lane->current));
                    lane->current.reset();
                }
                if (lane->session) sessions.push_back(std::move(lane->session));
                if (lane->resolver)
                    resolvers.push_back(std::move(lane->resolver));
                if (lane->socket) sockets.push_back(std::move(lane->socket));
                lane->connect_stage = LaneConnectStage::NONE;
                lane->state = LaneState::CLOSED;
            }
            group->probes_in_flight = 0;
            group->state = GroupState::CLOSED;
        }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        invokeLaneObserverHook(kLaneShutdownClean, 0, 0, 0, false);
#endif
    }

    // No handler is running after join. Reset every Asio-owning field while
    // TcpContext and its execution_context are still alive, then publish
    // terminal failure for work that had been BUSY.
    for (const auto& session : sessions)
        if (session) session->cancel();
    for (const auto& resolver : resolvers) {
        if (!resolver) continue;
        try {
            resolver->cancel();
        } catch (...) {
        }
    }
    for (const auto& socket : sockets) closeSocketNoThrow(socket);
    for (const auto& timer : retry_timers) {
        if (!timer) continue;
        asio::error_code timer_ec;
        timer->cancel(timer_ec);
    }
    sessions.clear();
    resolvers.clear();
    sockets.clear();
    retry_timers.clear();

    failWorkItems(std::move(deferred), WorkFailureReason::SHUTDOWN,
                  state->failure_counters);

    const auto& counters = state->failure_counters;
    VLOG(1) << "TCP lane failure totals: queue_full="
            << counters->queue_full.load(std::memory_order_relaxed)
            << ", connect_failed="
            << counters->connect_failed.load(std::memory_order_relaxed)
            << ", runtime_unavailable="
            << counters->runtime_unavailable.load(std::memory_order_relaxed)
            << ", session_failed="
            << counters->session_failed.load(std::memory_order_relaxed)
            << ", shutdown="
            << counters->shutdown.load(std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> state_lock(state->mutex);
        state->groups.clear();
        state->runtime.reset();
    }
    groups.clear();
    lane_runtime_.reset();
}

bool TcpTransport::validateAddress(uint64_t addr, uint64_t size) const {
    return validateTcpAddress(metadata_, addr, size);
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

    // Zero-length requests are complete by definition. v1 reported them
    // COMPLETED while the server silently rejected size==0 in address
    // validation; preserve that outcome without a round trip.
    if (slice->length == 0) {
        slice->markSuccess();
        return;
    }

    const ConnectionKey key{meta_entry.ip_or_host_name,
                            static_cast<uint16_t>(desc->tcp_data_port)};
    const bool use_v2 = desc->tcp_proto_version >= 2 && !forceLegacyTcpProto();

    if (enable_connection_pool_) {
        enqueuePooledTransfer(key, TcpWorkItem(slice, use_v2));
        return;
    }

    // Preserve the connection-pool-disabled synchronous connection path.
    auto socket = getConnection(key.host, key.port);
    if (!socket) {
        LOG(ERROR) << "TcpTransport::startTransfer failed to get connection to "
                   << key.host << ":" << key.port;
        slice->markFailed();
        return;
    }
    startTransferWithSocket(slice, use_v2, std::move(socket));
}

void TcpTransport::startTransferWithSocket(
    Slice* slice, bool use_v2,
    std::shared_ptr<asio::ip::tcp::socket> socket) noexcept {
    try {
        auto session = std::make_shared<ClientSession>(
            socket, use_v2,
            [slice, use_v2, socket](TransferStatusEnum status, bool) noexcept {
                closeSocketNoThrow(socket);
                completeTerminalAction(
                    TerminalAction(TcpWorkItem(slice, use_v2), status, false));
            });
        session->initiate(slice->source_addr, slice->tcp.dest_addr,
                          slice->length, slice->opcode);
    } catch (const std::exception& e) {
        LOG(ERROR) << "TcpTransport::startTransfer encountered an exception. "
                      "Slice details - source_addr: "
                   << slice->source_addr << ", length: " << slice->length
                   << ", opcode: " << (int)slice->opcode
                   << ", target_id: " << slice->target_id
                   << ". Exception: " << e.what();
        closeSocketNoThrow(socket);
        failWorkItem(TcpWorkItem(slice, use_v2),
                     WorkFailureReason::SESSION_FAILED,
                     lane_state_->failure_counters);
    } catch (...) {
        LOG(ERROR) << "TcpTransport::startTransfer encountered an unknown "
                      "exception. Slice details - source_addr: "
                   << slice->source_addr << ", length: " << slice->length
                   << ", opcode: " << (int)slice->opcode
                   << ", target_id: " << slice->target_id;
        closeSocketNoThrow(socket);
        failWorkItem(TcpWorkItem(slice, use_v2),
                     WorkFailureReason::SESSION_FAILED,
                     lane_state_->failure_counters);
    }
}

}  // namespace mooncake
