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
#include <asio/steady_timer.hpp>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <random>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

#include "cuda_alike.h"
#include "environ.h"

namespace mooncake {
using tcpsocket = asio::ip::tcp::socket;
static size_t getChunkSize() {
    static const size_t val = [] {
        std::string env = Environ::Get().GetTcpSliceSize();
        if (!env.empty()) {
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
    return Environ::Get().GetTcpProto() == "1";
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
    explicit ClientSession(std::shared_ptr<tcpsocket> socket, bool use_v2,
                           std::function<void(bool)> on_complete = nullptr)
        : socket_(std::move(socket)),
          v2_(use_v2),
          on_complete_(std::move(on_complete)) {}

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
        std::string env = Environ::Get().GetTcpStatusTimeoutSec();
        if (!env.empty()) {
            int v = std::atoi(env.c_str());
            if (v > 0) return v;
        }
        return 30;
    }
    std::optional<asio::steady_timer> status_timer_;
    bool status_deadline_disarmed_ = false;
    std::function<void(TransferStatusEnum)> on_finalize_;
    // Invoked exactly once per request with clean=true iff the protocol
    // exchange terminated in a well-defined connection state. A socket whose
    // request did not end cleanly must not be reused: the server-side session
    // may be mid-frame, and the next request's header would be consumed as
    // body bytes.
    std::function<void(bool)> on_complete_;

    void initiate(void* buffer, uint64_t dest_addr, size_t size,
                  TransferRequest::OpCode opcode) {
        local_buffer_ = (char*)buffer;
        header_.addr = htole64(dest_addr);
        header_.size = htole64(size);
        header_.opcode = (uint8_t)opcode | (v2_ ? kOpcodeV2Flag : 0);
        total_transferred_bytes_ = 0;
        writeHeader();
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
        if (status_timer_) status_timer_->cancel();
    }

    // Single terminal path: finish connection ownership, then report the
    // outcome. Posted so it runs after the invoking callback returns.
    void finalize(TransferStatusEnum status, bool clean) {
        cancelStatusDeadline();
        auto self(shared_from_this());
        asio::post(
            socket_->get_executor(),
            [this, self, status, clean, on_finalize = std::move(on_finalize_),
             on_complete = std::move(on_complete_)]() {
                // Finish connection ownership before publishing terminal
                // status. Once on_finalize marks the slice, the caller may
                // immediately free the batch or destroy the transport.
                if (on_complete) on_complete(clean);
                if (on_finalize) on_finalize(status);
            });
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

TcpTransport::TcpTransport() : context_(nullptr), running_(false) {
    std::string val = Environ::Get().GetTcpEnableConnectionPool();
    if (!val.empty()) {
        std::transform(val.begin(), val.end(), val.begin(),
                       [](unsigned char c) -> char { return std::tolower(c); });
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
    context_ = new TcpContext(tcp_port, [this](uint64_t addr, uint64_t size) {
        return validateAddress(addr, size);
    });
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
    for (auto& buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int TcpTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    int first_error = 0;
    for (auto& addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
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
    if (!enable_connection_pool_) {
        try {
            asio::ip::tcp::resolver resolver(context_->io_context);
            auto endpoint_iterator =
                resolver.resolve(host, std::to_string(port));
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
        new_socket->set_option(asio::ip::tcp::no_delay(true));
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

        for (auto entry_it = queue.begin(); entry_it != queue.end();) {
            auto& entry = *entry_it;
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
                    entry_it = queue.erase(entry_it);
                    continue;
                }
            }
            ++entry_it;
        }

        if (queue.empty()) {
            it = connection_pool_.erase(it);
        } else {
            ++it;
        }
    }
}

bool TcpTransport::validateAddress(uint64_t addr, uint64_t size) const {
    if (size == 0) return false;
    if (addr + size < addr) return false;

    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!desc) return false;

    for (const auto& buffer : desc->buffers) {
        if (buffer.addr + buffer.length < buffer.addr) continue;
        if (buffer.addr <= addr && addr + size <= buffer.addr + buffer.length)
            return true;
    }
    return false;
}

void TcpTransport::discardConnection(
    const std::string& host, uint16_t port,
    std::shared_ptr<asio::ip::tcp::socket> socket) {
    if (socket && socket->is_open()) {
        asio::error_code ec;
        socket->close(ec);
    }
    std::lock_guard<std::mutex> lock(pool_mutex_);
    auto it = connection_pool_.find(ConnectionKey{host, port});
    if (it != connection_pool_.end()) {
        auto& queue = it->second;
        for (auto queue_it = queue.begin(); queue_it != queue.end();
             ++queue_it) {
            if ((*queue_it)->socket == socket) {
                queue.erase(queue_it);
                break;
            }
        }
        if (queue.empty()) connection_pool_.erase(it);
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

    // Zero-length requests are complete by definition. v1 reported them
    // COMPLETED while the server silently rejected size==0 in address
    // validation; short-circuiting keeps that outcome (rather than turning
    // no-ops into v2 rejection failures) without the pointless round trip.
    if (slice->length == 0) {
        slice->markSuccess();
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
        const bool use_v2 =
            desc->tcp_proto_version >= 2 && !forceLegacyTcpProto();
        auto session = std::make_shared<ClientSession>(socket, use_v2);

        session->on_finalize_ = [slice](TransferStatusEnum status) {
            if (status == TransferStatusEnum::COMPLETED)
                slice->markSuccess();
            else
                slice->markFailed();
        };

        // Return connection to pool when the request terminated cleanly;
        // otherwise the server-side session state is unknown (it may be
        // mid-frame), so reusing the socket would desynchronize the next
        // request. Discard it instead.
        if (enable_connection_pool_) {
            session->on_complete_ = [this, host = meta_entry.ip_or_host_name,
                                     port = desc->tcp_data_port,
                                     socket](bool clean) {
                if (clean)
                    returnConnection(host, port, socket);
                else
                    discardConnection(host, port, socket);
            };
        } else {
            session->on_complete_ = [socket](bool) {
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
        // On exception, always close the socket and remove from pool if
        // present. Don't return it to the pool as it may be in an
        // inconsistent state.
        discardConnection(meta_entry.ip_or_host_name,
                          static_cast<uint16_t>(desc->tcp_data_port), socket);
        slice->markFailed();
    }
}

}  // namespace mooncake
