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

// Private implementation fragment included by tcp_transport.cpp from inside
// namespace mooncake. Keeping the protocol/session state here makes the public
// transport entry points and the lane scheduler independently reviewable
// without changing linkage or lifetime behavior.

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

static constexpr size_t kCpuTransferWindow = 256 * 1024;
static constexpr size_t kCpuStreamingMinSize = 128 * 1024;

static bool cpuStreamingEnabled() {
    // Preserve explicit chunk sizing, including its validation/fallback path.
    static const bool enabled = std::getenv("MC_TCP_SLICE_SIZE") == nullptr;
    return enabled;
}

// Connection-level ownership, not per-piece Store state. Retirement closes
// every accepted socket and waits until its final memory-using handler drops
// the session. The stopped executors themselves stay alive for late submitters.
struct TcpServerSessions {
    std::mutex mutex;
    std::condition_variable cv;
    bool closed = false;
    std::atomic<bool> require_strict_writes{false};
    size_t active = 0;
    std::vector<std::weak_ptr<tcpsocket>> sockets;

    bool attach(const std::shared_ptr<tcpsocket>& socket) {
        std::lock_guard<std::mutex> lock(mutex);
        if (closed) return false;
        sockets.erase(
            std::remove_if(sockets.begin(), sockets.end(),
                           [](const auto& value) { return value.expired(); }),
            sockets.end());
        sockets.push_back(socket);
        ++active;
        return true;
    }

    void detach() {
        std::lock_guard<std::mutex> lock(mutex);
        --active;
        if (active == 0) cv.notify_all();
    }

    bool isClosed() {
        std::lock_guard<std::mutex> lock(mutex);
        return closed;
    }

    void retireAndDrain() {
        std::vector<std::shared_ptr<tcpsocket>> live;
        {
            std::lock_guard<std::mutex> lock(mutex);
            live.reserve(sockets.size());
            for (const auto& value : sockets)
                if (auto socket = value.lock())
                    live.push_back(std::move(socket));
            closed = true;
        }
        for (const auto& socket : live) {
            asio::post(socket->get_executor(), [socket] {
                asio::error_code error;
                socket->cancel(error);
                socket->close(error);
            });
        }
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [this] { return active == 0; });
    }
};

struct SessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t opcode;
};

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
// Returns the CUDA device ordinal if addr is device memory, or -1 otherwise.
// Callers must call cudaSetDevice before any cudaMemcpy to avoid implicit
// GPU 0 context creation.
static int getCudaDeviceId(void* addr) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    recordStagingDeviceQueryForTest();
#endif
    cudaPointerAttributes attributes;
    auto status = cudaPointerGetAttributes(&attributes, addr);
    if (status != cudaSuccess) return -1;
    if (attributes.type == cudaMemoryTypeDevice) return attributes.device;
    return -1;
}
#endif

#ifdef MOONCAKE_TCP_ASCEND
static int getAscendDeviceId(void* addr) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    recordStagingDeviceQueryForTest();
#endif
    aclrtPtrAttributes attributes{};
    if (aclrtPointerGetAttributes(addr, &attributes) != ACL_SUCCESS ||
        attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE)
        return -1;
    // ACL returns a user device ID, as accepted by aclrtSetDevice.
    return static_cast<int>(attributes.location.id);
}

static aclError copyTcpAscendMemory(void* dst, const void* src, size_t size,
                                    int device, aclrtMemcpyKind kind) {
    auto status = aclrtSetDevice(device);
    if (status != ACL_SUCCESS) return status;
    return aclrtMemcpy(dst, size, src, size, kind);
}
#endif

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA) || defined(MOONCAKE_TCP_ASCEND)
class TcpStagingBuffer {
   public:
    char* ensure(size_t size) {
        if (buffer_.size() < size) {
            buffer_.resize(size);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            recordStagingBufferAllocationForTest();
#endif
        }
        return buffer_.data();
    }

   private:
    std::vector<char> buffer_;
};
#endif

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
// A connection may carry both ordinary and strict work. Keep strict intent on
// every WRITE so ordinary work cannot borrow a prior Hello on a pooled socket.
static constexpr uint8_t kOpcodeStrictWriteFlag = 0x40;
static constexpr uint8_t kOpcodeFlags = kOpcodeV2Flag | kOpcodeStrictWriteFlag;
// A zero-address/zero-length control request cannot read or write memory on an
// old server. New peers prove the expected listener incarnation before data.
static constexpr uint8_t kStrictHelloOpcode = 0xfe;
static constexpr size_t kTcpInstanceIdSize = 32;
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
                           ValidateAddrFn validate_addr,
                           std::string instance_id,
                           std::shared_ptr<TcpServerSessions> sessions)
        : socket_(std::move(socket)),
          validate_addr_(std::move(validate_addr)),
          instance_id_(std::move(instance_id)),
          sessions_(std::move(sessions)) {}

    ~ServerSession() {
        if (registered_) sessions_->detach();
    }

    std::shared_ptr<tcpsocket> socket_;
    ValidateAddrFn validate_addr_;
    std::string instance_id_;
    std::shared_ptr<TcpServerSessions> sessions_;
    bool registered_ = false;
    bool peer_instance_verified_ = false;
    std::array<char, kTcpInstanceIdSize> expected_instance_{};
    std::array<char, sizeof(uint64_t) + kTcpInstanceIdSize> hello_reply_{};
    SessionHeader header_;
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    bool stream_cpu_ = false;
    bool v2_ = false;
    uint64_t status_frame_;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
    int cuda_device_ = -1;
#endif
#ifdef MOONCAKE_TCP_ASCEND
    int ascend_device_ = -1;
#endif
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA) || defined(MOONCAKE_TCP_ASCEND)
    TcpStagingBuffer staging_buffer_;
#endif

    void start() {
        total_transferred_bytes_ = 0;
        readHeader();
    }

   private:
    void readStrictHello() {
        auto self(shared_from_this());
        asio::async_read(
            *socket_, asio::buffer(expected_instance_),
            [this, self](const asio::error_code& ec, size_t length) {
                if (ec || length != expected_instance_.size() ||
                    instance_id_.size() != expected_instance_.size() ||
                    !std::equal(expected_instance_.begin(),
                                expected_instance_.end(), instance_id_.begin()))
                    return;
                peer_instance_verified_ = true;
                const uint64_t status = htole64(kStatusOk);
                std::memcpy(hello_reply_.data(), &status, sizeof(status));
                std::copy(instance_id_.begin(), instance_id_.end(),
                          hello_reply_.begin() + sizeof(status));
                asio::async_write(
                    *socket_, asio::buffer(hello_reply_),
                    [this, self](const asio::error_code& error, size_t) {
                        if (!error) start();
                    });
            });
    }

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

                if (header_.opcode == kStrictHelloOpcode) {
                    if (header_.addr == 0 && header_.size == 0)
                        readStrictHello();
                    return;
                }
                v2_ = (header_.opcode & kOpcodeV2Flag) != 0;
                const uint8_t opcode = header_.opcode & ~kOpcodeFlags;
                const bool strict_write =
                    (header_.opcode & kOpcodeStrictWriteFlag) != 0;
                const bool write = opcode == (uint8_t)TransferRequest::WRITE;
                // Strict framing always needs this connection's Hello. Bound
                // targets also reject ordinary work on a previously verified
                // pooled connection: address validity alone is not a grant.
                if ((strict_write &&
                     (!write || !v2_ || !peer_instance_verified_)) ||
                    (write && !strict_write &&
                     sessions_->require_strict_writes.load(
                         std::memory_order_acquire))) {
                    if (v2_) sendStatus(kStatusAddrRejected, nullptr);
                    return;
                }
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
                stream_cpu_ =
                    size >= kCpuStreamingMinSize && cpuStreamingEnabled();
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
                cuda_device_ = getCudaDeviceId(local_buffer_);
                stream_cpu_ = stream_cpu_ && cuda_device_ < 0;
#endif
#ifdef MOONCAKE_TCP_ASCEND
                ascend_device_ = getAscendDeviceId(local_buffer_);
                stream_cpu_ = stream_cpu_ && ascend_device_ < 0;
#endif
                if (opcode == (uint8_t)TransferRequest::WRITE) {
                    readBody();
                } else if (v2_) {
                    if (stream_cpu_ && size != 0) {
                        writeBody(true);
                    } else {
                        sendStatus(kStatusOk, [this] { writeBody(); });
                    }
                } else {
                    writeBody();
                }
            });
    }

    void writeBody(bool include_status = false) {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size = size - total_transferred_bytes_;
        if (!stream_cpu_) buffer_size = std::min(getChunkSize(), buffer_size);
        if (buffer_size == 0) {
            // Transfer complete, wait for next request on this connection
            start();
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
        if (cuda_device_ >= 0) {
            dram_buffer = staging_buffer_.ensure(buffer_size);
            cudaSetDevice(cuda_device_);
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
                return;  // Connection will be closed
            }
        }
#endif

#ifdef MOONCAKE_TCP_ASCEND
        if (ascend_device_ >= 0) {
            dram_buffer = staging_buffer_.ensure(buffer_size);
            auto status = copyTcpAscendMemory(
                dram_buffer, addr + total_transferred_bytes_, buffer_size,
                ascend_device_, ACL_MEMCPY_DEVICE_TO_HOST);
            if (status != ACL_SUCCESS) {
                LOG(ERROR) << "TCP Ascend D2H copy failed: " << status;
                return;
            }
        }
#endif

        const size_t status_bytes = include_status ? sizeof(status_frame_) : 0;
        if (include_status) status_frame_ = htole64(kStatusOk);
        const std::array<asio::const_buffer, 2> buffers{
            asio::buffer(&status_frame_, status_bytes),
            asio::buffer(dram_buffer, buffer_size)};
        asio::async_write(
            *socket_, buffers,
            [this, addr, dram_buffer, self, status_bytes](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                if (ec) {
                    LOG(ERROR)
                        << "ServerSession::writeBody failed. "
                        << "Attempt to write data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    return;  // Connection will be closed
                }
                total_transferred_bytes_ += transferred_bytes - status_bytes;
                writeBody();
            });
    }

    void readBody() {
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(stream_cpu_ ? kCpuTransferWindow : getChunkSize(),
                     size - total_transferred_bytes_);
        if (buffer_size == 0) {
            // Destination memory now holds the complete payload. Under v2,
            // acknowledge before accepting the next request — this is what
            // makes the initiator's COMPLETED mean "applied at the
            // destination" rather than "left my socket buffer".
            if (v2_) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                (void)invokeSessionProgressHook(kSessionServerWriteAckReady,
                                                false);
#endif
                sendStatus(kStatusOk, [this] { start(); });
            } else {
                start();
            }
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
        if (cuda_device_ >= 0) {
            dram_buffer = staging_buffer_.ensure(buffer_size);
        }
#endif

#ifdef MOONCAKE_TCP_ASCEND
        if (ascend_device_ >= 0)
            dram_buffer = staging_buffer_.ensure(buffer_size);
#endif

        asio::async_read(
            *socket_, asio::buffer(dram_buffer, buffer_size),
            [buffer_size, stream_cpu = stream_cpu_](
                const asio::error_code& ec, std::size_t transferred_bytes) {
                // CPU reads can fill the bounded window in one socket read;
                // keep the original completion condition for staged buffers.
                return stream_cpu && !ec
                           ? buffer_size - transferred_bytes
                           : asio::transfer_all()(ec, transferred_bytes);
            },
            [this, addr, dram_buffer, self](const asio::error_code& ec,
                                            std::size_t transferred_bytes) {
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
                    return;  // Connection will be closed
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
                if (cuda_device_ >= 0) {
                    cudaSetDevice(cuda_device_);
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
                        return;  // Connection will be closed
                    }
                }
#endif
#ifdef MOONCAKE_TCP_ASCEND
                if (ascend_device_ >= 0) {
                    auto status = copyTcpAscendMemory(
                        addr + total_transferred_bytes_, dram_buffer,
                        transferred_bytes, ascend_device_,
                        ACL_MEMCPY_HOST_TO_DEVICE);
                    if (status != ACL_SUCCESS) {
                        LOG(ERROR) << "TCP Ascend H2D copy failed: " << status;
                        return;
                    }
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

    explicit ClientSession(
        std::shared_ptr<tcpsocket> socket, bool use_v2, OnTerminal on_terminal,
        std::shared_ptr<const Transport::StrictWriteRequirement> strict_write =
            nullptr,
        bool peer_verified = false,
        std::function<void()> on_peer_verified = nullptr)
        : socket_(std::move(socket)),
          v2_(use_v2),
          strict_write_(std::move(strict_write)),
          peer_verified_(peer_verified),
          on_peer_verified_(std::move(on_peer_verified)),
          on_terminal_(std::move(on_terminal)) {}

    std::shared_ptr<tcpsocket> socket_;
    SessionHeader header_;
    uint64_t total_transferred_bytes_;
    char* local_buffer_;
    bool stream_cpu_ = false;
    bool v2_;
    std::shared_ptr<const Transport::StrictWriteRequirement> strict_write_;
    bool peer_verified_ = false;
    std::function<void()> on_peer_verified_;
    SessionHeader hello_header_{};
    std::array<char, sizeof(uint64_t) + kTcpInstanceIdSize> hello_reply_{};
    uint64_t status_frame_;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
    int cuda_device_ = -1;
#endif
#ifdef MOONCAKE_TCP_ASCEND
    int ascend_device_ = -1;
#endif
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA) || defined(MOONCAKE_TCP_ASCEND)
    TcpStagingBuffer staging_buffer_;
#endif
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
    // READ receives borrow the caller's target buffer until their completion
    // handler runs, including after cancellation caused by another handler.
    bool read_body_in_flight_ = false;
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
    uint64_t status_epoch_ = 0;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    uint64_t last_status_arm_epoch_for_test_ = 0;
#endif
    // Unlike the status-frame deadline above, this is a sliding liveness
    // deadline for the whole request. It is refreshed only after an Asio
    // completion reports actual protocol or payload progress.
    static int progressTimeoutSec() {
        const char* env = std::getenv("MC_TCP_PROGRESS_TIMEOUT_SEC");
        if (env) {
            int v = std::atoi(env);
            if (v > 0) return v;
        }
        return 30;
    }
    std::optional<asio::steady_timer> progress_timer_;
    uint64_t progress_epoch_ = 0;
    bool progress_deadline_disarmed_ = true;
    bool progress_timeout_committed_ = false;
    bool terminal_reported_ = false;
    bool cancelled_ = false;
    bool initiating_ = false;
    OnTerminal on_terminal_;

    void initiate(void* buffer, uint64_t dest_addr, size_t size,
                  TransferRequest::OpCode opcode) {
        local_buffer_ = (char*)buffer;
        stream_cpu_ = size >= kCpuStreamingMinSize && cpuStreamingEnabled();
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
        cuda_device_ = getCudaDeviceId(local_buffer_);
        stream_cpu_ = stream_cpu_ && cuda_device_ < 0;
#endif
#ifdef MOONCAKE_TCP_ASCEND
        ascend_device_ = getAscendDeviceId(local_buffer_);
        stream_cpu_ = stream_cpu_ && ascend_device_ < 0;
#endif
        header_.addr = htole64(dest_addr);
        header_.size = htole64(size);
        header_.opcode = (uint8_t)opcode | (v2_ ? kOpcodeV2Flag : 0) |
                         (strict_write_ && opcode == TransferRequest::WRITE
                              ? kOpcodeStrictWriteFlag
                              : 0);
        total_transferred_bytes_ = 0;
        initiating_ = true;
        try {
            if (strict_write_ && !peer_verified_)
                writeStrictHello();
            else
                writeHeader();
        } catch (...) {
            initiating_ = false;
            throw;
        }
        initiating_ = false;
    }

    void cancel() noexcept {
        // close() does not revoke a successful completion already queued by
        // Asio. Gate that completion before releasing the lane's source buffer.
        cancelled_ = true;
        write_abort_requested_ = true;
        cancelStatusDeadline();
        cancelProgressDeadline();
        if (!socket_) return;
        asio::error_code cancel_ec;
        socket_->cancel(cancel_ec);
        asio::error_code close_ec;
        socket_->close(close_ec);
    }

   private:
    // Each handler retains its shared session. Failures stop future starts;
    // an accepted body operation retains its borrowed buffer until its handler
    // runs, for both READ targets and WRITE sources.
    template <typename Handler>
    auto guardHandler(Handler handler) {
        return [this,
                handler = std::move(handler)](auto&&... args) mutable noexcept {
            try {
                handler(std::forward<decltype(args)>(args)...);
            } catch (...) {
                failSession();
            }
        };
    }

    void failSession() noexcept {
        cancel();
        if (!write_body_in_flight_ && !read_body_in_flight_)
            finalize(TransferStatusEnum::FAILED, false);
    }

    void writeStrictHello() {
        if (!v2_ || strict_write_->tcp_instance.size() != kTcpInstanceIdSize) {
            finalize(TransferStatusEnum::FAILED, false);
            return;
        }
        hello_header_.opcode = kStrictHelloOpcode;
        refreshProgressDeadline();
        auto self(shared_from_this());
        std::array<asio::const_buffer, 2> buffers{
            asio::buffer(&hello_header_, sizeof(hello_header_)),
            asio::buffer(strict_write_->tcp_instance)};
        asio::async_write(
            *socket_, buffers,
            guardHandler([this, self](const asio::error_code& ec, size_t) {
                if (cancelled_ || terminal_reported_) return;
                if (ec || !acceptProgress()) {
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                injectAllocationFailureForTest(
                    TcpAllocationFailurePoint::HELLO_STATUS_TIMER);
#endif
                armStatusDeadline();
                asio::async_read(
                    *socket_, asio::buffer(hello_reply_),
                    guardHandler([this, self](const asio::error_code& error,
                                              size_t length) {
                        if (cancelled_ || terminal_reported_) return;
                        cancelStatusDeadline();
                        uint64_t status = 0;
                        std::memcpy(&status, hello_reply_.data(),
                                    sizeof(status));
                        if (error || length != hello_reply_.size() ||
                            le64toh(status) != kStatusOk ||
                            !std::equal(
                                strict_write_->tcp_instance.begin(),
                                strict_write_->tcp_instance.end(),
                                hello_reply_.begin() + sizeof(status)) ||
                            !acceptProgress()) {
                            finalize(TransferStatusEnum::FAILED, false);
                            return;
                        }
                        peer_verified_ = true;
                        if (on_peer_verified_) {
                            auto verified = std::move(on_peer_verified_);
                            try {
                                verified();
                            } catch (...) {
                                finalize(TransferStatusEnum::FAILED, false);
                                return;
                            }
                        }
                        // No caller payload has been touched before here.
                        writeHeader();
                    }));
            }));
    }

    // Hello and WRITE ACK phases share this timer. cancel() cannot revoke an
    // expiry already queued with a successful result, so bind it to the arm
    // epoch before a later phase can re-arm the same timer.
    void armStatusDeadline() {
        auto self(shared_from_this());
        status_deadline_disarmed_ = false;
        ++status_epoch_;
        if (status_epoch_ == 0) ++status_epoch_;
        const uint64_t status_epoch = status_epoch_;
        status_timer_.emplace(socket_->get_executor());
        status_timer_->expires_after(
            std::chrono::seconds(statusFrameTimeoutSec()));
        status_timer_->async_wait(guardHandler(
            [this, self, status_epoch](const asio::error_code& ec) {
                handleStatusDeadline(status_epoch, ec);
            }));
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        const uint64_t previous = last_status_arm_epoch_for_test_;
        last_status_arm_epoch_for_test_ = status_epoch;
        if (invokeSessionProgressHook(kSessionStatusDeadlineArmed, false) ==
                kSessionReplayPreviousStatusAfterArm &&
            previous != 0) {
            // Exercise a successful expiry already queued before cancellation,
            // but dispatched only after the next protocol phase re-arms it.
            asio::post(socket_->get_executor(),
                       guardHandler([this, self, previous] {
                           handleStatusDeadline(previous, asio::error_code{});
                           (void)invokeSessionProgressHook(
                               kSessionStatusReplayComplete, false);
                       }));
        }
#endif
    }

    void handleStatusDeadline(uint64_t status_epoch,
                              const asio::error_code& ec) {
        if (ec == asio::error::operation_aborted || status_deadline_disarmed_ ||
            terminal_reported_ || status_epoch != status_epoch_) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            (void)invokeSessionProgressHook(kSessionStatusTimeoutStale, !ec);
#endif
            return;
        }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        (void)invokeSessionProgressHook(kSessionStatusTimeoutCommitted, false);
#endif
        LOG(ERROR) << "ClientSession: no status frame within "
                   << statusFrameTimeoutSec()
                   << "s (peer likely speaks the legacy protocol); "
                      "dropping connection";
        if (socket_ && socket_->is_open()) {
            asio::error_code cec;
            socket_->close(cec);
        }
    }

    void cancelStatusDeadline() {
        status_deadline_disarmed_ = true;
        ++status_epoch_;
        if (status_epoch_ == 0) ++status_epoch_;
        if (status_timer_) {
            try {
                status_timer_->cancel();
            } catch (const std::exception&) {
            }
        }
    }

    void refreshProgressDeadline() {
        if (terminal_reported_ || progress_timeout_committed_) return;
        auto self(shared_from_this());
        progress_deadline_disarmed_ = false;
        ++progress_epoch_;
        if (progress_epoch_ == 0) ++progress_epoch_;
        const uint64_t progress_epoch = progress_epoch_;
        if (!progress_timer_) progress_timer_.emplace(socket_->get_executor());
        progress_timer_->expires_after(
            std::chrono::seconds(progressTimeoutSec()));
        progress_timer_->async_wait(guardHandler(
            [this, self, progress_epoch](const asio::error_code& ec) {
                handleProgressDeadline(progress_epoch, ec);
            }));
    }

    void handleProgressDeadline(uint64_t progress_epoch,
                                const asio::error_code& ec) {
        if (ec == asio::error::operation_aborted ||
            progress_deadline_disarmed_ || terminal_reported_ ||
            progress_epoch != progress_epoch_) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            (void)invokeSessionProgressHook(kSessionTimeoutStale, false);
#endif
            return;
        }
        progress_timeout_committed_ = true;
        progress_deadline_disarmed_ = true;
        ++progress_epoch_;
        if (progress_epoch_ == 0) ++progress_epoch_;
        if ((header_.opcode & ~kOpcodeFlags) ==
            static_cast<uint8_t>(TransferRequest::WRITE)) {
            write_abort_requested_ = true;
        }
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        (void)invokeSessionProgressHook(kSessionTimeoutCommitted,
                                        write_body_in_flight_);
#endif
        LOG(ERROR) << "ClientSession: no transfer progress within "
                   << progressTimeoutSec() << "s; dropping connection";
        if (socket_ && socket_->is_open()) {
            asio::error_code cancel_ec;
            socket_->cancel(cancel_ec);
            asio::error_code close_ec;
            socket_->close(close_ec);
        }
    }

    bool acceptProgress() {
        if (cancelled_ || terminal_reported_ || progress_timeout_committed_)
            return false;
        try {
            refreshProgressDeadline();
        } catch (...) {
            // Completion handlers must still reach their abort/finalize path
            // if refreshing a timer cannot allocate another Asio operation.
            progress_timeout_committed_ = true;
            return false;
        }
        return !progress_timeout_committed_;
    }

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    bool acceptProgressForTest(int event) {
        const uint64_t previous_epoch = progress_epoch_;
        const int action = invokeSessionProgressHook(event, false);
        if (action == kSessionCommitTimeoutBeforeProgress)
            handleProgressDeadline(previous_epoch, asio::error_code{});
        if (!acceptProgress()) return false;
        if (action == kSessionReplayPreviousTimeoutAfterProgress)
            handleProgressDeadline(previous_epoch, asio::error_code{});
        return !progress_timeout_committed_;
    }
#endif

    void cancelProgressDeadline() {
        progress_deadline_disarmed_ = true;
        ++progress_epoch_;
        if (progress_epoch_ == 0) ++progress_epoch_;
        if (progress_timer_) {
            try {
                progress_timer_->cancel();
            } catch (const std::exception&) {
            }
        }
    }

    // Single terminal path. The invoking Asio operation has already released
    // its buffer before entering its completion handler. The lane posts any
    // follow-up pump, so a clean socket cannot be reused inline here.
    void finalize(TransferStatusEnum status, bool clean) {
        if (terminal_reported_) return;
        terminal_reported_ = true;
        cancelStatusDeadline();
        cancelProgressDeadline();
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        (void)invokeSessionProgressHook(kSessionTerminal, clean);
#endif
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
        if (cancelled_ || terminal_reported_) return;
        try {
            auto self(shared_from_this());
            asio::async_write(
                *socket_, asio::buffer(&header_, sizeof(SessionHeader)),
                guardHandler([this, self](const asio::error_code& ec,
                                          std::size_t len) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                    (void)invokeSessionProgressHook(kSessionHeaderCompletion,
                                                    !ec);
#endif
                    if (cancelled_ || terminal_reported_) return;
                    if (ec || len != sizeof(SessionHeader)) {
                        LOG(ERROR)
                            << "ClientSession::writeHeader failed. Error: "
                            << ec.message() << " (value: " << ec.value() << ")"
                            << ", bytes written: " << len;
                        finalize(TransferStatusEnum::FAILED, false);
                        return;
                    }
                    if (!acceptProgress()) {
                        if ((header_.opcode & ~kOpcodeFlags) ==
                            static_cast<uint8_t>(TransferRequest::WRITE)) {
                            abortWrite();
                        } else {
                            finalize(TransferStatusEnum::FAILED, false);
                        }
                        return;
                    }
                    if ((header_.opcode & ~kOpcodeFlags) ==
                        (uint8_t)TransferRequest::WRITE) {
                        if (v2_) readWriteAck();  // concurrent with the body
                        writeBody();
                    } else if (v2_) {
                        readReadStatus();
                    } else {
                        readBody();
                    }
                }));
            // Asio does not invoke the completion handler inline. Arm only
            // after initiation succeeds so a synchronous initiation exception
            // cannot leave a timer handler retaining this otherwise-abandoned
            // session.
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
            injectAllocationFailureForTest(
                TcpAllocationFailurePoint::HEADER_PROGRESS_TIMER);
#endif
            refreshProgressDeadline();
        } catch (...) {
            // The header operation owns session memory only. Cancel and gate
            // its late completion before the caller can release source memory.
            cancel();
            if (initiating_) throw;
            finalize(TransferStatusEnum::FAILED, false);
        }
    }

    // v2 READ: the server prefixes the data with a status frame.
    void readReadStatus() {
        auto self(shared_from_this());
        armStatusDeadline();
        asio::async_read(
            *socket_, asio::buffer(&status_frame_, sizeof(status_frame_)),
            guardHandler([this, self](const asio::error_code& ec,
                                      std::size_t len) {
                cancelStatusDeadline();
                if (ec || len != sizeof(status_frame_)) {
                    LOG(ERROR)
                        << "ClientSession: failed to read READ status "
                           "frame. Error: "
                        << ec.message() << " (value: " << ec.value() << ")";
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                if (!acceptProgress()) {
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
            }));
    }

    // v2 WRITE: completion is the server's acknowledgment that the payload
    // has been applied to destination memory. Armed concurrently with the
    // body stream; a well-behaved v2 server only sends the frame after the
    // final chunk, so a frame arriving before the body is done is either a
    // rejection or a legacy peer's payload — both close the socket immediately
    // and publish failure only after the outstanding body write is quiescent.
    void readWriteAck() {
        auto self(shared_from_this());
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        injectAllocationFailureForTest(
            TcpAllocationFailurePoint::WRITE_ACK_READ);
#endif
        asio::async_read(
            *socket_, asio::buffer(&status_frame_, sizeof(status_frame_)),
            guardHandler([this, self](const asio::error_code& ec,
                                      std::size_t len) {
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
                bool progress_accepted = false;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                progress_accepted =
                    acceptProgressForTest(kSessionWriteAckSuccess);
#else
                progress_accepted = acceptProgress();
#endif
                if (!progress_accepted) {
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
            }));
    }

    void readBody() {
        if (cancelled_ || terminal_reported_) return;
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(stream_cpu_ ? kCpuTransferWindow : getChunkSize(),
                     size - total_transferred_bytes_);
        if (buffer_size == 0) {
            finalize(TransferStatusEnum::COMPLETED, true);
            return;
        }

        char* dram_buffer = addr + total_transferred_bytes_;

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
        if (cuda_device_ >= 0) {
            dram_buffer = staging_buffer_.ensure(buffer_size);
        }
#endif

#ifdef MOONCAKE_TCP_ASCEND
        if (ascend_device_ >= 0)
            dram_buffer = staging_buffer_.ensure(buffer_size);
#endif

        auto on_read = guardHandler(
            [this, addr, dram_buffer, self](const asio::error_code& ec,
                                            std::size_t transferred_bytes) {
                read_body_in_flight_ = false;
                if (terminal_reported_) return;
                if (cancelled_) {
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                if (ec) {
                    LOG(ERROR)
                        << "ClientSession::readBody failed. "
                        << "Attempt to read data " << static_cast<void*>(addr)
                        << " using buffer " << static_cast<void*>(dram_buffer)
                        << ". Error: " << ec.message()
                        << " (value: " << ec.value() << ")";
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }

                bool progress_accepted = !progress_timeout_committed_;
                if (progress_accepted && transferred_bytes > 0) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
                    // Inject the final-receive race after all bytes arrive;
                    // partial CPU reads still refresh the sliding deadline.
                    progress_accepted =
                        total_transferred_bytes_ + transferred_bytes ==
                                le64toh(header_.size)
                            ? acceptProgressForTest(kSessionReadBodySuccess)
                            : acceptProgress();
#else
                    progress_accepted = acceptProgress();
#endif
                }
                if (!progress_accepted) {
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
                if (cuda_device_ >= 0) {
                    cudaSetDevice(cuda_device_);
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
                        finalize(TransferStatusEnum::FAILED, false);
                        return;
                    }
                }
#endif
#ifdef MOONCAKE_TCP_ASCEND
                if (ascend_device_ >= 0) {
                    auto status = copyTcpAscendMemory(
                        addr + total_transferred_bytes_, dram_buffer,
                        transferred_bytes, ascend_device_,
                        ACL_MEMCPY_HOST_TO_DEVICE);
                    if (status != ACL_SUCCESS) {
                        LOG(ERROR) << "TCP Ascend H2D copy failed: " << status;
                        finalize(TransferStatusEnum::FAILED, false);
                        return;
                    }
                }
#endif
                total_transferred_bytes_ += transferred_bytes;
                readBody();
            });
        read_body_in_flight_ = true;
        try {
            if (stream_cpu_) {
                socket_->async_read_some(asio::buffer(dram_buffer, buffer_size),
                                         std::move(on_read));
            } else {
                asio::async_read(*socket_,
                                 asio::buffer(dram_buffer, buffer_size),
                                 std::move(on_read));
            }
        } catch (...) {
            // A throwing initiating function did not accept this buffer.
            read_body_in_flight_ = false;
            throw;
        }
    }

    void writeBody() {
        if (cancelled_ || terminal_reported_ || write_abort_requested_) return;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        (void)invokeSessionProgressHook(kSessionWriteBodyStarted, false);
#endif
        auto self(shared_from_this());
        uint64_t size = le64toh(header_.size);
        char* addr = local_buffer_;

        size_t buffer_size =
            std::min(stream_cpu_ ? kCpuTransferWindow : getChunkSize(),
                     size - total_transferred_bytes_);
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

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX) || defined(USE_SUPA)
        if (cuda_device_ >= 0) {
            dram_buffer = staging_buffer_.ensure(buffer_size);
            cudaSetDevice(cuda_device_);
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
                abortWrite();
                return;
            }
        }
#endif

#ifdef MOONCAKE_TCP_ASCEND
        if (ascend_device_ >= 0) {
            dram_buffer = staging_buffer_.ensure(buffer_size);
            auto status = copyTcpAscendMemory(
                dram_buffer, addr + total_transferred_bytes_, buffer_size,
                ascend_device_, ACL_MEMCPY_DEVICE_TO_HOST);
            if (status != ACL_SUCCESS) {
                LOG(ERROR) << "TCP Ascend D2H copy failed: " << status;
                abortWrite();
                return;
            }
        }
#endif

        auto on_write = guardHandler(
            [this, addr, dram_buffer, self](const asio::error_code& ec,
                                            std::size_t transferred_bytes) {
                write_body_in_flight_ = false;
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
                    // An early ACK failure or a committed progress timeout
                    // closed the socket while this operation still owned the
                    // caller's source buffer. It is safe to publish failure
                    // now that the handler has run.
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                if (transferred_bytes > 0 && !acceptProgress()) {
                    finalize(TransferStatusEnum::FAILED, false);
                    return;
                }
                total_transferred_bytes_ += transferred_bytes;
                writeBody();
            });
        write_body_in_flight_ = true;
        try {
            if (stream_cpu_) {
                socket_->async_write_some(
                    asio::buffer(dram_buffer, buffer_size),
                    std::move(on_write));
            } else {
                asio::async_write(*socket_,
                                  asio::buffer(dram_buffer, buffer_size),
                                  std::move(on_write));
            }
        } catch (...) {
            // A throwing initiating function did not accept this buffer.
            write_body_in_flight_ = false;
            throw;
        }
    }
};

struct TcpContext {
    TcpContext(TcpIoPool& pool, short port, ValidateAddrFn validate_addr,
               std::string instance_id)
        : pool_(pool),
          io_context(pool.context(0)),
          acceptor(io_context),
          validate_addr_(std::move(validate_addr)),
          instance_id_(std::move(instance_id)),
          sessions_(std::make_shared<TcpServerSessions>()) {
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
        // Round-robin each accepted connection onto a shard so ServerSession
        // I/O spreads across io threads. The accept handler runs on shard 0
        // (the acceptor's context); the accepted socket lives on the target
        // shard, so start its session there.
        acceptor.async_accept(
            pool_.context(pool_.nextShardIndex()),
            [this](asio::error_code ec, tcpsocket socket) {
                if (!ec) {
                    asio::error_code nodelay_ec;
                    socket.set_option(asio::ip::tcp::no_delay(true),
                                      nodelay_ec);
                    auto socket_ptr =
                        std::make_shared<tcpsocket>(std::move(socket));
                    auto session = std::make_shared<ServerSession>(
                        socket_ptr, validate_addr_, instance_id_, sessions_);
                    if (sessions_->attach(socket_ptr)) {
                        session->registered_ = true;
                        asio::post(socket_ptr->get_executor(),
                                   [session] { session->start(); });
                    }
                }
                if (!sessions_->isClosed()) doAccept();
            });
    }

    TcpIoPool& pool_;
    asio::io_context& io_context;
    asio::ip::tcp::acceptor acceptor;
    ValidateAddrFn validate_addr_;
    std::string instance_id_;
    std::shared_ptr<TcpServerSessions> sessions_;

    void requireStrictWrites() {
        sessions_->require_strict_writes.store(true, std::memory_order_release);
    }

    void retireAndDrain() { sessions_->retireAndDrain(); }
};
