// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_control_rpc.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <openssl/crypto.h>
#include <openssl/hmac.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <climits>
#include <cstring>
#include <deque>
#include <mutex>
#include <utility>
#include <ylt/struct_pack.hpp>
#include <ylt/util/utils.hpp>

#include "ssd_peer_directory.h"
#include "ssd_pipeline_io.h"

namespace mooncake::ssd_offload {
namespace {

using Clock = std::chrono::steady_clock;

constexpr std::array<char, 4> kEnvelopeMagic{'M', 'C', 'A', '1'};
constexpr size_t kHmacBytes = 32;
// Non-secret sentinel carried in the serialized wire grant field so the wire
// codec's non-zero credential check succeeds without ever placing raw secret
// key material on the network.
constexpr WriteCredential kWireAuthPlaceholder{0x484d41435f534947ULL, 1ULL};

void PutLe64(char* dst, uint64_t v) {
    for (size_t i = 0; i < 8; ++i)
        dst[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
}

uint64_t GetLe64(const char* src) {
    uint64_t v = 0;
    for (size_t i = 0; i < 8; ++i)
        v |= uint64_t(static_cast<unsigned char>(src[i])) << (8 * i);
    return v;
}

void EncodeIdentity(char* dst, PeerIdentity id) {
    PutLe64(dst, id.client_id_high);
    PutLe64(dst + 8, id.client_id_low);
    PutLe64(dst + 16, id.boot_epoch);
}

PeerIdentity DecodeIdentity(const char* src) {
    return {GetLe64(src), GetLe64(src + 8), GetLe64(src + 16)};
}

bool ComputeHmac(WriteCredential cred, std::string_view domain,
                 PeerIdentity requester, std::string_view binding,
                 std::string_view payload,
                 std::array<unsigned char, kHmacBytes>& out) {
    if (cred == WriteCredential{}) return false;
    unsigned char key[16];
    PutLe64(reinterpret_cast<char*>(key), cred.high);
    PutLe64(reinterpret_cast<char*>(key + 8), cred.low);
    char id_bytes[24];
    EncodeIdentity(id_bytes, requester);

    HMAC_CTX* ctx = HMAC_CTX_new();
    if (!ctx) {
        OPENSSL_cleanse(key, sizeof(key));
        return false;
    }
    unsigned int len = 0;
    const bool ok =
        HMAC_Init_ex(ctx, key, sizeof(key), EVP_sha256(), nullptr) == 1 &&
        HMAC_Update(ctx, reinterpret_cast<const unsigned char*>(domain.data()),
                    domain.size()) == 1 &&
        HMAC_Update(ctx, reinterpret_cast<const unsigned char*>(id_bytes),
                    sizeof(id_bytes)) == 1 &&
        (binding.empty() ||
         HMAC_Update(ctx,
                     reinterpret_cast<const unsigned char*>(binding.data()),
                     binding.size()) == 1) &&
        HMAC_Update(ctx, reinterpret_cast<const unsigned char*>(payload.data()),
                    payload.size()) == 1 &&
        HMAC_Final(ctx, out.data(), &len) == 1 && len == kHmacBytes;
    HMAC_CTX_free(ctx);
    OPENSSL_cleanse(key, sizeof(key));
    return ok;
}

bool ConstantTimeEqual(const unsigned char* a, const unsigned char* b,
                       size_t n) {
    return CRYPTO_memcmp(a, b, n) == 0;
}

std::string WrapReply(const SsdControlReply& reply,
                      SsdReadProtocolLimits limits,
                      std::optional<WriteCredential> cred,
                      PeerIdentity requester,
                      const std::array<unsigned char, kHmacBytes>& req_tag) {
    auto encoded = EncodeSsdControlReply(reply, limits);
    if (encoded.error != DestinationError::kOk) {
        SsdControlReply fallback{reply.command, encoded.error, {}};
        encoded = EncodeSsdControlReply(fallback, limits);
    }
    if (!cred) {
        // Unauthenticated error reply: zero tag; client treats non-matching
        // HMAC as kUnauthorized and never trusts any terminal state.
        std::string frame;
        frame.reserve(kSsdControlReplyAuthHeaderBytes + encoded.bytes.size());
        frame.append(kEnvelopeMagic.data(), kEnvelopeMagic.size());
        frame.append(kHmacBytes, '\0');
        frame.append(encoded.bytes);
        return frame;
    }
    std::array<unsigned char, kHmacBytes> tag{};
    const std::string_view req_tag_view(
        reinterpret_cast<const char*>(req_tag.data()), req_tag.size());
    if (!ComputeHmac(*cred, "MCREP1", requester, req_tag_view, encoded.bytes,
                     tag)) {
        return {};
    }
    std::string frame;
    frame.reserve(kSsdControlReplyAuthHeaderBytes + encoded.bytes.size());
    frame.append(kEnvelopeMagic.data(), kEnvelopeMagic.size());
    frame.append(reinterpret_cast<const char*>(tag.data()), tag.size());
    frame.append(encoded.bytes);
    return frame;
}

bool IsWildcardAddress(const std::string& host) {
    return !IsNonWildcardNumericHost(host);
}

struct Socket {
    int fd{-1};
    uint32_t sequence{0};
    Socket() = default;
    explicit Socket(int descriptor) : fd(descriptor) {}
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept
        : fd(std::exchange(other.fd, -1)), sequence(other.sequence) {}
    Socket& operator=(Socket&& other) noexcept {
        if (this != &other) {
            if (fd >= 0) close(fd);
            fd = std::exchange(other.fd, -1);
            sequence = other.sequence;
        }
        return *this;
    }
    ~Socket() {
        if (fd >= 0) close(fd);
    }
};

// LIFO connection cache scoped to one requester. Skips stale/closed idle
// sockets and keeps only completely consumed RPC streams.
class ControlConnectionPool {
    struct Idle {
        std::string host;
        uint16_t port;
        Clock::time_point since;
        Socket socket;
    };

   public:
    Socket Take(const std::string& host, uint16_t port) {
        for (;;) {
            Socket candidate;
            {
                std::lock_guard lock(mutex_);
                const auto now = Clock::now();
                for (auto it = idle_.begin(); it != idle_.end();) {
                    if (now - it->since >= std::chrono::seconds(30))
                        it = idle_.erase(it);
                    else
                        ++it;
                }
                for (auto it = idle_.rbegin(); it != idle_.rend(); ++it) {
                    if (it->host != host || it->port != port) continue;
                    candidate = std::move(it->socket);
                    idle_.erase(std::next(it).base());
                    break;
                }
            }
            if (candidate.fd < 0) return {};
            char byte = 0;
            ssize_t n = 0;
            do {
                n = recv(candidate.fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
            } while (n < 0 && errno == EINTR);
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) &&
                candidate.sequence != UINT32_MAX) {
                return candidate;
            }
            // Stale, closed, or sequence-exhausted socket: discard and check
            // whether another healthy idle connection exists.
        }
    }

    void Keep(const std::string& host, uint16_t port, Socket socket) noexcept {
        if (socket.fd < 0 || socket.sequence == UINT32_MAX) return;
        try {
            std::lock_guard lock(mutex_);
            if (idle_.size() == 128) idle_.pop_front();
            idle_.push_back({host, port, Clock::now(), std::move(socket)});
        } catch (...) {
        }
    }

   private:
    std::mutex mutex_;
    std::deque<Idle> idle_;
};

std::error_code Wait(int fd, short event, Clock::time_point deadline) {
    for (;;) {
        const auto now = Clock::now();
        if (now >= deadline) return std::make_error_code(std::errc::timed_out);
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                  now)
                .count();
        pollfd descriptor{fd, event, 0};
        const int ready = poll(
            &descriptor, 1,
            int(std::min<int64_t>(INT_MAX, std::max<int64_t>(1, remaining))));
        if (ready > 0) return {};
        if (ready == 0) return std::make_error_code(std::errc::timed_out);
        if (errno != EINTR) return {errno, std::generic_category()};
    }
}

std::error_code Transfer(int fd, char* buffer, size_t size, bool write,
                         Clock::time_point deadline,
                         size_t* bytes_transferred = nullptr) {
    size_t position = 0;
    while (position < size) {
        if (Clock::now() >= deadline) {
            if (bytes_transferred) *bytes_transferred = position;
            return std::make_error_code(std::errc::timed_out);
        }
        const ssize_t count =
            write ? send(fd, buffer + position, size - position,
#ifdef MSG_NOSIGNAL
                         MSG_NOSIGNAL
#else
                         0
#endif
                         )
                  : recv(fd, buffer + position, size - position, 0);
        if (count > 0)
            position += size_t(count);
        else if (count == 0) {
            if (bytes_transferred) *bytes_transferred = position;
            return std::make_error_code(std::errc::connection_reset);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            auto error = Wait(fd, write ? POLLOUT : POLLIN, deadline);
            if (error) {
                if (bytes_transferred) *bytes_transferred = position;
                return error;
            }
        } else if (errno != EINTR) {
            if (bytes_transferred) *bytes_transferred = position;
            return {errno, std::generic_category()};
        }
    }
    if (bytes_transferred) *bytes_transferred = position;
    return {};
}

std::error_code Connect(Socket& socket, const std::string& host, uint16_t port,
                        Clock::time_point deadline) {
    if (IsWildcardAddress(host)) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    addrinfo hint{}, *addresses = nullptr;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_family = AF_UNSPEC;
    hint.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
    const auto service = std::to_string(port);
    if (getaddrinfo(host.c_str(), service.c_str(), &hint, &addresses) != 0)
        return std::make_error_code(std::errc::invalid_argument);
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> cleanup(addresses,
                                                               freeaddrinfo);
    auto result = std::make_error_code(std::errc::host_unreachable);
    for (auto* address = addresses; address; address = address->ai_next) {
        Socket candidate{::socket(address->ai_family, address->ai_socktype,
                                  address->ai_protocol)};
        if (candidate.fd < 0) {
            result = {errno, std::generic_category()};
            continue;
        }
        const int no_delay = 1;
        if (setsockopt(candidate.fd, IPPROTO_TCP, TCP_NODELAY, &no_delay,
                       sizeof(no_delay)) != 0) {
            result = {errno, std::generic_category()};
            continue;
        }
#ifdef SO_NOSIGPIPE
        const int no_sigpipe = 1;
        setsockopt(candidate.fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
                   sizeof(no_sigpipe));
#endif
        const int flags = fcntl(candidate.fd, F_GETFL);
        if (flags < 0 || fcntl(candidate.fd, F_SETFL, flags | O_NONBLOCK) < 0 ||
            fcntl(candidate.fd, F_SETFD, FD_CLOEXEC) < 0) {
            result = {errno, std::generic_category()};
            continue;
        }
        int rc = connect(candidate.fd, address->ai_addr, address->ai_addrlen);
        if (rc < 0 && errno != EINPROGRESS && errno != EINTR) {
            result = {errno, std::generic_category()};
            continue;
        }
        if (rc < 0) {
            result = Wait(candidate.fd, POLLOUT, deadline);
            if (result) {
                if (result == std::errc::timed_out) return result;
                continue;
            }
            int error = 0;
            socklen_t size = sizeof(error);
            if (getsockopt(candidate.fd, SOL_SOCKET, SO_ERROR, &error, &size) !=
                0)
                error = errno;
            if (error) {
                result = {error, std::generic_category()};
                continue;
            }
        }
        socket.fd = candidate.fd;
        candidate.fd = -1;
        return {};
    }
    return result;
}

SsdRequesterCallResult CallOnSocket(Socket& socket, const std::string& host,
                                    uint16_t port,
                                    const SsdControlMessage& message,
                                    std::chrono::milliseconds timeout,
                                    SsdReadProtocolLimits limits,
                                    bool* wrote_any = nullptr) {
    if (wrote_any) *wrote_any = false;
    SsdRequesterCallResult out{DestinationError::kInvalidArgument, {}};
    if ((message.command != SsdControlCommand::kReadInto &&
         message.command != SsdControlCommand::kRecoverReadInto) ||
        host.empty() || host.size() > 4096 ||
        host.find('\0') != std::string::npos || !port ||
        timeout <= std::chrono::milliseconds::zero() ||
        timeout > std::chrono::seconds(60) ||
        message.reference.credential == WriteCredential{}) {
        return out;
    }

    // Strip the raw secret from the serialized wire payload and authenticate
    // with per-frame HMAC-SHA256 instead.
    SsdControlMessage wire_message = message;
    const WriteCredential secret = message.reference.credential;
    const PeerIdentity requester = message.reference.requester;
    wire_message.reference.credential = kWireAuthPlaceholder;

    auto encoded = EncodeSsdControlMessage(wire_message, limits);
    if (encoded.error != DestinationError::kOk) {
        out.error = encoded.error;
        return out;
    }

    std::array<unsigned char, kHmacBytes> req_tag{};
    if (!ComputeHmac(secret, "MCREQ1", requester, {}, encoded.bytes, req_tag)) {
        out.error = DestinationError::kUnauthorized;
        return out;
    }

    std::string envelope;
    envelope.reserve(kSsdControlAuthHeaderBytes + encoded.bytes.size());
    envelope.append(kEnvelopeMagic.data(), kEnvelopeMagic.size());
    char id_bytes[24];
    EncodeIdentity(id_bytes, requester);
    envelope.append(id_bytes, sizeof(id_bytes));
    envelope.append(reinterpret_cast<const char*>(req_tag.data()),
                    req_tag.size());
    envelope.append(encoded.bytes);

    auto body = struct_pack::serialize<std::string>(std::string_view(envelope));
    if (body.size() > kSsdControlMaxRpcBody) {
        out.error = DestinationError::kLimit;
        return out;
    }

    const auto deadline = Clock::now() + timeout;
    std::error_code io_err;
    if (socket.fd < 0) io_err = Connect(socket, host, port, deadline);
    if (io_err) {
        out.error = DestinationError::kTransportError;
        return out;
    }

    using Protocol = coro_rpc::protocol::coro_rpc_protocol;
    Protocol::req_header request{};
    request.magic = Protocol::magic_number;
    request.seq_num = ++socket.sequence;
    request.function_id = coro_rpc::func_id<&SsdReadIntoRpcEndpoint::Call>();
    request.length = uint32_t(body.size());
    auto head =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO,
                               std::string>(request);
    head.append(body);
    size_t sent_bytes = 0;
    io_err = Transfer(socket.fd, head.data(), head.size(), true, deadline,
                      &sent_bytes);
    if (wrote_any) *wrote_any = (sent_bytes > 0);
    std::array<char, Protocol::RESP_HEAD_LEN> response_head{};
    if (!io_err) {
        io_err = Transfer(socket.fd, response_head.data(), response_head.size(),
                          false, deadline);
    }
    if (io_err) {
        out.error = DestinationError::kTransportError;
        return out;
    }

    Protocol::resp_header response{};
    const auto decoded_head = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        response, std::string_view(response_head.data(), response_head.size()));
    if (decoded_head || response.magic != Protocol::magic_number ||
        response.version != Protocol::VERSION_NUMBER ||
        response.seq_num != request.seq_num || response.msg_type ||
        response.err_code) {
        out.error = DestinationError::kInvalidArgument;
        return out;
    }
    if (response.length > kSsdControlMaxRpcBody || response.attach_length) {
        out.error = DestinationError::kLimit;
        return out;
    }

    std::string response_body(response.length, '\0');
    io_err = Transfer(socket.fd, response_body.data(), response_body.size(),
                      false, deadline);
    if (io_err) {
        out.error = DestinationError::kTransportError;
        return out;
    }

    auto payload = struct_pack::deserialize<std::string_view>(response_body);
    if (!payload.has_value() ||
        payload->size() < kSsdControlReplyAuthHeaderBytes ||
        std::memcmp(payload->data(), kEnvelopeMagic.data(),
                    kEnvelopeMagic.size()) != 0) {
        out.error = DestinationError::kUnauthorized;
        return out;
    }

    const auto* reply_tag =
        reinterpret_cast<const unsigned char*>(payload->data() + 4);
    const std::string_view reply_wire =
        payload->substr(kSsdControlReplyAuthHeaderBytes);
    std::array<unsigned char, kHmacBytes> expected_reply_tag{};
    const std::string_view req_tag_view(
        reinterpret_cast<const char*>(req_tag.data()), req_tag.size());
    if (!ComputeHmac(secret, "MCREP1", requester, req_tag_view, reply_wire,
                     expected_reply_tag) ||
        !ConstantTimeEqual(reply_tag, expected_reply_tag.data(), kHmacBytes)) {
        out.error = DestinationError::kUnauthorized;
        return out;
    }

    SsdControlReply reply;
    out.error = DecodeSsdControlReply(reply_wire, reply, limits);
    if (out.error != DestinationError::kOk) return out;
    if (reply.command != message.command) {
        out.error = DestinationError::kWrongAttempt;
        return out;
    }
    if (reply.snapshot && reply.snapshot->result &&
        (reply.snapshot->result->requester != message.reference.requester ||
         reply.snapshot->result->attempt != message.reference.attempt)) {
        out.error = DestinationError::kWrongAttempt;
        return out;
    }
    out.reply = std::move(reply);
    return out;
}

}  // namespace

DestinationError ValidateSsdControlRpcRequestHeader(uint32_t body,
                                                    uint32_t attachment) {
    return body <= kSsdControlMaxRpcBody && !attachment
               ? DestinationError::kOk
               : DestinationError::kLimit;
}

SsdReadIntoRpcEndpoint::SsdReadIntoRpcEndpoint(
    std::shared_ptr<SsdDirectReadOwner> owner, SsdReadProtocolLimits limits,
    size_t threads, size_t outstanding)
    : owner_(std::move(owner)),
      limits_(limits),
      workers_(std::make_unique<BoundedExecutor>(threads, outstanding)),
      recovery_workers_(std::make_unique<BoundedExecutor>(
          2, std::max<size_t>(outstanding, 32))) {
    if (!owner_) {
        throw std::invalid_argument("SsdReadIntoRpcEndpoint requires owner");
    }
}

SsdReadIntoRpcEndpoint::~SsdReadIntoRpcEndpoint() { CloseAndDrain(); }

void SsdReadIntoRpcEndpoint::CloseAndDrain() {
    if (owner_) owner_->Close();
    if (workers_) workers_->Shutdown();
    if (recovery_workers_) recovery_workers_->Shutdown();
}

void SsdReadIntoRpcEndpoint::Call(SsdDirectRpcContext context,
                                  std::string_view frame) {
    const auto received = Clock::now();
    auto send_unauth = [limits = limits_](SsdDirectRpcContext ctx,
                                          SsdControlCommand cmd,
                                          DestinationError err) {
        const std::array<unsigned char, kHmacBytes> zero_tag{};
        ctx.response_msg(
            WrapReply({cmd, err, {}}, limits, std::nullopt, {}, zero_tag));
    };

    try {
        if (!owner_ || frame.size() < kSsdControlAuthHeaderBytes ||
            std::memcmp(frame.data(), kEnvelopeMagic.data(),
                        kEnvelopeMagic.size()) != 0) {
            send_unauth(std::move(context), SsdControlCommand::kReadInto,
                        DestinationError::kUnauthorized);
            return;
        }

        const PeerIdentity claimed_requester = DecodeIdentity(frame.data() + 4);
        std::array<unsigned char, kHmacBytes> claimed_tag{};
        std::memcpy(claimed_tag.data(), frame.data() + 28, kHmacBytes);
        const std::string_view wire_body =
            frame.substr(kSsdControlAuthHeaderBytes);

        // Quick command peek before dispatching onto read vs recovery pool.
        // The wire header is [magic:4][version:4][command:4].
        SsdControlCommand hinted_command = SsdControlCommand::kReadInto;
        if (wire_body.size() >= 12) {
            uint32_t raw_cmd = 0;
            for (size_t i = 0; i < 4; ++i)
                raw_cmd |=
                    uint32_t(static_cast<unsigned char>(wire_body[8 + i]))
                    << (8 * i);
            if (raw_cmd == uint32_t(SsdControlCommand::kRecoverReadInto)) {
                hinted_command = SsdControlCommand::kRecoverReadInto;
            } else if (raw_cmd != uint32_t(SsdControlCommand::kReadInto)) {
                send_unauth(std::move(context), SsdControlCommand::kReadInto,
                            DestinationError::kNotSupported);
                return;
            }
        }

        struct Work {
            SsdDirectRpcContext context;
            PeerIdentity claimed_requester;
            std::array<unsigned char, kHmacBytes> claimed_tag;
            std::string wire_body;
        };
        auto work = std::make_shared<Work>(Work{
            context, claimed_requester, claimed_tag, std::string(wire_body)});

        const bool is_recovery =
            hinted_command == SsdControlCommand::kRecoverReadInto;
        auto& pool = is_recovery ? *recovery_workers_ : *workers_;

        auto task = [this, owner = owner_, limits = limits_, work, received,
                     hinted_command] {
            std::optional<WriteCredential> cred;
            SsdControlReply reply{
                hinted_command, DestinationError::kUnauthorized, {}};
            try {
                cred = owner->LookupCredential(work->claimed_requester);
                std::array<unsigned char, kHmacBytes> expected_tag{};
                if (!cred ||
                    !ComputeHmac(*cred, "MCREQ1", work->claimed_requester, {},
                                 work->wire_body, expected_tag) ||
                    !ConstantTimeEqual(work->claimed_tag.data(),
                                       expected_tag.data(), kHmacBytes)) {
                    cred.reset();
                    reply.error = DestinationError::kUnauthorized;
                } else {
                    SsdControlMessage message;
                    const auto decoded = DecodeSsdControlMessage(
                        work->wire_body, message, limits);
                    if (decoded != DestinationError::kOk) {
                        reply.error = decoded;
                    } else if (message.command !=
                                   SsdControlCommand::kReadInto &&
                               message.command !=
                                   SsdControlCommand::kRecoverReadInto) {
                        reply.command = message.command;
                        reply.error = DestinationError::kNotSupported;
                    } else if (message.reference.requester !=
                                   work->claimed_requester ||
                               message.reference.credential !=
                                   kWireAuthPlaceholder) {
                        reply.command = message.command;
                        reply.error = DestinationError::kUnauthorized;
                    } else {
                        if (message.command == SsdControlCommand::kReadInto) {
                            read_requests_.fetch_add(1,
                                                     std::memory_order_relaxed);
                        }
                        // Restore the authenticated shared credential in memory
                        // for SsdDirectReadOwner::Execute.
                        message.reference.credential = *cred;
                        reply = owner->Execute(message, received);
                    }
                }
            } catch (...) {
                reply.error = DestinationError::kTransportError;
                reply.snapshot.reset();
            }
            work->context.response_msg(WrapReply(reply, limits, cred,
                                                 work->claimed_requester,
                                                 work->claimed_tag));
        };

        if (!pool.TrySubmit(std::move(task))) {
            // If the requester is already cached in memory and authenticated,
            // return a signed kLimit reply so the requester distinguishes
            // backpressure from kUnauthorized and can fall back cleanly.
            std::optional<WriteCredential> cached =
                owner_->LookupCachedCredential(claimed_requester);
            if (cached) {
                std::array<unsigned char, kHmacBytes> expected_tag{};
                if (!ComputeHmac(*cached, "MCREQ1", claimed_requester, {},
                                 wire_body, expected_tag) ||
                    !ConstantTimeEqual(claimed_tag.data(), expected_tag.data(),
                                       kHmacBytes)) {
                    cached.reset();
                }
            }
            context.response_msg(
                WrapReply({hinted_command, DestinationError::kLimit, {}},
                          limits_, cached, claimed_requester, claimed_tag));
        }
    } catch (...) {
        send_unauth(std::move(context), SsdControlCommand::kReadInto,
                    DestinationError::kTransportError);
    }
}

SsdRequesterControlCall MakeSsdRequesterControlCall(
    SsdReadProtocolLimits limits) {
    auto pool = std::make_shared<ControlConnectionPool>();
    return [limits, pool](const std::string& host, uint16_t port,
                          const SsdControlMessage& message,
                          std::chrono::milliseconds timeout) {
        const auto start = Clock::now();
        auto socket = pool->Take(host, port);
        const bool from_pool = (socket.fd >= 0);
        bool wrote_any = false;
        auto call = CallOnSocket(socket, host, port, message, timeout, limits,
                                 &wrote_any);
        if (from_pool && !wrote_any &&
            call.error == DestinationError::kTransportError) {
            // Stale pooled socket closed before a single byte was written;
            // retry once on a fresh connection within the remaining timeout.
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    Clock::now() - start);
            const auto remaining = timeout - elapsed;
            if (remaining > std::chrono::milliseconds::zero()) {
                socket = Socket{};
                call = CallOnSocket(socket, host, port, message, remaining,
                                    limits, &wrote_any);
            }
        }
        if (call.error == DestinationError::kOk) {
            pool->Keep(host, port, std::move(socket));
        }
        return call;
    };
}

}  // namespace mooncake::ssd_offload
