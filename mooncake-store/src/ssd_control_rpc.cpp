// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_control_rpc.h"
#include "ssd_pipeline_io.h"
#include <ylt/struct_pack.hpp>
#include <ylt/util/utils.hpp>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <deque>
#include <mutex>
#include <utility>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <array>
#include <climits>

namespace mooncake::ssd_offload {
SsdReadIntoRpcEndpoint::SsdReadIntoRpcEndpoint(
    std::shared_ptr<SsdDirectReadOwner> owner, SsdReadProtocolLimits limits,
    size_t threads, size_t outstanding)
    : owner_(std::move(owner)),
      limits_(limits),
      workers_(std::make_unique<BoundedExecutor>(threads, outstanding)) {}
SsdReadIntoRpcEndpoint::~SsdReadIntoRpcEndpoint() { CloseAndDrain(); }
void SsdReadIntoRpcEndpoint::CloseAndDrain() {
    owner_->Close();
    workers_->Shutdown();
}
void SsdReadIntoRpcEndpoint::Call(SsdDirectRpcContext context,
                                  std::string_view bytes) {
    const auto received = std::chrono::steady_clock::now();
    SsdControlMessage message;
    const auto decoded = DecodeSsdControlMessage(bytes, message, limits_);
    auto respond = [limits = limits_](SsdDirectRpcContext context,
                                      SsdControlReply reply) {
        auto encoded = EncodeSsdControlReply(reply, limits);
        if (encoded.error != DestinationError::kOk) {
            reply.error = encoded.error;
            reply.snapshot.reset();
            reply.destinations.clear();
            encoded = EncodeSsdControlReply(reply, limits);
        }
        context.response_msg(std::move(encoded.bytes));
    };
    if (decoded != DestinationError::kOk) {
        respond(std::move(context), {message.command, decoded, {}, {}});
        return;
    }
    if (message.command == SsdControlCommand::kRecoverReadInto) {
        // Recovery only reads/fences ledger metadata; a blocked SSD worker
        // must not prevent a caller from discovering the original task.
        respond(std::move(context), owner_->Execute(message, received));
        return;
    }
    if (message.command != SsdControlCommand::kReadInto) {
        respond(std::move(context),
                {message.command, DestinationError::kNotSupported, {}, {}});
        return;
    }
    struct Work {
        SsdDirectRpcContext context;
        SsdControlMessage message;
    };
    ++read_requests_;
    auto work = std::make_shared<Work>(Work{context, std::move(message)});
    if (!workers_->TrySubmit([owner = owner_, work, received, respond] {
            SsdControlReply reply{work->message.command,
                                  DestinationError::kTransportError,
                                  {},
                                  {}};
            try {
                reply = owner->Execute(work->message, received);
            } catch (...) {
            }
            // context serializes/copies the owned reply and queues its send
            // onto the live RPC connection. The worker never blocks the I/O
            // loop.
            respond(std::move(work->context), std::move(reply));
        }))
        respond(
            std::move(context),
            {SsdControlCommand::kReadInto, DestinationError::kLimit, {}, {}});
}

DestinationError ValidateSsdControlRpcRequestHeader(uint32_t body,
                                                    uint32_t attachment) {
    return body <= kSsdControlMaxRpcBody && !attachment
               ? DestinationError::kOk
               : DestinationError::kLimit;
}
SsdControlRpcEndpoint::SsdControlRpcEndpoint(
    std::shared_ptr<SsdRequesterGrants> grants,
    std::shared_ptr<SsdOwnerRequestLedger> ledger, Worker worker,
    SsdReadProtocolLimits limits, size_t worker_threads, size_t max_outstanding)
    : grants_(std::move(grants)),
      ledger_(std::move(ledger)),
      worker_(std::move(worker)),
      limits_(limits) {
    if (ledger_ && worker_)
        workers_ =
            std::make_unique<BoundedExecutor>(worker_threads, max_outstanding);
}
SsdControlRpcEndpoint::~SsdControlRpcEndpoint() {
    if (workers_) workers_->Shutdown();
}
std::string SsdControlRpcEndpoint::Call(std::string_view bytes) {
    const auto received_at = std::chrono::steady_clock::now();
    SsdControlMessage message;
    SsdControlReply reply;
    // Preserve a recognized command in syntax-error responses. No decoded
    // fields, addresses, or terminal state are trusted from a partial message.
    if (bytes.size() >= 12) {
        uint32_t c = 0;
        for (size_t i = 0; i < 4; ++i)
            c |= uint32_t(static_cast<unsigned char>(bytes[8 + i])) << (8 * i);
        if (c >= 1 && c <= 4) reply.command = SsdControlCommand(c);
    }
    try {
        reply.error = DecodeSsdControlMessage(bytes, message, limits_);
        if (reply.error == DestinationError::kOk) {
            reply.command = message.command;
            switch (message.command) {
                case SsdControlCommand::kResolve:
                    if (!grants_) {
                        reply.error = DestinationError::kNotSupported;
                        break;
                    }
                    {
                        auto resolved = grants_->Resolve(message.reference,
                                                         *message.request);
                        reply.error = resolved.error;
                        reply.destinations = std::move(resolved.destinations);
                    }
                    break;
                case SsdControlCommand::kSubmit:
                    if (!ledger_ || !worker_) {
                        reply.error = DestinationError::kNotSupported;
                        break;
                    }
                    {
                        auto admitted = ledger_->Admit(
                            *message.request, message.reference, received_at);
                        reply.error = admitted.error;
                        if (admitted.error == DestinationError::kOk) {
                            struct Work {
                                SsdReadRequestV1 request;
                                SsdGrantReferenceV1 reference;
                                SsdOwnerRequestLedger::Operation operation;
                            };
                            auto work = std::make_shared<Work>();
                            work->request = *message.request;
                            work->reference = message.reference;
                            work->operation = std::move(admitted.operation);
                            if (!workers_->TrySubmit([this, work] {
                                    try {
                                        worker_(work->request, work->reference,
                                                std::move(work->operation));
                                    } catch (...) {
                                        // Worker handle destruction
                                        // quarantines; an exception is not a
                                        // physical drain.
                                    }
                                })) {
                                // TrySubmit(false) accepted no worker and no
                                // source/TE submission for this admission.
                                SsdReadResultV1 rejected{
                                    1,
                                    message.reference.attempt,
                                    true,
                                    {},
                                    message.reference.requester};
                                for (size_t i = 0;
                                     i < message.request->objects.size(); ++i)
                                    rejected.objects.push_back(
                                        {i, AttemptOutcome::kRejected, 0, {}});
                                reply.error =
                                    work->operation.Complete(rejected);
                            }
                        }
                        if (reply.error == DestinationError::kOk ||
                            reply.error == DestinationError::kDuplicate) {
                            auto state = ledger_->Query(message.reference);
                            if (state.error == DestinationError::kOk)
                                reply.snapshot = std::move(state);
                            else
                                reply.error = state.error;
                        }
                    }
                    break;
                case SsdControlCommand::kQuery:
                    if (!ledger_) {
                        reply.error = DestinationError::kNotSupported;
                        break;
                    }
                    {
                        auto state = ledger_->Query(message.reference);
                        reply.error = state.error;
                        if (state.error == DestinationError::kOk)
                            reply.snapshot = std::move(state);
                    }
                    break;
                case SsdControlCommand::kForget:
                    reply.error = ledger_ ? ledger_->Forget(message.reference)
                                          : DestinationError::kNotSupported;
                    break;
                case SsdControlCommand::kReadInto:
                case SsdControlCommand::kRecoverReadInto:
                    reply.error = DestinationError::kNotSupported;
                    break;
            }
        }
    } catch (...) {
        // Abandoned owner handles quarantine their record. No network or
        // allocation exception is turned into a fabricated drain result.
        reply.error = DestinationError::kTransportError;
        reply.snapshot.reset();
        reply.destinations.clear();
    }
    auto encoded = EncodeSsdControlReply(reply, limits_);
    if (encoded.error != DestinationError::kOk) {
        reply.error = encoded.error;
        reply.snapshot.reset();
        reply.destinations.clear();
        encoded = EncodeSsdControlReply(reply, limits_);
    }
    return std::move(encoded.bytes);
}
namespace {
using Clock = std::chrono::steady_clock;
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
// Transport cache scoped to one Store requester, independent of worker-thread
// lifetime and membership. Only idle, completely consumed RPC streams are
// retained. Evicting an idle socket never rejects or limits an Owner read.
class ControlConnectionPool {
    struct Idle {
        std::string host;
        uint16_t port;
        Clock::time_point since;
        Socket socket;
    };

   public:
    Socket Take(const std::string& host, uint16_t port) {
        std::lock_guard lock(mutex_);
        const auto now = Clock::now();
        for (auto it = idle_.begin(); it != idle_.end();) {
            if (now - it->since >= std::chrono::seconds(30))
                it = idle_.erase(it);
            else
                ++it;
        }
        for (auto it = idle_.begin(); it != idle_.end(); ++it) {
            if (it->host != host || it->port != port) continue;
            auto socket = std::move(it->socket);
            idle_.erase(it);
            char byte;
            ssize_t n;
            do {
                n = recv(socket.fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
            } while (n < 0 && errno == EINTR);
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) &&
                socket.sequence != UINT32_MAX)
                return socket;
            // Closed, desynchronized, or sequence-exhausted: discard before
            // submitting this request. A failure after submission is NEVER
            // retried here and instead follows the existing recovery path.
            return {};
        }
        return {};
    }
    void Keep(const std::string& host, uint16_t port, Socket socket) noexcept {
        if (socket.fd < 0 || socket.sequence == UINT32_MAX) return;
        try {
            std::lock_guard lock(mutex_);
            if (idle_.size() == 128) idle_.pop_front();
            idle_.push_back({host, port, Clock::now(), std::move(socket)});
        } catch (...) {
            // A cache-allocation failure closes only this idle connection.
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
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                             deadline - now)
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
                         Clock::time_point deadline) {
    size_t position = 0;
    while (position < size) {
        auto error = Wait(fd, write ? POLLOUT : POLLIN, deadline);
        if (error) return error;
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
        else if (count == 0)
            return std::make_error_code(std::errc::connection_reset);
        else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
            return {errno, std::generic_category()};
    }
    return {};
}
std::error_code Connect(Socket& socket, const std::string& host, uint16_t port,
                        Clock::time_point deadline) {
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
        const int flags = fcntl(candidate.fd, F_GETFL);
        if (flags < 0 || fcntl(candidate.fd, F_SETFL, flags | O_NONBLOCK) < 0 ||
            fcntl(candidate.fd, F_SETFD, FD_CLOEXEC) < 0) {
            result = {errno, std::generic_category()};
            continue;
        }
        int rc = connect(candidate.fd, address->ai_addr, address->ai_addrlen);
        if (rc < 0 && errno != EINPROGRESS) {
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
SsdControlRpcCall CallOnSocket(Socket& socket, const std::string& host,
                               uint16_t port, const SsdControlMessage& message,
                               std::chrono::milliseconds timeout,
                               SsdReadProtocolLimits limits) {
    SsdControlRpcCall call;
    if (host.empty() || host.size() > 4096 ||
        host.find('\0') != std::string::npos || !port ||
        timeout <= std::chrono::milliseconds::zero() ||
        timeout > std::chrono::seconds(60))
        return call;
    auto encoded = EncodeSsdControlMessage(message, limits);
    call.error = encoded.error;
    if (encoded.error != DestinationError::kOk) return call;
    const auto deadline = Clock::now() + timeout;
    if (socket.fd < 0)
        call.transport_error = Connect(socket, host, port, deadline);
    if (call.transport_error) {
        call.error = DestinationError::kTransportError;
        return call;
    }
    // Exactly one string_view argument, matching coro_rpc's regular wire frame.
    auto body =
        struct_pack::serialize<std::string>(std::string_view(encoded.bytes));
    if (body.size() > kSsdControlMaxRpcBody) {
        call.error = DestinationError::kLimit;
        return call;
    }
    using Protocol = coro_rpc::protocol::coro_rpc_protocol;
    Protocol::req_header request{};
    request.magic = Protocol::magic_number;
    request.seq_num = ++socket.sequence;
    request.function_id =
        message.command == SsdControlCommand::kReadInto ||
                message.command == SsdControlCommand::kRecoverReadInto
            ? coro_rpc::func_id<&SsdReadIntoRpcEndpoint::Call>()
            : coro_rpc::func_id<&SsdControlRpcEndpoint::Call>();
    request.length = uint32_t(body.size());
    auto head =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO,
                               std::string>(request);
    call.transport_error =
        Transfer(socket.fd, head.data(), head.size(), true, deadline);
    if (!call.transport_error)
        call.transport_error =
            Transfer(socket.fd, body.data(), body.size(), true, deadline);
    std::array<char, Protocol::RESP_HEAD_LEN> response_head{};
    if (!call.transport_error)
        call.transport_error = Transfer(socket.fd, response_head.data(),
                                        response_head.size(), false, deadline);
    if (call.transport_error) {
        call.error = DestinationError::kTransportError;
        return call;
    }
    Protocol::resp_header response{};
    const auto decoded_head = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        response, std::string_view(response_head.data(), response_head.size()));
    if (decoded_head || response.magic != Protocol::magic_number ||
        response.version != Protocol::VERSION_NUMBER ||
        response.seq_num != request.seq_num || response.msg_type ||
        response.err_code) {
        call.error = DestinationError::kInvalidArgument;
        return call;
    }
    // Check hostile peer length/attachments BEFORE creating a response buffer.
    if (response.length > kSsdControlMaxRpcBody || response.attach_length) {
        call.error = DestinationError::kLimit;
        return call;
    }
    std::string response_body(response.length, '\0');
    call.transport_error = Transfer(socket.fd, response_body.data(),
                                    response_body.size(), false, deadline);
    if (call.transport_error) {
        call.error = DestinationError::kTransportError;
        return call;
    }
    auto payload = struct_pack::deserialize<std::string_view>(response_body);
    if (!payload.has_value()) {
        call.error = DestinationError::kInvalidArgument;
        return call;
    }
    SsdControlReply reply;
    call.error = DecodeSsdControlReply(*payload, reply, limits);
    if (call.error != DestinationError::kOk) return call;
    if (reply.command != message.command) {
        call.error = DestinationError::kWrongAttempt;
        return call;
    }
    if (reply.snapshot && reply.snapshot->result &&
        (reply.snapshot->result->requester != message.reference.requester ||
         reply.snapshot->result->attempt != message.reference.attempt)) {
        call.error = DestinationError::kWrongAttempt;
        return call;
    }
    if (reply.command == SsdControlCommand::kResolve &&
        reply.error == DestinationError::kOk) {
        size_t index = 0;
        for (const auto& object : message.request->objects)
            for (const auto& range : object.ranges)
                for (const auto& destination : range.destinations) {
                    if (index >= reply.destinations.size() ||
                        reply.destinations[index++].length !=
                            destination.length) {
                        call.error = DestinationError::kInvalidArgument;
                        return call;
                    }
                }
        if (index != reply.destinations.size()) {
            call.error = DestinationError::kInvalidArgument;
            return call;
        }
    }
    call.reply = std::move(reply);
    return call;
}
}  // namespace
SsdControlRpcCall CallSsdControlRpc(const std::string& host, uint16_t port,
                                    const SsdControlMessage& message,
                                    std::chrono::milliseconds timeout,
                                    SsdReadProtocolLimits limits) {
    Socket socket;
    return CallOnSocket(socket, host, port, message, timeout, limits);
}
SsdRequesterControlCall MakeSsdRequesterControlCall(
    SsdReadProtocolLimits limits) {
    auto pool = std::make_shared<ControlConnectionPool>();
    return [limits, pool](const auto& host, auto port, const auto& message,
                          auto timeout) {
        auto socket = pool->Take(host, port);
        auto call = CallOnSocket(socket, host, port, message, timeout, limits);
        if (call.error == DestinationError::kOk)
            pool->Keep(host, port, std::move(socket));
        return SsdRequesterCallResult{call.error, std::move(call.reply)};
    };
}
}  // namespace mooncake::ssd_offload
