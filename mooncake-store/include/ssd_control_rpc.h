// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ssd_control_wire.h"
#include "ssd_direct_read.h"
#include "ssd_requester_types.h"

namespace mooncake::ssd_offload {
class BoundedExecutor;

// Envelope overhead: 4B magic + 24B requester identity + 32B HMAC-SHA256.
inline constexpr size_t kSsdControlAuthHeaderBytes = 60;
inline constexpr size_t kSsdControlReplyAuthHeaderBytes = 36;
inline constexpr size_t kSsdControlMaxRpcBody =
    kSsdControlMaxWireBytes + kSsdControlAuthHeaderBytes + 128;

DestinationError ValidateSsdControlRpcRequestHeader(uint32_t body,
                                                    uint32_t attachment);

struct SsdControlRpcProtocol : coro_rpc::protocol::coro_rpc_protocol {
    using router = coro_rpc::protocol::router<SsdControlRpcProtocol>;
    template <typename Socket, typename... Context>
    static async_simple::coro::Lazy<std::error_code> read_payload(
        Socket& socket, req_header& header, std::string& body,
        coro_io::heterogeneous_buffer& attachment, Context...) {
        // Bound hostile headers before coro_rpc resizes body/attachment.
        if (ValidateSsdControlRpcRequestHeader(
                header.length, header.attach_length) != DestinationError::kOk)
            co_return std::make_error_code(std::errc::message_size);
        co_return co_await coro_rpc::protocol::coro_rpc_protocol::read_payload(
            socket, header, body, attachment);
    }
};

struct SsdControlRpcConfig : coro_rpc::config_t {
    using rpc_protocol = SsdControlRpcProtocol;
};

using SsdControlRpcServer = coro_rpc::coro_rpc_server_base<SsdControlRpcConfig>;
using SsdDirectRpcContext =
    coro_rpc::context_base<std::string, SsdControlRpcProtocol>;

// Authenticated Fixed-L2 control RPC endpoint (kReadInto / kRecoverReadInto).
// Every request and reply carries a per-frame HMAC-SHA256 tag keyed by the
// requester's trusted WriteCredential; raw credentials are never sent in
// plaintext on the wire. Both reads and recoveries execute off the coro_rpc
// I/O loop on separate bounded executors so blocked reads never starve
// recovery and directory lookups never stall I/O threads.
class SsdReadIntoRpcEndpoint {
   public:
    explicit SsdReadIntoRpcEndpoint(std::shared_ptr<SsdDirectReadOwner> owner,
                                    SsdReadProtocolLimits limits = {},
                                    size_t worker_threads = 4,
                                    size_t max_outstanding = 64);
    ~SsdReadIntoRpcEndpoint();

    void Call(SsdDirectRpcContext context, std::string_view frame);

    // Drain while the RPC server and its I/O executor are still alive, then
    // stop the server.
    void CloseAndDrain();

    uint64_t read_requests() const { return read_requests_.load(); }

   private:
    std::shared_ptr<SsdDirectReadOwner> owner_;
    SsdReadProtocolLimits limits_;
    std::unique_ptr<BoundedExecutor> workers_;
    std::unique_ptr<BoundedExecutor> recovery_workers_;
    std::atomic<uint64_t> read_requests_{0};
};

// Production requester control-call adapter using per-frame HMAC-SHA256
// authentication over pooled nonblocking TCP sockets.
// Network, timeout, or HMAC verification failures never fabricate a terminal
// drain response.
SsdRequesterControlCall MakeSsdRequesterControlCall(
    SsdReadProtocolLimits limits = {});

}  // namespace mooncake::ssd_offload
