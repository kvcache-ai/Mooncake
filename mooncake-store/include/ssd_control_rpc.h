// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "ssd_control_wire.h"
#include "ssd_push_requester.h"
#include "ssd_direct_read.h"
#include <chrono>
#include <atomic>
#include <functional>
#include <system_error>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace mooncake::ssd_offload {
class BoundedExecutor;
inline constexpr size_t kSsdControlMaxRpcBody = kSsdControlMaxWireBytes + 128;
DestinationError ValidateSsdControlRpcRequestHeader(uint32_t body,
                                                    uint32_t attachment);
struct SsdControlRpcProtocol : coro_rpc::protocol::coro_rpc_protocol {
    using router = coro_rpc::protocol::router<SsdControlRpcProtocol>;
    template <typename Socket, typename... Context>
    static async_simple::coro::Lazy<std::error_code> read_payload(
        Socket& socket, req_header& header, std::string& body,
        coro_io::heterogeneous_buffer& attachment, Context...) {
        // Before the base protocol resizes the body/attachment buffers.
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

class SsdReadIntoRpcEndpoint {
   public:
    explicit SsdReadIntoRpcEndpoint(std::shared_ptr<SsdDirectReadOwner>,
                                    SsdReadProtocolLimits = {},
                                    size_t worker_threads = 4,
                                    size_t max_outstanding = 64);
    ~SsdReadIntoRpcEndpoint();
    void Call(SsdDirectRpcContext, std::string_view);
    // Drain while the RPC server/its I/O executor is still alive, then stop it.
    void CloseAndDrain();
    uint64_t read_requests() const { return read_requests_.load(); }

   private:
    std::shared_ptr<SsdDirectReadOwner> owner_;
    SsdReadProtocolLimits limits_;
    std::unique_ptr<BoundedExecutor> workers_;
    std::atomic<uint64_t> read_requests_{0};
};

class SsdControlRpcEndpoint {
   public:
    using Worker =
        std::function<void(const SsdReadRequestV1&, const SsdGrantReferenceV1&,
                           SsdOwnerRequestLedger::Operation)>;
    SsdControlRpcEndpoint(std::shared_ptr<SsdRequesterGrants>,
                          std::shared_ptr<SsdOwnerRequestLedger>, Worker = {},
                          SsdReadProtocolLimits = {}, size_t worker_threads = 2,
                          size_t max_outstanding = 64);
    ~SsdControlRpcEndpoint();
    std::string Call(std::string_view);

   private:
    std::shared_ptr<SsdRequesterGrants> grants_;
    std::shared_ptr<SsdOwnerRequestLedger> ledger_;
    Worker worker_;
    SsdReadProtocolLimits limits_;
    std::unique_ptr<BoundedExecutor> workers_;
};
struct SsdControlRpcCall {
    std::error_code transport_error;
    // Envelope decoding only; remote operation status is reply->error.
    DestinationError error{DestinationError::kInvalidArgument};
    std::optional<SsdControlReply> reply;
};
// Blocking compatibility client for dedicated workers, never the RPC I/O loop.
// host must be an already resolved numeric IPv4/IPv6 address. Each call owns
// a fresh connection; both directions are bounded before allocation. Resolve
// peer DNS names during bounded setup, outside this per-request deadline path.
// Network errors/timeouts never fabricate a terminal drain response.
SsdControlRpcCall CallSsdControlRpc(const std::string& host, uint16_t port,
                                    const SsdControlMessage&,
                                    std::chrono::milliseconds timeout,
                                    SsdReadProtocolLimits = {});
// Production adapter for the owned requester. It preserves transport/decoding
// failures and performs one RPC per invocation on the caller's fixed route.
SsdRequesterControlCall MakeSsdRequesterControlCall(SsdReadProtocolLimits = {});
}  // namespace mooncake::ssd_offload
