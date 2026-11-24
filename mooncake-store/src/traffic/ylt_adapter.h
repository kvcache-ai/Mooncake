#pragma once

// The only file that depends on coro_rpc's router internals. Keeping it
// separate lets the recorder, file format, sink, and replay tool stay
// independent of the RPC framework.

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include <async_simple/coro/Lazy.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp>
#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>
#include <ylt/coro_rpc/impl/router.hpp>

#include "traffic_recorder.h"

namespace mooncake::traffic {

// The replay tool re-declares the wire headers because it links neither the
// store nor coro_rpc. These assertions sit where coro_rpc is included, so a
// wire layout change breaks the build instead of silently invalidating every
// recorded segment.
using WireProtocol = coro_rpc::protocol::coro_rpc_protocol;
static_assert(sizeof(WireProtocol::req_header) == 20);
static_assert(offsetof(WireProtocol::req_header, seq_num) == 4);
static_assert(offsetof(WireProtocol::req_header, function_id) == 8);
static_assert(offsetof(WireProtocol::req_header, length) == 12);
static_assert(offsetof(WireProtocol::req_header, attach_length) == 16);
static_assert(sizeof(WireProtocol::resp_header) == 16);
static_assert(offsetof(WireProtocol::resp_header, seq_num) == 4);
static_assert(offsetof(WireProtocol::resp_header, length) == 8);
static_assert(offsetof(WireProtocol::resp_header, attach_length) == 12);

namespace detail {

template <typename Context>
std::uint32_t RequestAttachmentLength(const Context& context) {
    return static_cast<std::uint32_t>(context->get_request_attachment().size());
}

}  // namespace detail

struct RecordingProtocol;
template <typename Protocol>
class RecordingRouter;

struct RecordingProtocol : coro_rpc::protocol::coro_rpc_protocol {
    using router = RecordingRouter<RecordingProtocol>;

    // coro_connection passes a context_info_t built for the protocol in use,
    // while coro_rpc_protocol::read_payload only accepts one built for
    // coro_rpc_protocol, so a derived protocol has to provide this overload.
    // The argument only carries URMA attachment views, and coro_rpc runs over
    // TCP here, so forward to the four-argument overload.
    template <typename Socket>
    static async_simple::coro::Lazy<std::error_code> read_payload(
        Socket& socket, req_header& req_head, std::string& buffer,
        coro_io::heterogeneous_buffer& attachment,
        coro_rpc::context_info_t<RecordingProtocol>* /*context_info*/) {
        co_return co_await coro_rpc::protocol::coro_rpc_protocol::read_payload(
            socket, req_head, buffer, attachment);
    }
};

template <typename Protocol>
class RecordingRouter : public coro_rpc::protocol::router<Protocol> {
   public:
    using Base = coro_rpc::protocol::router<Protocol>;

    template <typename Handler>
    std::pair<coro_rpc::err_code, std::string> route(
        std::uint64_t conn_id, std::uint64_t req_id, Handler handler,
        std::string_view data, coro_rpc::rpc_context<Protocol>& context_info,
        typename Protocol::supported_serialize_protocols protocols,
        const typename Protocol::route_key_t& route_key) {
        if (handler != nullptr && TrafficRecorder::Instance().Recording()) {
            Observe(data, conn_id, req_id, route_key,
                    detail::RequestAttachmentLength(context_info));
        }
        return Base::route(conn_id, req_id, handler, data, context_info,
                           protocols, route_key);
    }

    template <typename Handler>
    async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>
    route_coro(std::uint64_t conn_id, std::uint64_t req_id, Handler handler,
               std::string_view data,
               typename Protocol::supported_serialize_protocols protocols,
               const typename Protocol::route_key_t& route_key) {
        if (handler != nullptr && TrafficRecorder::Instance().Recording()) {
            auto* context_info =
                co_await coro_rpc::get_context_in_coro<Protocol>();
            Observe(data, conn_id, req_id, route_key,
                    detail::RequestAttachmentLength(context_info));
        }
        co_return co_await Base::route_coro(conn_id, req_id, handler, data,
                                            protocols, route_key);
    }

   private:
    void Observe(std::string_view data, std::uint64_t conn_id,
                 std::uint64_t req_id,
                 const typename Protocol::route_key_t& route_key,
                 std::uint32_t attach_length) {
        // YLT request ids are 64-bit; the record format stores 32 bits.
        // The recorder resolves the method name from the routes the service
        // registered: coro_rpc stores the handler it was given, which for a
        // pooled handler is the pool's coroutine and not the service method.
        TrafficRecorder::Instance().OnRequest(
            RequestView{static_cast<std::uint32_t>(route_key), data, conn_id,
                        static_cast<std::uint32_t>(req_id), attach_length});
    }
};

struct RecordingConfig : coro_rpc::config_t {
    using rpc_protocol = RecordingProtocol;
};

using RecordingServer = coro_rpc::coro_rpc_server_base<RecordingConfig>;

}  // namespace mooncake::traffic
