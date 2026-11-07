#pragma once

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

struct RecordingProtocol;
template <typename Protocol>
class RecordingRouter;

struct RecordingProtocol : coro_rpc::protocol::coro_rpc_protocol {
    using router = RecordingRouter<RecordingProtocol>;
};

template <typename Protocol>
class RecordingRouter : public coro_rpc::protocol::router<Protocol> {
   public:
    using Base = coro_rpc::protocol::router<Protocol>;

    template <typename Handler>
    std::pair<coro_rpc::err_code, std::string> route(
        std::uint64_t conn_id, std::uint64_t req_id, Handler handler,
        std::string_view data,
        coro_rpc::rpc_context<Protocol>& context_info,
        typename Protocol::supported_serialize_protocols protocols,
        const typename Protocol::route_key_t& route_key) {
        if (handler != nullptr) {
            TrafficRecorder::Instance().OnRequest(RequestView{
                static_cast<std::uint32_t>(route_key), this->get_name(route_key),
                data, conn_id, static_cast<std::uint32_t>(req_id), 0});
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
        if (handler != nullptr) {
            TrafficRecorder::Instance().OnRequest(RequestView{
                static_cast<std::uint32_t>(route_key), this->get_name(route_key),
                data, conn_id, static_cast<std::uint32_t>(req_id), 0});
        }
        co_return co_await Base::route_coro(conn_id, req_id, handler, data,
                                            protocols, route_key);
    }
};

struct RecordingConfig : coro_rpc::config_t {
    using rpc_protocol = RecordingProtocol;
};

using RecordingServer = coro_rpc::coro_rpc_server_base<RecordingConfig>;

}  // namespace mooncake::traffic
