#pragma once

#include <ylt/struct_json/json_writer.h>

#include <string_view>
#include <type_traits>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

template <typename T>
struct is_tl_expected : std::false_type {};

template <typename T>
struct is_tl_expected<tl::expected<T, ErrorCode>> : std::true_type {};

template <typename T>
concept TlExpected = is_tl_expected<std::decay_t<T>>::value;

/**
 * @brief A helper function to execute a single RPC call, handling common tasks
 * like logging, metrics, and error handling.
 *
 * @tparam RpcCallable A callable object that executes the RPC and returns a
 * tl::expected.
 * @tparam LogRequestCallable A callable object that logs the request
 * parameters.
 * @param rpc_name The name of the RPC function for logging.
 * @param rpc_call The callable that performs the actual RPC call.
 * @param log_request The callable that logs the request details.
 * @param inc_req_metric A function to increment the request counter metric.
 * @param inc_fail_metric A function to increment the failure counter metric.
 * @return The result of the RPC call, a tl::expected object.
 */
template <typename RpcCallable, typename LogRequestCallable,
          typename IncReqMetric, typename IncFailMetric>
auto execute_rpc(std::string_view rpc_name, RpcCallable&& rpc_call,
                 LogRequestCallable&& log_request,
                 IncReqMetric&& inc_req_metric, IncFailMetric&& inc_fail_metric)
    requires TlExpected<std::invoke_result_t<RpcCallable>>
{
    ScopedVLogTimer timer(1, rpc_name.data());
    log_request(timer);

    inc_req_metric();

    auto result = rpc_call();
    if (!result.has_value()) {
        inc_fail_metric();
    }
    timer.LogResponseExpected(result);

    return result;
}

}  // namespace mooncake
