#pragma once

#include <functional>
#include <string_view>
#include <type_traits>
#include <utility>

#include "master_metric_manager.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake {

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
template <typename RpcCallable, typename LogRequestCallable>
auto execute_rpc(std::string_view rpc_name, RpcCallable&& rpc_call,
                 LogRequestCallable&& log_request,
                 std::function<void()> inc_req_metric,
                 std::function<void()> inc_fail_metric) {
    ScopedVLogTimer timer(1, rpc_name.data());
    log_request(timer);

    inc_req_metric();

    auto result = rpc_call();

    if (result) {
        using ResultType = typename decltype(result)::value_type;
        // Special handling for bool to log the value, similar to the original
        // implementation of ExistKey.
        if constexpr (std::is_same_v<ResultType, bool>) {
            timer.LogResponse("success=true, exist=", result.value());
        } else {
            timer.LogResponse("success=true");
        }
    } else {
        inc_fail_metric();
        timer.LogResponse("success=false, error=", toString(result.error()));
    }

    return result;
}

}  // namespace mooncake
