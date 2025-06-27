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

/**
 * @brief A helper function to execute batch RPC calls that return response
 * structs.
 *
 * @tparam ResponseType The type of the response struct (e.g.,
 * BatchGetReplicaListResponse).
 * @tparam RpcCallable A callable object that executes the RPC and returns a
 * tl::expected.
 * @tparam LogRequestCallable A callable object that logs the request
 * parameters.
 * @param rpc_name The name of the RPC function for logging.
 * @param rpc_call The callable that performs the actual RPC call.
 * @param log_request The callable that logs the request details.
 * @param inc_fail_metric A function to increment the failure counter metric.
 * @return A response struct with the result and error code.
 */
template <typename ResponseType, typename RpcCallable,
          typename LogRequestCallable>
ResponseType execute_batch_rpc(std::string_view rpc_name,
                               RpcCallable&& rpc_call,
                               LogRequestCallable&& log_request,
                               std::function<void()> inc_fail_metric) {
    ScopedVLogTimer timer(1, rpc_name.data());
    log_request(timer);

    ResponseType response;
    auto result = rpc_call();

    if (result) {
        // Move the result into the response struct
        if constexpr (requires { response.batch_replica_list; }) {
            response.batch_replica_list = std::move(result.value());
        }
        response.error_code = ErrorCode::OK;
    } else {
        inc_fail_metric();
        response.error_code = result.error();
    }

    timer.LogResponseJson(response);
    return response;
}

}  // namespace mooncake
