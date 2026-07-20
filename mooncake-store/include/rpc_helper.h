#pragma once

#include <ylt/struct_json/json_writer.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_perf.h"
#include "mooncake_logging.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

// Slow-RPC log threshold (microseconds). RPCs slower than this emit a single
// WARNING carrying rpc_name + elapsed_us + rc. The timing is unconditional
// (does not depend on the VLOG gate), so it works in production where VLOG is
// off; the cost on the fast path is two steady_clock::now() calls.
constexpr uint64_t kMasterSlowLogThresholdUs = 3000;

template <typename T>
struct is_tl_expected : std::false_type {};

template <typename T>
struct is_tl_expected<tl::expected<T, ErrorCode>> : std::true_type {};

template <typename T>
concept TlExpected = is_tl_expected<std::decay_t<T>>::value;

/**
 * @brief A helper function to execute a single RPC call, handling common tasks
 * like logging, metrics, perf instrumentation, and error handling.
 *
 * @tparam RpcCallable A callable object that executes the RPC and returns a
 * tl::expected.
 * @tparam LogRequestCallable A callable object that logs the request
 * parameters.
 * @param rpc_name The name of the RPC function for logging.
 * @param perf_key Optional ubdiag perf point key. Pass std::nullopt (or use
 *                 the 5-arg overload below) for RPCs that do not need
 *                 instrumentation, e.g. low-frequency admin calls.
 * @param rpc_call The callable that performs the actual RPC call.
 * @param log_request The callable that logs the request details.
 * @param inc_req_metric A function to increment the request counter metric.
 * @param inc_fail_metric A function to increment the failure counter metric.
 * @return The result of the RPC call, a tl::expected object.
 */
template <typename RpcCallable, typename LogRequestCallable,
          typename IncReqMetric, typename IncFailMetric>
auto execute_rpc(std::string_view rpc_name, std::optional<PerfKey> perf_key,
                 RpcCallable&& rpc_call, LogRequestCallable&& log_request,
                 IncReqMetric&& inc_req_metric, IncFailMetric&& inc_fail_metric)
    requires TlExpected<std::invoke_result_t<RpcCallable>>
{
    // Optional ubdiag perf point (good/bad split by rc == 0).
    std::unique_ptr<UbDiag::PerfPoint> pt;
    if (perf_key.has_value()) {
        pt = std::make_unique<UbDiag::PerfPoint>(*perf_key,
                                                 UbDiag::PerfLevel::SUB_SYSTEM);
        pt->Start();
    }

    // Unconditional timing for the slow log (ScopedVLogTimer is a no-op when
    // VLOG is off, so it cannot serve this purpose).
    const auto t0 = std::chrono::steady_clock::now();

    ScopedVLogTimer timer(1, rpc_name.data());
    log_request(timer);
    inc_req_metric();

    auto result = rpc_call();
    int rc = 0;
    if (!result.has_value()) {
        inc_fail_metric();
        rc = static_cast<int>(result.error());
    }
    timer.LogResponseExpected(result);

    if (pt) pt->End(rc);

    const auto elapsed_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - t0)
            .count();
    if (static_cast<uint64_t>(elapsed_us) > kMasterSlowLogThresholdUs) {
        MC_LOG(WARNING) << rpc_name << "_slow elapsed_us[" << elapsed_us
                        << "] rc[" << rc << "]";
    }

    return result;
}

/**
 * @brief Backward-compatible 5-arg overload for RPCs that do not need ubdiag
 * instrumentation. Forwards to the 6-arg form with perf_key = std::nullopt, so
 * existing call sites continue to compile unchanged.
 */
template <typename RpcCallable, typename LogRequestCallable,
          typename IncReqMetric, typename IncFailMetric>
auto execute_rpc(std::string_view rpc_name, RpcCallable&& rpc_call,
                 LogRequestCallable&& log_request,
                 IncReqMetric&& inc_req_metric, IncFailMetric&& inc_fail_metric)
    requires TlExpected<std::invoke_result_t<RpcCallable>>
{
    return execute_rpc(rpc_name, std::nullopt,
                       std::forward<RpcCallable>(rpc_call),
                       std::forward<LogRequestCallable>(log_request),
                       std::forward<IncReqMetric>(inc_req_metric),
                       std::forward<IncFailMetric>(inc_fail_metric));
}

}  // namespace mooncake
