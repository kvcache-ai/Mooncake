#include <gflags/gflags.h>

#include <chrono>  // For std::chrono
#include <thread>  // For std::thread
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "ha_helper.h"
#include "rpc_service.h"
#include "types.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

DEFINE_int32(port, 50051,
             "Port for master service to listen on (deprecated, use rpc_port)");
DEFINE_int32(
    max_threads, 4,
    "Maximum number of threads to use (deprecated, use rpc_thread_num)");
DEFINE_bool(enable_gc, false, "Enable garbage collection");
DEFINE_bool(enable_metric_reporting, true, "Enable periodic metric reporting");
DEFINE_int32(metrics_port, 9003, "Port for HTTP metrics server to listen on");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects");
DEFINE_uint64(default_kv_soft_pin_ttl, mooncake::DEFAULT_KV_SOFT_PIN_TTL_MS,
              "Default soft pin ttl for kv objects");
DEFINE_bool(allow_evict_soft_pinned_objects,
            mooncake::DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS,
            "Whether to allow eviction of soft pinned objects during eviction");
DEFINE_double(eviction_ratio, mooncake::DEFAULT_EVICTION_RATIO,
              "Ratio of objects to evict when storage space is full");
DEFINE_double(eviction_high_watermark_ratio,
              mooncake::DEFAULT_EVICTION_HIGH_WATERMARK_RATIO,
              "Ratio of high watermark trigger eviction");
// RPC server configuration parameters (new, preferred)
// TODO: deprecate port and max_threads in the future
DEFINE_int32(rpc_thread_num, 0,
             "Number of threads for RPC server (0 = use max_threads, preferred "
             "over max_threads)");
DEFINE_int32(
    rpc_port, 0,
    "Port for RPC server to listen on (0 = use port, preferred over port)");
DEFINE_string(rpc_address, "0.0.0.0",
              "Address for RPC server to bind to, required in HA mode");
DEFINE_int32(rpc_conn_timeout_seconds, 0,
             "Connection timeout in seconds (0 = no timeout)");
DEFINE_bool(rpc_enable_tcp_no_delay, true,
            "Enable TCP_NODELAY for RPC connections");

DEFINE_validator(eviction_ratio, [](const char* flagname, double value) {
    if (value < 0.0 || value > 1.0) {
        LOG(FATAL) << "Eviction ratio must be between 0.0 and 1.0";
        return false;
    }
    return true;
});
DEFINE_bool(enable_ha, false,
            "Enable high availability, which depends on etcd");
DEFINE_string(
    etcd_endpoints, "",
    "Endpoints of ETCD server, separated by semicolon, required in HA mode");
DEFINE_int64(client_ttl, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
             "How long a client is considered alive after the last ping, only "
             "used in HA mode");

DEFINE_string(cluster_id, mooncake::DEFAULT_CLUSTER_ID,
              "Cluster ID for the master service, used for kvcache persistence in HA mode");

DEFINE_string(memory_allocator, "offset",
              "Memory allocator for global segments, cachelib | offset");

int main(int argc, char* argv[]) {
    easylog::set_min_severity(easylog::Severity::WARN);
    // Initialize gflags
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Master service started on port " << FLAGS_port
              << ", enable_gc=" << FLAGS_enable_gc
              << ", max_threads=" << FLAGS_max_threads
              << ", enable_metric_reporting=" << FLAGS_enable_metric_reporting
              << ", metrics_port=" << FLAGS_metrics_port
              << ", default_kv_lease_ttl=" << FLAGS_default_kv_lease_ttl
              << ", default_kv_soft_pin_ttl=" << FLAGS_default_kv_soft_pin_ttl
              << ", allow_evict_soft_pinned_objects="
              << FLAGS_allow_evict_soft_pinned_objects
              << ", eviction_ratio=" << FLAGS_eviction_ratio
              << ", eviction_high_watermark_ratio="
              << FLAGS_eviction_high_watermark_ratio
              << ", enable_ha=" << FLAGS_enable_ha
              << ", etcd_endpoints=" << FLAGS_etcd_endpoints
              << ", client_ttl=" << FLAGS_client_ttl
              << ", rpc_thread_num=" << FLAGS_rpc_thread_num
              << ", rpc_port=" << FLAGS_rpc_port
              << ", rpc_address=" << FLAGS_rpc_address
              << ", rpc_conn_timeout_seconds=" << FLAGS_rpc_conn_timeout_seconds
              << ", rpc_enable_tcp_no_delay=" << FLAGS_rpc_enable_tcp_no_delay
              << ", cluster_id=" << FLAGS_cluster_id
              << ", memory_allocator=" << FLAGS_memory_allocator;

    int server_thread_num =
        std::min(FLAGS_max_threads,
                 static_cast<int>(std::thread::hardware_concurrency()));

    if (FLAGS_max_threads != 4) {  // 4 is the default value
        LOG(WARNING) << "max_threads is deprecated, use rpc_thread_num instead";
    }
    if (FLAGS_port != 50051) {  // 50051 is the default value
        LOG(WARNING) << "port is deprecated, use rpc_port instead";
    }

    // Handle backward compatibility for RPC configuration
    // Determine RPC server thread number with compatibility warnings
    // TODO: remove this in the future
    size_t rpc_thread_num;
    if (FLAGS_rpc_thread_num > 0) {
        rpc_thread_num = static_cast<size_t>(FLAGS_rpc_thread_num);
        if (FLAGS_max_threads != 4) {  // 4 is the default value
            LOG(WARNING) << "Both rpc_thread_num and max_threads are set. "
                         << "Using rpc_thread_num=" << FLAGS_rpc_thread_num
                         << ". Please migrate to use rpc_thread_num only.";
        }
    } else {
        rpc_thread_num = static_cast<size_t>(server_thread_num);
    }

    // Determine RPC server port with compatibility warnings
    int rpc_port;
    if (FLAGS_rpc_port > 0) {
        rpc_port = FLAGS_rpc_port;
        if (FLAGS_port != 50051) {  // 50051 is the default value
            LOG(WARNING) << "Both rpc_port and port are set. "
                         << "Using rpc_port=" << FLAGS_rpc_port
                         << ". Please migrate to use rpc_port only.";
        }
    } else {
        rpc_port = FLAGS_port;
    }

    // Convert timeout from seconds to chrono duration
    // Note: 0 seconds means no timeout (infinite timeout)
    auto rpc_conn_timeout =
        std::chrono::seconds(FLAGS_rpc_conn_timeout_seconds);

    if (FLAGS_enable_ha && FLAGS_etcd_endpoints.empty()) {
        LOG(FATAL) << "Etcd endpoints must be set when enable_ha is true";
        return 1;
    }
    if (!FLAGS_enable_ha && !FLAGS_etcd_endpoints.empty()) {
        LOG(WARNING)
            << "Etcd endpoints are set but will not be used in non-HA mode";
    }

    mooncake::BufferAllocatorType allocator_type;
    if (FLAGS_memory_allocator == "cachelib") {
        allocator_type = mooncake::BufferAllocatorType::CACHELIB;
    } else if (FLAGS_memory_allocator == "offset") {
        allocator_type = mooncake::BufferAllocatorType::OFFSET;
    } else {
        LOG(FATAL) << "Invalid memory allocator type: " << FLAGS_memory_allocator;
        return 1;
    }

    if (FLAGS_enable_ha) {
        // Construct local hostname from rpc_address and rpc_port
        std::string local_hostname =
            FLAGS_rpc_address + ":" + std::to_string(rpc_port);

        mooncake::MasterServiceSupervisor supervisor(
            rpc_port, rpc_thread_num, FLAGS_enable_gc,
            FLAGS_enable_metric_reporting, FLAGS_metrics_port,
            FLAGS_default_kv_lease_ttl, FLAGS_default_kv_soft_pin_ttl,
            FLAGS_allow_evict_soft_pinned_objects, FLAGS_eviction_ratio,
            FLAGS_eviction_high_watermark_ratio, FLAGS_client_ttl,
            FLAGS_etcd_endpoints, local_hostname, FLAGS_rpc_address,
            rpc_conn_timeout, FLAGS_rpc_enable_tcp_no_delay, FLAGS_cluster_id,
            allocator_type);

        return supervisor.Start();
    } else {
        // version is not used in non-HA mode, just pass a dummy value
        mooncake::ViewVersionId version = 0;
        coro_rpc::coro_rpc_server server(rpc_thread_num, rpc_port,
                                         FLAGS_rpc_address, rpc_conn_timeout,
                                         FLAGS_rpc_enable_tcp_no_delay);
        mooncake::WrappedMasterService wrapped_master_service(
            FLAGS_enable_gc, FLAGS_default_kv_lease_ttl,
            FLAGS_default_kv_soft_pin_ttl,
            FLAGS_allow_evict_soft_pinned_objects,
            FLAGS_enable_metric_reporting, FLAGS_metrics_port,
            FLAGS_eviction_ratio, FLAGS_eviction_high_watermark_ratio, version,
            FLAGS_client_ttl, FLAGS_enable_ha, FLAGS_cluster_id,
            allocator_type);

        mooncake::RegisterRpcService(server, wrapped_master_service);
        return server.start();
    }
}