#include <gflags/gflags.h>

#include <chrono>  // For std::chrono
#include <thread>  // For std::thread
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "default_config.h"
#include "ha_helper.h"
#include "rpc_service.h"
#include "types.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

DEFINE_string(conf_path, "", "master service config file path");
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
              "Cluster ID for the master service, used for kvcache persistence "
              "in HA mode");

DEFINE_string(memory_allocator, "offset",
              "Memory allocator for global segments, cachelib | offset");

void InitMasterConf(const mooncake::DefaultConfig& default_config,
                    mooncake::MasterConfig& master_config) {
    // Initialize the master service configuration from the default config
    default_config.GetBool("enable_gc", &master_config.enable_gc,
                           FLAGS_enable_gc);
    default_config.GetBool("enable_metric_reporting",
                           &master_config.enable_metric_reporting,
                           FLAGS_enable_metric_reporting);
    default_config.GetUInt32("metrics_port", &master_config.metrics_port,
                             FLAGS_metrics_port);
    default_config.GetUInt32("rpc_port", &master_config.rpc_port,
                             FLAGS_rpc_port);
    default_config.GetUInt32("rpc_thread_num", &master_config.rpc_thread_num,
                             FLAGS_rpc_thread_num);
    default_config.GetString("rpc_address", &master_config.rpc_address,
                             FLAGS_rpc_address);
    default_config.GetInt32("rpc_conn_timeout_seconds",
                            &master_config.rpc_conn_timeout_seconds,
                            FLAGS_rpc_conn_timeout_seconds);
    default_config.GetBool("rpc_enable_tcp_no_delay",
                           &master_config.rpc_enable_tcp_no_delay,
                           FLAGS_rpc_enable_tcp_no_delay);
    default_config.GetUInt64("default_kv_lease_ttl",
                             &master_config.default_kv_lease_ttl,
                             FLAGS_default_kv_lease_ttl);
    default_config.GetUInt64("default_kv_soft_pin_ttl",
                             &master_config.default_kv_soft_pin_ttl,
                             FLAGS_default_kv_soft_pin_ttl);
    default_config.GetBool("allow_evict_soft_pinned_objects",
                           &master_config.allow_evict_soft_pinned_objects,
                           FLAGS_allow_evict_soft_pinned_objects);
    default_config.GetDouble("eviction_ratio", &master_config.eviction_ratio,
                             FLAGS_eviction_ratio);
    default_config.GetDouble("eviction_high_watermark_ratio",
                             &master_config.eviction_high_watermark_ratio,
                             FLAGS_eviction_high_watermark_ratio);
    default_config.GetInt64("client_live_ttl_sec",
                            &master_config.client_live_ttl_sec,
                            FLAGS_client_ttl);

    default_config.GetBool("enable_ha", &master_config.enable_ha,
                           FLAGS_enable_ha);
    default_config.GetString("etcd_endpoints", &master_config.etcd_endpoints,
                             FLAGS_etcd_endpoints);
    default_config.GetString("cluster_id", &master_config.cluster_id,
                             FLAGS_cluster_id);
    default_config.GetString("memory_allocator",
                             &master_config.memory_allocator,
                             FLAGS_memory_allocator);
}

void LoadConfigFromCmdline(mooncake::MasterConfig& master_config,
                           bool conf_set) {
    if (FLAGS_max_threads != 4) {  // 4 is the default value
        LOG(WARNING) << "max_threads is deprecated, use rpc_thread_num instead";
    }
    if (FLAGS_port != 50051) {  // 50051 is the default value
        LOG(WARNING) << "port is deprecated, use rpc_port instead";
    }
    int server_thread_num =
        std::min(FLAGS_max_threads,
                 static_cast<int>(std::thread::hardware_concurrency()));

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
        master_config.rpc_thread_num = rpc_thread_num;
    } else {
        if (!conf_set) {
            master_config.rpc_thread_num = server_thread_num;
        }
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
        master_config.rpc_port = rpc_port;
    } else {
        if (!conf_set) {
            master_config.rpc_port = FLAGS_port;
        }
    }

    google::CommandLineFlagInfo info;
    if ((google::GetCommandLineFlagInfo("rpc_address", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_address = FLAGS_rpc_address;
    }
    if ((google::GetCommandLineFlagInfo("rpc_conn_timeout_seconds", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_conn_timeout_seconds = FLAGS_rpc_conn_timeout_seconds;
    }
    if ((google::GetCommandLineFlagInfo("rpc_enable_tcp_no_delay", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_enable_tcp_no_delay = FLAGS_rpc_enable_tcp_no_delay;
    }
    if ((google::GetCommandLineFlagInfo("enable_gc", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_gc = FLAGS_enable_gc;
    }
    if ((google::GetCommandLineFlagInfo("enable_metric_reporting", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_metric_reporting = FLAGS_enable_metric_reporting;
    }
    if ((google::GetCommandLineFlagInfo("metrics_port", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.metrics_port = FLAGS_metrics_port;
    }
    if ((google::GetCommandLineFlagInfo("default_kv_lease_ttl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.default_kv_lease_ttl = FLAGS_default_kv_lease_ttl;
    }
    if ((google::GetCommandLineFlagInfo("default_kv_soft_pin_ttl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.default_kv_soft_pin_ttl = FLAGS_default_kv_soft_pin_ttl;
    }
    if ((google::GetCommandLineFlagInfo("allow_evict_soft_pinned_objects",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.allow_evict_soft_pinned_objects =
            FLAGS_allow_evict_soft_pinned_objects;
    }
    if ((google::GetCommandLineFlagInfo("eviction_ratio", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.eviction_ratio = FLAGS_eviction_ratio;
    }
    if ((google::GetCommandLineFlagInfo("eviction_high_watermark_ratio",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.eviction_high_watermark_ratio =
            FLAGS_eviction_high_watermark_ratio;
    }
    if ((google::GetCommandLineFlagInfo("enable_ha", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_ha = FLAGS_enable_ha;
    }
    if ((google::GetCommandLineFlagInfo("etcd_endpoints", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.etcd_endpoints = FLAGS_etcd_endpoints;
    }
    if ((google::GetCommandLineFlagInfo("client_live_ttl_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.client_live_ttl_sec = FLAGS_client_ttl;
    }
    if ((google::GetCommandLineFlagInfo("cluster_id", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cluster_id = FLAGS_cluster_id;
    }
    if ((google::GetCommandLineFlagInfo("memory_allocator", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.memory_allocator = FLAGS_memory_allocator;
    }
}

int main(int argc, char* argv[]) {
    easylog::set_min_severity(easylog::Severity::WARN);
    // Initialize gflags
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // Initialize the master configuration
    mooncake::MasterConfig master_config;
    std::string conf_path = FLAGS_conf_path;
    if (!conf_path.empty()) {
        mooncake::DefaultConfig default_config;
        default_config.SetPath(conf_path);
        try {
            default_config.Load();
        } catch (const std::exception& e) {
            LOG(FATAL) << "Failed to initialize default config: " << e.what();
            return 1;
        }
        InitMasterConf(default_config, master_config);
    }
    LoadConfigFromCmdline(master_config, !conf_path.empty());

    if (master_config.enable_ha && master_config.etcd_endpoints.empty()) {
        LOG(FATAL) << "Etcd endpoints must be set when enable_ha is true";
        return 1;
    }
    if (!master_config.enable_ha && !master_config.etcd_endpoints.empty()) {
        LOG(WARNING)
            << "Etcd endpoints are set but will not be used in non-HA mode";
    }
    if (master_config.memory_allocator != "cachelib" &&
        master_config.memory_allocator != "offset") {
        LOG(FATAL) << "Invalid memory allocator: "
                   << master_config.memory_allocator
                   << ", must be 'cachelib' or 'offset'";
        return 1;
    }

    LOG(INFO) << "Master service started on port " << master_config.rpc_port
              << ", enable_gc=" << master_config.enable_gc
              << ", max_threads=" << master_config.rpc_thread_num
              << ", enable_metric_reporting="
              << master_config.enable_metric_reporting
              << ", metrics_port=" << master_config.metrics_port
              << ", default_kv_lease_ttl=" << master_config.default_kv_lease_ttl
              << ", default_kv_soft_pin_ttl="
              << master_config.default_kv_soft_pin_ttl
              << ", allow_evict_soft_pinned_objects="
              << master_config.allow_evict_soft_pinned_objects
              << ", eviction_ratio=" << master_config.eviction_ratio
              << ", eviction_high_watermark_ratio="
              << master_config.eviction_high_watermark_ratio
              << ", enable_ha=" << master_config.enable_ha
              << ", etcd_endpoints=" << master_config.etcd_endpoints
              << ", client_ttl=" << master_config.client_live_ttl_sec
              << ", rpc_thread_num=" << master_config.rpc_thread_num
              << ", rpc_port=" << master_config.rpc_port
              << ", rpc_address=" << master_config.rpc_address
              << ", rpc_conn_timeout_seconds="
              << master_config.rpc_conn_timeout_seconds
              << ", rpc_enable_tcp_no_delay="
              << master_config.rpc_enable_tcp_no_delay
              << ", cluster_id=" << master_config.cluster_id
              << ", memory_allocator=" << master_config.memory_allocator;

    if (master_config.enable_ha) {
        // Construct local hostname from rpc_address and rpc_port
        mooncake::MasterServiceSupervisor supervisor(master_config);

        return supervisor.Start();
    } else {
        // version is not used in non-HA mode, just pass a dummy value
        mooncake::ViewVersionId version = 0;
        mooncake::BufferAllocatorType allocator_type;
        if (master_config.memory_allocator == "cachelib") {
            allocator_type = mooncake::BufferAllocatorType::CACHELIB;
        } else {
            allocator_type = mooncake::BufferAllocatorType::OFFSET;
        }
        coro_rpc::coro_rpc_server server(
            master_config.rpc_thread_num, master_config.rpc_port,
            master_config.rpc_address,
            std::chrono::seconds(master_config.rpc_conn_timeout_seconds),
            master_config.rpc_enable_tcp_no_delay);
        mooncake::WrappedMasterService wrapped_master_service(
            master_config.enable_gc, master_config.default_kv_lease_ttl,
            master_config.default_kv_soft_pin_ttl,
            master_config.allow_evict_soft_pinned_objects,
            master_config.enable_metric_reporting, master_config.metrics_port,
            master_config.eviction_ratio,
            master_config.eviction_high_watermark_ratio, version,
            master_config.client_live_ttl_sec, master_config.enable_ha,
            master_config.cluster_id, allocator_type);

        mooncake::RegisterRpcService(server, wrapped_master_service);
        return server.start();
    }
}