#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>  // For std::chrono
#include <memory>  // For std::unique_ptr
#include <thread>  // For std::thread
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "default_config.h"
#include "ha_helper.h"
#include "http_metadata_server.h"
#include "rpc_service.h"
#include "types.h"

#include "master_config.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

DEFINE_string(config_path, "", "master service config file path");
DEFINE_int32(port, 50051,
             "Port for master service to listen on (deprecated, use rpc_port)");
DEFINE_int32(
    max_threads, 4,
    "Maximum number of threads to use (deprecated, use rpc_thread_num)");
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
DEFINE_bool(enable_offload, false, "Enable offload availability");
DEFINE_string(
    etcd_endpoints, "",
    "Endpoints of ETCD server, separated by semicolon, required in HA mode");
DEFINE_int64(client_ttl, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
             "How long a client is considered alive after the last ping, only "
             "used in HA mode");

DEFINE_string(root_fs_dir, mooncake::DEFAULT_ROOT_FS_DIR,
              "Root directory for storage backend, used in HA mode");
DEFINE_int64(global_file_segment_size,
             mooncake::DEFAULT_GLOBAL_FILE_SEGMENT_SIZE,
             "Size of global NFS/3FS segment in bytes");
DEFINE_string(cluster_id, mooncake::DEFAULT_CLUSTER_ID,
              "Cluster ID for the master service, used for kvcache persistence "
              "in HA mode");

DEFINE_string(memory_allocator, "offset",
              "Memory allocator for global segments, cachelib | offset");
DEFINE_bool(enable_http_metadata_server, false,
            "Enable HTTP metadata server instead of etcd");
DEFINE_int32(http_metadata_server_port, 8080,
             "Port for HTTP metadata server to listen on");
DEFINE_string(http_metadata_server_host, "0.0.0.0",
              "Host for HTTP metadata server to bind to");
DEFINE_bool(
    enable_metadata_cleanup_on_timeout, false,
    "Enable cleanup of HTTP metadata (mooncake/ram/*, mooncake/rpc_meta/*) "
    "when client heartbeat times out. Only effective when "
    "enable_http_metadata_server is true.");

DEFINE_uint64(put_start_discard_timeout_sec,
              mooncake::DEFAULT_PUT_START_DISCARD_TIMEOUT,
              "Timeout for discarding uncompleted PutStart operations");
DEFINE_uint64(put_start_release_timeout_sec,
              mooncake::DEFAULT_PUT_START_RELEASE_TIMEOUT,
              "Timeout for releasing space allocated in uncompleted PutStart "
              "operations");
DEFINE_bool(enable_disk_eviction, true,
            "Enable disk eviction feature for storage backend (default: true)");
DEFINE_uint64(
    quota_bytes, 0,
    "Quota for storage backend in bytes (0 = use default 90% of capacity)");

// Task manager configuration
DEFINE_uint32(max_total_finished_tasks, 10000,
              "Maximum number of finished tasks to keep in memory");
DEFINE_uint32(max_total_pending_tasks, 10000,
              "Maximum number of pending tasks to keep in memory");
DEFINE_uint32(max_total_processing_tasks, 10000,
              "Maximum number of processing tasks to keep in memory");
DEFINE_uint64(pending_task_timeout_sec, 300,
              "Timeout in seconds for pending tasks (0 = no timeout)");
DEFINE_uint64(processing_task_timeout_sec, 300,
              "Timeout in seconds for processing tasks (0 = no timeout)");

void InitMasterConf(const mooncake::DefaultConfig& default_config,
                    mooncake::MasterConfig& master_config) {
    // Initialize the master service configuration from the default config
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
    default_config.GetBool("enable_offload", &master_config.enable_offload,
                           FLAGS_enable_offload);
    default_config.GetString("etcd_endpoints", &master_config.etcd_endpoints,
                             FLAGS_etcd_endpoints);
    default_config.GetString("cluster_id", &master_config.cluster_id,
                             FLAGS_cluster_id);
    default_config.GetString("root_fs_dir", &master_config.root_fs_dir,
                             FLAGS_root_fs_dir);
    default_config.GetInt64("global_file_segment_size",
                            &master_config.global_file_segment_size,
                            FLAGS_global_file_segment_size);
    default_config.GetString("memory_allocator",
                             &master_config.memory_allocator,
                             FLAGS_memory_allocator);
    default_config.GetBool("enable_http_metadata_server",
                           &master_config.enable_http_metadata_server,
                           FLAGS_enable_http_metadata_server);
    default_config.GetUInt32("http_metadata_server_port",
                             &master_config.http_metadata_server_port,
                             FLAGS_http_metadata_server_port);
    default_config.GetString("http_metadata_server_host",
                             &master_config.http_metadata_server_host,
                             FLAGS_http_metadata_server_host);
    default_config.GetBool("enable_metadata_cleanup_on_timeout",
                           &master_config.enable_metadata_cleanup_on_timeout,
                           FLAGS_enable_metadata_cleanup_on_timeout);
    default_config.GetUInt64("put_start_discard_timeout_sec",
                             &master_config.put_start_discard_timeout_sec,
                             FLAGS_put_start_discard_timeout_sec);
    default_config.GetUInt64("put_start_release_timeout_sec",
                             &master_config.put_start_release_timeout_sec,
                             FLAGS_put_start_release_timeout_sec);
    default_config.GetBool("enable_disk_eviction",
                           &master_config.enable_disk_eviction,
                           FLAGS_enable_disk_eviction);
    default_config.GetUInt64("quota_bytes", &master_config.quota_bytes,
                             FLAGS_quota_bytes);
    default_config.GetUInt32("max_total_finished_tasks",
                             &master_config.max_total_finished_tasks,
                             FLAGS_max_total_finished_tasks);
    default_config.GetUInt32("max_total_pending_tasks",
                             &master_config.max_total_pending_tasks,
                             FLAGS_max_total_pending_tasks);
    default_config.GetUInt32("max_total_processing_tasks",
                             &master_config.max_total_processing_tasks,
                             FLAGS_max_total_processing_tasks);
    default_config.GetUInt64("pending_task_timeout_sec",
                             &master_config.pending_task_timeout_sec,
                             FLAGS_pending_task_timeout_sec);
    default_config.GetUInt64("processing_task_timeout_sec",
                             &master_config.processing_task_timeout_sec,
                             FLAGS_processing_task_timeout_sec);
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
    if ((google::GetCommandLineFlagInfo("enable_offload", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_offload = FLAGS_enable_offload;
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
    if ((google::GetCommandLineFlagInfo("root_fs_dir", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.root_fs_dir = FLAGS_root_fs_dir;
    }
    if ((google::GetCommandLineFlagInfo("global_file_segment_size", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.global_file_segment_size = FLAGS_global_file_segment_size;
    }
    if ((google::GetCommandLineFlagInfo("memory_allocator", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.memory_allocator = FLAGS_memory_allocator;
    }
    if ((google::GetCommandLineFlagInfo("enable_http_metadata_server", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_http_metadata_server =
            FLAGS_enable_http_metadata_server;
    }
    if ((google::GetCommandLineFlagInfo("http_metadata_server_port", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.http_metadata_server_port =
            FLAGS_http_metadata_server_port;
    }
    if ((google::GetCommandLineFlagInfo("http_metadata_server_host", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.http_metadata_server_host =
            FLAGS_http_metadata_server_host;
    }
    if ((google::GetCommandLineFlagInfo("enable_metadata_cleanup_on_timeout",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_metadata_cleanup_on_timeout =
            FLAGS_enable_metadata_cleanup_on_timeout;
    }
    if ((google::GetCommandLineFlagInfo("put_start_discard_timeout_sec",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.put_start_discard_timeout_sec =
            FLAGS_put_start_discard_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("put_start_release_timeout_sec",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.put_start_release_timeout_sec =
            FLAGS_put_start_release_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("enable_disk_eviction", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_disk_eviction = FLAGS_enable_disk_eviction;
    }
    if ((google::GetCommandLineFlagInfo("quota_bytes", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.quota_bytes = FLAGS_quota_bytes;
    }
    if ((google::GetCommandLineFlagInfo("max_total_finished_tasks", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_total_finished_tasks = FLAGS_max_total_finished_tasks;
    }
    if ((google::GetCommandLineFlagInfo("max_total_pending_tasks", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_total_pending_tasks = FLAGS_max_total_pending_tasks;
    }
    if ((google::GetCommandLineFlagInfo("max_total_processing_tasks", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_total_processing_tasks =
            FLAGS_max_total_processing_tasks;
    }
    if ((google::GetCommandLineFlagInfo("pending_task_timeout_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.pending_task_timeout_sec = FLAGS_pending_task_timeout_sec;
    }
    if ((google::GetCommandLineFlagInfo("processing_task_timeout_sec", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.processing_task_timeout_sec =
            FLAGS_processing_task_timeout_sec;
    }
}

// Function to start HTTP metadata server
std::unique_ptr<mooncake::HttpMetadataServer> StartHttpMetadataServer(
    int port, const std::string& host) {
    LOG(INFO) << "Starting C++ HTTP metadata server on " << host << ":" << port;

    try {
        auto server =
            std::make_unique<mooncake::HttpMetadataServer>(port, host);
        server->start();

        // Check if server started successfully
        if (server->is_running()) {
            LOG(INFO) << "C++ HTTP metadata server started successfully";
            return server;
        } else {
            LOG(ERROR) << "Failed to start C++ HTTP metadata server";
            return nullptr;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start C++ HTTP metadata server: " << e.what();
        return nullptr;
    }
}

int main(int argc, char* argv[]) {
    mooncake::init_ylt_log_level();
    // Initialize gflags
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }

    // Initialize the master configuration
    mooncake::MasterConfig master_config;
    std::string conf_path = FLAGS_config_path;
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

    const char* value = std::getenv("MC_RPC_PROTOCOL");
    std::string protocol = "tcp";
    if (value && std::string_view(value) == "rdma") {
        protocol = "rdma";
    }

    // Validate: enable_metadata_cleanup_on_timeout requires
    // enable_http_metadata_server
    if (master_config.enable_metadata_cleanup_on_timeout &&
        !master_config.enable_http_metadata_server) {
        LOG(WARNING) << "enable_metadata_cleanup_on_timeout is set to true but "
                     << "enable_http_metadata_server is false. "
                     << "Disabling metadata cleanup on timeout.";
        master_config.enable_metadata_cleanup_on_timeout = false;
    }

    LOG(INFO)
        << "Master service started on port " << master_config.rpc_port
        << ", max_threads=" << master_config.rpc_thread_num
        << ", enable_metric_reporting=" << master_config.enable_metric_reporting
        << ", metrics_port=" << master_config.metrics_port
        << ", default_kv_lease_ttl=" << master_config.default_kv_lease_ttl
        << ", default_kv_soft_pin_ttl=" << master_config.default_kv_soft_pin_ttl
        << ", allow_evict_soft_pinned_objects="
        << master_config.allow_evict_soft_pinned_objects
        << ", eviction_ratio=" << master_config.eviction_ratio
        << ", eviction_high_watermark_ratio="
        << master_config.eviction_high_watermark_ratio
        << ", enable_ha=" << master_config.enable_ha
        << ", enable_offload=" << master_config.enable_offload
        << ", etcd_endpoints=" << master_config.etcd_endpoints
        << ", client_ttl=" << master_config.client_live_ttl_sec
        << ", rpc_thread_num=" << master_config.rpc_thread_num
        << ", rpc_port=" << master_config.rpc_port
        << ", rpc_address=" << master_config.rpc_address
        << ", rpc_conn_timeout_seconds="
        << master_config.rpc_conn_timeout_seconds
        << ", rpc_enable_tcp_no_delay=" << master_config.rpc_enable_tcp_no_delay
        << ", rpc protocol=" << protocol
        << ", cluster_id=" << master_config.cluster_id
        << ", root_fs_dir=" << master_config.root_fs_dir
        << ", global_file_segment_size="
        << master_config.global_file_segment_size
        << ", memory_allocator=" << master_config.memory_allocator
        << ", enable_http_metadata_server="
        << master_config.enable_http_metadata_server
        << ", http_metadata_server_port="
        << master_config.http_metadata_server_port
        << ", http_metadata_server_host="
        << master_config.http_metadata_server_host
        << ", enable_metadata_cleanup_on_timeout="
        << master_config.enable_metadata_cleanup_on_timeout
        << ", put_start_discard_timeout_sec="
        << master_config.put_start_discard_timeout_sec
        << ", put_start_release_timeout_sec="
        << master_config.put_start_release_timeout_sec
        << ", max_total_finished_tasks="
        << master_config.max_total_finished_tasks
        << ", max_total_pending_tasks=" << master_config.max_total_pending_tasks
        << ", max_total_processing_tasks="
        << master_config.max_total_processing_tasks
        << ", pending_task_timeout_sec="
        << master_config.pending_task_timeout_sec
        << ", processing_task_timeout_sec="
        << master_config.processing_task_timeout_sec;

    // Start HTTP metadata server if enabled
    std::unique_ptr<mooncake::HttpMetadataServer> http_metadata_server;
    if (master_config.enable_http_metadata_server) {
        http_metadata_server =
            StartHttpMetadataServer(master_config.http_metadata_server_port,
                                    master_config.http_metadata_server_host);

        if (!http_metadata_server) {
            LOG(FATAL) << "Failed to start HTTP metadata server";
            return 1;
        }

        // Give the server some time to start
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (master_config.enable_ha) {
        mooncake::MasterServiceSupervisor supervisor(
            mooncake::MasterServiceSupervisorConfig{master_config});
        return supervisor.Start();
    } else {
        // version is not used in non-HA mode, just pass a dummy value
        mooncake::ViewVersionId version = 0;
        coro_rpc::coro_rpc_server server(
            master_config.rpc_thread_num, master_config.rpc_port,
            master_config.rpc_address,
            std::chrono::seconds(master_config.rpc_conn_timeout_seconds),
            master_config.rpc_enable_tcp_no_delay);
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            server.init_ibv();
        }
        // Only pass HttpMetadataServer pointer if cleanup is enabled
        // Note: enable_metadata_cleanup_on_timeout is automatically disabled
        // if enable_http_metadata_server is false (validated earlier)
        mooncake::HttpMetadataServer* metadata_server_ptr = nullptr;
        if (master_config.enable_metadata_cleanup_on_timeout) {
            metadata_server_ptr = http_metadata_server.get();
        }

        mooncake::WrappedMasterService wrapped_master_service(
            mooncake::WrappedMasterServiceConfig(master_config, version),
            metadata_server_ptr);

        mooncake::RegisterRpcService(server, wrapped_master_service);
        return server.start();
    }
}
