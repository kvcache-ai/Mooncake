#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>  // For std::chrono
#include <csignal>
#include <memory>  // For std::unique_ptr
#include <thread>  // For std::thread
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog/record.hpp>

#include "default_config.h"
#include "duration_utils.h"
#include "ha/leadership/master_service_supervisor.h"
#include "http_metadata_server.h"
#include "rpc_service.h"
#include "types.h"
#include "utils.h"

#include "master_config.h"

using namespace coro_rpc;
using namespace async_simple;
using namespace async_simple::coro;

static_assert(mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL == 5000,
              "Update kDefaultKvLeaseTtlFlagValue when "
              "DEFAULT_DEFAULT_KV_LEASE_TTL changes");
static_assert(mooncake::DEFAULT_KV_SOFT_PIN_TTL_MS == 30 * 60 * 1000,
              "Update kDefaultKvSoftPinTtlFlagValue when "
              "DEFAULT_KV_SOFT_PIN_TTL_MS changes");

constexpr char kDefaultKvLeaseTtlFlagValue[] = "5000";
constexpr char kDefaultKvSoftPinTtlFlagValue[] = "1800000";

namespace {

bool ValidateDurationFlag(const char* flagname, const std::string& value) {
    uint64_t parsed_value = 0;
    std::string error;
    if (!mooncake::ParseDurationMs(value, &parsed_value, &error)) {
        LOG(ERROR) << "Invalid value for --" << flagname << ": " << value
                   << ". " << error;
        return false;
    }
    return true;
}

uint64_t ParseDurationFlagOrDie(const char* flag_name,
                                const std::string& value) {
    uint64_t parsed_value = 0;
    std::string error;
    if (!mooncake::ParseDurationMs(value, &parsed_value, &error)) {
        LOG(FATAL) << "Invalid value for --" << flag_name << ": " << value
                   << ". " << error;
    }
    return parsed_value;
}

}  // namespace

DEFINE_string(config_path, "", "master service config file path");
DEFINE_int32(port, 50051,
             "Port for master service to listen on (deprecated, use rpc_port)");
DEFINE_int32(
    max_threads, 4,
    "Maximum number of threads to use (deprecated, use rpc_thread_num)");
DEFINE_bool(enable_metric_reporting, true, "Enable periodic metric reporting");
DEFINE_int32(metrics_port, 9003, "Port for HTTP metrics server to listen on");
DEFINE_string(default_kv_lease_ttl, kDefaultKvLeaseTtlFlagValue,
              "Default lease time for kv objects. Supports raw milliseconds "
              "or duration strings with ms, s, m, or h suffixes");
DEFINE_string(default_kv_soft_pin_ttl, kDefaultKvSoftPinTtlFlagValue,
              "Default soft pin TTL for kv objects. Supports raw milliseconds "
              "or duration strings with ms, s, m, or h suffixes");
DEFINE_bool(allow_evict_soft_pinned_objects,
            mooncake::DEFAULT_ALLOW_EVICT_SOFT_PINNED_OBJECTS,
            "Whether to allow eviction of soft pinned objects during eviction");
DEFINE_validator(default_kv_lease_ttl, ValidateDurationFlag);
DEFINE_validator(default_kv_soft_pin_ttl, ValidateDurationFlag);
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
DEFINE_string(rpc_interface, "",
              "Network interface name for RPC server address resolution. "
              "When set, its IPv4 address overrides rpc_address");
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
DEFINE_string(ha_backend_type, "etcd",
              "HA backend type, e.g. etcd | redis | k8s");
DEFINE_string(ha_backend_connstring, "",
              "HA backend connection string. If unset, fallback to "
              "etcd_endpoints for backward compatibility");
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
DEFINE_string(
    allocation_strategy, "random",
    "Allocation strategy for segments, random | free_ratio_first | cxl");
DEFINE_bool(enable_http_metadata_server, false,
            "Enable HTTP metadata server instead of etcd");
DEFINE_int32(http_metadata_server_port, 8080,
             "Port for HTTP metadata server to listen on");
DEFINE_string(http_metadata_server_host, "0.0.0.0",
              "Host for HTTP metadata server to bind to");

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

// Snapshot related configuration flags (migrated from global_flags)
DEFINE_string(snapshot_backup_dir, "",
              "Optional local directory for snapshot and restore backup. "
              "If empty, local backup is disabled");
DEFINE_bool(enable_snapshot_restore, false, "enable restore from snapshot");
DEFINE_bool(enable_snapshot, false, "Enable periodic snapshot of master data");
DEFINE_uint64(snapshot_interval_seconds,
              mooncake::DEFAULT_SNAPSHOT_INTERVAL_SEC,
              "Interval in second between periodic snapshots of master data");
DEFINE_uint64(snapshot_child_timeout_seconds,
              mooncake::DEFAULT_SNAPSHOT_CHILD_TIMEOUT_SEC,
              "Timeout for snapshot child process in seconds");
DEFINE_uint32(snapshot_retention_count,
              mooncake::DEFAULT_SNAPSHOT_RETENTION_COUNT,
              "Number of recent snapshots to keep (older snapshots will be "
              "automatically deleted)");
DEFINE_string(snapshot_object_store_type, "",
              "Snapshot object store type: 'local' for local filesystem, "
              "'s3' for S3 storage");
DEFINE_string(snapshot_payload_store_type, "",
              "Deprecated alias of --snapshot_object_store_type");
DEFINE_string(snapshot_payload_backend_type, "",
              "Deprecated alias of --snapshot_object_store_type");
DEFINE_string(snapshot_catalog_store_type, "",
              "Snapshot catalog store type: ''/'embedded' or 'redis' "
              "('payload' is kept as a deprecated alias)");
DEFINE_string(snapshot_catalog_backend_type, "",
              "Deprecated alias of --snapshot_catalog_store_type");
DEFINE_string(snapshot_catalog_store_connstring, "",
              "Optional connection string for snapshot catalog store");
DEFINE_string(snapshot_catalog_backend_connstring, "",
              "Deprecated alias of --snapshot_catalog_store_connstring");
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
DEFINE_uint32(max_retry_attempts, 10,
              "Maximum number of retry attempts for failed tasks");

DEFINE_string(cxl_path, mooncake::DEFAULT_CXL_PATH,
              "DAX device path for CXL memory");
DEFINE_uint64(cxl_size, mooncake::DEFAULT_CXL_SIZE, "CXL memory size in bytes");
DEFINE_bool(enable_cxl, false, "Whether to enable CXL memory support");

namespace {

std::string ResolveHABackendConnstring(
    const mooncake::MasterConfig& master_config) {
    if (!master_config.ha_backend_connstring.empty()) {
        return master_config.ha_backend_connstring;
    }
    return master_config.etcd_endpoints;
}

void ResolveRpcAddressFromInterfaceOrDie(
    mooncake::MasterConfig& master_config) {
    if (master_config.rpc_interface.empty()) {
        return;
    }

    if (!master_config.rpc_address.empty() &&
        master_config.rpc_address != "0.0.0.0") {
        LOG(WARNING) << "rpc_interface is set. Overriding rpc_address="
                     << master_config.rpc_address;
    }

    auto resolved_address =
        mooncake::GetInterfaceIPv4Address(master_config.rpc_interface);
    if (!resolved_address) {
        LOG(FATAL) << "Failed to resolve rpc_interface="
                   << master_config.rpc_interface << ": "
                   << resolved_address.error();
    }

    master_config.rpc_address = resolved_address.value();
    LOG(INFO) << "Resolved rpc_interface=" << master_config.rpc_interface
              << " to rpc_address=" << master_config.rpc_address;
}

}  // namespace

void InitMasterConf(const mooncake::DefaultConfig& default_config,
                    mooncake::MasterConfig& master_config) {
    // Initialize the master service configuration from the default config
    default_config.GetBool("enable_cxl", &master_config.enable_cxl,
                           FLAGS_enable_cxl);
    default_config.GetString("cxl_path", &master_config.cxl_path,
                             FLAGS_cxl_path);
    default_config.GetUInt64("cxl_size", &master_config.cxl_size,
                             FLAGS_cxl_size);
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
    default_config.GetString("rpc_interface", &master_config.rpc_interface,
                             FLAGS_rpc_interface);
    default_config.GetInt32("rpc_conn_timeout_seconds",
                            &master_config.rpc_conn_timeout_seconds,
                            FLAGS_rpc_conn_timeout_seconds);
    default_config.GetBool("rpc_enable_tcp_no_delay",
                           &master_config.rpc_enable_tcp_no_delay,
                           FLAGS_rpc_enable_tcp_no_delay);
    default_config.GetDurationMs("default_kv_lease_ttl",
                                 &master_config.default_kv_lease_ttl,
                                 mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL);
    default_config.GetDurationMs("default_kv_soft_pin_ttl",
                                 &master_config.default_kv_soft_pin_ttl,
                                 mooncake::DEFAULT_KV_SOFT_PIN_TTL_MS);
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
    default_config.GetString("ha_backend_type", &master_config.ha_backend_type,
                             FLAGS_ha_backend_type);
    default_config.GetString("ha_backend_connstring",
                             &master_config.ha_backend_connstring,
                             FLAGS_ha_backend_connstring);
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
    default_config.GetString("allocation_strategy",
                             &master_config.allocation_strategy,
                             FLAGS_allocation_strategy);
    default_config.GetBool("enable_http_metadata_server",
                           &master_config.enable_http_metadata_server,
                           FLAGS_enable_http_metadata_server);
    default_config.GetUInt32("http_metadata_server_port",
                             &master_config.http_metadata_server_port,
                             FLAGS_http_metadata_server_port);
    default_config.GetString("http_metadata_server_host",
                             &master_config.http_metadata_server_host,
                             FLAGS_http_metadata_server_host);
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

    default_config.GetString("snapshot_backup_dir",
                             &master_config.snapshot_backup_dir,
                             FLAGS_snapshot_backup_dir);
    default_config.GetBool("enable_snapshot_restore",
                           &master_config.enable_snapshot_restore,
                           FLAGS_enable_snapshot_restore);
    default_config.GetBool("enable_snapshot", &master_config.enable_snapshot,
                           FLAGS_enable_snapshot);
    default_config.GetUInt64("snapshot_interval_seconds",
                             &master_config.snapshot_interval_seconds,
                             FLAGS_snapshot_interval_seconds);
    default_config.GetUInt64("snapshot_child_timeout_seconds",
                             &master_config.snapshot_child_timeout_seconds,
                             FLAGS_snapshot_child_timeout_seconds);
    default_config.GetUInt32("snapshot_retention_count",
                             &master_config.snapshot_retention_count,
                             FLAGS_snapshot_retention_count);
    default_config.GetString("snapshot_object_store_type",
                             &master_config.snapshot_object_store_type,
                             FLAGS_snapshot_object_store_type);
    if (master_config.snapshot_object_store_type.empty()) {
        default_config.GetString("snapshot_payload_store_type",
                                 &master_config.snapshot_object_store_type,
                                 master_config.snapshot_object_store_type);
    }
    if (master_config.snapshot_object_store_type.empty()) {
        default_config.GetString("snapshot_payload_backend_type",
                                 &master_config.snapshot_object_store_type,
                                 master_config.snapshot_object_store_type);
    }
    default_config.GetString("snapshot_catalog_store_type",
                             &master_config.snapshot_catalog_store_type,
                             FLAGS_snapshot_catalog_store_type);
    if (master_config.snapshot_catalog_store_type.empty()) {
        default_config.GetString("snapshot_catalog_backend_type",
                                 &master_config.snapshot_catalog_store_type,
                                 master_config.snapshot_catalog_store_type);
    }
    default_config.GetString("snapshot_catalog_store_connstring",
                             &master_config.snapshot_catalog_store_connstring,
                             FLAGS_snapshot_catalog_store_connstring);
    if (master_config.snapshot_catalog_store_connstring.empty()) {
        default_config.GetString(
            "snapshot_catalog_backend_connstring",
            &master_config.snapshot_catalog_store_connstring,
            master_config.snapshot_catalog_store_connstring);
    }
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
    default_config.GetUInt32("max_retry_attempts",
                             &master_config.max_retry_attempts,
                             FLAGS_max_retry_attempts);
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
    if ((google::GetCommandLineFlagInfo("enable_cxl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_cxl = FLAGS_enable_cxl;
    }
    if ((google::GetCommandLineFlagInfo("cxl_path", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cxl_path = FLAGS_cxl_path;
    }
    if ((google::GetCommandLineFlagInfo("cxl_size", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.cxl_size = FLAGS_cxl_size;
    }
    if ((google::GetCommandLineFlagInfo("rpc_address", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_address = FLAGS_rpc_address;
    }
    if ((google::GetCommandLineFlagInfo("rpc_interface", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.rpc_interface = FLAGS_rpc_interface;
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
        master_config.default_kv_lease_ttl = ParseDurationFlagOrDie(
            "default_kv_lease_ttl", FLAGS_default_kv_lease_ttl);
    }
    if ((google::GetCommandLineFlagInfo("default_kv_soft_pin_ttl", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.default_kv_soft_pin_ttl = ParseDurationFlagOrDie(
            "default_kv_soft_pin_ttl", FLAGS_default_kv_soft_pin_ttl);
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
    if ((google::GetCommandLineFlagInfo("ha_backend_type", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.ha_backend_type = FLAGS_ha_backend_type;
    }
    if ((google::GetCommandLineFlagInfo("ha_backend_connstring", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.ha_backend_connstring = FLAGS_ha_backend_connstring;
    }
    if ((google::GetCommandLineFlagInfo("etcd_endpoints", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.etcd_endpoints = FLAGS_etcd_endpoints;
    }
    if ((google::GetCommandLineFlagInfo("client_ttl", &info) &&
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
    if ((google::GetCommandLineFlagInfo("allocation_strategy", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.allocation_strategy = FLAGS_allocation_strategy;
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
    if ((google::GetCommandLineFlagInfo("max_retry_attempts", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.max_retry_attempts = FLAGS_max_retry_attempts;
    }
    if ((google::GetCommandLineFlagInfo("enable_snapshot", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_snapshot = FLAGS_enable_snapshot;
    }
    if ((google::GetCommandLineFlagInfo("enable_snapshot_restore", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.enable_snapshot_restore = FLAGS_enable_snapshot_restore;
    }
    if ((google::GetCommandLineFlagInfo("snapshot_interval_seconds", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.snapshot_interval_seconds =
            FLAGS_snapshot_interval_seconds;
    }
    if ((google::GetCommandLineFlagInfo("snapshot_child_timeout_seconds",
                                        &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.snapshot_child_timeout_seconds =
            FLAGS_snapshot_child_timeout_seconds;
    }
    if ((google::GetCommandLineFlagInfo("snapshot_retention_count", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.snapshot_retention_count = FLAGS_snapshot_retention_count;
    }
    if ((google::GetCommandLineFlagInfo("snapshot_backup_dir", &info) &&
         !info.is_default) ||
        !conf_set) {
        master_config.snapshot_backup_dir = FLAGS_snapshot_backup_dir;
    }
    bool use_snapshot_object_store_flag = false;
    bool use_snapshot_payload_store_flag = false;
    bool use_snapshot_payload_backend_flag = false;
    if (google::GetCommandLineFlagInfo("snapshot_object_store_type", &info) &&
        !info.is_default) {
        use_snapshot_object_store_flag = true;
    }
    if (google::GetCommandLineFlagInfo("snapshot_payload_store_type", &info) &&
        !info.is_default) {
        use_snapshot_payload_store_flag = true;
    }
    if (google::GetCommandLineFlagInfo("snapshot_payload_backend_type",
                                       &info) &&
        !info.is_default) {
        use_snapshot_payload_backend_flag = true;
    }
    if (use_snapshot_object_store_flag) {
        master_config.snapshot_object_store_type =
            FLAGS_snapshot_object_store_type;
    } else if (use_snapshot_payload_store_flag) {
        LOG(WARNING) << "--snapshot_payload_store_type is deprecated; use "
                     << "--snapshot_object_store_type instead";
        master_config.snapshot_object_store_type =
            FLAGS_snapshot_payload_store_type;
    } else if (use_snapshot_payload_backend_flag) {
        LOG(WARNING) << "--snapshot_payload_backend_type is deprecated; use "
                     << "--snapshot_object_store_type instead";
        master_config.snapshot_object_store_type =
            FLAGS_snapshot_payload_backend_type;
    } else if (!conf_set) {
        master_config.snapshot_object_store_type =
            FLAGS_snapshot_object_store_type;
    }
    bool use_snapshot_catalog_store_flag = false;
    bool use_snapshot_catalog_backend_flag = false;
    if (google::GetCommandLineFlagInfo("snapshot_catalog_store_type", &info) &&
        !info.is_default) {
        use_snapshot_catalog_store_flag = true;
    }
    if (google::GetCommandLineFlagInfo("snapshot_catalog_backend_type",
                                       &info) &&
        !info.is_default) {
        use_snapshot_catalog_backend_flag = true;
    }
    if (use_snapshot_catalog_store_flag) {
        master_config.snapshot_catalog_store_type =
            FLAGS_snapshot_catalog_store_type;
    } else if (use_snapshot_catalog_backend_flag) {
        LOG(WARNING) << "--snapshot_catalog_backend_type is deprecated; use "
                     << "--snapshot_catalog_store_type instead";
        master_config.snapshot_catalog_store_type =
            FLAGS_snapshot_catalog_backend_type;
    } else if (!conf_set) {
        master_config.snapshot_catalog_store_type =
            FLAGS_snapshot_catalog_store_type;
    }
    bool use_snapshot_catalog_store_connstring_flag = false;
    bool use_snapshot_catalog_backend_connstring_flag = false;
    if (google::GetCommandLineFlagInfo("snapshot_catalog_store_connstring",
                                       &info) &&
        !info.is_default) {
        use_snapshot_catalog_store_connstring_flag = true;
    }
    if (google::GetCommandLineFlagInfo("snapshot_catalog_backend_connstring",
                                       &info) &&
        !info.is_default) {
        use_snapshot_catalog_backend_connstring_flag = true;
    }
    if (use_snapshot_catalog_store_connstring_flag) {
        master_config.snapshot_catalog_store_connstring =
            FLAGS_snapshot_catalog_store_connstring;
    } else if (use_snapshot_catalog_backend_connstring_flag) {
        LOG(WARNING)
            << "--snapshot_catalog_backend_connstring is deprecated; use "
            << "--snapshot_catalog_store_connstring instead";
        master_config.snapshot_catalog_store_connstring =
            FLAGS_snapshot_catalog_backend_connstring;
    } else if (!conf_set) {
        master_config.snapshot_catalog_store_connstring =
            FLAGS_snapshot_catalog_store_connstring;
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
    ResolveRpcAddressFromInterfaceOrDie(master_config);

    const std::string ha_backend_connstring =
        ResolveHABackendConnstring(master_config);
    if (master_config.enable_ha && ha_backend_connstring.empty()) {
        LOG(FATAL) << "HA backend connection string must be set when "
                   << "enable_ha is true";
        return 1;
    }
    if (!master_config.enable_ha && (!ha_backend_connstring.empty() ||
                                     !master_config.etcd_endpoints.empty())) {
        LOG(WARNING)
            << "HA backend connection string is set but will not be used in "
            << "non-HA mode";
    }
    if (!master_config.ha_backend_connstring.empty() &&
        !master_config.etcd_endpoints.empty() &&
        master_config.ha_backend_connstring != master_config.etcd_endpoints) {
        LOG(WARNING) << "Both ha_backend_connstring and etcd_endpoints are "
                     << "set. Using ha_backend_connstring="
                     << master_config.ha_backend_connstring
                     << " for HA coordinator setup";
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
        << ", ha_backend_type=" << master_config.ha_backend_type
        << ", ha_backend_connstring=" << ha_backend_connstring
        << ", etcd_endpoints=" << master_config.etcd_endpoints
        << ", client_ttl=" << master_config.client_live_ttl_sec
        << ", rpc_thread_num=" << master_config.rpc_thread_num
        << ", rpc_port=" << master_config.rpc_port
        << ", rpc_address=" << master_config.rpc_address
        << ", rpc_interface=" << master_config.rpc_interface
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
        << master_config.processing_task_timeout_sec
        << ", enable_snapshot=" << master_config.enable_snapshot
        << ", enable_snapshot_restore=" << master_config.enable_snapshot_restore
        << ", snapshot_interval_seconds="
        << master_config.snapshot_interval_seconds
        << ", snapshot_backup_dir=" << master_config.snapshot_backup_dir
        << ", snapshot_object_store_type="
        << master_config.snapshot_object_store_type
        << ", snapshot_catalog_store_type="
        << master_config.snapshot_catalog_store_type
        << ", snapshot_retention_count="
        << master_config.snapshot_retention_count
        << ", max_retry_attempts=" << master_config.max_retry_attempts
        << ", enable_cxl=" << master_config.enable_cxl
        << ", cxl_path=" << master_config.cxl_path
        << ", cxl_size=" << master_config.cxl_size;

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
        mooncake::ha::MasterServiceSupervisor supervisor(
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
        mooncake::WrappedMasterService wrapped_master_service(
            mooncake::WrappedMasterServiceConfig(master_config, version));

        mooncake::RegisterRpcService(server, wrapped_master_service);
        return server.start();
    }
}
