#include "version.h"
#include <gflags/gflags.h>

#include "p2p/client/p2p_client_config_builder.h"
#include "real_client.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server connection string");
DEFINE_string(device_names, "", "Device names");
DEFINE_string(master_server_address, "127.0.0.1:50051",
              "Master server address");
DEFINE_string(protocol, "tcp", "Protocol");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(redis_cluster_id, mooncake::DEFAULT_CLUSTER_ID,
              "Redis HA cluster ID for redis:// master discovery");
DEFINE_string(redis_username, "",
              "Redis ACL username for redis:// master discovery");
DEFINE_string(redis_password, "",
              "Redis AUTH password for redis:// master discovery");
DEFINE_int32(redis_db_index, 0,
             "Redis database index for redis:// master discovery");
DEFINE_int32(redis_master_view_ttl_sec, 4,
             "Redis master view TTL for redis:// master discovery");
DEFINE_int32(redis_heartbeat_interval_sec, 1,
             "Redis heartbeat interval for redis:// master discovery");
DEFINE_uint32(
    heartbeat_rpc_port, 0,
    "Port of the master's dedicated heartbeat RPC server. When > 0, "
    "heartbeats are sent to <master host>:heartbeat_rpc_port instead of "
    "the main RPC port so heavy metadata RPCs cannot head-of-line-block "
    "them. Must match the master's --heartbeat_rpc_port. 0 = disabled.");
DEFINE_int32(threads, 1, "Number of rpc threads for dummy client");
DEFINE_string(tiered_backend_config, "conf/tiered_backend.json",
              "Tiered backend config: accepts a JSON string or a path to a "
              "JSON config file.");
DEFINE_uint32(client_rpc_port, 12345, "Client RPC service port (P2P mode)");
DEFINE_uint32(rpc_thread_num, 16, "Number of threads for P2P RPC service");
DEFINE_uint64(lock_shard_count, 1024,
              "Number of metadata shards in TieredBackend (and matching "
              "pending-write/pinned-key lease shards in DataManager)");
DEFINE_string(route_cache_max_memory, "300 MB", "Max memory for RouteCache");
DEFINE_uint64(route_cache_ttl_ms, 60 * 1000,
              "TTL for RouteCache entries in ms");
DEFINE_uint64(async_sender_thread_count, 4,
              "Async route notifier sender thread count. "
              "0=disabled (sync RPCs), >0=enable async notifier");
DEFINE_uint64(async_max_batch_size, 2000,
              "Max ops per batch in async route notifier.");
DEFINE_uint64(async_route_queue_size, 0,
              "Async route notifier queue size when async is enabled "
              "(min='async_max_batch_size * async_sender_thread_count').");
DEFINE_bool(
    p2p_start_local_only, false,
    "Start P2P locally without contacting Master; join via POST /register.");
DEFINE_string(p2p_local_transfer_mode, "te",
              "Local transfer mode for P2P local Get/Put path: memcpy|te");
DEFINE_string(p2p_transfer_direction_mode, "reverse",
              "Cross-node transfer direction for P2P: reverse|forward");
DEFINE_uint64(local_memcpy_async_worker_num, 32,
              "If set p2p_local_transfer_mode=memcpy, Worker number for async "
              "local memcpy executor (P2P), 0 means forbid async memcpy");
DEFINE_uint64(te_async_poll_worker_num, 32,
              "Worker number for async TE batch polling in DataManager (P2P), "
              "0 means synchronous TE wait");
DEFINE_uint32(http_port, 9003, "Port for client HTTP server");
DEFINE_bool(enable_http_server, true, "Enable client HTTP server");
DEFINE_uint32(
    p2p_key_lease_duration_ms, 0,
    "Maximum time (ms) a key may remain in PreWrite or PinKey "
    "lease-protected intermediate state. 0 uses the built-in default (5000).");
DEFINE_uint32(
    p2p_key_lease_scan_interval_ms, 0,
    "Interval (ms) for background scanning of expired key leases (PreWrite / "
    "PinKey). 0 uses the built-in default (1000).");
DEFINE_string(runtime_config, "",
              "Runtime read/write config: a JSON string or a path to a JSON "
              "file. Can also be set via env MC_RUNTIME_CONFIG. For example,"
              "conf/p2p_runtime_config.json.");
DEFINE_bool(enable_client_metric_collection, true,
            "Enable client metric collection. When false, no metrics are "
            "collected and the /metrics endpoint is unavailable.");
DEFINE_uint64(metric_report_interval_seconds, 60,
              "Interval (seconds) for periodic client metric reporting. "
              "0 disables periodic reporting (metrics are still collected).");

int main(int argc, char* argv[]) {
    gflags::SetVersionString(mooncake::GetMooncakeStoreBuildInfo());
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Attention !!!
    // Initialize ResourceTracker before any business threads are started.
    // Otherwise, the main thread will not apply signal mask before other
    // spawning threads, leading to missing signal processing.
    mooncake::ResourceTracker::getInstance();

    // when separately deploy real client,
    // local buffer is shared by dummy client,
    // real client does not have local buffer
    const uint64_t local_buffer_size = 0;

    LOG(INFO) << "Using P2P client type"
              << ", client_rpc_port=" << FLAGS_client_rpc_port;

    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        FLAGS_host, FLAGS_metadata_server, FLAGS_protocol,
        FLAGS_device_names.empty()
            ? std::nullopt
            : std::optional<std::string>(FLAGS_device_names),
        FLAGS_master_server_address, FLAGS_tiered_backend_config,
        local_buffer_size, nullptr,
        "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
        static_cast<uint16_t>(FLAGS_client_rpc_port),
        static_cast<uint32_t>(FLAGS_rpc_thread_num), FLAGS_lock_shard_count,
        string_to_byte_size(FLAGS_route_cache_max_memory),
        FLAGS_route_cache_ttl_ms, FLAGS_p2p_local_transfer_mode,
        static_cast<size_t>(FLAGS_local_memcpy_async_worker_num),
        static_cast<uint16_t>(FLAGS_http_port), FLAGS_enable_http_server,
        {},  // labels
        FLAGS_async_sender_thread_count, FLAGS_async_max_batch_size,
        FLAGS_async_route_queue_size, FLAGS_p2p_key_lease_duration_ms,
        FLAGS_p2p_key_lease_scan_interval_ms, FLAGS_p2p_transfer_direction_mode,
        FLAGS_runtime_config, FLAGS_enable_client_metric_collection,
        FLAGS_metric_report_interval_seconds, FLAGS_redis_cluster_id,
        FLAGS_redis_password, FLAGS_redis_db_index,
        FLAGS_redis_master_view_ttl_sec, FLAGS_redis_heartbeat_interval_sec,
        FLAGS_redis_username, static_cast<uint16_t>(FLAGS_heartbeat_rpc_port),
        static_cast<size_t>(FLAGS_te_async_poll_worker_num),
        FLAGS_p2p_start_local_only);

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(
        config, /*enable_dummy_service=*/true, FLAGS_threads, FLAGS_port);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    return client_inst->wait_dummy_rpc_server();
}
