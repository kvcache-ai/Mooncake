#include <gflags/gflags.h>
#include <csignal>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "common.h"
#include "config.h"
#include "real_client.h"
#include "p2p/client/p2p_client_service.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server connection string");
DEFINE_string(device_names, "", "Device names");
DEFINE_string(master_server_address, "127.0.0.1:50051",
              "Master server address");
DEFINE_string(protocol, "tcp", "Protocol");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(global_segment_size, "4 GB", "Size of global segment");
DEFINE_string(local_buffer_size, "0", "Size of local buffer (e.g., 16MB, 1GB)");
DEFINE_int32(threads, 1, "Number of threads for client service");
DEFINE_string(tenant_id, "default", "Tenant identifier");
DEFINE_bool(enable_offload, false, "Enable offload availability");
DEFINE_bool(start_offload_rpc_server, true,
            "Expose TCP RPC for disk-tier reads "
            "(batch_get_offload_object / release_offload_buffer). "
            "Effective only when --enable_offload is true. "
            "Disable for a write-only owner.");
DECLARE_bool(enable_http_server);
DECLARE_int32(http_port);

DEFINE_string(deployment_mode, "Centralization",
              "Client type: 'Centralization' or 'P2P'");
DEFINE_uint32(client_rpc_port, 12345, "Client RPC service port (P2P mode)");
DEFINE_uint32(rpc_thread_num, 16, "Number of threads for P2P RPC service");
DEFINE_uint64(lock_shard_count, 1024, "Lock shard count (P2P mode)");
DEFINE_string(route_cache_max_memory, "300 MB", "Max memory for RouteCache");
DEFINE_uint64(route_cache_ttl_ms, 60000, "TTL for RouteCache entries in ms");
DEFINE_string(p2p_local_transfer_mode, "te", "Local transfer mode for P2P");
DEFINE_string(p2p_transfer_direction_mode, "reverse", "Cross-node transfer direction");
DEFINE_uint64(local_memcpy_async_worker_num, 32, "Async memcpy workers");
DEFINE_string(tiered_backend_config, "", "Tiered backend config");
DEFINE_uint64(async_sender_thread_count, 4, "Async route notifier sender threads");
DEFINE_uint64(async_max_batch_size, 2000, "Max ops per batch");
DEFINE_uint64(async_route_queue_size, 0, "Async route notifier queue size");
DEFINE_uint64(p2p_key_lease_duration_ms, 0, "Key lease duration ms");
DEFINE_uint64(p2p_key_lease_scan_interval_ms, 0, "Key lease scan interval ms");
DEFINE_bool(enable_client_metric_collection, false, "Enable client metric collection");
DEFINE_uint32(metric_report_interval_seconds, 5, "Metric report interval seconds");
DEFINE_string(runtime_config, "", "Runtime config JSON");

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server &server,
                              RealClient &real_client) {
    server.register_handler<&RealClient::put_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::batchRemove_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        &real_client);
    server.register_handler<
        &RealClient::batch_put_from_multi_buffers_dummy_helper>(&real_client);
    server.register_handler<&RealClient::batch_put_from_cuda_ipc_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_dummy_helper>(&real_client);
    server.register_handler<&RealClient::upsert_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_parts_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_upsert_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::upsert_batch_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        &real_client);
    server.register_handler<
        &RealClient::batch_get_into_multi_buffers_dummy_helper>(&real_client);
    server.register_handler<&RealClient::batch_get_into_cuda_ipc_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::get_into_range_shm_helper>(
        &real_client);
    server.register_handler<&RealClient::get_into_ranges_shm_helper>(
        &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_ipc_shm_internal>(&real_client);
    server.register_handler<&RealClient::ascend_unmap_shm_internal>(
        &real_client);
    server.register_handler<&RealClient::is_shm_mapped_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
    server.register_handler<&RealClient::ping>(&real_client);
    server.register_handler<&RealClient::acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::release_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_release_hot_cache>(&real_client);
    server.register_handler<&RealClient::acquire_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::release_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::batch_acquire_buffer_dummy>(
        &real_client);
    server.register_handler<&RealClient::allocate_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::create_copy_task>(&real_client);
    server.register_handler<&RealClient::create_move_task>(&real_client);
    server.register_handler<&RealClient::query_task>(&real_client);
    server.register_handler<&RealClient::batch_get_offload_object>(
        &real_client);
    server.register_handler<&RealClient::release_offload_buffer>(&real_client);
}
}  // namespace mooncake

int main(int argc, char *argv[]) {
    // Attention !!!
    // Initialization of ResourceTracker must be the most earliest.
    // Otherwise, the main thread will not apply signal mask before other
    // spawning threads, leading to missing signal processing.
    mooncake::ResourceTracker::getInstance();

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }

    size_t global_segment_size = string_to_byte_size(FLAGS_global_segment_size);
    size_t local_buffer_size = string_to_byte_size(FLAGS_local_buffer_size);
#ifdef USE_ASCEND_DIRECT
    // just set to true, does not affect GPU process.
    globalConfig().ascend_agent_mode = true;
#endif

    auto client_inst = RealClient::create();

    if (FLAGS_deployment_mode == "P2P") {
        LOG(INFO) << "Using P2P client mode";
        auto res = client_inst->setup_p2p(
            FLAGS_host, FLAGS_metadata_server, FLAGS_protocol,
            FLAGS_device_names, FLAGS_master_server_address,
            FLAGS_tiered_backend_config,
            local_buffer_size,
            "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
            static_cast<uint16_t>(FLAGS_client_rpc_port),
            static_cast<uint32_t>(FLAGS_rpc_thread_num),
            FLAGS_lock_shard_count,
            string_to_byte_size(FLAGS_route_cache_max_memory),
            FLAGS_route_cache_ttl_ms,
            FLAGS_p2p_local_transfer_mode,
            static_cast<size_t>(FLAGS_local_memcpy_async_worker_num),
            static_cast<uint16_t>(FLAGS_http_port),
            FLAGS_enable_http_server,
            FLAGS_async_sender_thread_count,
            FLAGS_async_max_batch_size,
            FLAGS_async_route_queue_size,
            FLAGS_p2p_key_lease_duration_ms,
            FLAGS_p2p_key_lease_scan_interval_ms,
            FLAGS_p2p_transfer_direction_mode,
            FLAGS_runtime_config,
            FLAGS_enable_client_metric_collection,
            FLAGS_metric_report_interval_seconds);
        if (res != 0) {
            LOG(ERROR) << "Failed to setup P2P client";
            return -1;
        }
        return 0;
    }

    auto res = client_inst->setup_internal(
        FLAGS_host, FLAGS_metadata_server, global_segment_size,
        local_buffer_size, FLAGS_protocol, FLAGS_device_names,
        FLAGS_master_server_address, nullptr,
        "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock", FLAGS_port,
        FLAGS_enable_offload, FLAGS_start_offload_rpc_server, "",
        FLAGS_tenant_id, FLAGS_enable_http_server, FLAGS_http_port);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    if (client_inst->start_dummy_client_monitor()) {
        LOG(FATAL) << "Failed to start dummy client monitor thread";
        return -1;
    }

    auto rpc_bind_host = getHostNameWithoutPort(FLAGS_host);
    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, rpc_bind_host);
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Starting real client service on " << rpc_bind_host << ":"
              << FLAGS_port;

    return server.start();
}
