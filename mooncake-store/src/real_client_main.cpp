#include <gflags/gflags.h>
#include <csignal>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "common.h"
#include "config.h"
#include "real_client.h"

#ifdef STORE_USE_ETCD
#include "libetcd_wrapper.h"
#include "etcd_helper.h"
#endif

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

#ifdef STORE_USE_ETCD
DEFINE_string(etcd_ca_file, "", "Path to CA certificate file for etcd TLS");
DEFINE_string(etcd_cert_file, "",
              "Path to client certificate file for etcd TLS");
DEFINE_string(etcd_key_file, "", "Path to client key file for etcd TLS");
#endif

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

#ifdef STORE_USE_ETCD
    if (!FLAGS_etcd_ca_file.empty() || !FLAGS_etcd_cert_file.empty() ||
        !FLAGS_etcd_key_file.empty()) {
        // Configure TLS for transfer engine's etcd client (globalClient)
        SetGlobalTLSConfig(const_cast<char *>(FLAGS_etcd_ca_file.c_str()),
                           const_cast<char *>(FLAGS_etcd_cert_file.c_str()),
                           const_cast<char *>(FLAGS_etcd_key_file.c_str()));
        // Configure TLS for HA backend's etcd store client (storeClient)
        mooncake::EtcdHelper::SetTLSConfig(
            FLAGS_etcd_ca_file, FLAGS_etcd_cert_file, FLAGS_etcd_key_file);
        LOG(INFO) << "etcd TLS configured for client (ca=" << FLAGS_etcd_ca_file
                  << ")";
    }
#endif

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(
        FLAGS_host, FLAGS_metadata_server, global_segment_size,
        local_buffer_size, FLAGS_protocol, FLAGS_device_names,
        FLAGS_master_server_address, nullptr,
        "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock", FLAGS_port,
        FLAGS_enable_offload, FLAGS_start_offload_rpc_server, "",
        FLAGS_tenant_id);
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
