#include <gflags/gflags.h>
#include <csignal>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
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
DEFINE_string(global_segment_size, "4 GB", "Size of global segment");
DEFINE_int32(threads, 1, "Number of threads for client service");
DEFINE_bool(enable_offload, false, "Enable offload availability");

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server &server,
                              RealClient &real_client) {
    server.register_handler<&RealClient::put_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::get_buffer_info_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
    server.register_handler<&RealClient::ping>(&real_client);
    server.register_handler<&RealClient::create_copy_task>(&real_client);
    server.register_handler<&RealClient::create_move_task>(&real_client);
    server.register_handler<&RealClient::query_task>(&real_client);
    server.register_handler<&RealClient::batch_get_offload_object>(
        &real_client);
    server.register_handler<&RealClient::release_offload_buffer>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_offload_object_internal>(
        &real_client);
}
}  // namespace mooncake

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    size_t global_segment_size = string_to_byte_size(FLAGS_global_segment_size);

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(
        FLAGS_host, FLAGS_metadata_server, global_segment_size, 0,
        FLAGS_protocol, FLAGS_device_names, FLAGS_master_server_address,
        nullptr, "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
        FLAGS_port, FLAGS_enable_offload);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    if (client_inst->start_dummy_client_monitor()) {
        LOG(FATAL) << "Failed to start dummy client monitor thread";
        return -1;
    }

    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, "127.0.0.1");
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Starting real client service on 127.0.0.1:" << FLAGS_port;

    return server.start();
}
