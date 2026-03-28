#include <gflags/gflags.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <variant>
#include "client_config_builder.h"
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
DEFINE_string(tiered_backend_config, "",
              "Tiered backend config json. Empty means load from env "
              "MOONCAKE_TIERED_CONFIG");
DEFINE_string(deployment_mode, "Centralization",
              "Client type: 'Centralization' or 'P2P'");
DEFINE_uint32(client_rpc_port, 12345, "Client RPC service port (P2P mode)");
DEFINE_uint32(rpc_thread_num, 16, "Number of threads for P2P RPC service");
DEFINE_uint64(lock_shard_count, 1024,
              "Number of key lock shards for DataManager");
DEFINE_string(route_cache_max_memory, "300 MB", "Max memory for RouteCache");
DEFINE_uint64(route_cache_ttl_ms, 5 * 60 * 1000,
              "TTL for RouteCache entries in ms");

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              RealClient& real_client) {
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
}
}  // namespace mooncake

int main(int argc, char* argv[]) {
    // Attention !!!
    // Initialization of ResourceTracker must be the most earliest.
    // Otherwise, the main thread will not apply signal mask before other
    // spawning threads, leading to missing signal processing.
    mooncake::ResourceTracker::getInstance();

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // when separately deploy real client,
    // local buffer is shared by dummy client,
    // real client does not have local buffer
    const uint64_t local_buffer_size = 0;
    const size_t global_segment_size =
        string_to_byte_size(FLAGS_global_segment_size);

    auto config =
        [&]() -> std::variant<CentralizedClientConfig, P2PClientConfig> {
        if (FLAGS_deployment_mode == "P2P") {
            LOG(INFO) << "Using P2P client type"
                      << ", client_rpc_port=" << FLAGS_client_rpc_port;
            return ClientConfigBuilder::build_p2p_real_client(
                FLAGS_host, FLAGS_metadata_server, FLAGS_protocol,
                FLAGS_device_names.empty()
                    ? std::nullopt
                    : std::optional<std::string>(FLAGS_device_names),
                FLAGS_master_server_address, FLAGS_tiered_backend_config,
                local_buffer_size, nullptr,
                "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
                static_cast<uint16_t>(FLAGS_client_rpc_port),
                static_cast<uint32_t>(FLAGS_rpc_thread_num),
                FLAGS_lock_shard_count,
                string_to_byte_size(FLAGS_route_cache_max_memory),
                FLAGS_route_cache_ttl_ms);
        } else {
            if (FLAGS_deployment_mode != "Centralization") {
                LOG(WARNING)
                    << "Unknown deployment_mode '" << FLAGS_deployment_mode
                    << "', defaulting to Centralization";
            }
            return ClientConfigBuilder::build_centralized_real_client(
                FLAGS_host, FLAGS_metadata_server, FLAGS_protocol,
                FLAGS_device_names.empty()
                    ? std::nullopt
                    : std::optional<std::string>(FLAGS_device_names),
                FLAGS_master_server_address, global_segment_size,
                local_buffer_size, nullptr,
                "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
                FLAGS_enable_offload);
        }
    }();

    auto client_inst = RealClient::create();
    auto res = std::visit(
        [&](auto& cfg) { return client_inst->setup_internal(cfg); }, config);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, "127.0.0.1");
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Starting real client service on 127.0.0.1:" << FLAGS_port;

    return server.start();
}
