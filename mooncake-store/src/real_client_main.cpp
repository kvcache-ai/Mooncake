#include "version.h"
#include <gflags/gflags.h>

#include "centralized_client_config_builder.h"
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
DEFINE_int32(threads, 1, "Number of rpc threads for dummy client");
DEFINE_bool(enable_offload, false, "Enable offload availability");
DEFINE_uint32(client_rpc_port, 0,
              "Native client-to-client RPC port; 0 selects an available port");

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
    const size_t global_segment_size =
        string_to_byte_size(FLAGS_global_segment_size);

    auto config = CentralizedClientConfigBuilder::build_centralized_real_client(
        FLAGS_host, FLAGS_metadata_server, FLAGS_protocol,
        FLAGS_device_names.empty()
            ? std::nullopt
            : std::optional<std::string>(FLAGS_device_names),
        FLAGS_master_server_address, global_segment_size, local_buffer_size,
        nullptr, "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
        FLAGS_enable_offload);
    config.client_rpc_port = static_cast<uint16_t>(FLAGS_client_rpc_port);

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(
        config, /*enable_dummy_service=*/true, FLAGS_threads, FLAGS_port);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    return client_inst->wait_dummy_rpc_server();
}
