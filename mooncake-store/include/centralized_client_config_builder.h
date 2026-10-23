#pragma once

#include "client_config_builder.h"

namespace mooncake {

struct CentralizedClientConfig : RealClientConfigBase {
    // Size of global segment to mount (0 to skip)
    uint64_t global_segment_size = 0;

    // Whether to enable file storage offloading.
    bool enable_offload = false;

    // Native client-to-client RPC listener; 0 requests an available port.
    uint16_t client_rpc_port = 0;
};

class CentralizedClientConfigBuilder : private ClientConfigBuilder {
   public:
    static CentralizedClientConfig build_centralized_real_client(
        const std::string& local_hostname,
        const std::string& metadata_connstring,
        const std::string& protocol = "tcp",
        const std::optional<std::string>& rdma_devices = std::nullopt,
        const std::string& master_server_entry = "127.0.0.1:50051",
        uint64_t global_segment_size = 0, uint64_t local_buffer_size = 0,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        const std::string& ipc_socket_path = "", bool enable_offload = false,
        const std::map<std::string, std::string>& labels = {},
        uint16_t client_rpc_port = 0) {
        CentralizedClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, labels);
        config.global_segment_size = global_segment_size;
        config.enable_offload = enable_offload;
        config.client_rpc_port = client_rpc_port;
        return config;
    }

    static CentralizedClientConfig build_centralized_real_client(
        const std::unordered_map<std::string, std::string>& config) {
        auto devices = get_config_str(config, DictCommon::kRdmaDevices);
        auto client_port = get_config_port(config, "client_rpc_port");
        return build_centralized_real_client(
            get_config_str(config, DictCommon::kLocalHostname),
            get_config_str(config, DictCommon::kMetadataServer),
            get_config_str(config, DictCommon::kProtocol,
                           DictCommon::kDefaultProtocol),
            devices.empty() ? std::nullopt
                            : std::optional<std::string>(devices),
            get_config_str(config, DictCommon::kMasterServerAddr,
                           DictCommon::kDefaultMasterServerAddr),
            get_config_size(config, DictCentralized::kGlobalSegmentSize,
                            DictCentralized::kDefaultGlobalSegmentSize),
            get_config_size(config, DictCommon::kLocalBufferSize,
                            DictCommon::kDefaultLocalBufferSize),
            nullptr, get_config_str(config, DictCommon::kIpcSocketPath),
            get_config_bool(config, "enable_offload", false), {}, client_port);
    }

   private:
    struct DictCentralized {
        // Keys
        static constexpr const char* kGlobalSegmentSize = "global_segment_size";
        // Defaults
        static constexpr size_t kDefaultGlobalSegmentSize = 1024 * 1024 * 16;
    };

    static uint16_t get_config_port(
        const std::unordered_map<std::string, std::string>& config,
        const std::string& key) {
        auto it = config.find(key);
        if (it == config.end()) {
            return 0;
        }
        const auto& value = it->second;
        try {
            size_t parsed = 0;
            auto port = std::stoul(value, &parsed);
            if (value.empty() || value.front() == '-' ||
                parsed != value.size() || port > UINT16_MAX) {
                throw std::invalid_argument("out of range or malformed port");
            }
            return static_cast<uint16_t>(port);
        } catch (const std::exception& error) {
            LOG(ERROR) << "Invalid " << key << ": " << error.what();
            throw std::invalid_argument("Invalid " + key);
        }
    }
};

}  // namespace mooncake
