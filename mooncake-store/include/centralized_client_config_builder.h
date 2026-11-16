#pragma once

#include <ylt/util/tl/expected.hpp>

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
    static tl::expected<CentralizedClientConfig, ErrorCode>
    build_centralized_real_client(
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
        if (local_hostname.empty()) {
            LOG(ERROR) << "local_hostname is empty";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        CentralizedClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, labels);
        if (config.rdma_devices && (config.rdma_devices->empty() ||
                                    *config.rdma_devices == "auto-discovery")) {
            config.rdma_devices = std::nullopt;
        }
        config.global_segment_size = global_segment_size;
        config.enable_offload = enable_offload;
        config.client_rpc_port = client_rpc_port;
        return config;
    }

    static tl::expected<CentralizedClientConfig, ErrorCode>
    build_centralized_real_client(
        const std::unordered_map<std::string, std::string>& config) {
        auto local_hostname =
            get_config_str(config, DictCommon::kLocalHostname);
        auto metadata_server =
            get_config_str(config, DictCommon::kMetadataServer);
        auto global_size =
            get_config_size(config, DictCentralized::kGlobalSegmentSize,
                            DictCentralized::kDefaultGlobalSegmentSize);
        auto local_size = get_config_size(config, DictCommon::kLocalBufferSize,
                                          DictCommon::kDefaultLocalBufferSize);
        if (!global_size || !local_size) {
            LOG(ERROR) << "Failed to parse centralized buffer sizes";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto devices = get_config_str(config, DictCommon::kRdmaDevices);
        auto client_port =
            get_config_integer<uint16_t>(config, "client_rpc_port", 0);
        if (!client_port) {
            LOG(ERROR) << "Failed to parse centralized client_rpc_port";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto result = build_centralized_real_client(
            local_hostname, metadata_server,
            get_config_str(config, DictCommon::kProtocol,
                           DictCommon::kDefaultProtocol),
            devices.empty() ? std::nullopt
                            : std::optional<std::string>(devices),
            get_config_str(config, DictCommon::kMasterServerAddr,
                           DictCommon::kDefaultMasterServerAddr),
            *global_size, *local_size, nullptr,
            get_config_str(config, DictCommon::kIpcSocketPath),
            get_config_bool(config, "enable_offload", false), {}, *client_port);
        if (!result) {
            LOG(ERROR) << "Centralized dict configuration is invalid: "
                       << result.error();
        }
        return result;
    }

   private:
    struct DictCentralized {
        // Keys
        static constexpr const char* kGlobalSegmentSize = "global_segment_size";
        // Defaults
        static constexpr size_t kDefaultGlobalSegmentSize = 1024 * 1024 * 16;
    };
};

}  // namespace mooncake
