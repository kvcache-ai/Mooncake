#pragma once

#include <string>
#include <optional>
#include <map>
#include <memory>
#include <cstdint>

#include <glog/logging.h>
#include <json/json.h>
#include "common.h"

namespace mooncake {

class TransferEngine;

// ============================================================================
// Config classes
// ============================================================================

/**
 * @brief Configuration for a dummy client deployment.
 *
 * A dummy client communicates with a separately deployed real client via
 * shared memory and RPC. It does not directly interact with the master or
 * transfer engine.
 */
struct DummyClientConfig {
    // Size of the memory pool in bytes.
    size_t mem_pool_size = 0;

    // Size of the local buffer in bytes.
    // The local buffer will be registered as shm and shared to real client.
    size_t local_buffer_size = 0;

    // RPC connection string to real client ("ip:port").
    std::string real_client_addr;

    // The IPC socket path between dummy and real client.
    std::string ipc_socket_path;
};

/**
 * @brief Common base configuration shared by centralized and P2P real clients.
 *
 * Contains all fields needed for transfer engine initialization, master
 * connection, segment mounting, and local buffer registration.
 */
struct RealClientConfigBase {
    // Local IP address
    std::string local_ip;

    // Transfer engine port (0 means randomly assigned)
    uint16_t te_port = 0;

    /**
     * @brief Returns the "ip:port" endpoint string.
     */
    std::string local_endpoint() const {
        return local_ip + ":" + std::to_string(te_port);
    }

    // Connection string for metadata service
    std::string metadata_connstring;

    // Transport protocol (e.g., "tcp", "rdma", "ascend").
    std::string protocol = "tcp";

    // Comma-separated RDMA device names.
    // Optional with default auto-discovery.
    // Only required when auto-discovery is disabled
    // (set env `MC_MS_AUTO_DISC=0`).
    std::optional<std::string> rdma_devices = std::nullopt;

    // The entry of master server:
    // 1. "IP:Port" for non-HA mode
    // 2. "etcd://IP:Port;...;IP:Port" for HA mode
    std::string master_server_entry = "127.0.0.1:50051";

    // Size of the local buffer (0 to skip).
    // For the case which separately deploys real client and dummy client,
    // the `local_buffer_size` could be 0, which means the local buffer is
    // shared by dummy client.
    // For the case which integrates real client,
    // if the `local_buffer_size` is 0, some interfaces might fail to work.
    uint64_t local_buffer_size = 0;

    // Optional metric labels for the client
    std::map<std::string, std::string> labels = {};

    // Optional TransferEngine instance.
    // If not provided, it will be created by client_service.
    std::shared_ptr<TransferEngine> transfer_engine = nullptr;

    // The IPC socket path between dummy and real clients.
    // If use integrated deployment, this could be empty.
    std::string ipc_socket_path;
};

/**
 * @brief Configuration for a centralized real client.
 *
 * Inherits all common real client fields and adds centralized-specific options.
 */
struct CentralizedClientConfig : RealClientConfigBase {
    // Size of global segment to mount (0 to skip)
    uint64_t global_segment_size = 0;

    // Whether to enable file storage offloading.
    bool enable_offload = false;
};

/**
 * @brief Configuration for a P2P real client.
 *
 * Inherits all common real client fields and adds P2P-specific options.
 */
struct P2PClientConfig : RealClientConfigBase {
    // Port for P2P RPC service.
    uint16_t client_rpc_port = 12345;

    // Num threads for P2P RPC service.
    uint32_t rpc_thread_num = 2;

    // Parsed custom tiered backend configuration
    Json::Value tiered_backend_config;

    // Number of key lock shards for DataManager.
    // Higher values reduce contention of key.
    size_t lock_shard_count = 1024;

    // RouteCache configuration
    // each size of route entry is about 240B:
    // Aligned Node(64B) + hash_bucket(8B) + Key(assume 64B)
    // + P2PRouteData(each item is 96B and count is 8B)
    size_t route_cache_max_memory_bytes = 300 * 1024 * 1024;  // 300MB
    uint64_t route_cache_ttl_ms = 5 * 60 * 1000;              // 5min
};

// ============================================================================
// Factory class
// ============================================================================

/**
 * @brief Factory class for building typed client configurations.
 */
class ClientConfigBuilder {
   public:
    static DummyClientConfig build_dummy(size_t mem_pool_size,
                                         size_t local_buffer_size,
                                         const std::string& real_client_addr,
                                         const std::string& ipc_socket_path) {
        DummyClientConfig config;
        config.mem_pool_size = mem_pool_size;
        config.local_buffer_size = local_buffer_size;
        config.real_client_addr = real_client_addr;
        config.ipc_socket_path = ipc_socket_path;
        return config;
    }

    static CentralizedClientConfig build_centralized_real_client(
        const std::string& local_hostname,
        const std::string& metadata_connstring,
        const std::string& protocol = "tcp",
        const std::optional<std::string>& rdma_devices = std::nullopt,
        const std::string& master_server_entry = "127.0.0.1:50051",
        uint64_t global_segment_size = 0, uint64_t local_buffer_size = 0,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        const std::string& ipc_socket_path = "", bool enable_offload = false,
        const std::map<std::string, std::string>& labels = {}) {
        CentralizedClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, labels);
        config.global_segment_size = global_segment_size;
        config.enable_offload = enable_offload;
        return config;
    }

    static P2PClientConfig build_p2p_real_client(
        const std::string& local_hostname,
        const std::string& metadata_connstring,
        const std::string& protocol = "tcp",
        const std::optional<std::string>& rdma_devices = std::nullopt,
        const std::string& master_server_entry = "127.0.0.1:50051",
        const std::string& tiered_backend_config_json = "",
        uint64_t local_buffer_size = 0,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        const std::string& ipc_socket_path = "",
        uint16_t client_rpc_port = 12345, uint32_t rpc_thread_num = 2,
        size_t lock_shard_count = 1024,
        size_t route_cache_max_memory_bytes = 300 * 1024 * 1024,
        uint64_t route_cache_ttl_ms = 5 * 60 * 1000,
        const std::map<std::string, std::string>& labels = {}) {
        P2PClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, labels);
        config.client_rpc_port = client_rpc_port;
        config.rpc_thread_num = rpc_thread_num;
        config.lock_shard_count = lock_shard_count;
        config.route_cache_max_memory_bytes = route_cache_max_memory_bytes;
        config.route_cache_ttl_ms = route_cache_ttl_ms;

        Json::Value tiered_config;
        std::string actual_json = tiered_backend_config_json;
        if (actual_json.empty()) {
            if (const char* env_p = std::getenv("MOONCAKE_TIERED_CONFIG")) {
                actual_json = env_p;
            }
        }

        if (!actual_json.empty()) {
            Json::CharReaderBuilder builder;
            auto reader =
                std::unique_ptr<Json::CharReader>(builder.newCharReader());
            std::string errors;
            if (!reader->parse(actual_json.data(),
                               actual_json.data() + actual_json.length(),
                               &tiered_config, &errors)) {
                LOG(ERROR) << "Failed to parse tiered config: " << errors;
            }
        }

        if (tiered_config.isNull() || !tiered_config.isMember("tiers") ||
            tiered_config["tiers"].empty()) {
            throw std::runtime_error(
                "Tiered backend configuration is missing. Please provide "
                "tiered_backend_config_json or set MOONCAKE_TIERED_CONFIG "
                "environment variable.");
        }
        config.tiered_backend_config = tiered_config;

        return config;
    }

   private:
    static void fill_real_client_config_base(
        RealClientConfigBase& config, const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& rdma_devices,
        const std::string& master_server_entry, uint64_t local_buffer_size,
        const std::shared_ptr<TransferEngine>& transfer_engine,
        const std::string& ipc_socket_path,
        const std::map<std::string, std::string>& labels) {
        // Parse local_hostname into IP and optional port.
        // Only set te_port when the user explicitly provides a port;
        // otherwise keep the default value (0 = randomly assigned).
        auto bracket_pos = local_hostname.find(']');
        if (bracket_pos != std::string::npos) {
            // Bracketed IPv6, e.g. "[2001:db8::1]" or "[2001:db8::1]:1234"
            config.local_ip = local_hostname.substr(1, bracket_pos - 1);
            auto colon_after = local_hostname.find(':', bracket_pos);
            if (colon_after != std::string::npos) {
                config.te_port = getPortFromString(
                    local_hostname.substr(colon_after + 1), 0);
            }
        } else if (isValidIpV6(local_hostname)) {
            // Raw IPv6 without brackets, no way to specify port
            config.local_ip = local_hostname;
        } else {
            // IPv4 or hostname, optionally with port
            auto colon_pos = local_hostname.rfind(':');
            if (colon_pos != std::string::npos) {
                config.local_ip = local_hostname.substr(0, colon_pos);
                config.te_port =
                    getPortFromString(local_hostname.substr(colon_pos + 1), 0);
            } else {
                config.local_ip = local_hostname;
            }
        }
        config.metadata_connstring = metadata_connstring;
        config.protocol = protocol;
        config.rdma_devices = rdma_devices;
        config.master_server_entry = master_server_entry;
        config.local_buffer_size = local_buffer_size;
        config.transfer_engine = transfer_engine;
        config.ipc_socket_path = ipc_socket_path;
        config.labels = labels;
    }
};

}  // namespace mooncake
