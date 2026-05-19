#pragma once

#include <string>
#include <optional>
#include <map>
#include <memory>
#include <cstdint>
#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <fstream>
#include <sstream>

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

    // Port for metrics HTTP server.
    // Only used when enable_metrics_http is true.
    uint16_t metrics_port = 9003;

    // Whether to enable metrics HTTP server.
    // The metrics server exposes /metrics, /metrics/summary, and /health
    // endpoints.
    bool enable_metrics_http = true;
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

enum class LocalTransferMode {
    MEMCPY = 0,
    TE = 1,
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

    // Async route notification.
    // async_sender_thread_count > 0 enables async notifier.
    // async_route_queue_size controls queue capacity
    // (minimum async_max_batch_size * async_sender_thread_count).
    size_t async_sender_thread_count = 0;
    size_t async_max_batch_size = 2000;
    size_t async_route_queue_size = 0;

    // Local transfer mode for P2P local Get/Put path.
    // - MEMCPY: copy through local CPU memory path
    // - TE: transfer through local TransferEngine path
    LocalTransferMode local_transfer_mode = LocalTransferMode::TE;

    // When local_transfer_mode == MEMCPY, the following parameter is used:
    // 0 means forbid async memcpy (fall back to synchronous).
    size_t local_memcpy_async_worker_num = 32;

    // PreWrite / PinKey key lease: maximum time (ms) a key may stay in
    // intermediate (lease-protected) state before expiring.
    static constexpr uint32_t kP2pDefaultKeyLeaseDurationMs = 5000;
    // Interval (ms) for the background scanner that removes expired leases.
    static constexpr uint32_t kP2pDefaultKeyLeaseScanIntervalMs = 1000;
    uint32_t p2p_key_lease_duration_ms = kP2pDefaultKeyLeaseDurationMs;
    uint32_t p2p_key_lease_scan_interval_ms = kP2pDefaultKeyLeaseScanIntervalMs;
};

// ============================================================================
// Factory class
// ============================================================================

/**
 * @brief Factory class for building typed client configurations.
 */
class ClientConfigBuilder {
   public:
    static constexpr const char* kDefaultTieredBackendConfigPath =
        "conf/tiered_backend.json";

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
        uint16_t metrics_port = 9003, bool enable_metrics_http = true,
        const std::map<std::string, std::string>& labels = {}) {
        CentralizedClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, metrics_port, enable_metrics_http, labels);
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
        const std::string& tiered_backend_config_json =
            kDefaultTieredBackendConfigPath,
        uint64_t local_buffer_size = 0,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        const std::string& ipc_socket_path = "",
        uint16_t client_rpc_port = 12345, uint32_t rpc_thread_num = 2,
        size_t lock_shard_count = 1024,
        size_t route_cache_max_memory_bytes = 300 * 1024 * 1024,
        uint64_t route_cache_ttl_ms = 5 * 60 * 1000,
        const std::string& local_transfer_mode = "te",
        size_t local_memcpy_async_worker_num = 32, uint16_t metrics_port = 9003,
        bool enable_metrics_http = true,
        const std::map<std::string, std::string>& labels = {},
        size_t async_sender_thread_count = 0,
        size_t async_max_batch_size = 2000, size_t async_route_queue_size = 0,
        uint32_t p2p_key_lease_duration_ms = 0,
        uint32_t p2p_key_lease_scan_interval_ms = 0) {
        P2PClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, metrics_port, enable_metrics_http, labels);
        config.client_rpc_port = client_rpc_port;
        config.rpc_thread_num = rpc_thread_num;
        config.lock_shard_count = lock_shard_count;
        config.route_cache_max_memory_bytes = route_cache_max_memory_bytes;
        config.route_cache_ttl_ms = route_cache_ttl_ms;
        config.local_transfer_mode =
            parse_p2p_local_transfer_mode(local_transfer_mode);
        if (config.local_transfer_mode == LocalTransferMode::MEMCPY) {
            config.local_memcpy_async_worker_num =
                local_memcpy_async_worker_num;
        }
        config.async_sender_thread_count = async_sender_thread_count;
        config.async_max_batch_size = async_max_batch_size;
        config.async_route_queue_size = async_route_queue_size;

        Json::Value tiered_config =
            LoadTieredConfig(tiered_backend_config_json);

        if (tiered_config.isNull() || !tiered_config.isMember("tiers") ||
            tiered_config["tiers"].empty()) {
            throw std::runtime_error(
                "Tiered backend configuration is missing or invalid. Please "
                "provide a valid JSON string or a path to a JSON config file "
                "via tiered_backend_config_json parameter.");
        }
        config.tiered_backend_config = tiered_config;

        if (p2p_key_lease_duration_ms > 0) {
            config.p2p_key_lease_duration_ms = p2p_key_lease_duration_ms;
        }
        if (p2p_key_lease_scan_interval_ms > 0) {
            config.p2p_key_lease_scan_interval_ms =
                p2p_key_lease_scan_interval_ms;
        }

        return config;
    }

   private:
    static Json::Value LoadTieredConfig(const std::string& json_or_path) {
        Json::Value config;
        std::string json_content;

        // Determine if input is a JSON string or a file path
        std::string trimmed = json_or_path;
        size_t start = trimmed.find_first_not_of(" \t\n\r");
        if (start != std::string::npos) {
            trimmed = trimmed.substr(start);
        }

        if (!trimmed.empty() && trimmed[0] == '{') {
            // Treat as JSON string
            json_content = json_or_path;
        } else {
            // Treat as file path
            std::ifstream file(json_or_path);
            if (!file.is_open()) {
                LOG(ERROR) << "Failed to open tiered backend config file: "
                           << json_or_path;
                return config;  // Returns null Json::Value
            }
            std::ostringstream ss;
            ss << file.rdbuf();
            json_content = ss.str();
        }

        // Parse JSON
        Json::CharReaderBuilder builder;
        auto reader =
            std::unique_ptr<Json::CharReader>(builder.newCharReader());
        std::string errors;
        if (!reader->parse(json_content.data(),
                           json_content.data() + json_content.length(), &config,
                           &errors)) {
            LOG(ERROR) << "Failed to parse tiered config: " << errors;
        }
        return config;
    }

    static void fill_real_client_config_base(
        RealClientConfigBase& config, const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& rdma_devices,
        const std::string& master_server_entry, uint64_t local_buffer_size,
        const std::shared_ptr<TransferEngine>& transfer_engine,
        const std::string& ipc_socket_path, uint16_t metrics_port = 9003,
        bool enable_metrics_http = true,
        const std::map<std::string, std::string>& labels = {}) {
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
        config.metrics_port = metrics_port;
        config.enable_metrics_http = enable_metrics_http;
        config.labels = labels;
    }

    static LocalTransferMode parse_p2p_local_transfer_mode(std::string mode) {
        std::transform(
            mode.begin(), mode.end(), mode.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (mode == "memcpy") {
            return LocalTransferMode::MEMCPY;
        }
        if (mode == "te") {
            return LocalTransferMode::TE;
        }
        throw std::runtime_error(
            "Invalid p2p local transfer mode. Expected 'memcpy' or 'te'.");
    }
};

}  // namespace mooncake
