#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "bool_parser.h"
#include "common/byte_size.h"
#include "egm_store_pool_config.h"
#include "integer_parser.h"
#include "storage_backend.h"
#include "types.h"

namespace mooncake {

class TransferEngine;

// Internal setup contract. Public argument-based APIs remain adapters; leaf
// component configuration is migrated separately under RFC #4396.
struct ClientConfig {
    std::string local_hostname;
    std::string metadata_server;
    size_t global_segment_size = DEFAULT_GLOBAL_SEGMENT_SIZE;
    size_t local_buffer_size = DEFAULT_LOCAL_BUFFER_SIZE;
    std::string protocol = DEFAULT_PROTOCOL;
    std::string rdma_devices;
    std::string master_server_addr = DEFAULT_MASTER_SERVER_ADDR;
    std::shared_ptr<TransferEngine> transfer_engine;
    std::string ipc_socket_path;
    int local_rpc_port = 50052;
    bool enable_ssd_offload = false;
    bool start_offload_rpc_server = false;
    std::string ssd_offload_path;
    std::string tenant_id = "default";
    bool enable_client_http_server = false;
    int client_http_port = DEFAULT_CLIENT_HTTP_PORT;
    bool enable_embedded_master = false;
    bool enable_ssd_prefetch = false;
    int64_t ssd_prefetch_cooldown_sec = DEFAULT_SSD_PREFETCH_COOLDOWN_SEC;
    int64_t ssd_prefetch_dedup_ttl_sec = DEFAULT_SSD_PREFETCH_DEDUP_TTL_SEC;
    EgmStorePoolConfig egm{};
    std::optional<size_t> cxl_device_size = std::nullopt;
    std::optional<FileStorageConfig> file_storage = std::nullopt;

    void ResolveOverrides(bool embedded_master_from_environment,
                          bool http_server_from_flags,
                          int http_port_from_flags) {
        enable_embedded_master |= embedded_master_from_environment;
        if (metadata_server.empty() && enable_embedded_master &&
            !transfer_engine) {
            metadata_server = "P2PHANDSHAKE";
        }
        if (!enable_client_http_server) {
            client_http_port = http_port_from_flags;
        }
        enable_client_http_server |= http_server_from_flags;
    }

    void ResolveFileStorage(FileStorageConfig config, bool uses_kvcs) {
        if (uses_kvcs) {
            config.storage_backend_type = StorageBackendType::kDistributed;
        }
        if (!ssd_offload_path.empty()) {
            config.storage_filepath = ssd_offload_path;
        }
        config.storage_paths = ResolveOffloadDiskPaths(config.storage_filepath);
        file_storage = std::move(config);
    }

    tl::expected<void, ErrorCode> Validate() const {
        if (local_hostname.empty()) {
            LOG(ERROR) << "local_hostname is empty";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (metadata_server.empty() && !transfer_engine &&
            !enable_embedded_master) {
            LOG(ERROR) << "metadata_server is empty";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        // Global capacity may span several registrations. Only the local
        // staging buffer is constrained by the single-registration limit.
        if ((global_segment_size != 0 &&
             global_segment_size < MIN_SEGMENT_SIZE) ||
            (local_buffer_size != 0 && local_buffer_size < MIN_SEGMENT_SIZE) ||
            local_buffer_size > MAX_SEGMENT_SIZE) {
            LOG(ERROR) << "Invalid Store segment sizes: global_segment_size="
                       << global_segment_size
                       << ", local_buffer_size=" << local_buffer_size;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (enable_client_http_server &&
            (client_http_port <= 0 || client_http_port > 65535)) {
            LOG(ERROR) << "Invalid client HTTP server port: "
                       << client_http_port;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (protocol == "cxl" && !cxl_device_size.has_value()) {
            LOG(ERROR) << "MC_CXL_DEV_SIZE not set";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (file_storage && !file_storage->Validate()) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        return {};
    }
};

namespace client_config_detail {

inline std::string Get(const ConfigDict& config, const std::string& key,
                       const std::string& fallback = "") {
    const auto it = config.find(key);
    return it == config.end() ? fallback : it->second;
}

inline std::optional<size_t> GetSize(const ConfigDict& config,
                                     const std::string& key, size_t fallback) {
    const auto it = config.find(key);
    if (it == config.end()) return fallback;
    const auto parsed = try_string_to_byte_size(it->second);
    if (!parsed || *parsed > std::numeric_limits<size_t>::max()) {
        LOG(ERROR) << "Invalid size value for config key '" << key
                   << "': " << it->second;
        return std::nullopt;
    }
    return static_cast<size_t>(*parsed);
}

inline bool GetBool(const ConfigDict& config, const std::string& key,
                    bool fallback = false) {
    const auto it = config.find(key);
    if (it == config.end()) return fallback;
    const auto parsed = TryParseBool(it->second);
    if (parsed) return *parsed;
    LOG(WARNING) << "Invalid boolean value for config key '" << key
                 << "': " << it->second << ", using default: " << fallback;
    return fallback;
}

inline std::optional<int> GetInt(const ConfigDict& config,
                                 const std::string& key, int fallback) {
    const auto it = config.find(key);
    if (it == config.end()) return fallback;
    const auto parsed =
        TryParseInteger<int>(it->second, {.trim_ascii_whitespace = true});
    if (!parsed) {
        LOG(ERROR) << "Invalid integer value for config key '" << key
                   << "': " << it->second;
    }
    return parsed;
}

}  // namespace client_config_detail

inline tl::expected<ClientConfig, ErrorCode> ParseClientConfig(
    const ConfigDict& input) {
    using namespace client_config_detail;
    ClientConfig config;
    config.local_hostname = Get(input, CONFIG_KEY_LOCAL_HOSTNAME);
    config.metadata_server = Get(input, CONFIG_KEY_METADATA_SERVER);
    config.protocol = Get(input, CONFIG_KEY_PROTOCOL, DEFAULT_PROTOCOL);
    config.rdma_devices = Get(input, CONFIG_KEY_RDMA_DEVICES);
    config.master_server_addr =
        Get(input, CONFIG_KEY_MASTER_SERVER_ADDR, DEFAULT_MASTER_SERVER_ADDR);
    config.ipc_socket_path = Get(input, CONFIG_KEY_IPC_SOCKET_PATH);
    config.start_offload_rpc_server = true;
    config.ssd_offload_path = Get(input, "ssd_offload_path");
    config.tenant_id = Get(input, CONFIG_KEY_TENANT_ID, "default");
    config.enable_ssd_offload = GetBool(input, "enable_ssd_offload");
    config.enable_embedded_master =
        GetBool(input, CONFIG_KEY_ENABLE_EMBEDDED_MASTER);
    config.enable_client_http_server =
        GetBool(input, CONFIG_KEY_ENABLE_CLIENT_HTTP_SERVER);
    config.enable_ssd_prefetch = GetBool(input, CONFIG_KEY_ENABLE_SSD_PREFETCH);

    const auto global_size = GetSize(input, CONFIG_KEY_GLOBAL_SEGMENT_SIZE,
                                     DEFAULT_GLOBAL_SEGMENT_SIZE);
    const auto local_size =
        GetSize(input, CONFIG_KEY_LOCAL_BUFFER_SIZE, DEFAULT_LOCAL_BUFFER_SIZE);
    const auto http_port =
        GetInt(input, CONFIG_KEY_CLIENT_HTTP_PORT, DEFAULT_CLIENT_HTTP_PORT);
    const auto cooldown = GetSize(input, CONFIG_KEY_SSD_PREFETCH_COOLDOWN_SEC,
                                  DEFAULT_SSD_PREFETCH_COOLDOWN_SEC);
    const auto dedup_ttl = GetSize(input, CONFIG_KEY_SSD_PREFETCH_DEDUP_TTL_SEC,
                                   DEFAULT_SSD_PREFETCH_DEDUP_TTL_SEC);
    if (!global_size || !local_size || !http_port || !cooldown || !dedup_ttl) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    config.global_segment_size = *global_size;
    config.local_buffer_size = *local_size;
    config.client_http_port = *http_port;
    config.ssd_prefetch_cooldown_sec = static_cast<int64_t>(*cooldown);
    config.ssd_prefetch_dedup_ttl_sec = static_cast<int64_t>(*dedup_ttl);
    const auto egm = ParseEgmStorePoolConfig(input, config.protocol,
                                             config.global_segment_size);
    if (!egm) return tl::unexpected(egm.error());
    config.egm = *egm;
    return config;
}

}  // namespace mooncake
