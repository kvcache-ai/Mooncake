#pragma once

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <glog/logging.h>
#include "common.h"
#include "common_types.h"

namespace mooncake {

class TransferEngine;

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

struct RealClientConfigBase {
    // Original TE server name, including any transport-specific information.
    std::string local_hostname;

    // Connection string for metadata service
    std::string metadata_connstring;

    // Transport protocol (e.g., "tcp", "rdma", "ascend").
    std::string protocol = "tcp";

    // Comma-separated RDMA device names.
    // Optional with default auto-discovery.
    // Only required when auto-discovery is disabled
    // (set env `MC_MS_AUTO_DISC=0`).
    std::optional<std::string> rdma_devices = std::nullopt;

    // Master discovery entry, interpreted by the selected native client.
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

   protected:
    // Parsing helpers are available only to architecture-specific builders.
    struct DictCommon {
        static constexpr const char* kLocalHostname = "local_hostname";
        static constexpr const char* kMetadataServer = "metadata_server";
        static constexpr const char* kLocalBufferSize = "local_buffer_size";
        static constexpr const char* kProtocol = "protocol";
        static constexpr const char* kRdmaDevices = "rdma_devices";
        static constexpr const char* kMasterServerAddr = "master_server_addr";
        static constexpr const char* kIpcSocketPath = "ipc_socket_path";
        static constexpr size_t kDefaultLocalBufferSize = 1024 * 1024 * 16;
        static constexpr const char* kDefaultProtocol = "tcp";
        static constexpr const char* kDefaultMasterServerAddr =
            "127.0.0.1:50051";
    };

    static std::string get_config_str(
        const std::unordered_map<std::string, std::string>& config,
        const std::string& key, const std::string& default_value = "") {
        auto it = config.find(key);
        return (it != config.end()) ? it->second : default_value;
    }

    static size_t get_config_size(
        const std::unordered_map<std::string, std::string>& config,
        const std::string& key, size_t default_value) {
        auto it = config.find(key);
        if (it == config.end()) {
            return default_value;
        }
        const std::string& value = it->second;
        // Check for negative numbers (stoull incorrectly parses "-1" as large
        // val)
        if (!value.empty() && value[0] == '-') {
            LOG(WARNING) << "Invalid negative value for config key '" << key
                         << "': " << value
                         << ", using default: " << default_value;
            return default_value;
        }
        try {
            return std::stoull(value);
        } catch (const std::invalid_argument&) {
            LOG(WARNING) << "Invalid non-numeric value for config key '" << key
                         << "': " << value
                         << ", using default: " << default_value;
            return default_value;
        } catch (const std::out_of_range&) {
            LOG(WARNING) << "Value out of range for config key '" << key
                         << "': " << value
                         << ", using default: " << default_value;
            return default_value;
        }
    }

    static int get_config_int(
        const std::unordered_map<std::string, std::string>& config,
        const std::string& key, int default_value) {
        auto it = config.find(key);
        if (it == config.end()) {
            return default_value;
        }
        try {
            return std::stoi(it->second);
        } catch (const std::invalid_argument&) {
            LOG(WARNING) << "Invalid non-numeric value for config key '" << key
                         << "': " << it->second
                         << ", using default: " << default_value;
            return default_value;
        } catch (const std::out_of_range&) {
            LOG(WARNING) << "Value out of range for config key '" << key
                         << "': " << it->second
                         << ", using default: " << default_value;
            return default_value;
        }
    }

    static bool get_config_bool(
        const std::unordered_map<std::string, std::string>& config,
        const std::string& key, bool default_value) {
        auto it = config.find(key);
        if (it == config.end()) {
            return default_value;
        }
        std::string value = it->second;
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (value == "1" || value == "true" || value == "yes" ||
            value == "on" || value == "enable") {
            return true;
        }
        if (value == "0" || value == "false" || value == "no" ||
            value == "off" || value == "disable") {
            return false;
        }
        LOG(WARNING) << "Invalid boolean value for config key '" << key
                     << "': " << it->second
                     << ", using default: " << default_value;
        return default_value;
    }

    static void fill_real_client_config_base(
        RealClientConfigBase& config, const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        const std::optional<std::string>& rdma_devices,
        const std::string& master_server_entry, uint64_t local_buffer_size,
        const std::shared_ptr<TransferEngine>& transfer_engine,
        const std::string& ipc_socket_path,
        const std::map<std::string, std::string>& labels = {}) {
        config.local_hostname = local_hostname;
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
