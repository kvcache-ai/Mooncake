// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/common/config.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <sstream>

namespace mooncake {
namespace tent {
Status Config::load(const std::string& content) {
    std::lock_guard<std::mutex> lock(mutex_);
    try {
        config_data_ = json::parse(content);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(std::string("Invalid JSON: ") +
                                       e.what() + LOC_MARK);
    }
}

Status Config::loadFile(const std::string& file_path) {
    std::ifstream ifs(file_path);
    if (!ifs.is_open()) {
        return Status::InvalidArgument(
            std::string("Cannot open config file: ") + file_path + LOC_MARK);
    }
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        std::istreambuf_iterator<char>());
    return load(content);
}

std::string Config::dump(int indent) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_data_.dump(indent);
}

static inline void setConfig(Config& config, const std::string& env_key,
                             const std::string& config_key) {
    const char* val = std::getenv(env_key.c_str());
    if (val) config.setFromString(config_key, std::string(val));
}

// Like setConfig, but parses the env value as a comma-separated list and
// stores it as a string array. Empty/whitespace-only items are dropped so a
// trailing comma or spaces around names are tolerated (e.g. "mlx5_0, mlx5_1").
static inline void setArrayConfig(Config& config, const std::string& env_key,
                                  const std::string& config_key) {
    const char* val = std::getenv(env_key.c_str());
    if (!val) return;
    std::vector<std::string> items;
    std::stringstream ss(val);
    std::string item;
    while (std::getline(ss, item, ',')) {
        item.erase(0, item.find_first_not_of(" \t"));
        item.erase(item.find_last_not_of(" \t") + 1);
        if (!item.empty()) items.push_back(item);
    }
    if (!items.empty()) config.set(config_key, items);
}

static inline void setPortArrayConfig(Config& config,
                                      const std::string& env_key,
                                      const std::string& config_key) {
    const char* val = std::getenv(env_key.c_str());
    if (!val || !*val) return;
    std::vector<uint16_t> ports;
    std::stringstream ss(val);
    std::string item;
    while (std::getline(ss, item, ',')) {
        item.erase(0, item.find_first_not_of(" \t"));
        item.erase(item.find_last_not_of(" \t") + 1);
        if (item.empty()) continue;
        try {
            unsigned long port = std::stoul(item);
            if (port == 0 || port > 65535) {
                LOG(WARNING)
                    << "Ignoring invalid port in " << env_key << ": " << item;
                continue;
            }
            ports.push_back(static_cast<uint16_t>(port));
        } catch (const std::exception& e) {
            LOG(WARNING) << "Ignoring invalid port in " << env_key << ": "
                         << item << " (" << e.what() << ")";
        }
    }
    if (!ports.empty()) config.set(config_key, ports);
}

Status ConfigHelper::loadFromEnv(Config& config) {
    const char* conf_str = std::getenv("MC_TENT_CONF");
    Status status = Status::OK();
    if (conf_str && *conf_str != '\0') {
        std::string conf(conf_str);
        bool is_file = false;
        try {
            is_file = std::filesystem::exists(conf);
        } catch (const std::filesystem::filesystem_error& e) {
            LOG(WARNING) << "Failed to check file existence for MC_TENT_CONF="
                         << conf << ": " << e.what()
                         << ", treating as JSON string";
        }
        if (is_file) {
            status = config.loadFile(conf);
            if (!status.ok()) {
                LOG(WARNING)
                    << "Failed to load config file from MC_TENT_CONF=" << conf
                    << ": " << status.ToString();
            } else {
                LOG(INFO) << "Loaded tent config from file: " << conf;
            }
        } else {
            status = config.load(conf);
            if (!status.ok()) {
                LOG(WARNING)
                    << "Failed to parse MC_TENT_CONF: " << status.ToString();
            }
        }
    }

    // Legacy keys for backward compatibility (MC_* env vars)
    setConfig(config, "MC_RDMA_BIND_ADDRESS", "transports/rdma/bind_address");
    setConfig(config, "MC_NUM_CQ_PER_CTX",
              "transports/rdma/device/num_cq_list");
    setConfig(config, "MC_NUM_COMP_CHANNELS_PER_CTX",
              "transports/rdma/device/num_comp_channels");
    setConfig(config, "MC_IB_PORT", "transports/rdma/device/port");
    setConfig(config, "MC_GID_INDEX", "transports/rdma/device/gid_index");
    setConfig(config, "NCCL_IB_GID_INDEX", "transports/rdma/device/gid_index");
    setConfig(config, "MC_AUTO_GID_MAX_RETRIES",
              "transports/rdma/device/auto_gid_max_retries");
    setConfig(config, "MC_MAX_CQE_PER_CTX", "transports/rdma/device/max_cqe");
    setConfig(config, "MC_MAX_EP_PER_CTX",
              "transports/rdma/endpoint/endpoint_store_cap");
    setConfig(config, "MC_CONN_PAUSE_TTL_MS",
              "transports/rdma/endpoint/conn_pause_ttl_ms");
    setConfig(config, "MC_NUM_QP_PER_EP",
              "transports/rdma/endpoint/qp_mul_factor");
    setConfig(config, "MC_MAX_SGE", "transports/rdma/endpoint/max_sge");
    setConfig(config, "MC_MAX_WR", "transports/rdma/endpoint/max_qp_wr");
    setConfig(config, "MC_MAX_INLINE",
              "transports/rdma/endpoint/max_inline_bytes");
    setConfig(config, "MC_PKEY_INDEX", "transports/rdma/endpoint/pkey_index");
    setConfig(config, "MC_MTU", "transports/rdma/endpoint/path_mtu");
    setConfig(config, "MC_IB_TC", "transports/rdma/endpoint/traffic_class");
    setConfig(config, "MC_IB_SL", "transports/rdma/endpoint/service_level");
    setPortArrayConfig(config, "MC_MLX5_QP_UDP_SPORTS",
                       "transports/rdma/endpoint/mlx5_qp_udp_sports");
    setConfig(config, "MC_MLX5_QP_LAG_PORT_BALANCE",
              "transports/rdma/endpoint/mlx5_qp_lag_port_balance");
    setConfig(config, "MC_IB_PCI_RELAXED_ORDERING",
              "transports/rdma/pci_relaxed_ordering");
    setConfig(config, "MC_WORKERS_PER_CTX",
              "transports/rdma/workers/num_workers");
    setConfig(config, "MC_SLICE_SIZE", "transports/rdma/workers/block_size");
    setConfig(config, "MC_RETRY_CNT",
              "transports/rdma/workers/max_retry_count");
    setConfig(config, "MC_TRACK_RDMA_POSTED_SLICES",
              "transports/rdma/workers/track_posted_slices");
    setConfig(config, "MC_DISABLE_GPU_DIRECT_RDMA",
              "transports/rdma/disable_gpu_direct_rdma");
    setConfig(config, "MC_LOG_RDMA_SLICE_AFFINITY",
              "transports/rdma/log_slice_affinity");
    // Restrict which RDMA NICs the engine discovers/uses (comma-separated
    // device names). MC_TE_FILTERS is an allow-list — same name and semantics
    // as the legacy Transfer Engine's device whitelist, so a single env works
    // across both engines. MC_TE_FILTERS_EXCLUDE is a deny-list (new; the
    // legacy engine has no deny-list). Unset = discover all (default).
    // Consumed by filterInfiniBandDevices() in the platform probes.
    setArrayConfig(config, "MC_TE_FILTERS", "topology/rdma_whitelist");
    setArrayConfig(config, "MC_TE_FILTERS_EXCLUDE", "topology/rdma_blacklist");
    return status;
}

bool ConfigHelper::parseBool(const std::string& str, bool default_value) {
    std::string lower_str = str;
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower_str == "true" || lower_str == "1" || lower_str == "yes" ||
        lower_str == "on") {
        return true;
    } else if (lower_str == "false" || lower_str == "0" || lower_str == "no" ||
               lower_str == "off") {
        return false;
    } else {
        LOG(WARNING) << "Invalid boolean value '" << str
                     << "', using default: " << default_value;
        return default_value;
    }
}

int ConfigHelper::parseInt(const std::string& str, int default_value) {
    try {
        return std::stoi(str);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse integer '" << str << "': " << e.what()
                     << ", using default: " << default_value;
        return default_value;
    }
}

uint16_t ConfigHelper::parsePort(const std::string& str,
                                 uint16_t default_value) {
    try {
        int port = std::stoi(str);
        if (port > 0 && port <= 65535) {
            return static_cast<uint16_t>(port);
        } else {
            LOG(WARNING) << "Port " << port
                         << " out of range (1-65535), using default: "
                         << default_value;
            return default_value;
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to parse port '" << str << "': " << e.what()
                     << ", using default: " << default_value;
        return default_value;
    }
}

std::vector<double> ConfigHelper::parseDoubleArray(const std::string& str) {
    std::vector<double> result;
    std::stringstream ss(str);
    std::string item;

    while (std::getline(ss, item, ',')) {
        try {
            // Trim whitespace
            item.erase(0, item.find_first_not_of(" \t"));
            item.erase(item.find_last_not_of(" \t") + 1);

            if (!item.empty()) {
                double value = std::stod(item);
                if (value > 0) {
                    result.push_back(value);
                }
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to parse double value '" << item
                         << "': " << e.what();
        }
    }

    // Sort the result
    std::sort(result.begin(), result.end());

    return result;
}

}  // namespace tent
}  // namespace mooncake
