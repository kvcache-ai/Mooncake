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

#include "config.h"
#include "environ.h"

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <unistd.h>

namespace mooncake {
void loadGlobalConfig(GlobalConfig &config) {
    auto &env = Environ::Get();

    int num_cq_per_ctx = env.GetNumCqPerCtx();
    if (num_cq_per_ctx > 0 && num_cq_per_ctx < 256) {
        config.num_cq_per_ctx = num_cq_per_ctx;
        // In URMA, JFC and JFCE are bound one-to-one.
        config.num_jfc_per_ctx = num_cq_per_ctx;
        config.num_jfce_per_ctx = num_cq_per_ctx;
    }

    int num_comp_channels_per_ctx = env.GetNumCompChannelsPerCtx();
    if (num_comp_channels_per_ctx > 0 && num_comp_channels_per_ctx < 256)
        config.num_comp_channels_per_ctx = num_comp_channels_per_ctx;

    int port = env.GetIbPort();
    if (port >= 0 && port < 256) config.port = uint8_t(port);

    int gid_index = env.GetGidIndex();
    if (gid_index >= 0 && gid_index < 256) config.gid_index = gid_index;

    // MC_PKEY_INDEX: QP attribute override (validated with stoi for
    // non-numeric input detection)
    const char *pkey_index_env = std::getenv("MC_PKEY_INDEX");
    if (pkey_index_env) {
        try {
            int val = std::stoi(pkey_index_env);
            if (val >= 0 && val <= UINT16_MAX) {
                config.pkey_index = static_cast<uint16_t>(val);
            } else {
                LOG(WARNING)
                    << "Ignore value from environment variable MC_PKEY_INDEX, "
                    << "value " << pkey_index_env
                    << " out of range (should be 0-65535)";
            }
        } catch (const std::exception &e) {
            LOG(WARNING) << "Invalid MC_PKEY_INDEX environment value: "
                         << pkey_index_env << ". Error: " << e.what();
        }
    }

    int max_cqe_per_ctx = env.GetMaxCqePerCtx();
    if (max_cqe_per_ctx > 0 && max_cqe_per_ctx <= UINT16_MAX)
        config.max_cqe = max_cqe_per_ctx;

    int max_ep_per_ctx = env.GetMaxEpPerCtx();
    if (max_ep_per_ctx > 0 && max_ep_per_ctx <= UINT16_MAX)
        config.max_ep_per_ctx = max_ep_per_ctx;

    int num_qp_per_ep = env.GetNumQpPerEp();
    if (num_qp_per_ep > 0 && num_qp_per_ep < 256)
        config.num_qp_per_ep = num_qp_per_ep;

    int max_sge = env.GetMaxSge();
    if (max_sge > 0 && max_sge <= UINT16_MAX) config.max_sge = max_sge;

    int max_wr = env.GetMaxWr();
    if (max_wr > 0 && max_wr <= UINT16_MAX) config.max_wr = max_wr;

    int max_inline = env.GetMaxInline();
    if (max_inline >= 0 && max_inline <= UINT16_MAX)
        config.max_inline = max_inline;

    int mtu = env.GetMtu();
    if (mtu == 512)
        config.mtu_length = IBV_MTU_512;
    else if (mtu == 1024)
        config.mtu_length = IBV_MTU_1024;
    else if (mtu == 2048)
        config.mtu_length = IBV_MTU_2048;
    else if (mtu == 4096)
        config.mtu_length = IBV_MTU_4096;
    else if (mtu != 0) {
        LOG(ERROR) << "Ignore value from environment variable MC_MTU, it "
                      "should be 512|1024|2048|4096";
        exit(EXIT_FAILURE);
    }

    int handshake_port = env.GetHandshakePort();
    if (handshake_port > 0 && handshake_port < 65536)
        config.handshake_port = handshake_port;

    int workers_per_ctx = env.GetWorkersPerCtx();
    if (workers_per_ctx > 0 && workers_per_ctx <= 8)
        config.workers_per_ctx = workers_per_ctx;

    size_t slice_size = env.GetSliceSize();
    if (slice_size > 0) config.slice_size = slice_size;

    size_t min_reg_size = env.GetMinRegSize();
    if (min_reg_size > 0) {
        config.eic_max_block_size = min_reg_size;
        LOG(INFO) << "Barex set MC_MIN_REG_SIZE=" << min_reg_size;
    }

    int retry_cnt = env.GetRetryCnt();
    if (retry_cnt > 0 && retry_cnt < 128) config.retry_cnt = retry_cnt;

    uint64_t max_mr_size = env.GetMaxMrSize();
    if (max_mr_size > 0) {
        config.max_mr_size = max_mr_size;
    }

    const char* auto_gid_max_retries_env =
        std::getenv("MC_AUTO_GID_MAX_RETRIES");
    if (auto_gid_max_retries_env) {
        int val = atoi(auto_gid_max_retries_env);
        if (val >= 0 && val <= 16) {
            config.auto_gid_max_retries = val;
        } else {
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_AUTO_GID_MAX_RETRIES";
        }
    }

    if (env.GetDisableMetacache()) {
        config.metacache = false;
    }

    int handshake_listen_backlog = env.GetHandshakeListenBacklog();
    if (handshake_listen_backlog > 0) {
        config.handshake_listen_backlog = handshake_listen_backlog;
    }

    std::string log_level = env.GetLogLevel();
    config.trace = false;
    if (!log_level.empty()) {
        if (log_level == "TRACE") {
            config.log_level = google::INFO;
            config.trace = true;
        } else if (log_level == "INFO")
            config.log_level = google::INFO;
        else if (log_level == "WARNING")
            config.log_level = google::WARNING;
        else if (log_level == "ERROR")
            config.log_level = google::ERROR;
    }
    FLAGS_minloglevel = config.log_level;

    int slice_timeout = env.GetSliceTimeout();
    if (slice_timeout > 0 && slice_timeout < 65536)
        config.slice_timeout = slice_timeout;

    std::string log_dir_path = env.GetLogDir();
    if (!log_dir_path.empty()) {
        google::InitGoogleLogging("mooncake-transfer-engine");
        std::error_code ec;
        if (!std::filesystem::is_directory(log_dir_path, ec)) {
            LOG(WARNING)
                << "Path [" << log_dir_path
                << "] is not a valid directory path. Still logging to stderr.";
        } else if (access(log_dir_path.c_str(), W_OK) != 0) {
            LOG(WARNING)
                << "Path [" << log_dir_path
                << "] is not a permitted directory path for the current user. \
                Still logging to stderr.";
        } else {
            FLAGS_log_dir = log_dir_path;
            FLAGS_logtostderr = 0;
            FLAGS_stop_logging_if_full_disk = true;
        }
    }

    // Environ::GetMinRpcPort()/GetMaxRpcPort() already handle both
    // MC_MIN_RPC_PORT and MC_MIN_PRC_PORT fallbacks.
    {
        int raw_min = env.GetMinRpcPort();
        int raw_max = env.GetMaxRpcPort();
        auto [validated_min, validated_max] =
            ValidatePortRange(raw_min, raw_max, 15000, 17000);
        config.rpc_min_port = validated_min;
        config.rpc_max_port = validated_max;
    }

    if (env.GetUseIpv6()) {
        config.use_ipv6 = true;
    }

    int fragment_ratio = env.GetFragmentRatio();
    if (fragment_ratio > 0 && fragment_ratio < (int)config.slice_size)
        config.fragment_limit = config.slice_size / fragment_ratio;
    else if (fragment_ratio != 0) {
        LOG(WARNING) << "Ignore value from environment variable "
                        "MC_FRAGMENT_RATIO and set it to 4 as default";
        config.fragment_limit = config.slice_size / 4;
    }

    if (env.GetEnableDestDeviceAffinity()) {
        config.enable_dest_device_affinity = true;
    }

    int enable_parallel_reg_mr = env.GetEnableParallelRegMr();
    if (enable_parallel_reg_mr >= -1 && enable_parallel_reg_mr <= 1) {
        config.parallel_reg_mr = enable_parallel_reg_mr;
    }

    std::string endpoint_store_type = env.GetEndpointStoreType();
    if (endpoint_store_type == "FIFO") {
        config.endpoint_store_type = EndpointStoreType::FIFO;
    } else if (endpoint_store_type == "SIEVE") {
        config.endpoint_store_type = EndpointStoreType::SIEVE;
    } else if (!endpoint_store_type.empty()) {
        LOG(WARNING) << "Ignore value from environment variable "
                        "MC_ENDPOINT_STORE_TYPE, it should be FIFO|SIEVE";
    }

    // Use stoi for MC_IB_TC to properly detect and warn on non-numeric values
    const char *traffic_class_env = std::getenv("MC_IB_TC");
    if (traffic_class_env) {
        try {
            int val = std::stoi(traffic_class_env);
            if (val >= 0 && val <= 255) {
                config.ib_traffic_class = val;
            } else {
                LOG(WARNING)
                    << "Ignore value from environment variable MC_IB_TC, "
                    << "value " << traffic_class_env
                    << " out of range (should be 0-255)";
            }
        } catch (const std::exception &e) {
            LOG(WARNING) << "Invalid MC_IB_TC environment value: "
                         << traffic_class_env << ". Error: " << e.what();
        }
    }

    int ib_pci_relaxed_ordering = env.GetIbPciRelaxedOrdering();
    if (ib_pci_relaxed_ordering >= 0 && ib_pci_relaxed_ordering <= 2)
        config.ib_pci_relaxed_ordering_mode = ib_pci_relaxed_ordering;
    else if (ib_pci_relaxed_ordering != 0)
        LOG(WARNING) << "Ignore value from environment variable "
                        "MC_IB_PCI_RELAXED_ORDERING, it should be 0|1|2";
    const char *mlx5_qp_udp_sports_env = std::getenv("MC_MLX5_QP_UDP_SPORTS");
    if (mlx5_qp_udp_sports_env && *mlx5_qp_udp_sports_env) {
        std::vector<uint16_t> ports;
        std::stringstream ss(mlx5_qp_udp_sports_env);
        std::string item;
        bool ok = true;
        while (std::getline(ss, item, ',')) {
            // Trim leading/trailing whitespace.
            auto l = item.find_first_not_of(" \t");
            auto r = item.find_last_not_of(" \t");
            if (l == std::string::npos) continue;
            item = item.substr(l, r - l + 1);
            try {
                int val = std::stoi(item);
                if (val < 0 || val > 65535) {
                    LOG(WARNING)
                        << "MC_MLX5_QP_UDP_SPORTS entry out of range: " << item;
                    ok = false;
                    break;
                }
                ports.push_back(static_cast<uint16_t>(val));
            } catch (const std::exception &e) {
                LOG(WARNING) << "Invalid MC_MLX5_QP_UDP_SPORTS entry: " << item
                             << ". Error: " << e.what();
                ok = false;
                break;
            }
        }
        if (ok && !ports.empty()) {
            config.mlx5_qp_udp_sports = std::move(ports);
        } else if (!ok) {
            LOG(WARNING) << "Ignore MC_MLX5_QP_UDP_SPORTS entirely due to "
                            "parse errors";
        }
    }

    const char *mlx5_qp_lag_port_balance_env =
        std::getenv("MC_MLX5_QP_LAG_PORT_BALANCE");
    if (mlx5_qp_lag_port_balance_env && *mlx5_qp_lag_port_balance_env) {
        std::string val(mlx5_qp_lag_port_balance_env);
        // Trim leading/trailing whitespace.
        auto l = val.find_first_not_of(" \t");
        auto r = val.find_last_not_of(" \t");
        if (l != std::string::npos) {
            val = val.substr(l, r - l + 1);
            if (val == "1" || val == "true")
                config.mlx5_qp_lag_port_balance = true;
            else if (val == "0" || val == "false")
                config.mlx5_qp_lag_port_balance = false;
            else
                LOG(WARNING) << "Ignore MC_MLX5_QP_LAG_PORT_BALANCE: expected "
                                "0/1/true/false, got: "
                             << val;
        }
    }
}

std::string mtuLengthToString(ibv_mtu mtu) {
    if (mtu == IBV_MTU_512)
        return "IBV_MTU_512";
    else if (mtu == IBV_MTU_1024)
        return "IBV_MTU_1024";
    else if (mtu == IBV_MTU_2048)
        return "IBV_MTU_2048";
    else if (mtu == IBV_MTU_4096)
        return "IBV_MTU_4096";
    else
        return "UNKNOWN";
}

void updateGlobalConfig(ibv_device_attr &device_attr) {
    auto &config = globalConfig();
    if (config.max_ep_per_ctx * config.num_qp_per_ep >
        (size_t)device_attr.max_qp)
        config.max_ep_per_ctx = device_attr.max_qp / config.num_qp_per_ep;
    if (config.num_cq_per_ctx > (size_t)device_attr.max_cq)
        config.num_cq_per_ctx = device_attr.max_cq;
    if (config.max_wr > (size_t)device_attr.max_qp_wr)
        config.max_wr = device_attr.max_qp_wr;
    if (config.max_sge > (size_t)device_attr.max_sge)
        config.max_sge = device_attr.max_sge;
    if (config.max_cqe > (size_t)device_attr.max_cqe)
        config.max_cqe = device_attr.max_cqe;
    if (config.max_mr_size > device_attr.max_mr_size)
        config.max_mr_size = device_attr.max_mr_size;
}

void dumpGlobalConfig() {
    auto &config = globalConfig();
    LOG(INFO) << "=== GlobalConfig ===";
    LOG(INFO) << "num_cq_per_ctx = " << config.num_cq_per_ctx;
    LOG(INFO) << "num_comp_channels_per_ctx = "
              << config.num_comp_channels_per_ctx;
    LOG(INFO) << "port = " << config.port;
    LOG(INFO) << "gid_index = " << config.gid_index;
    LOG(INFO) << "pkey_index = " << config.pkey_index;
    LOG(INFO) << "max_mr_size = " << config.max_mr_size;
    LOG(INFO) << "max_cqe = " << config.max_cqe;
    LOG(INFO) << "max_ep_per_ctx = " << config.max_ep_per_ctx;
    LOG(INFO) << "num_qp_per_ep = " << config.num_qp_per_ep;
    LOG(INFO) << "max_sge = " << config.max_sge;
    LOG(INFO) << "max_wr = " << config.max_wr;
    LOG(INFO) << "max_inline = " << config.max_inline;
    LOG(INFO) << "mtu_length = " << mtuLengthToString(config.mtu_length);
    LOG(INFO) << "parallel_reg_mr = " << config.parallel_reg_mr;
    LOG(INFO) << "ib_traffic_class = " << config.ib_traffic_class;
    {
        std::ostringstream oss;
        for (size_t i = 0; i < config.mlx5_qp_udp_sports.size(); ++i) {
            if (i) oss << ",";
            oss << config.mlx5_qp_udp_sports[i];
        }
        LOG(INFO) << "mlx5_qp_udp_sports = ["
                  << (config.mlx5_qp_udp_sports.empty() ? "<unset>" : oss.str())
                  << "]";
    }
    LOG(INFO) << "mlx5_qp_lag_port_balance = "
              << (config.mlx5_qp_lag_port_balance ? "true" : "false");
}

GlobalConfig &globalConfig() {
    static GlobalConfig config;
    static std::once_flag g_once_flag;
    std::call_once(g_once_flag, []() { loadGlobalConfig(config); });
    return config;
}

uint16_t getDefaultHandshakePort() { return globalConfig().handshake_port; }

std::pair<int, int> ValidatePortRange(int min_port, int max_port,
                                      int default_min, int default_max) {
    constexpr int kMinAllowed = 1024;
    constexpr int kEphemeralStart = 32768;
    constexpr int kEphemeralEnd = 60999;
    constexpr int kMaxAllowed = 65535;

    auto is_valid_port = [&](int p) {
        return p >= kMinAllowed && p <= kMaxAllowed &&
               !(p >= kEphemeralStart && p <= kEphemeralEnd);
    };

    if (!is_valid_port(min_port) || !is_valid_port(max_port) ||
        min_port > max_port) {
        LOG(WARNING) << "Invalid port range [" << min_port << ", " << max_port
                     << "], falling back to default [" << default_min << ", "
                     << default_max << "]";
        return {default_min, default_max};
    }
    return {min_port, max_port};
}
}  // namespace mooncake
