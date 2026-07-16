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
#include <strings.h>
#include <unistd.h>

namespace mooncake {
namespace {

std::string trimConfigToken(const std::string& value) {
    const auto begin = value.find_first_not_of(" \t\n\r");
    if (begin == std::string::npos) return "";
    const auto end = value.find_last_not_of(" \t\n\r");
    return value.substr(begin, end - begin + 1);
}

std::vector<std::string> splitConfigString(const std::string& value,
                                           char delim) {
    std::vector<std::string> result;
    std::stringstream stream(value);
    std::string item;
    while (std::getline(stream, item, delim)) {
        result.push_back(trimConfigToken(item));
    }
    return result;
}

bool parseBoolConfigEnv(const char* value, const char* env_name, bool& output) {
    if (strcmp(value, "1") == 0 || strcasecmp(value, "true") == 0) {
        output = true;
        return true;
    }
    if (strcmp(value, "0") == 0 || strcasecmp(value, "false") == 0) {
        output = false;
        return true;
    }
    LOG(WARNING) << "Ignore value from environment variable " << env_name
                 << ", it should be 0|1|true|false";
    return false;
}

void parseNicPeerAffinity(
    const char* env,
    std::unordered_map<std::string, std::vector<std::string>>& affinity) {
    affinity.clear();
    if (!env || env[0] == '\0') return;

    for (const auto& raw_rule : splitConfigString(env, ';')) {
        const auto rule = trimConfigToken(raw_rule);
        if (rule.empty()) continue;

        auto delim = rule.find('=');
        if (delim == std::string::npos) {
            LOG(WARNING) << "Invalid MC_NIC_PEER_AFFINITY rule '" << rule
                         << "'. Expected local_hca=peer_hca[,peer_hca].";
            continue;
        }

        auto local_hca = trimConfigToken(rule.substr(0, delim));
        if (local_hca.empty()) {
            LOG(WARNING) << "Invalid MC_NIC_PEER_AFFINITY rule '" << rule
                         << "': local HCA is empty.";
            continue;
        }

        std::vector<std::string> peer_hcas;
        for (const auto& peer_hca :
             splitConfigString(rule.substr(delim + 1), ',')) {
            if (!peer_hca.empty()) peer_hcas.push_back(peer_hca);
        }

        if (peer_hcas.empty()) {
            LOG(WARNING) << "Invalid MC_NIC_PEER_AFFINITY rule '" << rule
                         << "': peer HCA list is empty.";
            continue;
        }

        affinity[local_hca] = std::move(peer_hcas);
    }
}

}  // namespace

void loadGlobalConfig(GlobalConfig& config) {
    const auto& env = Environ::Get();

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

    int pkey_index = env.GetPkeyIndex();
    if (pkey_index >= 0 && pkey_index <= UINT16_MAX)
        config.pkey_index = static_cast<uint16_t>(pkey_index);

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
    else
        LOG(ERROR) << "Ignore value from environment variable MC_MTU, it "
                      "should be 512|1024|2048|4096";

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

    uint64_t max_mr_size = env.GetMaxMrSize();
    if (max_mr_size > 0) config.max_mr_size = max_mr_size;

    int retry_cnt = env.GetRetryCnt();
    if (retry_cnt > 0 && retry_cnt < 128) config.retry_cnt = retry_cnt;

    int auto_gid_max_retries = env.GetAutoGidMaxRetries();
    if (auto_gid_max_retries >= 0 && auto_gid_max_retries <= 16)
        config.auto_gid_max_retries = auto_gid_max_retries;

    if (env.GetDisableMetacache()) config.metacache = false;

    std::string te_metadata_refresh_interval_seconds =
        env.GetTeMetadataRefreshIntervalSeconds();
    if (!te_metadata_refresh_interval_seconds.empty()) {
        try {
            int val = std::stoi(te_metadata_refresh_interval_seconds);
            if (val >= 0) {
                config.te_metadata_refresh_interval_seconds =
                    static_cast<uint64_t>(val);
            } else {
                LOG(WARNING) << "Ignore value from environment variable "
                                "MC_TE_METADATA_REFRESH_INTERVAL_SECONDS";
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Invalid MC_TE_METADATA_REFRESH_INTERVAL_SECONDS "
                            "environment value: "
                         << te_metadata_refresh_interval_seconds
                         << ". Error: " << e.what();
        }
    }

    int handshake_listen_backlog = env.GetHandshakeListenBacklog();
    if (handshake_listen_backlog > 0)
        config.handshake_listen_backlog = handshake_listen_backlog;

    int handshake_connect_timeout = env.GetHandshakeConnectTimeout();
    if (handshake_connect_timeout > 0 && handshake_connect_timeout < 3600)
        config.handshake_connect_timeout = handshake_connect_timeout;

    std::string log_level = env.GetLogLevel();
    config.trace = false;
    if (!log_level.empty()) {
        if (log_level == "TRACE") {
            config.log_level = google::INFO;
            config.trace = true;
        }
        if (log_level == "INFO")
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

    {
        auto [validated_min, validated_max] = ValidatePortRange(
            env.GetMinRpcPort(), env.GetMaxRpcPort(), 15000, 17000);
        config.rpc_min_port = validated_min;
        config.rpc_max_port = validated_max;
    }

    if (env.GetUseIpv6()) config.use_ipv6 = true;

    int fragment_ratio = env.GetFragmentRatio();
    if (fragment_ratio > 0 && fragment_ratio < (int)config.slice_size)
        config.fragment_limit = config.slice_size / fragment_ratio;
    else {
        LOG(WARNING) << "Ignore value from environment variable "
                        "MC_FRAGMENT_RATIO and set it to 4 as default";
        config.fragment_limit = config.slice_size / 4;
    }

    if (env.GetEnableDestDeviceAffinity()) {
        config.enable_dest_device_affinity = true;
    }

    std::string enable_hca_peer_affinity = env.GetEnableHcaPeerAffinity();
    if (!enable_hca_peer_affinity.empty()) {
        parseBoolConfigEnv(enable_hca_peer_affinity.c_str(),
                           "MC_ENABLE_HCA_PEER_AFFINITY",
                           config.enable_hca_peer_affinity);
    }

    std::string nic_peer_affinity = env.GetNicPeerAffinity();
    parseNicPeerAffinity(nic_peer_affinity.c_str(), config.nic_peer_affinity);

    if (config.enable_hca_peer_affinity && config.enable_dest_device_affinity) {
        LOG(ERROR) << "MC_ENABLE_HCA_PEER_AFFINITY and "
                      "MC_ENABLE_DEST_DEVICE_AFFINITY cannot be enabled at "
                      "the same time; falling back to default peer device "
                      "selection.";
        config.enable_hca_peer_affinity = false;
        config.enable_dest_device_affinity = false;
    }

    std::string log_rdma_slice_affinity = env.GetLogRdmaSliceAffinity();
    if (!log_rdma_slice_affinity.empty()) {
        parseBoolConfigEnv(log_rdma_slice_affinity.c_str(),
                           "MC_LOG_RDMA_SLICE_AFFINITY",
                           config.log_rdma_slice_affinity);
    }

    std::string track_rdma_posted_slices = env.GetTrackRdmaPostedSlices();
    if (!track_rdma_posted_slices.empty()) {
        parseBoolConfigEnv(track_rdma_posted_slices.c_str(),
                           "MC_TRACK_RDMA_POSTED_SLICES",
                           config.track_rdma_posted_slices);
    }

    int enable_parallel_reg_mr = env.GetEnableParallelRegMr();
    if (enable_parallel_reg_mr >= -1 && enable_parallel_reg_mr <= 1)
        config.parallel_reg_mr = enable_parallel_reg_mr;

    std::string endpoint_store_type = env.GetEndpointStoreType();
    if (!endpoint_store_type.empty()) {
        if (endpoint_store_type == "FIFO") {
            config.endpoint_store_type = EndpointStoreType::FIFO;
        } else if (endpoint_store_type == "SIEVE") {
            config.endpoint_store_type = EndpointStoreType::SIEVE;
        } else {
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_ENDPOINT_STORE_TYPE, it should be FIFO|SIEVE";
        }
    }

    int traffic_class = env.GetIbTc();
    if (traffic_class >= 0 && traffic_class <= 255)
        config.ib_traffic_class = traffic_class;

    int service_level = env.GetIbServiceLevel();
    if (service_level >= 0 && service_level <= 15)
        config.ib_service_level = service_level;

    int ib_relaxed_ordering = env.GetIbPciRelaxedOrdering();
    if (ib_relaxed_ordering >= 0 && ib_relaxed_ordering <= 2)
        config.ib_pci_relaxed_ordering_mode = ib_relaxed_ordering;

    std::string mlx5_qp_udp_sports = env.GetMlx5QpUdpSports();
    if (!mlx5_qp_udp_sports.empty()) {
        std::vector<uint16_t> ports;
        std::stringstream ss(mlx5_qp_udp_sports);
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
            } catch (const std::exception& e) {
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

    std::string val = env.GetMlx5QpLagPortBalance();
    if (!val.empty()) {
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

void updateGlobalConfig(ibv_device_attr& device_attr) {
    auto& config = globalConfig();
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
    auto& config = globalConfig();
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
    LOG(INFO) << "ib_service_level = " << config.ib_service_level;
    LOG(INFO) << "te_metadata_refresh_interval_seconds = "
              << config.te_metadata_refresh_interval_seconds;
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
    LOG(INFO) << "log_rdma_slice_affinity = "
              << (config.log_rdma_slice_affinity ? "true" : "false");
    LOG(INFO) << "track_rdma_posted_slices = "
              << (config.track_rdma_posted_slices ? "true" : "false");
}

GlobalConfig& globalConfig() {
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
