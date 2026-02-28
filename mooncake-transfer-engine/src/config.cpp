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

#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <dirent.h>
#include <unistd.h>

namespace mooncake {
void loadGlobalConfig(GlobalConfig &config) {
    const char *num_cq_per_ctx_env = std::getenv("MC_NUM_CQ_PER_CTX");
    if (num_cq_per_ctx_env) {
        int val = atoi(num_cq_per_ctx_env);
        if (val > 0 && val < 256)
            config.num_cq_per_ctx = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_NUM_CQ_PER_CTX";
    }

    const char *num_comp_channels_per_ctx_env =
        std::getenv("MC_NUM_COMP_CHANNELS_PER_CTX");
    if (num_comp_channels_per_ctx_env) {
        int val = atoi(num_comp_channels_per_ctx_env);
        if (val > 0 && val < 256)
            config.num_comp_channels_per_ctx = val;
        else
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_NUM_COMP_CHANNELS_PER_CTX";
    }

    const char *port_env = std::getenv("MC_IB_PORT");
    if (port_env) {
        int val = atoi(port_env);
        if (val >= 0 && val < 256)
            config.port = uint8_t(val);
        else
            LOG(WARNING) << "Ignore value from environment variable MC_IB_PORT";
    }

    const char *gid_index_env = std::getenv("MC_GID_INDEX");
    if (!gid_index_env) gid_index_env = std::getenv("NCCL_IB_GID_INDEX");

    if (gid_index_env) {
        int val = atoi(gid_index_env);
        if (val >= 0 && val < 256)
            config.gid_index = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_GID_INDEX";
    }

    const char *max_cqe_per_ctx_env = std::getenv("MC_MAX_CQE_PER_CTX");
    if (max_cqe_per_ctx_env) {
        size_t val = atoi(max_cqe_per_ctx_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_cqe = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_MAX_CQE_PER_CTX";
    }

    const char *max_ep_per_ctx_env = std::getenv("MC_MAX_EP_PER_CTX");
    if (max_ep_per_ctx_env) {
        size_t val = atoi(max_ep_per_ctx_env);
        if (val > 0 && val <= UINT32_MAX)
            config.max_ep_per_ctx = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_MAX_EP_PER_CTX";
    }

    const char *num_qp_per_ep_env = std::getenv("MC_NUM_QP_PER_EP");
    if (num_qp_per_ep_env) {
        int val = atoi(num_qp_per_ep_env);
        if (val > 0 && val < 256)
            config.num_qp_per_ep = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_NUM_QP_PER_EP";
    }

    const char *max_sge_env = std::getenv("MC_MAX_SGE");
    if (max_sge_env) {
        size_t val = atoi(max_sge_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_sge = val;
        else
            LOG(WARNING) << "Ignore value from environment variable MC_MAX_SGE";
    }

    const char *max_wr_env = std::getenv("MC_MAX_WR");
    if (max_wr_env) {
        size_t val = atoi(max_wr_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_wr = val;
        else
            LOG(WARNING) << "Ignore value from environment variable MC_MAX_WR";
    }

    const char *max_inline_env = std::getenv("MC_MAX_INLINE");
    if (max_inline_env) {
        size_t val = atoi(max_inline_env);
        if (val <= UINT16_MAX)
            config.max_inline = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_MAX_INLINE";
    }

    const char *mtu_length_env = std::getenv("MC_MTU");
    if (mtu_length_env) {
        size_t val = atoi(mtu_length_env);
        if (val == 512)
            config.mtu_length = IBV_MTU_512;
        else if (val == 1024)
            config.mtu_length = IBV_MTU_1024;
        else if (val == 2048)
            config.mtu_length = IBV_MTU_2048;
        else if (val == 4096)
            config.mtu_length = IBV_MTU_4096;
        else {
            LOG(ERROR) << "Ignore value from environment variable MC_MTU, it "
                          "should be 512|1024|2048|4096";
            exit(EXIT_FAILURE);
        }
    }

    const char *handshake_port_env = std::getenv("MC_HANDSHAKE_PORT");
    if (handshake_port_env) {
        int val = atoi(handshake_port_env);
        if (val > 0 && val < 65536)
            config.handshake_port = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_HANDSHAKE_PORT";
    }

    const char *workers_per_ctx_env = std::getenv("MC_WORKERS_PER_CTX");
    if (workers_per_ctx_env) {
        size_t val = atoi(workers_per_ctx_env);
        if (val > 0 && val <= 8)
            config.workers_per_ctx = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_WORKERS_PER_CTX";
    }

    const char *slice_size_env = std::getenv("MC_SLICE_SIZE");
    if (slice_size_env) {
        size_t val = atoi(slice_size_env);
        if (val > 0)
            config.slice_size = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_SLICE_SIZE";
    }

    const char *min_reg_size_env = std::getenv("MC_MIN_REG_SIZE");
    if (min_reg_size_env) {
        size_t val = atoll(min_reg_size_env);
        if (val > 0) {
            config.eic_max_block_size = val;
            LOG(INFO) << "Barex set MC_MIN_REG_SIZE=" << val;
        } else
            LOG(WARNING)
                << "Ignore value from environment variable MC_MIN_REG_SIZE";
    }

    const char *retry_cnt_env = std::getenv("MC_RETRY_CNT");
    if (retry_cnt_env) {
        size_t val = atoi(retry_cnt_env);
        if (val > 0 && val < 128)
            config.retry_cnt = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_RETRY_CNT";
    }

    const char *disable_metacache = std::getenv("MC_DISABLE_METACACHE");
    if (disable_metacache) {
        config.metacache = false;
    }

    const char *handshake_listen_backlog =
        std::getenv("MC_HANDSHAKE_LISTEN_BACKLOG");
    if (handshake_listen_backlog) {
        int val = std::stoi(handshake_listen_backlog);
        if (val > 0) {
            config.handshake_listen_backlog = val;
        } else {
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_HANDSHAKE_LISTEN_BACKLOG";
        }
    }

    const char *log_level = std::getenv("MC_LOG_LEVEL");
    config.trace = false;
    if (log_level) {
        if (strcmp(log_level, "TRACE") == 0) {
            config.log_level = google::INFO;
            config.trace = true;
        }
        if (strcmp(log_level, "INFO") == 0)
            config.log_level = google::INFO;
        else if (strcmp(log_level, "WARNING") == 0)
            config.log_level = google::WARNING;
        else if (strcmp(log_level, "ERROR") == 0)
            config.log_level = google::ERROR;
    }
    FLAGS_minloglevel = config.log_level;

    const char *slice_timeout_env = std::getenv("MC_SLICE_TIMEOUT");
    if (slice_timeout_env) {
        int val = atoi(slice_timeout_env);
        if (val > 0 && val < 65536)
            config.slice_timeout = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_SLICE_TIMEOUT";
    }

    const char *log_dir_path = std::getenv("MC_LOG_DIR");
    if (log_dir_path) {
        google::InitGoogleLogging("mooncake-transfer-engine");
        if (opendir(log_dir_path) == NULL) {
            LOG(WARNING)
                << "Path [" << log_dir_path
                << "] is not a valid directory path. Still logging to stderr.";
        } else if (access(log_dir_path, W_OK) != 0) {
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

    const char *min_port_env = std::getenv("MC_MIN_PRC_PORT");
    if (min_port_env) {
        int val = atoi(min_port_env);
        if (val > 0 && val < 65536)
            config.rpc_min_port = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_PRC_MIN_PORT";
    }

    const char *max_port_env = std::getenv("MC_MAX_PRC_PORT");
    if (max_port_env) {
        int val = atoi(max_port_env);
        if (val > 0 && val < 65536)
            config.rpc_max_port = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_PRC_MAX_PORT";
    }

    if (std::getenv("MC_USE_IPV6")) {
        config.use_ipv6 = true;
    }

    const char *fragment_ratio = std::getenv("MC_FRAGMENT_RATIO");
    if (fragment_ratio) {
        size_t val = atoi(fragment_ratio);
        if (val > 0 && val < config.slice_size)
            config.fragment_limit = config.slice_size / val;
        else {
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_FRAGMENT_RATIO and set it to 4 as default";
            config.fragment_limit = config.slice_size / 4;
        }
    }

    if (std::getenv("MC_ENABLE_DEST_DEVICE_AFFINITY")) {
        config.enable_dest_device_affinity = true;
    }

    const char *enable_parallel_reg_mr =
        std::getenv("MC_ENABLE_PARALLEL_REG_MR");
    if (enable_parallel_reg_mr) {
        int val = atoi(enable_parallel_reg_mr);
        if (val >= -1 && val <= 1) {
            config.parallel_reg_mr = val;
        } else {
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_ENABLE_PARALLEL_REG_MR";
        }
    }

    const char *endpoint_store_type_env = std::getenv("MC_ENDPOINT_STORE_TYPE");
    if (endpoint_store_type_env) {
        if (strcmp(endpoint_store_type_env, "FIFO") == 0) {
            config.endpoint_store_type = EndpointStoreType::FIFO;
        } else if (strcmp(endpoint_store_type_env, "SIEVE") == 0) {
            config.endpoint_store_type = EndpointStoreType::SIEVE;
        } else {
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_ENDPOINT_STORE_TYPE, it should be FIFO|SIEVE";
        }
    }

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

    const char *ib_relaxed_ordering_env =
        std::getenv("MC_IB_PCI_RELAXED_ORDERING");
    if (ib_relaxed_ordering_env) {
        int val = atoi(ib_relaxed_ordering_env);
        if (val >= 0 && val <= 2)
            config.ib_pci_relaxed_ordering_mode = val;
        else
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_IB_PCI_RELAXED_ORDERING, it should be 0|1|2";
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
}

GlobalConfig &globalConfig() {
    static GlobalConfig config;
    static std::once_flag g_once_flag;
    std::call_once(g_once_flag, []() { loadGlobalConfig(config); });
    return config;
}

uint16_t getDefaultHandshakePort() { return globalConfig().handshake_port; }
}  // namespace mooncake
