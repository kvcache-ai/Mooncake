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
#include <dirent.h>
#include <unistd.h>

namespace mooncake {
void loadGlobalConfig(GlobalConfig &config) {
    auto& env = Environ::Get();

    int num_cq_per_ctx = env.GetNumCqPerCtx();
    if (num_cq_per_ctx > 0 && num_cq_per_ctx < 256)
        config.num_cq_per_ctx = num_cq_per_ctx;

    int num_comp_channels_per_ctx = env.GetNumCompChannelsPerCtx();
    if (num_comp_channels_per_ctx > 0 && num_comp_channels_per_ctx < 256)
        config.num_comp_channels_per_ctx = num_comp_channels_per_ctx;

    int port = env.GetIbPort();
    if (port >= 0 && port < 256)
        config.port = uint8_t(port);

    int gid_index = env.GetGidIndex();
    if (gid_index == 0) {
        const char *nccl_gid = std::getenv("NCCL_IB_GID_INDEX");
        if (nccl_gid) gid_index = atoi(nccl_gid);
    }
    if (gid_index >= 0 && gid_index < 256)
        config.gid_index = gid_index;

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
    if (max_sge > 0 && max_sge <= UINT16_MAX)
        config.max_sge = max_sge;

    int max_wr = env.GetMaxWr();
    if (max_wr > 0 && max_wr <= UINT16_MAX)
        config.max_wr = max_wr;

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
    if (slice_size > 0)
        config.slice_size = slice_size;

    size_t min_reg_size = env.GetMinRegSize();
    if (min_reg_size > 0) {
        config.eic_max_block_size = min_reg_size;
        LOG(INFO) << "Barex set MC_MIN_REG_SIZE=" << min_reg_size;
    }

    int retry_cnt = env.GetRetryCnt();
    if (retry_cnt > 0 && retry_cnt < 128)
        config.retry_cnt = retry_cnt;

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
        if (opendir(log_dir_path.c_str()) == NULL) {
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

    int min_prc_port = env.GetMinPrcPort();
    if (min_prc_port > 0 && min_prc_port < 65536)
        config.rpc_min_port = min_prc_port;

    int max_prc_port = env.GetMaxPrcPort();
    if (max_prc_port > 0 && max_prc_port < 65536)
        config.rpc_max_port = max_prc_port;

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

    int ib_tc = env.GetIbTc();
    if (ib_tc >= 0 && ib_tc <= 255) {
        config.ib_traffic_class = ib_tc;
    } else if (ib_tc < -1) {
        LOG(WARNING)
            << "Ignore value from environment variable MC_IB_TC, "
            << "value out of range (should be 0-255)";
    }

    int ib_pci_relaxed_ordering = env.GetIbPciRelaxedOrdering();
    if (ib_pci_relaxed_ordering >= 0 && ib_pci_relaxed_ordering <= 2)
        config.ib_pci_relaxed_ordering_mode = ib_pci_relaxed_ordering;
    else if (ib_pci_relaxed_ordering != 0)
        LOG(WARNING) << "Ignore value from environment variable "
                        "MC_IB_PCI_RELAXED_ORDERING, it should be 0|1|2";
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
