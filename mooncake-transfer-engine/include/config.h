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

#ifndef CONFIG_H
#define CONFIG_H

#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <mutex>

namespace mooncake {
struct GlobalConfig {
    size_t num_cq_per_ctx = 1;
    size_t num_comp_channels_per_ctx = 1;
    uint8_t port = 1;
    int gid_index = 0;
    uint64_t max_mr_size = 0x10000000000;
    size_t max_cqe = 4096;
    int max_ep_per_ctx = 256;
    size_t num_qp_per_ep = 2;
    size_t max_sge = 4;
    size_t max_wr = 256;
    size_t max_inline = 64;
    ibv_mtu mtu_length = IBV_MTU_4096;
    uint16_t handshake_port = 12001;
    int workers_per_ctx = 2;
    size_t slice_size = 65536;
    int retry_cnt = 9;
    int handshake_listen_backlog = 128;
    bool metacache = true;
    int log_level = google::INFO;
    bool trace = false;
    int64_t slice_timeout = -1;
    uint16_t rpc_min_port = 15000;
    uint16_t rpc_max_port = 17000;
    bool use_ipv6 = false;
    size_t fragment_limit = 16384;
    bool enable_dest_device_affinity = false;
};

void loadGlobalConfig(GlobalConfig &config);

void dumpGlobalConfig();

void updateGlobalConfig(ibv_device_attr &device_attr);

GlobalConfig &globalConfig();

uint16_t getDefaultHandshakePort();
}  // namespace mooncake

#endif  // CONFIG_H