// Copyright 2025 KVCache.AI
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

#ifndef RDMA_PARAMS_H
#define RDMA_PARAMS_H

#include <cstddef>
#include <cstdint>

namespace mooncake {
namespace v1 {

struct DeviceParams {
    int num_cq_list = 4; // == num_workers is preferred
    int num_comp_channels = 1;
    uint8_t port = 1;
    int gid_index = 0;
    int max_cqe = 4096;
};

struct EndPointParams {
    int endpoint_store_cap = 256;
    int qp_mul_factor = 4; // == num_workers is preferred
    int max_sge = 4;
    int max_qp_wr = 256;
    int max_inline_bytes = 64;
    ibv_mtu path_mtu = IBV_MTU_4096;

    // Advanced parameters, do not change unless you understand them
    // INIT State
    uint16_t pkey_index = 0;
    // RTR State
    uint8_t hop_limit = 16;
    uint32_t flow_label = 0;
    uint8_t traffic_class = 0;
    uint8_t service_level = 0;
    uint8_t src_path_bits = 0;
    uint8_t static_rate = 0;
    uint32_t rq_psn = 0;
    uint8_t max_dest_rd_atomic = 16;
    uint8_t min_rnr_timer = 12;
    // RTS State
    uint32_t sq_psn = 0;
    uint8_t send_timeout = 14;
    uint8_t send_retry_count = 7;
    uint8_t send_rnr_count = 7;
    uint8_t max_rd_atomic = 16;
};

struct WorkerParams {
    int num_workers = 4;
    int max_retry_count = 8;
    int block_size = 65536;
    uint64_t grace_period_ns = 50000; // 50us
};

struct RdmaParams {
    DeviceParams device;
    EndPointParams endpoint;
    WorkerParams workers;
};
}  // namespace v1
}  // namespace mooncake
#endif  // RDMA_PARAMS_H