// Copyright 2025 Huawei Technologies Co., Ltd
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

#ifndef HCCL_TRANSPORT_MEM_C_H
#define HCCL_TRANSPORT_MEM_C_H

#include <functional>
#include "runtime/dev.h"
#include "hccl_transport_mem_internals.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define READ 0
#define WRITE 1
#define DDR 0
#define HBM 1
#define VECTOR_RESERVE_SIZE 200
#define ASCEND_DEFAULT_HOST_PORT 10000
#define BLOCK_AGGREGATION_THRESHOLD 0x40000

/* Public External Interface */
extern bool enableAscendLogging();
extern int initTransportMem(RankInfo *local_rank_info, bool aggregateEnabled);
extern int transportMemAccept(RankInfo *local_rank_info, bool aggregateEnabled);

/* Non-Aggregated External Interface */
extern void nonAggRegLocalMem(uint64_t addr, uint64_t length, bool is_pool);
extern int nonAggTransportMemTask(RankInfo *local_rank_info,
                                  RankInfo *remote_rank_info, int op_code,
                                  uint64_t offset, uint64_t req_len,
                                  void *local_mem, int mem_type,
                                  aclrtStream stream);
extern int transportMemAddOpFence(RankInfo *remote_rank_info,
                                  aclrtStream stream);
extern int transportMemTarget(aclrtStream stream);
#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // HCCL_TRANSPORT_MEM_C_H
