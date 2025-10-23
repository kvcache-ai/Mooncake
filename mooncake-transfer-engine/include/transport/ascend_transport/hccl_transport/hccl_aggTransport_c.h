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

#ifndef HCCL_AGGTRANSPORT_C_H
#define HCCL_AGGTRANSPORT_C_H

#include "hccl_transport_mem_c.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

struct HugeBuffer {
    MemBlock memBlock;
    std::atomic<bool> freed{true};
    HugeBuffer(const MemBlock &mb, bool is_freed = true)
        : memBlock(mb), freed{is_freed} {}
};

struct transferReq {
    void *local_addr;
    void *remote_addr;
    uint64_t len;
    int opcode;
    int isMerge;
    int mergeIdx;
    std::string key_str;
};

/* Aggregated External Interface */
extern int aggTransportMemTask(RankInfo *local_rank_info,
                               RankInfo *remote_rank_info,
                               std::vector<MemBlock> &local_memPool,
                               std::vector<MemBlock> &remote_memPool,
                               int opcode, aclrtStream stream, int mem_type);
extern int aggTransportMemTransfer(aclrtStream stream);
extern int aggTransportMemTarget(aclrtStream stream);
extern void aggRegLocalMem(uint64_t addr, uint64_t length, bool isAggBuffer);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // HCCL_AGGTRANSPORT_C_H
