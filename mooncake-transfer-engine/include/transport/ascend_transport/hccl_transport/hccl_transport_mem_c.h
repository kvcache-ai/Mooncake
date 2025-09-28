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

#include <condition_variable>
#include <glog/logging.h>
#include <functional>
#include "acl/acl.h"
#include "adapter_hccp_common.h"
#include "dispatcher.h"
#include "dtype_common.h"
#include "externalinput_pub.h"
#include "hccl.h"
#include "hccl_types.h"
#include "hccl_check_buf_init.h"
#include "hccl_check_common.h"
#include "hccl_ip_address.h"
#include "hccl_network_pub.h"
#include "hccl_opbase_rootinfo_base.h"
#include "hccl_socket.h"
#include "mem_device_pub.h"
#include "notify_pool.h"
#include "p2p_mgmt_pub.h"
#include "sal_pub.h"
#include "transport_mem.h"
#include "transport_pub.h"
#include "hccl_mem.h"
#include "hccl_mem_defs.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

struct RankInfo {
    uint64_t rankId = 0xFFFFFFFF;
    uint64_t serverIdx;
    struct in_addr hostIp;
    uint64_t hostPort;
    uint64_t deviceLogicId;
    uint64_t devicePhyId;
    DevType deviceType{DevType::DEV_TYPE_NOSOC};
    struct in_addr deviceIp;
    uint64_t devicePort;
    uint64_t pid;
};

struct RankControlInfo {
    uint64_t deviceLogicId;
    uint64_t devicePhyId;
    struct in_addr hostIp;
    struct in_addr deviceIp;
    uint64_t pid;
};

struct MergeMem {
    void *addr = nullptr;
    uint64_t len = 0;
    MergeMem(void *addr_, size_t len_) : addr(addr_), len(len_) {}
};

struct ConnectionInfo {
    int tcp_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    std::shared_ptr<hccl::TransportMem> transport_mem;
};

// Retry mechanism for initialization function failure
#define RETRY_CALL(funcCall, errorMsg)                                  \
    do {                                                                \
        int retryCount = 0;                                             \
        int __ret = funcCall;                                           \
        while (__ret && retryCount < 3) {                               \
            LOG(ERROR) << errorMsg << ", retrying... (" << ++retryCount \
                       << "/3), ret :" << __ret;                        \
            __ret = funcCall;                                           \
        }                                                               \
        if (__ret) {                                                    \
            LOG(ERROR) << errorMsg                                      \
                       << " failed after 3 retries, ret: " << __ret;    \
            return __ret;                                               \
        }                                                               \
    } while (0)

extern int initTransportMem(RankInfo *local_rank_info);
extern void freeTransportMem();

extern int clearTransportMem(RankInfo *remote_rank_info);
extern int clearTransportMems();

extern int transportMemTask(RankInfo *local_rank_info,
                            RankInfo *remote_rank_info, int op_code,
                            uint64_t offset, uint64_t req_len, void *local_mem,
                            aclrtStream stream);

extern int transportMemAccept(RankInfo *local_rank_info);

extern int regLocalRmaMem(void *addr, uint64_t length);
extern int unregLocalRmaMems();

extern bool printEnabled();

extern int transportMemAddOpFence(RankInfo *remote_rank_info,
                                  aclrtStream stream);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // HCCL_TRANSPORT_MEM_C_H
