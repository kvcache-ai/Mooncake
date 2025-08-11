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

#ifndef HCCL_TRANSPORT_MEM_INTERNALS_H
#define HCCL_TRANSPORT_MEM_INTERNALS_H

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

#define MAX_EVENTS 32
#define CONNECT_MAX 1000
#define HCCL_RETRY_TIMES 3

struct RankControlInfo {
    uint64_t deviceLogicId;
    uint64_t devicePhyId;
    struct in_addr hostIp;
    struct in_addr deviceIp;
    uint64_t pid;  // npu device pid
};

struct RankInfo {
    uint64_t rankId = 0xFFFFFFFF;
    uint64_t serverIdx = 0;
    struct in_addr hostIp;
    uint64_t hostPort = 0;
    uint64_t deviceLogicId = 0;
    uint64_t devicePhyId = 0;
    DevType deviceType{DevType::DEV_TYPE_NOSOC};
    struct in_addr deviceIp;
    uint64_t devicePort = 16666;
    uint64_t pid;  // npu device pid
    RankInfo() = default;
    RankInfo(const RankControlInfo &controlInfo)
        : hostIp(controlInfo.hostIp),
          deviceLogicId(controlInfo.deviceLogicId),
          devicePhyId(controlInfo.devicePhyId),
          deviceIp(controlInfo.deviceIp),
          pid(controlInfo.pid) {}
};

struct MemBlock {
    uint64_t addr = 0;
    uint64_t len = 0;
    int mem_type = 0;
    MemBlock() : addr(0), len(0), mem_type(1) {}
    MemBlock(uint64_t a, uint64_t l, int m) : addr(a), len(l), mem_type(m) {}
};

// Retry mechanism for initialization function failure
#define RETRY_CALL(funcCall, errorMsg)                                     \
    do {                                                                   \
        int retryCount = 0;                                                \
        int __ret = funcCall;                                              \
        while (__ret && retryCount < 3) {                                  \
            LOG(ERROR) << errorMsg << ", retrying... (" << ++retryCount    \
                       << "/3)" << __ret;                                  \
            __ret = funcCall;                                              \
        }                                                                  \
        if (__ret) {                                                       \
            LOG(ERROR) << errorMsg << " failed after 3 retries." << __ret; \
            return __ret;                                                  \
        }                                                                  \
    } while (0)

struct ConnectionInfo {
    int tcp_socket;
    int recv_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    std::shared_ptr<hccl::TransportMem> transport_mem;
    int total_len;
};

struct SingleCopyInfo {
    uint64_t host_addr;
    uint64_t device_addr;
    uint64_t len;
    bool is_read;
    bool is_copy;
    uint64_t local_id;
    uint64_t remote_id;
    uint64_t offset;
};

extern std::unordered_map<std::string, ConnectionInfo>
    g_target_key_to_connection_map;
extern std::vector<MemBlock> g_localBuffer;
extern int g_epoll_fd_agg;
extern int g_epoll_fd_target;

extern int initServerNetSocket(RankInfo *local_rank_info);

extern int initControlSocket(RankInfo *local_rank_info, bool aggregateEnabled);

extern int controlInfoSend(RankInfo *local_rank_info,
                           RankInfo *remote_rank_info);

extern int createClientSocket(std::shared_ptr<hccl::HcclSocket> &hccl_socket,
                              RankInfo *local_rank_info,
                              RankInfo *remote_rank_info, bool is_cross_hccs,
                              std::string tag);

extern int createTransportMem(
    RankInfo *local_rank_info, RankInfo *remote_rank_info, std::string key_str,
    bool is_cross_hccs, std::shared_ptr<hccl::TransportMem> &transport_mem);

extern int socketEpollWait();

extern int acceptFromTarget();

extern int acceptHcclSocket(std::shared_ptr<hccl::HcclSocket> &hccl_socket,
                            std::string baseTag_,
                            hccl::HcclIpAddress rempoteDevIp,
                            bool is_cross_hccs);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // HCCL_TRANSPORT_MEM_INTERNALS_H
