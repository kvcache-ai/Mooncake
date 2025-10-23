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

#include <iostream>
#include <string>
#include <pthread.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include "mpi.h"
#include "transport/ascend_transport/hccl_transport/hccl_transport_mem_c.h"
#include "transport/ascend_transport/hccl_transport/hccl_transport_mem_internals.h"

#include <iomanip>
#include <cstdint>
#include <bitset>
#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

void *g_dev_addr;

bool enableAscendLogging() {
    char *env = getenv("ASCEND_TRANSPORT_PRINT");
    return env != nullptr && std::string(env) == "1";
}

int initTransportMem(RankInfo *local_rank_info, bool aggregateEnabled) {
    int ret = 0;
    if (local_rank_info == NULL) {
        LOG(ERROR) << "initTransportMem local_rank_info is NULL";
        return -1;
    }

    ret = rtGetDevicePhyIdByIndex(local_rank_info->deviceLogicId,
                                  &local_rank_info->devicePhyId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: rtGetDevicePhyIdByIndex failed, ret: "
                   << ret;
        return ret;
    }

    local_rank_info->hostPort =
        ASCEND_DEFAULT_HOST_PORT + local_rank_info->devicePhyId;

    getDevIpAddresses(local_rank_info);

    if (a3Enabled()) {
        const int infoTypeSdid = 26;
        ret = rtGetDeviceInfo(local_rank_info->deviceLogicId,
                              RT_MODULE_TYPE_SYSTEM, infoTypeSdid,
                              &local_rank_info->sdid);
        if (ret) {
            LOG(ERROR) << "rtGetDeviceInfo failed, ret: " << ret;
            return ret;
        }
        ret = rtGetServerIDBySDID(local_rank_info->sdid,
                                  &local_rank_info->serverId);
        if (ret) {
            LOG(ERROR) << "rtGetServerIDBySDID failed, ret: " << ret;
            return ret;
        }
    }

    ret = SalGetBareTgid(&local_rank_info->devPid);
    if (ret) {
        LOG(ERROR) << "SalGetBareTgid failed: " << ret;
        return ret;
    }

    LOG(INFO) << "initTransportMem local_rank_info rankId: "
              << local_rank_info->rankId
              << ", serverId: " << local_rank_info->serverId
              << ", deviceLogicId: " << local_rank_info->deviceLogicId
              << ", devicePhyId: " << local_rank_info->devicePhyId
              << ", deviceIp: " << local_rank_info->deviceIp
              << ", devicePort: " << local_rank_info->devicePort
              << ", hostIp: " << local_rank_info->hostIp
              << ", hostPort: " << local_rank_info->hostPort
              << ", device Pid: " << local_rank_info->devPid
              << ", vnicIp: " << local_rank_info->vnicIp
              << ", sdid: " << local_rank_info->sdid;

    // Initialize the virtual network card and socket for the data channel,
    // exchange RmaMem, and create the QP connection
    ret = initServerNetSocket(local_rank_info);
    if (ret) {
        LOG(ERROR) << "initServerNetSocket failed, ret: " << ret;
        return ret;
    }

    ret = initControlSocket(local_rank_info, aggregateEnabled);
    if (ret) {
        LOG(ERROR) << "initControlSocket failed, ret: " << ret;
        return ret;
    }

    g_localBuffer.reserve(VECTOR_RESERVE_SIZE);

    return 0;
}

int transportMemAddOpFence(RankInfo *remote_rank_info, aclrtStream stream) {
    std::string key_str = std::string(remote_rank_info->hostIp) + '-' +
                          std::to_string(remote_rank_info->devicePhyId);
    int ret = g_target_key_to_connection_map[key_str].transport_mem->AddOpFence(
        stream);
    if (ret) {
        LOG(ERROR) << "transport_mem AddOpFence failed, ret: " << ret;
        return ret;
    }

    return 0;
}

int nonAggTransportMemTask(RankInfo *local_rank_info,
                           RankInfo *remote_rank_info, int op_code,
                           uint64_t offset, uint64_t req_len, void *local_mem,
                           int mem_type, aclrtStream stream) {
    // Check if a connection has been established with the peer, and send local
    // information to the peer
    int ret = 0;
    std::string key_str = std::string(remote_rank_info->hostIp) + '-' +
                          std::to_string(remote_rank_info->devicePhyId);
    uint64_t local_buffer = 0;
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    auto iter = g_target_key_to_connection_map.find(key_str);
    if (iter == g_target_key_to_connection_map.end() ||
        g_target_key_to_connection_map[key_str].tcp_socket == 0) {
        ret = controlInfoSend(local_rank_info, remote_rank_info);
        if (ret) {
            LOG(ERROR) << "controlInfoSend failed, ret: " << ret;
            return ret;
        }
        bool is_cross_hccs = false;
        if (a3Enabled()) {
            is_cross_hccs = false;
        } else {
            bool same_host = (strcmp(local_rank_info->hostIp,
                                     remote_rank_info->hostIp) == 0);
            // For A2 series, internal communication among 8 cards does not
            // cross HCCS, such as communication among cards 0-7
            bool same_group = (local_rank_info->devicePhyId / 8) ==
                              (remote_rank_info->devicePhyId / 8);
            is_cross_hccs = !(same_host && same_group);
        }
        if (enableAscendLogging()) {
            LOG(INFO) << "hccl transport is cross_hccs: "
                      << (is_cross_hccs ? "true (cross-hccs)"
                                        : "false (same-hccs)");
        }
        ret = createClientSocket(hccl_ctrl_socket, local_rank_info,
                                 remote_rank_info, is_cross_hccs, "ctrl");
        if (ret) {
            LOG(ERROR) << "createClientSocket hccl_ctrl_socket failed, ret: "
                       << ret;
            return ret;
        }
        g_target_key_to_connection_map[key_str].hccl_ctrl_socket =
            hccl_ctrl_socket;
        ret = createClientSocket(hccl_data_socket, local_rank_info,
                                 remote_rank_info, is_cross_hccs, "data");
        if (ret) {
            LOG(ERROR) << "createClientSocket hccl_data_socket failed, ret: "
                       << ret;
            return ret;
        }
        g_target_key_to_connection_map[key_str].hccl_data_socket =
            hccl_data_socket;

        ret = createTransportMem(local_rank_info, remote_rank_info, key_str,
                                 is_cross_hccs, transport_mem, false);
        if (ret) {
            LOG(ERROR) << "createTransportMem failed, ret: " << ret;
            return ret;
        }
    } else {
        transport_mem = g_target_key_to_connection_map[key_str].transport_mem;
    }
    hccl::TransportMem::RmaOpMem localMem;
    localMem.addr = local_mem;
    localMem.size = req_len;
    hccl::TransportMem::RmaOpMem remoteMem;
    remoteMem.addr = (void *)offset;
    remoteMem.size = req_len;

    if (op_code == WRITE) {
        ret = transport_mem->Write(remoteMem, localMem, stream);
        if (ret) {
            LOG(ERROR) << "transport_mem Write failed, localMem.addr: "
                       << local_mem << "local_mem.size: " << req_len
                       << ", remoteMem.addr: " << remoteMem.addr
                       << ", remoteMem.size: " << req_len << ", ret: " << ret;
            return ret;
        }
    } else {
        ret = transport_mem->Read(localMem, remoteMem, stream);
        if (ret) {
            LOG(ERROR) << "transport_mem Read failed, localMem.addr: "
                       << local_mem << "local_mem.size: " << req_len
                       << ", remoteMem.addr: " << remoteMem.addr
                       << ", remoteMem.size: " << req_len << ", ret: " << ret;
            return ret;
        }
    }

    return 0;
}

int transportMemAccept(RankInfo *local_rank_info, bool aggregateEnabled) {
    // Self-built out-of-band, host socket for receiving control plane
    int ret = socketEpollWait();
    if (ret == 0) {
        return 0;
    } else if (ret < 0) {
        LOG(ERROR) << "socketEpollWait failed, ret: " << ret;
        return ret;
    }

    int recv_socket = acceptFromTarget();
    if (recv_socket < 0) {
        return recv_socket;
    }

    RankControlInfo remote_control_info;
    ret = recv(recv_socket, &remote_control_info, sizeof(RankControlInfo), 0);
    if (ret <= 0) {
        LOG(ERROR) << "Failed to receive remote_control_info, ret: " << ret
                   << ", errno: " << errno << ", error: " << strerror(errno);
        return -1;
    }

    LOG(INFO) << "Received remote_control_info, deviceLogicId: "
              << remote_control_info.deviceLogicId
              << ", devicePhyId: " << remote_control_info.devicePhyId
              << ", hostIp: " << std::string(remote_control_info.hostIp)
              << ", deviceIp: " << std::string(remote_control_info.deviceIp)
              << ", device pid: " << remote_control_info.devPid;

    // Check if TransportMem has been established with the peer
    std::string key_str = std::string(remote_control_info.hostIp) + '-' +
                          std::to_string(remote_control_info.devicePhyId);
    auto iter = g_target_key_to_connection_map.find(key_str);
    if (iter != g_target_key_to_connection_map.end()) {
        LOG(WARNING)
            << "A duplicate connection request from the same remote endpoint "
               "has been detected, the remote side may have restarted.";
    }

    std::string baseTag_ = key_str + std::string(local_rank_info->hostIp) +
                           '-' + std::to_string(local_rank_info->devicePhyId);
    hccl::HcclIpAddress remoteDevIp;
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    bool is_cross_hccs = false;
    if (a3Enabled()) {
        is_cross_hccs = false;
    } else {
        bool same_host =
            (strcmp(local_rank_info->hostIp, remote_control_info.hostIp) == 0);
        // For A2 series, internal communication among 8 cards does not cross
        // HCCS, such as communication among cards 0-7
        bool same_group = (local_rank_info->devicePhyId / 8) ==
                          (remote_control_info.devicePhyId / 8);
        is_cross_hccs = !(same_host && same_group);
    }
    if (enableAscendLogging()) {
        LOG(INFO) << "transport is cross_hccs: "
                  << (is_cross_hccs ? "true (cross-hccs)"
                                    : "false (same-hccs)");
    }
    if (!is_cross_hccs) {
        std::vector<unsigned int> remoteDevPhyId;
        if (a3Enabled()) {
            remoteDevIp = hccl::HcclIpAddress(remote_control_info.vnicIp);
        } else {
            ret = hrtRaGetSingleSocketVnicIpInfo(
                local_rank_info->devicePhyId,
                DeviceIdType::DEVICE_ID_TYPE_PHY_ID,
                remote_control_info.devicePhyId, remoteDevIp);
            if (ret) {
                LOG(ERROR) << "hrtRaGetSingleSocketVnicIpInfo failed, ret: "
                           << ret;
                return ret;
            }
        }
        remoteDevPhyId.push_back(remote_control_info.devicePhyId);
        ret = hccl::P2PMgmtPub::EnableP2P(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub EnableP2P failed, ret: " << ret;
            return ret;
        }
        ret = hccl::P2PMgmtPub::WaitP2PEnabled(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub EnableP2P failed, ret: " << ret;
            return ret;
        }
        ret = acceptHcclSocket(hccl_ctrl_socket, baseTag_ + "ctrl", remoteDevIp,
                               is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket ctrl failed, ret: " << ret;
            return ret;
        }

        ret = acceptHcclSocket(hccl_data_socket, baseTag_ + "data", remoteDevIp,
                               is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket data failed, ret: " << ret;
            return ret;
        }
    } else {
        remoteDevIp = hccl::HcclIpAddress(remote_control_info.deviceIp);
        ret = acceptHcclSocket(hccl_ctrl_socket, baseTag_ + "ctrl", remoteDevIp,
                               is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket ctrl failed, ret: " << ret;
            return ret;
        }

        ret = acceptHcclSocket(hccl_data_socket, baseTag_ + "data", remoteDevIp,
                               is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket data failed, ret: " << ret;
            return ret;
        }
    }

    LOG(INFO) << "accept hccl socket success.";

    g_target_key_to_accept_map[key_str].hccl_ctrl_socket = hccl_ctrl_socket;
    g_target_key_to_accept_map[key_str].hccl_data_socket = hccl_data_socket;

    RankInfo remote_rank_info(remote_control_info);
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    ret = createTransportMem(local_rank_info, &remote_rank_info, key_str,
                             is_cross_hccs, transport_mem, true);
    if (ret) {
        LOG(ERROR) << "createTransportMem failed, ret: " << ret;
        return ret;
    }

    g_target_key_to_accept_map[key_str].tcp_socket = recv_socket;
    struct epoll_event event;
    event.events = EPOLLIN;

    if (aggregateEnabled) {
        event.data.fd = recv_socket;
        if (epoll_ctl(g_epoll_fd_agg, EPOLL_CTL_ADD, recv_socket, &event) ==
            -1) {
            LOG(ERROR) << "epoll_ctl: ADD";
            return -1;
        }
    }
    int ack = 1;
    ret = send(recv_socket, &ack, sizeof(int), 0);
    if (ret < 0) {
        LOG(ERROR) << "Failed to send ack, ret: " << ret << ", errno: " << errno
                   << ", error: " << strerror(errno);
        return -1;
    }

    return 0;
}

int recvMemInfo1(int client_socket, aclrtStream stream) {
    int ret = 0;
    SingleCopyInfo single_copy_info;
    ret = recv(client_socket, &single_copy_info, sizeof(single_copy_info), 0);
    if (ret <= 0) {
        LOG(ERROR) << "failed to receive single_copy_info, ret: " << ret
                   << ", errno: " << errno << ", error: " << strerror(errno);
        return -1;
    }
    uint64_t device_addr =
        reinterpret_cast<uint64_t>(g_dev_addr) + single_copy_info.offset;
    if (single_copy_info.is_read) {
        if (single_copy_info.is_copy) {
            ret = aclrtMemcpy(
                reinterpret_cast<void *>(device_addr), single_copy_info.len,
                reinterpret_cast<void *>(single_copy_info.host_addr),
                single_copy_info.len, ACL_MEMCPY_HOST_TO_DEVICE);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to copy data from host to device, ret:"
                           << ret;
                return ret;
            }
        }
    } else {
        if (single_copy_info.is_copy) {
            ret = aclrtMemcpy(
                reinterpret_cast<void *>(single_copy_info.host_addr),
                single_copy_info.len, reinterpret_cast<void *>(device_addr),
                single_copy_info.len, ACL_MEMCPY_DEVICE_TO_HOST);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to copy data from host to device, ret:"
                           << ret;
                return ret;
            }
        }
    }
    SingleCopyInfo single_copy_infos;
    single_copy_infos.device_addr = device_addr;
    ret = send(client_socket, &single_copy_infos, sizeof(SingleCopyInfo), 0);
    if (ret < 0) {
        LOG(ERROR) << "send to receive single_copy_info, ret: " << ret
                   << ", errno: " << errno << ", error: " << strerror(errno);
        // close(client_socket);
        return ret;
    }
    return client_socket;
}

void nonAggRegLocalMem(uint64_t addr, uint64_t length, bool is_pool) {
    if (is_pool) {
        g_dev_addr = (void *)addr;
    }
    MemBlock memBlock;
    memBlock.addr = addr;
    memBlock.len = length;
    LOG(INFO) << "addr:" << addr << ", len: " << length
              << ", is_pool:" << is_pool;
    g_localBuffer.emplace_back(memBlock);
    return;
}
#ifdef __cplusplus
}
#endif  // __cplusplus