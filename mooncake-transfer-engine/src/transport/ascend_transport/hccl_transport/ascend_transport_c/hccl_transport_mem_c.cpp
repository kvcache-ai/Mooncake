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

#include <iomanip>
#include <cstdint>
#include <bitset>
#include <cstring>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

void* g_dev_addr;

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

    uint32_t devPid;
    ret = SalGetBareTgid(reinterpret_cast<uint32_t *>(&devPid));
    if (ret) {
        LOG(ERROR) << "SalGetBareTgid failed: " << ret;
        return ret;
    }

    local_rank_info->pid = (uint64_t)devPid;

    LOG(INFO) << "initTransportMem local_rank_info rankId: "
              << local_rank_info->rankId
              << ", serverIdx: " << local_rank_info->serverIdx
              << ", deviceLogicId: " << local_rank_info->deviceLogicId
              << ", devicePhyId: " << local_rank_info->devicePhyId
              << ", deviceIp: " << inet_ntoa(local_rank_info->deviceIp)
              << ", devicePort: " << local_rank_info->devicePort
              << ", hostIp: " << inet_ntoa(local_rank_info->hostIp)
              << ", hostPort: " << local_rank_info->hostPort
              << ", device pid: " << local_rank_info->pid;

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
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    int ret = g_target_key_to_connection_map[key_str].transport_mem->AddOpFence(
        stream);
    if (ret) {
        LOG(ERROR) << "transport_mem AddOpFence failed, ret: " << ret;
        return ret;
    }

    return 0;
}

uint64_t g_dev_read_offset = 0;
uint64_t g_dev_write_offset = 0;

// // 打印单个BFloat16值的详细信息
// void print_bfloat16_detail(uint16_t bf16_value) {
//     uint16_t sign = (bf16_value >> 15) & 0x1;
//     uint16_t exponent = (bf16_value >> 7) & 0xFF;
//     uint16_t mantissa = bf16_value & 0x7F;
    
//     LOG(INFO) << "0x" << std::hex << std::setw(4) << std::setfill('0') << bf16_value 
//     << " (二进制: " << std::bitset<16>(bf16_value) << ")" << " [符号: " << sign << ", 指数: " 
//     << std::bitset<8>(exponent) << "(" << std::dec << exponent << ")"
//     << ", 尾数: " << std::bitset<7>(mantissa) << "(" << mantissa << ")]";
// }

// // 将BFloat16转换为近似的float值（简单近似，不处理特殊情况）
// float bfloat16_to_float_approx(uint16_t bf16_value) {
//     // 直接将BFloat16的位模式扩展到float（32位）
//     uint32_t float_bits = static_cast<uint32_t>(bf16_value) << 16;
//     float result;
//     std::memcpy(&result, &float_bits, sizeof(float));
//     return result;
// }

// void print_bfloat16_memory(const void* data, size_t len) {
//     // 确保长度是2的倍数（BFloat16是2字节）
//     size_t num_elements = len / sizeof(uint16_t);
//     if (len % sizeof(uint16_t) != 0) {
//         std::cout << "警告: 长度不是2的倍数，可能会截断数据" << std::endl;
//     }
    
//     const uint16_t* bf16_data = static_cast<const uint16_t*>(data);
    
//     LOG(INFO) << "内存地址: " << data;
//     LOG(INFO) << "数据长度: " << len << " 字节 (" << num_elements << " 个BFloat16值)";
//     LOG(INFO) << "==========================================";
    
//     for (size_t i = 0; i < num_elements; ++i) {
//         uint16_t value = bf16_data[i];
        
//         std::cout << "[" << i << "] ";
//         print_bfloat16_detail(value);
        
//         // 显示近似的float值
//         float approx_float = bfloat16_to_float_approx(value);
//         std::cout << " ≈ " << approx_float << "f";
        
//         std::cout << std::endl;
//     }
    
//     // 如果有剩余字节，打印它们
//     if (len % sizeof(uint16_t) != 0) {
//         std::cout << "剩余字节: ";
//         const uint8_t* byte_data = static_cast<const uint8_t*>(data);
//         for (size_t i = num_elements * sizeof(uint16_t); i < len; ++i) {
//             std::cout << "0x" << std::hex << std::setw(2) << std::setfill('0') 
//                      << static_cast<int>(byte_data[i]) << " ";
//         }
//         std::cout << std::dec << std::endl;
//     }
// }

int nonAggTransportMemTask(RankInfo *local_rank_info,
                           RankInfo *remote_rank_info, int op_code,
                           uint64_t offset, uint64_t req_len, void *local_mem,
                           int mem_type, aclrtStream stream) {
    // Check if a connection has been established with the peer, and send local
    // information to the peer
    int ret = 0;
    g_dev_write_offset = 0;
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    uint64_t local_buffer = 0;
    if (mem_type == DDR) {
        std::string local_key = inet_ntoa(local_rank_info->hostIp) + std::to_string(local_rank_info->devicePhyId);
        // PUT OWN OBJECT / GET OWN OBJECT
        if (local_key == key_str) {
            if (op_code == WRITE) {
                ret = aclrtMemcpy(reinterpret_cast<void *>(offset), req_len, local_mem, req_len, ACL_MEMCPY_DEVICE_TO_HOST);
                if (ret != ACL_ERROR_NONE) {
                    LOG(ERROR) << "Failed to copy data from device to host, ret: " << ret << ", local" << local_mem << ", dest:" << offset << ", len:" << req_len;
                    return ret;
                }
                LOG(INFO) << "PUT: copy data from device to host, ret: " << ret << ", local" << local_mem << ", dest:" << offset << ", len:" << req_len;
                // print_bfloat16_memory(reinterpret_cast<void *>(offset), req_len);
                return ret;
            }
            ret = aclrtMemcpy(local_mem, req_len, reinterpret_cast<void *>(offset), req_len, ACL_MEMCPY_HOST_TO_DEVICE);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to copy data from host to device, ret: " << ret << ", local" << local_mem << ", dest:" << offset << ", len:" << req_len;
                return ret;
            }
            LOG(INFO) << "GET: copy data from host to device, ret: " << ret << ", local:" << local_mem << ", dest:" << offset << ", len:" << req_len;
            return ret;
        }
        local_buffer = (uint64_t)g_dev_addr + g_dev_read_offset;
        ret = aclrtMemcpy(reinterpret_cast<void *>(local_buffer), req_len, local_mem, req_len, ACL_MEMCPY_DEVICE_TO_DEVICE);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to copy data from device to device, ret: " << ret << ", local" << local_mem << ", dest:" << offset << ", len:" << req_len;
            return ret;
        }
        g_dev_read_offset += req_len;
    }
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    auto iter = g_target_key_to_connection_map.find(key_str);
    if (iter == g_target_key_to_connection_map.end()) {
        ret = controlInfoSend(local_rank_info, remote_rank_info);
        if (ret) {
            LOG(ERROR) << "controlInfoSend failed, ret: " << ret;
            return ret;
        }
        bool same_host =
            local_rank_info->hostIp.s_addr == remote_rank_info->hostIp.s_addr;
        // For A2 series, internal communication among 8 cards does not cross
        // HCCS, such as communication among cards 0-7
        bool same_group = (local_rank_info->devicePhyId / 8) ==
                          (remote_rank_info->devicePhyId / 8);
        bool is_cross_hccs = !(same_host && same_group);
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
                                 is_cross_hccs, transport_mem);
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
    if (mem_type == DDR) {
        int client_socket = g_target_key_to_connection_map[key_str].tcp_socket;
        bool isRead = op_code != WRITE;
        SingleCopyInfo copy_info;
        copy_info.host_addr = offset;
        copy_info.len = req_len;
        copy_info.is_read = isRead;
        copy_info.is_copy = isRead;
        copy_info.local_id = local_rank_info->devicePhyId;
        copy_info.remote_id = remote_rank_info->devicePhyId;
        copy_info.offset = g_dev_read_offset;
        LOG(INFO) << "send client:" << client_socket
                << ", offset:" << offset
                << ", local_id:" << local_rank_info->devicePhyId
                << ", remote_id:" << remote_rank_info->devicePhyId
                << ", offset:" << copy_info.offset
                << ", len:" << req_len;
        ret = send(client_socket, &copy_info, sizeof(SingleCopyInfo), 0);
        if (ret < 0) {
            LOG(ERROR) << "send copy_info failed, ret: " << ret
                    << ", errno: " << errno
                    << ", error: " << strerror(errno);
            // close(client_socket);
            return ret;
        } 
        LOG(INFO) << "send ok:"
                << ", local_id:" << local_rank_info->devicePhyId
                << ", remote_id:" << remote_rank_info->devicePhyId;
        
        SingleCopyInfo single_copy_info;
        ret = recv(client_socket, &single_copy_info, sizeof(SingleCopyInfo), 0);
        if (ret <= 0) {
            if (ret < 0) {
                LOG(ERROR) << "recv single_copy_info failed, ret: " << ret;
            } else {
                LOG(ERROR) << "peer close the connection, ret: " << ret;
            }
            LOG(ERROR) << "failed to receive copy_info, ret: " << ret
                    << ", errno: " << errno
                    << ", error: " << strerror(errno);
            return ret;
        }
        LOG(INFO) << "recv ok client_socket:" << client_socket
                << ", device:" << single_copy_info.device_addr
                << ", local_id:" << local_rank_info->devicePhyId
                << ", remote_id:" << remote_rank_info->devicePhyId
                << ", op_code:" << op_code;
        localMem.addr = (void *)local_buffer;
        remoteMem.addr = (void *)single_copy_info.device_addr;
    }

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

int transportMemIntegrate(RankInfo *local_rank_info, RankInfo *remote_rank_info, int op_code,
                          uint64_t offset, uint64_t req_len,
                          void *local_mem, aclrtStream stream) {
    g_dev_read_offset = 0;
    int ret = 0;
    uint64_t local_buffer = 0;
    uint64_t local_dev_addr = reinterpret_cast<uint64_t>(g_dev_addr);
    if (op_code == WRITE) {
        std::string key_str = inet_ntoa(remote_rank_info->hostIp) + std::to_string(remote_rank_info->devicePhyId);
        int client_socket = g_target_key_to_connection_map[key_str].tcp_socket;
        SingleCopyInfo copy_info;
        copy_info.host_addr = offset;
        copy_info.len = req_len;
        copy_info.is_read = false;
        copy_info.is_copy = true;
        copy_info.local_id = local_rank_info->devicePhyId;
        copy_info.remote_id = remote_rank_info->devicePhyId;
        copy_info.offset = g_dev_write_offset;
        LOG(INFO) << "put send client:" << client_socket
                   << " , offset:" << offset
                   << " , local_id:" << local_rank_info->devicePhyId
                   << " , remote_id:" << remote_rank_info->devicePhyId
                   << " , offset:" << g_dev_write_offset
                  << ", len:" << req_len;
        ret = send(client_socket, &copy_info, sizeof(SingleCopyInfo), 0);
        if (ret < 0) {
            LOG(ERROR) << "send copy failed, ret: " << ret
                    << ", errno: " << errno
                    << ", error: " << strerror(errno);
            // close(client_socket);
            return -1;
        }
        LOG(INFO) << "put send ok client:" << client_socket
                   << " , offset:" << offset
                   << " , local_id:" << local_rank_info->devicePhyId
                   << " , remote_id:" << remote_rank_info->devicePhyId;
        SingleCopyInfo status_info;
        ret = recv(client_socket, &status_info, sizeof(SingleCopyInfo), 0);
        if (ret <= 0) {
            LOG(ERROR) << "failed to receive copy_info, ret: " << ret
                    << ", errno: " << errno
                    << ", error: " << strerror(errno);
            // close(client_socket);
            return -1;
        }
        LOG(INFO) << "put recv ok client:" << client_socket
            << " , offset:" << offset
            << " , local_id" << local_rank_info->devicePhyId
            << " , remote_id" << remote_rank_info->devicePhyId;
        g_dev_write_offset += req_len;
        return 0;
    } else{
        local_buffer = (uint64_t)g_dev_addr + g_dev_write_offset;
        ret = aclrtMemcpy(local_mem, req_len, reinterpret_cast<void *>(local_buffer), req_len, ACL_MEMCPY_DEVICE_TO_DEVICE);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to copy data from device to device, ret: " << ret << ", local" << local_buffer << ", dest:" << local_mem << ", len:" << req_len;
            return ret;
        }
        LOG(INFO) << "Get to copy data from device to device, ret: " << ret << ", local" << local_buffer << ", dest:" << local_mem << ", len:" << req_len;
    }
    g_dev_write_offset += req_len;
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
    
    int client_socket = acceptFromTarget();
    if (client_socket < 0) {
        LOG(ERROR) << "acceptFromTarget failed, client_socket: "
                   << client_socket;
        return client_socket;
    }
    RankControlInfo remote_control_info;
    ret = recv(recv_socket, &remote_control_info, sizeof(RankControlInfo), 0);
    if (ret <= 0) {
        LOG(ERROR) << "Failed to receive remote_control_info, ret: " << ret
                   << ", errno: " << errno << ", error: " << strerror(errno);
        return -1;
    }
    // RankControlInfo remote_control_info;
    // ret = recv(recv_socket, &remote_control_info, sizeof(RankControlInfo), 0);
    // if (ret <= 0) {
    //     LOG(ERROR) << "Failed to receive remote_control_info, ret: " << ret
    //                << ", errno: " << errno << ", error: " << strerror(errno);
    //     return -1;
    // }

    LOG(INFO) << "Received remote_control_info, deviceLogicId: "
              << remote_control_info.deviceLogicId
              << ", devicePhyId: " << remote_control_info.devicePhyId
              << ", hostIp: " << inet_ntoa(remote_control_info.hostIp)
              << ", deviceIp: " << inet_ntoa(remote_control_info.deviceIp)
              << ", device pid: " << remote_control_info.pid;

    // Check if TransportMem has been established with the peer
    std::string key_str = inet_ntoa(remote_control_info.hostIp) +
                          std::to_string(remote_control_info.devicePhyId);
    auto iter = g_target_key_to_connection_map.find(key_str);
    if (iter != g_target_key_to_connection_map.end()) {
        LOG(WARNING)
            << "A duplicate connection request from the same remote endpoint "
               "has been detected, the remote side may have restarted.";
    }

    std::string baseTag_ = key_str + inet_ntoa(local_rank_info->hostIp) +
                           std::to_string(local_rank_info->devicePhyId);
    hccl::HcclIpAddress rempoteDevIp;
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    bool same_host =
        local_rank_info->hostIp.s_addr == remote_control_info.hostIp.s_addr;
    // For A2 series, internal communication among 8 cards does not cross HCCS,
    // such as communication among cards 0-7
    bool same_group = (local_rank_info->devicePhyId / 8) ==
                      (remote_control_info.devicePhyId / 8);
    bool is_cross_hccs = !(same_host && same_group);
    if (enableAscendLogging()) {
        LOG(INFO) << "transport is cross_hccs: "
                  << (is_cross_hccs ? "true (cross-hccs)"
                                    : "false (same-hccs)");
    }
    if (!is_cross_hccs) {
        std::vector<unsigned int> remoteDevPhyId;
        rempoteDevIp = hccl::HcclIpAddress(remote_control_info.devicePhyId);
        remoteDevPhyId.emplace_back(remote_control_info.devicePhyId);
        ret = hrtRaGetSingleSocketVnicIpInfo(
            local_rank_info->devicePhyId, DeviceIdType::DEVICE_ID_TYPE_PHY_ID,
            remote_control_info.devicePhyId, rempoteDevIp);
        if (ret) {
            LOG(ERROR) << "hrtRaGetSingleSocketVnicIpInfo failed, ret: " << ret;
            return ret;
        }
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
        ret = acceptHcclSocket(hccl_ctrl_socket, baseTag_ + "ctrl",
                               rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket ctrl failed, ret: " << ret;
            return ret;
        }

        ret = acceptHcclSocket(hccl_data_socket, baseTag_ + "data",
                               rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket data failed, ret: " << ret;
            return ret;
        }
    } else {
        rempoteDevIp = hccl::HcclIpAddress(remote_control_info.deviceIp);
        ret = acceptHcclSocket(hccl_ctrl_socket, baseTag_ + "ctrl",
                               rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket ctrl failed, ret: " << ret;
            return ret;
        }

        ret = acceptHcclSocket(hccl_data_socket, baseTag_ + "data",
                               rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptHcclSocket data failed, ret: " << ret;
            return ret;
        }
    }

    LOG(INFO) << "accept hccl socket success.";

    g_target_key_to_connection_map[key_str].hccl_ctrl_socket = hccl_ctrl_socket;
    g_target_key_to_connection_map[key_str].hccl_data_socket = hccl_data_socket;

    RankInfo remote_rank_info(remote_control_info);
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    ret = createTransportMem(local_rank_info, &remote_rank_info, key_str,
                             is_cross_hccs, transport_mem);
    if (ret) {
        LOG(ERROR) << "createTransportMem failed, ret: " << ret;
        return ret;
    }

    g_target_key_to_connection_map[key_str].tcp_socket = client_socket;
    g_target_key_to_connection_map[key_str].recv_socket = recv_socket;
    g_target_key_to_connection_map[key_str].transport_mem = transport_mem;
    struct epoll_event event;
    event.events = EPOLLIN;
    // event.data.fd = recv_socket;
    // if (epoll_ctl(g_epoll_fd_target, EPOLL_CTL_ADD, recv_socket, &event) == -1) {
    //     LOG(ERROR) << "epoll_ctl: add failed, recv_socket:" << recv_socket;
    //     return -1;
    // }
    // if (aggregateEnabled) {
    //     event.data.fd = recv_socket;
    //     if (epoll_ctl(g_epoll_fd_agg, EPOLL_CTL_ADD, recv_socket, &event) ==
    //         -1) {
    //         LOG(ERROR) << "epoll_ctl: ADD";
    //         return -1;
    //     }
    // }
    if (aggregateEnabled) {
        event.data.fd = recv_socket;
        if (epoll_ctl(g_epoll_fd_agg, EPOLL_CTL_ADD, recv_socket, &event) ==
            -1) {
            LOG(ERROR) << "epoll_ctl: ADD";
            return -1;
        }
    }
    return 0;
}

int recvMemInfo (int client_socket, aclrtStream stream) {
    int ret = 0;
    SingleCopyInfo single_copy_info;
    ret = recv(client_socket, &single_copy_info, sizeof(single_copy_info), 0);
    if (ret <= 0) {
        LOG(ERROR) << "failed to receive single_copy_info, ret: " << ret
            << ", errno: " << errno
            << ", error: " << strerror(errno);
        return -1;
    }
    LOG(INFO) << "recv host addr:" << single_copy_info.host_addr
            << " , client_socket" << client_socket
            << " , local_id" << single_copy_info.remote_id
            << " , remote_id" <<  single_copy_info.local_id
            << ", offset" << single_copy_info.offset
            << ", len:" << single_copy_info.len;
    uint64_t device_addr = reinterpret_cast<uint64_t>(g_dev_addr) + single_copy_info.offset;
    if (single_copy_info.is_read) {
        if (single_copy_info.is_copy) {
            ret = aclrtMemcpy(reinterpret_cast<void*>(device_addr), single_copy_info.len, reinterpret_cast<void*>(single_copy_info.host_addr), single_copy_info.len,
                              ACL_MEMCPY_HOST_TO_DEVICE);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to copy data from host to device, ret:" << ret;
                return ret;
            }
            LOG(INFO) << "remote read ok:" << "device_addr:" << device_addr << ", host:" << single_copy_info.host_addr << ", len:" << single_copy_info.len;
            // print_bfloat16_memory(reinterpret_cast<void *>(single_copy_info.host_addr), single_copy_info.len);
        }
    } else {
        if (single_copy_info.is_copy) {
            ret = aclrtMemcpy(reinterpret_cast<void*>(single_copy_info.host_addr), single_copy_info.len, reinterpret_cast<void*>(device_addr), single_copy_info.len,
                              ACL_MEMCPY_DEVICE_TO_HOST);
            if (ret != ACL_ERROR_NONE) {
                LOG(ERROR) << "Failed to copy data from host to device, ret:" << ret;
                return ret;
            }
            LOG(INFO) << "remote put ok:" << "device_addr:" << device_addr << ", host:" << single_copy_info.host_addr << ", len:" << single_copy_info.len;
        }
    }
    SingleCopyInfo single_copy_infos;
    single_copy_infos.device_addr = device_addr;
    LOG(INFO) << "send device addr:" << device_addr
        << " , client_socket" << client_socket
        << " , local_id" << single_copy_info.remote_id
        << " , remote_id" <<  single_copy_info.local_id;
    ret = send(client_socket, &single_copy_infos,
               sizeof(SingleCopyInfo), 0);
    if (ret < 0) {
        LOG(ERROR) << "send to receive single_copy_info, ret: " << ret
            << ", errno: " << errno
            << ", error: " << strerror(errno);
        // close(client_socket);
        return ret;
    }
    return client_socket;
}

int transportMemTarget(aclrtStream stream) {
    int ret = 0;
    struct epoll_event events[MAX_EVENTS];
    int n_events = epoll_wait(g_epoll_fd_target, events, MAX_EVENTS, -1);
    if (n_events == -1) {
        if (errno == EINTR) {
            return 0;
        } else {
            LOG(ERROR) << "epoll_wait failed: " << strerror(errno);
            return -1;
        }
    }

    for (int i = 0; i < n_events; ++i) {
        if (events[i].events & EPOLLIN) {
            int fd = events[i].data.fd;
            ret =recvMemInfo(fd, stream);
            if (ret <= 0) {
                if (ret == 0) {
                    LOG(ERROR) << "epoll_wait failed: " << strerror(errno);
                    epoll_ctl(g_epoll_fd_target, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                } else {
                    LOG(ERROR) << "Failed to recv Mem info, ret:" << ret << ", errno: " << errno;
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        epoll_ctl(g_epoll_fd_target, EPOLL_CTL_DEL, fd, NULL);
                        close(fd);
                    }
                }
            }
        }
    }

    return 0;
}

void nonAggRegLocalMem(uint64_t addr, uint64_t length, bool is_pool) {
    if (is_pool) {
        g_dev_addr = (void *)addr;
    }
    MemBlock memBlock;
    memBlock.addr = addr;
    memBlock.len = length;
    LOG(ERROR) << "addr:" << addr << ", len: " << length << ", is_pool:" << is_pool;
    g_localBuffer.emplace_back(memBlock);
    return;
}
#ifdef __cplusplus
}
#endif  // __cplusplus