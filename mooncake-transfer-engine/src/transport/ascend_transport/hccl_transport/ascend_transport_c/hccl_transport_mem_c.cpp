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

#include <cassert>
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <string>
#include <pthread.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include <errno.h>
#include "transport/ascend_transport/hccl_transport/hccl_transport_mem_c.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#define READ 0
#define WRITE 1
#define CONNECT_MAX 1000
#define RETRY_TIMES 3
#define MAX_EVENTS 32
#define VECTOR_RESERVE_SIZE 100

HcclNetDevCtx vnicNetDevCtx_{nullptr};
HcclNetDevCtx nicNetDevCtx_{nullptr};

std::shared_ptr<hccl::HcclSocket> vnicServerSocket_{nullptr};
std::shared_ptr<hccl::HcclSocket> nicServerSocket_{nullptr};

std::unique_ptr<hccl::NotifyPool> notifyPool_;
HcclDispatcher dispatcher_{nullptr};

std::unordered_map<std::string, int>  target_key_to_control_socket_map_;
std::unordered_map<std::string, std::shared_ptr<hccl::HcclSocket>> target_key_to_hccl_ctrl_socket_map_;
std::unordered_map<std::string, std::shared_ptr<hccl::HcclSocket>> target_key_to_hccl_data_socket_map_;
std::unordered_map<std::string, std::shared_ptr<hccl::TransportMem>> target_key_to_transport_mem_map_;
std::vector<hccl::TransportMem::RmaMem *> localRmaMem_;

std::vector<void *> g_localMemAddr;
std::vector<uint64_t> g_localMemLen;

int g_server_socket_ = 0;
struct sockaddr_in g_server_addr_;

int g_epoll_fd = 0;
struct epoll_event g_ev;
struct epoll_event g_events[MAX_EVENTS];

// Retry mechanism for initialization function failure
#define RETRY_CALL(funcCall, errorMsg) \
    do { \
        int retryCount = 0; \
        int __ret = funcCall; \
        while (__ret && retryCount < 3) { \
            LOG(ERROR) << errorMsg << ", retrying... (" << ++retryCount << "/3)"; \
            __ret = funcCall; \
        } \
        if (__ret) { \
            LOG(ERROR) << errorMsg << " failed after 3 retries."; \
            return __ret; \
        } \
    } while (0)

bool printEnabled() {
    char* env = getenv("ASCEND_TRANSPORT_PRINT");
    return env != nullptr && std::string(env) == "1";
}

static int initServerNetSocket(RankInfo *local_rank_info) {
    RETRY_CALL(HcclNetInit(NICDeployment::NIC_DEPLOYMENT_DEVICE,
    local_rank_info->devicePhyId, local_rank_info->deviceLogicId, false), "HcclNetInit failed");

    // Use the physical network card of the device across HCCS
    hccl::HcclIpAddress localIp(local_rank_info->deviceIp);
    RETRY_CALL(HcclNetOpenDev(&nicNetDevCtx_, NicType::DEVICE_NIC_TYPE, local_rank_info->devicePhyId, 
        local_rank_info->deviceLogicId, localIp), "HcclNetOpenDev DEVICE_NIC_TYPE failed");

    nicServerSocket_ = std::make_shared<hccl::HcclSocket>(nicNetDevCtx_, local_rank_info->devicePort);
    if (nicServerSocket_ == NULL) {
        LOG(ERROR) << "make nicNetDevCtx_ failed";
        return -1;
    }

    RETRY_CALL(nicServerSocket_->Init(), "nicServerSocket_ Init failed");
    RETRY_CALL(nicServerSocket_->Listen(), "nicServerSocket_ Listen failed");

    // Use virtual network card within HCCS
    hccl::HcclIpAddress localVnicIp(local_rank_info->devicePhyId);
    RETRY_CALL(hrtRaGetSingleSocketVnicIpInfo(
            local_rank_info->devicePhyId, DeviceIdType::DEVICE_ID_TYPE_PHY_ID,
            local_rank_info->devicePhyId, localVnicIp),
            "hrtRaGetSingleSocketVnicIpInfo failed");

    RETRY_CALL(HcclNetOpenDev(&vnicNetDevCtx_, NicType::VNIC_TYPE,
            local_rank_info->devicePhyId,
            local_rank_info->deviceLogicId, localVnicIp),
            "HcclNetOpenDev vnicNetDevCtx_ failed");

    // control plane connection, creat serversocket, listening client
    vnicServerSocket_ = std::make_shared<hccl::HcclSocket>(vnicNetDevCtx_, local_rank_info->devicePort);
    if (vnicServerSocket_ == NULL) {
        LOG(ERROR) << "vnicServerSocket_ make failed";
        return -1;
    }

    RETRY_CALL(vnicServerSocket_->Init(), "vnicServerSocket_ Init failed");
    RETRY_CALL(vnicServerSocket_->Listen(), "vnicServerSocket_ Listen failed");

    RETRY_CALL(HcclDispatcherInit(DispatcherType::DISPATCHER_NORMAL, local_rank_info->devicePhyId, &dispatcher_), 
            "client HcclDispatcherInit failed");
    
    notifyPool_.reset(new (std::nothrow) hccl::NotifyPool());
    if (notifyPool_ == nullptr) {
        LOG(ERROR) << "create notifyPool error";
        return -1;
    }

    RETRY_CALL(notifyPool_->Init(local_rank_info->devicePhyId), "Init notifyPool error");

    return 0;
}

// The out-of-band socket on the host side that ascend_transport depends on, used to convey control information such as deviceId and deviceIp
static int initControlSocket(RankInfo *local_rank_info) {
    int ret = 0;
    g_server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (g_server_socket_ < 0) {
        LOG(ERROR) << "Socket create failed";
        return g_server_socket_;
    }

    int optval = 1;
    ret = setsockopt(g_server_socket_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (ret < 0) {
        LOG(ERROR) << "set sock opt failed, ret: " << ret;
        close(g_server_socket_);
        return ret;
    }

    memset(&g_server_addr_, 0, sizeof(g_server_addr_));
    g_server_addr_.sin_family = AF_INET;
    g_server_addr_.sin_addr.s_addr = INADDR_ANY;
    g_server_addr_.sin_port = htons(local_rank_info->hostPort);

    ret = bind(g_server_socket_, (struct sockaddr*)&g_server_addr_, sizeof(g_server_addr_));
    if (ret < 0) {
        LOG(ERROR) << "Bind Failed, ret: " << ret;
        close(g_server_socket_);
        return ret;
    }

    struct timeval timeout;
    timeout.tv_sec = 120;
    timeout.tv_usec = 0;
    ret = setsockopt(g_server_socket_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
    if (ret < 0) {
        LOG(ERROR) << "Set recv timeout failed, ret: " << ret;
        close(g_server_socket_);
        return ret;
    }

    ret = listen(g_server_socket_, CONNECT_MAX);
    if (ret < 0) {
        LOG(ERROR) << "Listen Failed, ret: " << ret;
        close(g_server_socket_);
        return ret;
    }
    LOG(INFO) << "initControlSocket successful, Server listening on host port" << ntohs(g_server_addr_.sin_port) << "..." << "g_server_socket_" << g_server_socket_;
    g_epoll_fd = epoll_create1(0);
    if (g_epoll_fd == -1) {
        LOG(ERROR) << "epoll create Failed, ret: " << ret;
        close(g_server_socket_);
        return ret;
    }
    g_ev.events = EPOLLIN;
    g_ev.data.fd = g_server_socket_;
    ret = epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, g_server_socket_, &g_ev);
    if (ret == -1) {
        LOG(ERROR) << "epoll epoll_ctl Failed, ret: " << ret;
        close(g_server_socket_);
        return ret;
    }
    return 0;
}

int initTransportMem(RankInfo *local_rank_info) {
    int ret = 0;
    if (local_rank_info == NULL) {
        LOG(ERROR) << "initTransportMem local_rank_info is NULL";
        return -1;
    }

    uint32_t devPid;
    ret = SalGetBareTgid(reinterpret_cast<uint32_t*>(&devPid));
    if (ret) {
        LOG(ERROR) << "HcclTransport: SalGetBareTgid failed: " << ret;
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
              << ", pid: " << local_rank_info->pid;

    // Initialize the virtual network card and socket for the data channel, exchange RmaMem, and create the QP connection
    ret = initServerNetSocket(local_rank_info);
    if (ret) {
        LOG(ERROR) << "initServerNetSocket failed";
        return ret;
    }
    
    ret = initControlSocket(local_rank_info);
    if (ret) {
        LOG(ERROR) << "initControlSocket failed";
        return ret;
    }

    g_localMemAddr.reserve(VECTOR_RESERVE_SIZE);
    g_localMemLen.reserve(VECTOR_RESERVE_SIZE);

    return 0;
}

static int connectToTarget(std::string target_ip, int target_port) {
    int client_socket;
    struct sockaddr_in server_addr;

    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0) {
        LOG(ERROR) << "Socket creation failed";
        return client_socket;
    }

    int optval = 1;
    int ret = setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (ret < 0) {
        LOG(ERROR) << "set sock opt failed, ret: " << ret;
        close(client_socket);
        return ret;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(target_port);
    server_addr.sin_addr.s_addr = inet_addr(target_ip.c_str());

    if (server_addr.sin_addr.s_addr == INADDR_NONE) {
        LOG(ERROR) << "Invalid server IP address";
        close(client_socket);
        return -1;
    }
    
    const int max_retries = 5;
    int connected = 0;

    for (int i = 0; i < max_retries; ++i) {
        if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) == 0) {
            LOG(INFO) << "Connect to host server successful" << target_ip << ":" << ntohs(server_addr.sin_port);
            connected = 1;
            break;
        }

        LOG(ERROR) << "Connect attempt " << i << " failed: " << strerror(errno);

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (!connected) {
        LOG(ERROR) << "Failed to connect to server after " << max_retries << " retries.";
        close(client_socket);
        return -1;
    }

    return client_socket;
}

int controlInfoSend(RankInfo *local_rank_info, RankInfo *remote_rank_info) {
    int ret = 0;
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) + std::to_string(remote_rank_info->devicePhyId);
    LOG(INFO) << "transportMemTask local_rank_info rankId: "
              << local_rank_info->rankId
              << ", serverIdx: " << local_rank_info->serverIdx
              << ", deviceLogicId: " << local_rank_info->deviceLogicId
              << ", devicePhyId: " << local_rank_info->devicePhyId
              << ", deviceIp: " << inet_ntoa(local_rank_info->deviceIp)
              << ", devicePort: " << local_rank_info->devicePort
              << ", hostIp: " << inet_ntoa(local_rank_info->hostIp)
              << ", hostPort: " << local_rank_info->hostPort
              << ", pid: " << local_rank_info->pid;
    LOG(INFO) << "transportMemTask remote_rank_info rankId: "
            << remote_rank_info->rankId
            << ", serverIdx: " << remote_rank_info->serverIdx
            << ", deviceLogicId: " << remote_rank_info->deviceLogicId
            << ", devicePhyId: " << remote_rank_info->devicePhyId
            << ", deviceIp: " << inet_ntoa(remote_rank_info->deviceIp)
            << ", devicePort: " << remote_rank_info->devicePort
            << ", hostIp: " << inet_ntoa(remote_rank_info->hostIp)
            << ", hostPort: " << remote_rank_info->hostPort
            << ", pid: " << remote_rank_info->pid;

    // Encapsulate control information
    RankControlInfo control_info;
    control_info.deviceLogicId = local_rank_info->deviceLogicId;
    control_info.devicePhyId = local_rank_info->devicePhyId;
    control_info.hostIp = local_rank_info->hostIp;
    control_info.deviceIp = local_rank_info->deviceIp;
    control_info.pid = local_rank_info->pid;
    // Self-built out-of-band, host socket for sending control plane
    int client_socket = connectToTarget(inet_ntoa(remote_rank_info->hostIp), 
                                        remote_rank_info->hostPort);
    if (client_socket < 0) {
        LOG(ERROR) << "client connect failed";
        return client_socket;
    }
    ret = send(client_socket, &control_info, sizeof(RankControlInfo), 0);
    if (ret < 0) {
        LOG(ERROR) << "send control_info failed, ret: " << ret;
        close(client_socket);
        return ret;
    }
    target_key_to_control_socket_map_[key_str] = client_socket;
    return 0;
}

int createClientSocket(std::shared_ptr<hccl::HcclSocket> &hccl_socket, RankInfo *local_rank_info, RankInfo *remote_rank_info, bool is_cross_hccs, std::string tag) {
    int ret = 0;
    hccl::HcclIpAddress rempoteDevIp;
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) + std::to_string(remote_rank_info->devicePhyId);
    std::string baseTag_ = inet_ntoa(local_rank_info->hostIp) + std::to_string(local_rank_info->devicePhyId) + key_str + tag;
    if (!is_cross_hccs) {
        std::vector<unsigned int> remoteDevPhyId;
        remoteDevPhyId.push_back(remote_rank_info->devicePhyId);
        ret = hccl::P2PMgmtPub::EnableP2P(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub EnableP2P failed, ret:" << ret;
            return ret;
        }
        ret = hccl::P2PMgmtPub::WaitP2PEnabled(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub EnableP2P failed, ret:" << ret;
            return ret;
        }
        rempoteDevIp = hccl::HcclIpAddress(remote_rank_info->devicePhyId);
        ret = hrtRaGetSingleSocketVnicIpInfo(local_rank_info->devicePhyId,
                                            DeviceIdType::DEVICE_ID_TYPE_PHY_ID,
                                            remote_rank_info->devicePhyId, rempoteDevIp);
        if (ret) {
            LOG(ERROR) << "hrtRaGetSingleSocketVnicIpInfo, ret:" << ret;
            return ret;
        }                                    
        hccl_socket = std::make_shared<hccl::HcclSocket>(
                baseTag_, vnicNetDevCtx_, rempoteDevIp, remote_rank_info->devicePort,
                hccl::HcclSocketRole::SOCKET_ROLE_CLIENT);
    } else {
        rempoteDevIp = hccl::HcclIpAddress(remote_rank_info->deviceIp);
        hccl_socket = 
            std::make_shared<hccl::HcclSocket>(
                baseTag_, nicNetDevCtx_, rempoteDevIp, remote_rank_info->devicePort,
                hccl::HcclSocketRole::SOCKET_ROLE_CLIENT);
    }
    
    ret = hccl_socket->Init();
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client hccl_socket init failed, target rank_id:" 
                    << remote_rank_info->devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", remote port:" << remote_rank_info->devicePort
                    << ", ret: " << ret;
        return ret;
    }
    ret = hccl_socket->Connect();
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client hccl_socket Connect failed, target rank_id:" 
                    << remote_rank_info->devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", remote port:" << remote_rank_info->devicePort
                    << ", ret: " << ret;
        return ret;
    }
    LOG(INFO) << "hccl_socket begin to connect";
    hccl::HcclSocketStatus status;
    do {
        status = hccl_socket->GetStatus();
    } while (status != hccl::HcclSocketStatus::SOCKET_OK);
    LOG(INFO) << "hccl_socket connect success";

    return 0;
}

int createTransportMem(RankInfo *local_rank_info, RankInfo *remote_rank_info, std::shared_ptr<hccl::TransportMem>& transport_mem) {
    int ret = 0;
    bool same_host = local_rank_info->hostIp.s_addr == remote_rank_info->hostIp.s_addr;
    // For A2 series, internal communication among 8 cards does not cross HCCS, such as communication among cards 0-7
    bool same_group = (local_rank_info->devicePhyId / 8) == (remote_rank_info->devicePhyId / 8);
    bool is_cross_hccs = !(same_host && same_group);
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) + std::to_string(remote_rank_info->devicePhyId);
    if (printEnabled()) {
        LOG(INFO) << "transport is cross_hccs: " << (is_cross_hccs ? "true (cross-hccs)" : "false (same-hccs)");
    }
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    ret = createClientSocket(hccl_ctrl_socket, local_rank_info, remote_rank_info, is_cross_hccs, "ctrl");
    if (ret) {
        LOG(ERROR) << "createClientSocket hccl_ctrl_socket failed, ret: " << ret;
        return ret;
    }
    target_key_to_hccl_ctrl_socket_map_[key_str] = hccl_ctrl_socket;
    ret = createClientSocket(hccl_data_socket, local_rank_info, remote_rank_info, is_cross_hccs, "data");
    if (ret) {
        LOG(ERROR) << "createClientSocket hccl_data_socket failed, ret: " << ret;
        return ret;
    }
    target_key_to_hccl_data_socket_map_[key_str] = hccl_data_socket;
    hccl::TransportMem::AttrInfo attrInfo;
    attrInfo.localRankId = local_rank_info->deviceLogicId;
    attrInfo.remoteRankId = remote_rank_info->deviceLogicId;
    attrInfo.sdid = 0xFFFFFFFF;
    attrInfo.serverId = local_rank_info->serverIdx;
    if (is_cross_hccs) {
        transport_mem = hccl::TransportMem::Create(
            hccl::TransportMem::TpType::ROCE, notifyPool_, nicNetDevCtx_, 
            dispatcher_, attrInfo);
    } else {
        transport_mem = hccl::TransportMem::Create(
            hccl::TransportMem::TpType::IPC, notifyPool_, vnicNetDevCtx_, 
            dispatcher_, attrInfo);
    }
    ret = transport_mem->SetDataSocket(hccl_data_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client SetDataSocket failed, target rank_id:" 
                    << remote_rank_info->devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", remote port:" << remote_rank_info->devicePort
                    << ", ret: " << ret;
        return ret;
    }
    ret = transport_mem->SetSocket(hccl_ctrl_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client SetSocket failed, target rank_id:" 
                    << remote_rank_info->devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", remote port:" << remote_rank_info->devicePort
                    << ", ret: " << ret;
        return ret;
    }
    ret = transport_mem->Connect(120);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client Connect failed, target rank_id:" 
                    << remote_rank_info->devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", remote port:" << remote_rank_info->devicePort
                    << ", ret: " << ret;
        return ret;
    }
    LOG(INFO) << "transport_mem connect success";
    target_key_to_transport_mem_map_[key_str] = transport_mem;
    size_t m_num = g_localMemAddr.size();
    std::vector<hccl::TransportMem::RmaMemDesc> rmaMemDescs(m_num);

    for (size_t i = 0; i < m_num; ++i) {
        HcclMem mem;
        HcclBuf buf;
        mem.addr = g_localMemAddr[i];
        mem.size = (uint64_t)g_localMemLen[i];
        mem.type = HCCL_MEM_TYPE_DEVICE;
        if (!is_cross_hccs) {
            ret = HcclMemReg(vnicNetDevCtx_, &mem, &buf);
        } else {
            ret = HcclMemReg(nicNetDevCtx_, &mem, &buf);
        }
        if (ret != 0 && ret != 20) {
            LOG(ERROR) << "HcclMemReg failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];
            return ret;
        }
        char *desc = nullptr;
        uint64_t desc_len = 0;
        ret = HcclMemExport(&buf, &desc, &desc_len);
        if (ret) {
            LOG(ERROR) << "HcclMemExport failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];
            return ret;
        }
        
        rmaMemDescs[i].localRankId = local_rank_info->deviceLogicId;
        rmaMemDescs[i].remoteRankId = remote_rank_info->deviceLogicId;
        memset_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, 0, hccl::TRANSPORT_EMD_ESC_SIZE);
        if (memcpy_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, desc, desc_len + 1) != EOK) {
            LOG(ERROR) << "memcpy_s failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];
            return -1;
        }

        // In the scenario within HCCS, it is necessary to call HcclMemGrant to authorize peer memory
        if (!is_cross_hccs) {
            HcclMemGrantInfo grant_info;
            grant_info.remotePid = (int32_t)remote_rank_info->pid;
            LOG(INFO) << "HcclMemGrant begin, remote pid:" << grant_info.remotePid;
            grant_info.remoteSdid = 0xFFFFFFFF;
            ret = HcclMemGrant(&buf, &grant_info);
            if (ret) {
                LOG(ERROR) << "HcclMemGrant failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];;
                return ret;
            }
        }
    }
    hccl::TransportMem::RmaMemDescs localRmaMemDescs;
    localRmaMemDescs.array = rmaMemDescs.data();
    localRmaMemDescs.arrayLength = rmaMemDescs.size();
    uint32_t actualNumOfRemote = 0;
    std::vector<hccl::TransportMem::RmaMemDesc> remoteRmaMemDescArray(m_num);
    hccl::TransportMem::RmaMemDescs remoteRmaMemDescs;
    remoteRmaMemDescs.array = remoteRmaMemDescArray.data();
    remoteRmaMemDescs.arrayLength = m_num;
    ret = transport_mem->ExchangeMemDesc(localRmaMemDescs, remoteRmaMemDescs, actualNumOfRemote);
    if (ret) {
        LOG(ERROR) << "transport_mem->ExchangeMemDesc failed, ret:" << ret << "local_rank: " << local_rank_info->devicePhyId << "remote_rank: " << remote_rank_info->devicePhyId;
        return ret;
    }
    std::vector<hccl::TransportMem::RmaMem> remoteRmaMemArray(m_num);
    for (uint32_t i = 0; i < m_num; ++i) {
        ret = transport_mem->EnableMemAccess(remoteRmaMemDescArray[i], remoteRmaMemArray[i]);
        if (ret) {
            LOG(ERROR) << "transport_mem->EnableMemAccess failed, ret:" << ret << " i:" << i << "local_rank: " << local_rank_info->devicePhyId << "remote_rank: " << remote_rank_info->devicePhyId;
            return ret;
        }
    }

    LOG(INFO) << "ExchangeMem and EnableMemAccess Success!";
    return 0;
}

int transportMemTask(RankInfo *local_rank_info, RankInfo *remote_rank_info, 
                    int op_code, uint64_t offset,
                    uint64_t req_len, void *local_mem, aclrtStream stream)
{
    int ret = 0;
    // 1. Check if a connection has been established with the peer, and send local information to the peer
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) + std::to_string(remote_rank_info->devicePhyId);
    auto iter = target_key_to_control_socket_map_.find(key_str);
    if (iter == target_key_to_control_socket_map_.end()) {
        ret = controlInfoSend(local_rank_info, remote_rank_info);
        if (ret) {
            LOG(ERROR) << "controlInfoSend failed, ret: " << ret;
            return ret;
        }
    }

    // 2. Check if transport_mem has been established with the peer, and send local information to the peer.
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    auto iter_mem = target_key_to_transport_mem_map_.find(key_str);
    if (iter_mem == target_key_to_transport_mem_map_.end()) {
        ret = createTransportMem(local_rank_info, remote_rank_info, transport_mem);
        if (ret) {
            LOG(ERROR) << "createTransportMem failed, ret: " << ret;
            return ret;
        }
    } else {
        transport_mem = target_key_to_transport_mem_map_[key_str];
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    pid_t pid = getpid();

    hccl::TransportMem::RmaOpMem localMem;
    localMem.addr = local_mem;
    localMem.size = req_len;
    hccl::TransportMem::RmaOpMem remoteMem;
    remoteMem.addr = (void *)offset;
    remoteMem.size = req_len;
    if (op_code == WRITE) {
        ret = transport_mem->Write(remoteMem, localMem, stream);
        if (ret) {
            LOG(ERROR) << "transport_mem Read failed, localMem.addr: " 
                    << local_mem << "local_mem.size: " << req_len
                    << ", remoteMem.addr: "  << remoteMem.addr 
                    << ", remoteMem.size: " << req_len
                    << ", ret: " << ret
                    << "retry failed..";
            return ret;
        }
    } else {
        ret = transport_mem->Read(localMem, remoteMem, stream);
        if (ret) {
            LOG(ERROR) << "transport_mem Read failed, localMem.addr: " 
                << local_mem << "local_mem.size: " << req_len
                << ", remoteMem.addr: "  << remoteMem.addr 
                << ", remoteMem.size: " << req_len
                << ", ret: " << ret
                << "retry failed..";
            return ret;
        }
    }
    auto mid = std::chrono::high_resolution_clock::now();
    ret = transport_mem->AddOpFence(stream);
    if (ret) {
        LOG(ERROR) << "transport_mem AddOpFence failed, ret: " << ret;
        return ret; 
    }

    ret = aclrtSynchronizeStream(stream);
    if (ret) {
        LOG(ERROR) << "aclrtSynchronizeStream failed, localMem.addr: " 
                        << local_mem << "local_mem.size: " << req_len
                        << ", remoteMem.addr: "  << remoteMem.addr 
                        << ", remoteMem.size: " << req_len
                        << ", ret: " << ret;
        return ret;
    }

    auto stop = std::chrono::high_resolution_clock::now();
    if (printEnabled()) {
        auto duration_call = std::chrono::duration_cast<std::chrono::microseconds>(mid - start);
        auto duration_sync = std::chrono::duration_cast<std::chrono::microseconds>(stop - mid);
        LOG(INFO) << "pid: " << pid << "; " << "thread submit one block size: "<< req_len;
        LOG(INFO) << "pid: " << pid << "; " << "thread call write/read spent: "<< duration_call.count() << "us";
        LOG(INFO) << "pid: " << pid << "; " << "thread sync stream spent: "<< duration_sync.count() << "us";
    } else {
        (void)start;
        (void)mid;
        (void)stop;
        (void)pid;
    }

    return 0;
}

static int acceptFromTarget() {
    int client_socket;
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    client_socket = accept(g_server_socket_, (struct sockaddr*)&client_addr, &client_len);
    if (client_socket < 0) {
        LOG(ERROR) << "Accept failed";
        return client_socket;
    }

    LOG(INFO) << "host client connected from " << inet_ntoa(client_addr.sin_addr) << ":" << ntohs(client_addr.sin_port);
    return client_socket;
}

int acceptSocket(std::shared_ptr<hccl::HcclSocket> &hccl_socket, RankInfo *local_rank_info, RankControlInfo remote_control_info, std::string baseTag_, hccl::HcclIpAddress rempoteDevIp, bool is_cross_hccs) {
    int ret = 0;
    std::vector<SocketWlistInfo> wlistInfoVec;
    SocketWlistInfo wlistInfo = {};
    wlistInfo.connLimit = 1;
    memcpy(&wlistInfo.tag[0], baseTag_.c_str(), baseTag_.size() + 1);
    wlistInfo.remoteIp.addr = rempoteDevIp.GetBinaryAddress().addr;
    wlistInfo.remoteIp.addr6 = rempoteDevIp.GetBinaryAddress().addr6;
    wlistInfoVec.push_back(wlistInfo);
    auto serverSocket = is_cross_hccs ? nicServerSocket_ : vnicServerSocket_;
    ret = serverSocket->AddWhiteList(wlistInfoVec);
    if (ret) {
        LOG(ERROR) << "serverSocket AddWhiteList failed, ret: " << ret;
        return ret;
    }
    // Before using the device-side network card for communication, it is necessary to add the client device address to the whitelist.
    LOG(INFO) << "Add the client's Device IP address to the whitelist success.";

    ret = serverSocket->Accept(baseTag_, hccl_socket);
    if (ret) {
        LOG(ERROR) << "serverSocket transportMemAccept ctrl socket failed ret:" << ret;
        return ret;
    }
    return 0;
}

int transportMemAccept(RankInfo *local_rank_info) {
    // Self-built out-of-band, host socket for receiving control plane
    int ret = 0;
    int nfds = epoll_wait(g_epoll_fd, g_events, MAX_EVENTS, -1);
    if (nfds == -1) {
        return 0;
    }
    int client_socket = acceptFromTarget();
    if (client_socket < 0) {
        return client_socket;
    }

    RankControlInfo remote_control_info;
    ret = recv(client_socket, &remote_control_info, sizeof(RankControlInfo), 0);
    if (ret <= 0) {
        if (ret < 0) {
            LOG(ERROR) << "recv failed, ret: " << ret;
        } else {
            LOG(ERROR) << "Peer close the connection, ret: " << ret;
        }
        close(client_socket);
        return ret;
    }

    LOG(INFO) << "Received remote_control_info, deviceLogicId: "
              << remote_control_info.deviceLogicId
              << ", devicePhyId: " << remote_control_info.devicePhyId
              << ", hostIp: " << inet_ntoa(remote_control_info.hostIp)
              << ", deviceIp: " << inet_ntoa(remote_control_info.deviceIp);

    // Check if TransportMem has been established with the peer
    std::string key_str = inet_ntoa(remote_control_info.hostIp) + std::to_string(remote_control_info.devicePhyId);
    auto iter = target_key_to_transport_mem_map_.find(key_str);
    if (iter != target_key_to_transport_mem_map_.end()) {
        LOG(WARNING) << "A duplicate connection request from the same remote endpoint has been detected, the remote side may have restarted.";
    }

    std::string baseTag_ = key_str + inet_ntoa(local_rank_info->hostIp) + std::to_string(local_rank_info->devicePhyId);
    hccl::HcclIpAddress rempoteDevIp;
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    bool same_host = local_rank_info->hostIp.s_addr == remote_control_info.hostIp.s_addr;
    // For A2 series, internal communication among 8 cards does not cross HCCS, such as communication among cards 0-7
    bool same_group = (local_rank_info->devicePhyId / 8) == (remote_control_info.devicePhyId / 8);
    bool is_cross_hccs = !(same_host && same_group);
    if (printEnabled()) {
        LOG(INFO) << "transport is cross_hccs: " << (is_cross_hccs ? "true (cross-hccs)" : "false (same-hccs)");
    }
    if (!is_cross_hccs) {
        std::vector<unsigned int> remoteDevPhyId;
        rempoteDevIp = hccl::HcclIpAddress(remote_control_info.devicePhyId);
        remoteDevPhyId.push_back(remote_control_info.devicePhyId);
        ret = hrtRaGetSingleSocketVnicIpInfo(local_rank_info->devicePhyId,
                                            DeviceIdType::DEVICE_ID_TYPE_PHY_ID,
                                            remote_control_info.devicePhyId,
                                            rempoteDevIp);
        if (ret) {
            LOG(ERROR) << "hrtRaGetSingleSocketVnicIpInfo failed, ret:" << ret;
            return ret;
        }
        ret = hccl::P2PMgmtPub::EnableP2P(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub EnableP2P failed, ret:" << ret;
            return ret;
        }
        ret = hccl::P2PMgmtPub::WaitP2PEnabled(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub EnableP2P failed, ret:" << ret;
            return ret;
        }
        ret = acceptSocket(hccl_ctrl_socket, local_rank_info, remote_control_info, baseTag_ + "ctrl", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket ctrl failed, ret:" << ret;
            return ret;
        }

        ret = acceptSocket(hccl_data_socket, local_rank_info, remote_control_info, baseTag_ + "data", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket data failed, ret:" << ret;
            return ret;
        }
    } else {
        rempoteDevIp = hccl::HcclIpAddress(remote_control_info.deviceIp);
        ret = acceptSocket(hccl_ctrl_socket, local_rank_info, remote_control_info, baseTag_ + "ctrl", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket ctrl failed, ret:" << ret;
            return ret;
        }

        ret = acceptSocket(hccl_data_socket, local_rank_info, remote_control_info, baseTag_ + "data", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket data failed, ret:" << ret;
            return ret;
        }
    }

    target_key_to_hccl_ctrl_socket_map_[key_str] = hccl_ctrl_socket;
    target_key_to_hccl_data_socket_map_[key_str] = hccl_data_socket;
    LOG(INFO) << "Creating transfer_mem on the accept side";
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    hccl::TransportMem::AttrInfo attrInfo;
    attrInfo.localRankId = local_rank_info->deviceLogicId;
    attrInfo.remoteRankId = remote_control_info.deviceLogicId;
    attrInfo.sdid = 0xFFFFFFFF;
    attrInfo.serverId = local_rank_info->serverIdx;
    if (is_cross_hccs) {
        transport_mem = hccl::TransportMem::Create(
            hccl::TransportMem::TpType::ROCE, notifyPool_, nicNetDevCtx_, 
            dispatcher_, attrInfo);
    } else {
        transport_mem = hccl::TransportMem::Create(
            hccl::TransportMem::TpType::IPC, notifyPool_, vnicNetDevCtx_, 
            dispatcher_, attrInfo);
    }
    ret = transport_mem->SetDataSocket(hccl_data_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client SetDataSocket failed, target rank_id:" 
                    << remote_control_info.devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", ret: " << ret;
        return ret;
    }
    
    ret = transport_mem->SetSocket(hccl_ctrl_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client Connect failed, target rank_id:" 
                    << remote_control_info.devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", ret: " << ret;
        return ret;
    }
    ret = transport_mem->Connect(120);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client Connect failed, target rank_id:" 
                    << remote_control_info.devicePhyId
                    << ",local rank_id:" << local_rank_info->devicePhyId
                    << ", rempoteDevIp:" << deviceIp 
                    << ", ret: " << ret;
        return ret;
    }

    target_key_to_transport_mem_map_[key_str] = transport_mem;    

    size_t m_num = g_localMemAddr.size();
    std::vector<hccl::TransportMem::RmaMemDesc> rmaMemDescs(m_num);
    for (size_t i = 0; i < m_num; ++i) {
        HcclBuf buf;
        HcclMem mem;
        mem.addr = g_localMemAddr[i];
        mem.size = g_localMemLen[i];
        mem.type = HCCL_MEM_TYPE_DEVICE;
        if (!is_cross_hccs) {
            ret = HcclMemReg(vnicNetDevCtx_, &mem, &buf);
        } else {
            ret = HcclMemReg(nicNetDevCtx_, &mem, &buf);
        }
        if (ret != 0 && ret != 20) {
            LOG(ERROR) << "HcclMemReg failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];
            return ret;
        }
        char *desc = nullptr;
        uint64_t desc_len = 0;
        ret = HcclMemExport(&buf, &desc, &desc_len);
        if (ret) {
            LOG(ERROR) << "HcclMemExport failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];
            return ret;
        }
        
        rmaMemDescs[i].localRankId = local_rank_info->deviceLogicId;
        rmaMemDescs[i].remoteRankId = remote_control_info.deviceLogicId;
        memset_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, 0, hccl::TRANSPORT_EMD_ESC_SIZE);
        if (memcpy_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, desc, desc_len + 1) != EOK) {
            LOG(ERROR) << "memcpy_s failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];
            return -1;
        }

        // In the scenario within HCCS, it is necessary to call HcclMemGrant to authorize peer memory
        if (!is_cross_hccs) {
            HcclMemGrantInfo grant_info;
            grant_info.remotePid = (int32_t)remote_control_info.pid;
            grant_info.remoteSdid = 0xFFFFFFFF;
            LOG(INFO) << "device pid:" << grant_info.remotePid;
            ret = HcclMemGrant(&buf, &grant_info);
            if (ret) {
                LOG(ERROR) << "HcclMemGrant failed, ret:" << ret << " addr: " << g_localMemAddr[i] << " len: " << g_localMemLen[i];
                return ret;
            }
        }
    }
    hccl::TransportMem::RmaMemDescs localRmaMemDescs;
    localRmaMemDescs.array = rmaMemDescs.data();
    localRmaMemDescs.arrayLength = rmaMemDescs.size();
    uint32_t actualNumOfRemote = 0;
    std::vector<hccl::TransportMem::RmaMemDesc> remoteRmaMemDescArray(m_num);
    hccl::TransportMem::RmaMemDescs remoteRmaMemDescs;
    remoteRmaMemDescs.array = remoteRmaMemDescArray.data();
    remoteRmaMemDescs.arrayLength = m_num;
    ret = transport_mem->ExchangeMemDesc(localRmaMemDescs, remoteRmaMemDescs, actualNumOfRemote);
    if (ret) {
        LOG(ERROR) << "transport_mem->ExchangeMemDesc failed, ret:" << ret << "local_rank: " << local_rank_info->devicePhyId << "remote_rank: " << remote_control_info.devicePhyId;
        return ret;
    }
    std::vector<hccl::TransportMem::RmaMem> remoteRmaMemArray(m_num);
    for (uint32_t i = 0; i < m_num; ++i) {
        ret = transport_mem->EnableMemAccess(remoteRmaMemDescArray[i], remoteRmaMemArray[i]);
        if (ret) {
            LOG(ERROR) << "transport_mem->EnableMemAccess failed, ret:" << ret << " i:" << i << "local_rank: " << local_rank_info->devicePhyId << "remote_rank: " << remote_control_info.devicePhyId;
            return ret;
        }
    }

    LOG(INFO) << "ExchangeMem and EnableMemAccess Success, with remote";
    return 0;
}

int regLocalRmaMem(void *addr, uint64_t length)
{
    g_localMemAddr.push_back(addr);
    g_localMemLen.push_back(length);
    return 0;
}

#ifdef __cplusplus
}
#endif // __cplusplus
