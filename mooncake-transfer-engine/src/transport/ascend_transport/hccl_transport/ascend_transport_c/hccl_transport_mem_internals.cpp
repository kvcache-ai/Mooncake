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
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <string>
#include <errno.h>
#include <condition_variable>
#include <random>
#include <sys/epoll.h>
#include <unordered_map>
#include "transport/ascend_transport/hccl_transport/hccl_transport_mem_internals.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

HcclNetDevCtx vnicNetDevCtx_{nullptr};
HcclNetDevCtx nicNetDevCtx_{nullptr};
std::shared_ptr<hccl::HcclSocket> vnicServerSocket_{nullptr};
std::shared_ptr<hccl::HcclSocket> nicServerSocket_{nullptr};
std::unique_ptr<hccl::NotifyPool> notifyPool_;
HcclDispatcher dispatcher_{nullptr};

std::unordered_map<std::string, ConnectionInfo> g_target_key_to_connection_map;
std::vector<MemBlock> g_localBuffer;

int g_epoll_fd_agg = 0;
int g_server_socket = 0;
int g_epoll_fd = 0;
int g_epoll_fd_target = 0;
struct epoll_event g_ev;
struct epoll_event g_events[MAX_EVENTS];

static uint16_t findAvailableTcpPort(int &sockfd, bool use_ipv6) {
    static std::random_device rand_gen;
    std::mt19937 gen(rand_gen());
    const int min_port = 15000;
    const int max_port = 17000;
    const int max_attempts = 500;
    std::uniform_int_distribution<> rand_dist(min_port, max_port);

    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        int port = rand_dist(rand_gen);
        if (use_ipv6) {
            sockaddr_in6 bind_address;
            memset(&bind_address, 0, sizeof(sockaddr_in6));
            bind_address.sin6_family = AF_INET6;
            bind_address.sin6_port = htons(port);
            bind_address.sin6_addr = IN6ADDR_ANY_INIT;
            if (bind(sockfd, (sockaddr *)&bind_address, sizeof(sockaddr_in6)) <
                0) {
                continue;
            }
        } else {
            sockaddr_in bind_address;
            memset(&bind_address, 0, sizeof(sockaddr_in));
            bind_address.sin_family = AF_INET;
            bind_address.sin_port = htons(port);
            bind_address.sin_addr.s_addr = INADDR_ANY;
            if (bind(sockfd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) <
                0) {
                continue;
            }
        }

        return port;
    }
    return 0;
}

int initServerNetSocket(RankInfo *local_rank_info) {
    RETRY_CALL(HcclNetInit(NICDeployment::NIC_DEPLOYMENT_DEVICE,
                           local_rank_info->devicePhyId,
                           local_rank_info->deviceLogicId, false),
               "HcclNetInit failed");

    // Use the physical network card of the device across HCCS
    hccl::HcclIpAddress localIp(local_rank_info->deviceIp);
    RETRY_CALL(HcclNetOpenDev(&nicNetDevCtx_, NicType::DEVICE_NIC_TYPE,
                              local_rank_info->devicePhyId,
                              local_rank_info->deviceLogicId, localIp),
               "HcclNetOpenDev DEVICE_NIC_TYPE failed");

    nicServerSocket_ = std::make_shared<hccl::HcclSocket>(
        nicNetDevCtx_, local_rank_info->devicePort);
    if (nicServerSocket_ == NULL) {
        LOG(ERROR) << "make nicNetDevCtx_ failed";
        return -1;
    }

    RETRY_CALL(nicServerSocket_->Init(), "nicServerSocket_ Init failed");
    RETRY_CALL(nicServerSocket_->Listen(), "nicServerSocket_ Listen failed");

    // Use virtual network card within HCCS
    hccl::HcclIpAddress localVnicIp(local_rank_info->devicePhyId);
    RETRY_CALL(
        hrtRaGetSingleSocketVnicIpInfo(
            local_rank_info->devicePhyId, DeviceIdType::DEVICE_ID_TYPE_PHY_ID,
            local_rank_info->devicePhyId, localVnicIp),
        "hrtRaGetSingleSocketVnicIpInfo failed");

    RETRY_CALL(HcclNetOpenDev(&vnicNetDevCtx_, NicType::VNIC_TYPE,
                              local_rank_info->devicePhyId,
                              local_rank_info->deviceLogicId, localVnicIp),
               "HcclNetOpenDev vnicNetDevCtx_ failed");

    // control plane connection, creat serversocket, listening client
    vnicServerSocket_ = std::make_shared<hccl::HcclSocket>(
        vnicNetDevCtx_, local_rank_info->devicePort);
    if (vnicServerSocket_ == NULL) {
        LOG(ERROR) << "vnicServerSocket_ make failed";
        return -1;
    }

    RETRY_CALL(vnicServerSocket_->Init(), "vnicServerSocket_ Init failed");
    RETRY_CALL(vnicServerSocket_->Listen(), "vnicServerSocket_ Listen failed");

    RETRY_CALL(HcclDispatcherInit(DispatcherType::DISPATCHER_NORMAL,
                                  local_rank_info->devicePhyId, &dispatcher_),
               "client HcclDispatcherInit failed");

    notifyPool_.reset(new (std::nothrow) hccl::NotifyPool());
    if (notifyPool_ == nullptr) {
        LOG(ERROR) << "reset notifyPool error";
        return -1;
    }

    RETRY_CALL(notifyPool_->Init(local_rank_info->devicePhyId),
               "Init notifyPool error");

    return 0;
}

// The out-of-band socket on the host side that ascend_transport depends on,
// used to convey control information such as deviceId and deviceIp
int initControlSocket(RankInfo *local_rank_info, bool aggregateEnabled) {
    int ret = 0;
    g_server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (g_server_socket < 0) {
        LOG(ERROR) << "ascend transport out-of-band socket create failed";
        return g_server_socket;
    }

    int optval = 1;
    ret = setsockopt(g_server_socket, SOL_SOCKET, SO_REUSEADDR, &optval,
                     sizeof(optval));
    if (ret < 0) {
        LOG(ERROR) << "set sock opt failed, ret: " << ret;
        close(g_server_socket);
        return ret;
    }

    struct sockaddr_in bind_address;
    memset(&bind_address, 0, sizeof(sockaddr_in));
    bind_address.sin_family = AF_INET;
    bind_address.sin_addr.s_addr = INADDR_ANY;
    bind_address.sin_port = htons(local_rank_info->hostPort);

    ret = bind(g_server_socket, (struct sockaddr *)&bind_address,
               sizeof(bind_address));
    if (ret < 0) {
        LOG(INFO) << "bind failed on the default port, default port: "
                  << local_rank_info->hostPort << ", will find available port";
        uint16_t port = findAvailableTcpPort(g_server_socket, false);
        if (port == 0) {
            LOG(ERROR) << "findAvailableTcpPort failed";
            close(g_server_socket);
            return -1;
        }
        local_rank_info->hostPort = (uint64_t)port;
    }

    ret = listen(g_server_socket, CONNECT_MAX);
    if (ret < 0) {
        LOG(ERROR) << "Listen Failed, ret: " << ret;
        close(g_server_socket);
        return ret;
    }
    LOG(INFO) << "initControlSocket successful, listening on host port: "
              << local_rank_info->hostPort << "..." << " g_server_socket"
              << g_server_socket;
    g_epoll_fd = epoll_create1(0);
    if (g_epoll_fd == -1) {
        LOG(ERROR) << "epoll create Failed, ret: " << g_epoll_fd;
        close(g_server_socket);
        return g_epoll_fd;
    }

    g_epoll_fd_target = epoll_create1(0);
    if (g_epoll_fd_target == -1) {
        LOG(ERROR) << "epoll fd target create Failed, ret: " << g_epoll_fd_target;
        close(g_server_socket);
        return g_epoll_fd_target;
    }

    if (aggregateEnabled) {
        g_epoll_fd_agg = epoll_create1(0);
        if (g_epoll_fd_agg == -1) {
            LOG(ERROR) << "epoll fd target create Failed, ret: " << ret;
            close(g_server_socket);
            return g_epoll_fd;
        }
        LOG(INFO) << "g_epoll_fd_agg create end";
    }

    g_ev.events = EPOLLIN;
    g_ev.data.fd = g_server_socket;
    ret = epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, g_server_socket, &g_ev);
    if (ret < 0) {
        LOG(ERROR) << "epoll epoll_ctl Failed, ret: " << ret;
        close(g_server_socket);
        return ret;
    }
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
    int ret = setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &optval,
                         sizeof(optval));
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

    int connected = 0;

    const char *tcp_timeout_str = std::getenv("Ascend_TCP_TIMEOUT");
    int ascend_tcp_timeout = tcp_timeout_str ? std::atoi(tcp_timeout_str) : 30;
    int connect_retry_times = ascend_tcp_timeout * 100;

    for (int i = 0; i < connect_retry_times; ++i) {
        if (connect(client_socket, (struct sockaddr *)&server_addr,
                    sizeof(server_addr)) == 0) {
            LOG(INFO) << "Connect to host server " << target_ip << ":"
                      << ntohs(server_addr.sin_port) << " successful";
            connected = 1;
            break;
        }

        LOG(INFO) << "Connect attempt " << i << " failed: " << strerror(errno)
                  << ", retry once";

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!connected) {
        LOG(ERROR) << "Failed to connect to server after "
                   << connect_retry_times << " retries";
        close(client_socket);
        return HCCL_E_TIMEOUT;
    }
    return client_socket;
}

int controlInfoSend(RankInfo *local_rank_info, RankInfo *remote_rank_info) {
    int ret = 0;
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    LOG(INFO) << "aggTransportMemTask local_rank_info rankId: "
              << local_rank_info->rankId
              << ", serverIdx: " << local_rank_info->serverIdx
              << ", deviceLogicId: " << local_rank_info->deviceLogicId
              << ", devicePhyId: " << local_rank_info->devicePhyId
              << ", deviceIp: " << inet_ntoa(local_rank_info->deviceIp)
              << ", devicePort: " << local_rank_info->devicePort
              << ", hostIp: " << inet_ntoa(local_rank_info->hostIp)
              << ", hostPort: " << local_rank_info->hostPort
              << ", device pid: " << local_rank_info->pid;

    LOG(INFO) << "aggTransportMemTask remote_rank_info rankId: "
              << remote_rank_info->rankId
              << ", serverIdx: " << remote_rank_info->serverIdx
              << ", deviceLogicId: " << remote_rank_info->deviceLogicId
              << ", devicePhyId: " << remote_rank_info->devicePhyId
              << ", deviceIp: " << inet_ntoa(remote_rank_info->deviceIp)
              << ", devicePort: " << remote_rank_info->devicePort
              << ", hostIp: " << inet_ntoa(remote_rank_info->hostIp)
              << ", hostPort: " << remote_rank_info->hostPort
              << ", device pid: " << remote_rank_info->pid;

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
    int recv_socket = connectToTarget(inet_ntoa(remote_rank_info->hostIp), 
                                      remote_rank_info->hostPort);
    if (recv_socket < 0) {
        LOG(ERROR) << "recv_socket connect failed";
        return recv_socket;
    }
    ret = send(client_socket, &control_info, sizeof(RankControlInfo), 0);
    if (ret < 0) {
        LOG(ERROR) << "send control_info failed, ret: " << ret
                   << ", errno: " << errno << ", error: " << strerror(errno);
        return ret;
    }
    g_target_key_to_connection_map[key_str].tcp_socket = client_socket;
    g_target_key_to_connection_map[key_str].recv_socket = recv_socket;
    LOG(INFO) << "target_key:" << key_str << ", tcp_socket:" << client_socket
              << ", recv_socket:" << recv_socket;
    
    struct epoll_event event;
    event.events = EPOLLIN;
    event.data.fd = recv_socket;
    if (epoll_ctl(g_epoll_fd_agg, EPOLL_CTL_ADD, recv_socket, &event) == -1) {
        LOG(ERROR) << "epoll_ctl add client_socket fd failed";
        return -1;
    }
    // if (epoll_ctl(g_epoll_fd_target, EPOLL_CTL_ADD, client_socket, &event) == -1) {
    //     LOG(ERROR) << "epoll_ctl add client_socket fd failed";
    //     return -1;
    // }

    return 0;
}

int createClientSocket(std::shared_ptr<hccl::HcclSocket> &hccl_socket,
                       RankInfo *local_rank_info, RankInfo *remote_rank_info,
                       bool is_cross_hccs, std::string tag) {
    int ret = 0;
    hccl::HcclIpAddress rempoteDevIp;
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    std::string baseTag_ = inet_ntoa(local_rank_info->hostIp) +
                           std::to_string(local_rank_info->devicePhyId) +
                           key_str + tag;
    if (!is_cross_hccs) {
        std::vector<unsigned int> remoteDevPhyId;
        remoteDevPhyId.emplace_back(remote_rank_info->devicePhyId);
        ret = hccl::P2PMgmtPub::EnableP2P(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub EnableP2P failed, ret: " << ret;
            return ret;
        }
        ret = hccl::P2PMgmtPub::WaitP2PEnabled(remoteDevPhyId);
        if (ret) {
            LOG(ERROR) << "P2PMgmtPub WaitP2PEnabled failed, ret: " << ret;
            return ret;
        }
        rempoteDevIp = hccl::HcclIpAddress(remote_rank_info->devicePhyId);
        ret = hrtRaGetSingleSocketVnicIpInfo(
            local_rank_info->devicePhyId, DeviceIdType::DEVICE_ID_TYPE_PHY_ID,
            remote_rank_info->devicePhyId, rempoteDevIp);
        if (ret) {
            LOG(ERROR) << "hrtRaGetSingleSocketVnicIpInfo, ret: " << ret;
            return ret;
        }
        hccl_socket = std::make_shared<hccl::HcclSocket>(
            baseTag_, vnicNetDevCtx_, rempoteDevIp,
            remote_rank_info->devicePort,
            hccl::HcclSocketRole::SOCKET_ROLE_CLIENT);
    } else {
        rempoteDevIp = hccl::HcclIpAddress(remote_rank_info->deviceIp);
        hccl_socket = std::make_shared<hccl::HcclSocket>(
            baseTag_, nicNetDevCtx_, rempoteDevIp, remote_rank_info->devicePort,
            hccl::HcclSocketRole::SOCKET_ROLE_CLIENT);
    }

    ret = hccl_socket->Init();
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client hccl_socket init failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp
                   << ", remote port: " << remote_rank_info->devicePort
                   << ", ret: " << ret;
        return ret;
    }
    ret = hccl_socket->Connect();
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "client hccl_socket Connect failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp
                   << ", remote port: " << remote_rank_info->devicePort
                   << ", ret: " << ret;
        return ret;
    }
    LOG(INFO) << "hccl_socket begin to connect, local devicePhyId: "
              << local_rank_info->devicePhyId
              << ", target devicePhyId: " << remote_rank_info->devicePhyId;
    hccl::HcclSocketStatus status;
    struct timespec start, end;
    const char *hccl_socket_timeout_str =
        std::getenv("Ascend_HCCL_SOCKET_TIMEOUT");
    int hccl_socket_timeout =
        hccl_socket_timeout_str ? std::atoi(hccl_socket_timeout_str) : 30;
    long long hccl_socket_timeout_ns =
        static_cast<long long>(hccl_socket_timeout) * 1000000000LL;
    clock_gettime(CLOCK_MONOTONIC, &start);
    do {
        status = hccl_socket->GetStatus();
        clock_gettime(CLOCK_MONOTONIC, &end);
        long long elapsed_time = (end.tv_sec - start.tv_sec) * 1000000000LL +
                                 (end.tv_nsec - start.tv_nsec);
        if (elapsed_time >
            hccl_socket_timeout_ns) {  // Exceeds 20 seconds,TimeOut
            LOG(ERROR) << "hccl_socket connect timeout, local devicePhyId: "
                       << local_rank_info->devicePhyId
                       << ", target devicePhyId: "
                       << remote_rank_info->devicePhyId;
            return HCCL_E_TIMEOUT;
        }
    } while (status != hccl::HcclSocketStatus::SOCKET_OK);
    LOG(INFO) << "hccl_socket connect success, local devicePhyId: "
              << local_rank_info->devicePhyId
              << ", target devicePhyId: " << remote_rank_info->devicePhyId;

    return 0;
}

int createTransportMem(RankInfo *local_rank_info, RankInfo *remote_rank_info,
                       std::string key_str, bool is_cross_hccs,
                       std::shared_ptr<hccl::TransportMem> &transport_mem) {
    int ret = 0;
    hccl::TransportMem::AttrInfo attrInfo;
    attrInfo.localRankId = local_rank_info->deviceLogicId;
    attrInfo.remoteRankId = remote_rank_info->deviceLogicId;
    attrInfo.sdid = 0xFFFFFFFF;
    attrInfo.serverId = local_rank_info->serverIdx;
    attrInfo.trafficClass = 132;
    attrInfo.serviceLevel = 4;
    if (is_cross_hccs) {
        transport_mem = hccl::TransportMem::Create(
            hccl::TransportMem::TpType::ROCE, notifyPool_, nicNetDevCtx_,
            dispatcher_, attrInfo);
    } else {
        transport_mem = hccl::TransportMem::Create(
            hccl::TransportMem::TpType::IPC, notifyPool_, vnicNetDevCtx_,
            dispatcher_, attrInfo);
    }
    ret = transport_mem->SetDataSocket(
        g_target_key_to_connection_map[key_str].hccl_data_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp,
                  sizeof(deviceIp));
        LOG(ERROR) << "client SetDataSocket failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp << ", ret: " << ret;
        return ret;
    }
    ret = transport_mem->SetSocket(
        g_target_key_to_connection_map[key_str].hccl_ctrl_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp,
                  sizeof(deviceIp));
        LOG(ERROR) << "client SetSocket failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp << ", ret: " << ret;
        return ret;
    }
    const char *transport_mem_timeout_str =
        std::getenv("Ascend_TRANSPORT_MEM_TIMEOUT");
    int transport_mem_timeout =
        transport_mem_timeout_str ? std::atoi(transport_mem_timeout_str) : 120;
    ret = transport_mem->Connect(transport_mem_timeout);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp,
                  sizeof(deviceIp));
        LOG(ERROR) << "client Connect failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp << ", ret: " << ret;
        return ret;
    }
    LOG(INFO) << "transport_mem connect success";
    g_target_key_to_connection_map[key_str].transport_mem = transport_mem;
    uint32_t m_num = g_localBuffer.size();
    std::vector<hccl::TransportMem::RmaMemDesc> rmaMemDescs(m_num);

    for (uint32_t i = 0; i < m_num; ++i) {
        HcclMem mem;
        HcclBuf buf;
        mem.addr = (void *)g_localBuffer[i].addr;
        mem.size = g_localBuffer[i].len;
        mem.type = HCCL_MEM_TYPE_DEVICE;
        if (!is_cross_hccs) {
            ret = HcclMemReg(vnicNetDevCtx_, &mem, &buf);
        } else {
            ret = HcclMemReg(nicNetDevCtx_, &mem, &buf);
        }
        if (ret != 0 && ret != 20) {
            LOG(ERROR) << "HcclMemReg failed, ret: " << ret
                       << " addr: " << mem.addr << " len: " << mem.size;
            return ret;
        }
        char *desc = nullptr;
        uint64_t desc_len = 0;
        ret = HcclMemExport(&buf, &desc, &desc_len);
        if (ret) {
            LOG(ERROR) << "HcclMemExport failed, ret: " << ret
                       << ", addr: " << mem.addr << ", len: " << mem.size;
            return ret;
        }

        rmaMemDescs[i].localRankId = local_rank_info->deviceLogicId;
        rmaMemDescs[i].remoteRankId = remote_rank_info->deviceLogicId;
        memset_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, 0,
                 hccl::TRANSPORT_EMD_ESC_SIZE);
        if (memcpy_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, desc,
                     desc_len + 1) != EOK) {
            LOG(ERROR) << "memcpy_s failed, ret: " << ret
                       << ", addr: " << mem.addr << ", len: " << mem.size;
            return -1;
        }

        // In the scenario within HCCS, it is necessary to call HcclMemGrant to
        // authorize peer memory
        if (!is_cross_hccs) {
            HcclMemGrantInfo grant_info;
            grant_info.remotePid = (int32_t)remote_rank_info->pid;
            grant_info.remoteSdid = 0xFFFFFFFF;
            ret = HcclMemGrant(&buf, &grant_info);
            if (ret) {
                LOG(ERROR) << "HcclMemGrant failed, ret: " << ret
                           << ", addr: " << mem.addr << ", len: " << mem.size;
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
    ret = transport_mem->ExchangeMemDesc(localRmaMemDescs, remoteRmaMemDescs,
                                         actualNumOfRemote);
    if (ret) {
        LOG(ERROR) << "transport_mem->ExchangeMemDesc failed, ret: " << ret
                   << ", local_rank: " << local_rank_info->devicePhyId
                   << ", remote_rank: " << remote_rank_info->devicePhyId;
        return ret;
    }
    std::vector<hccl::TransportMem::RmaMem> remoteRmaMemArray(m_num);
    for (uint32_t i = 0; i < m_num; ++i) {
        ret = transport_mem->EnableMemAccess(remoteRmaMemDescArray[i],
                                             remoteRmaMemArray[i]);
        if (ret) {
            LOG(ERROR) << "transport_mem->EnableMemAccess failed, ret: " << ret
                       << ", i: " << i
                       << ", local_rank: " << local_rank_info->devicePhyId
                       << ", remote_rank: " << remote_rank_info->devicePhyId;
            return ret;
        }
    }
    LOG(INFO) << "ExchangeMem and EnableMemAccess Success, local devicePhyId: "
              << local_rank_info->devicePhyId
              << ", target devicePhyId: " << remote_rank_info->devicePhyId;
    return 0;
}

int socketEpollWait() {
    int nfds = epoll_wait(g_epoll_fd, g_events, MAX_EVENTS, -1);
    if (nfds == -1) {
        if (errno == EINTR) {
            return 0;
        } else {
            LOG(ERROR) << "epoll_wait failed: " << strerror(errno);
            return -1;
        }
    }

    return nfds;
}

int acceptFromTarget() {
    int client_socket;
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    client_socket =
        accept(g_server_socket, (struct sockaddr *)&client_addr, &client_len);
    if (client_socket < 0) {
        LOG(ERROR) << "Accept failed";
        return client_socket;
    }

    LOG(INFO) << "host client connected from "
              << inet_ntoa(client_addr.sin_addr) << ":"
              << ntohs(client_addr.sin_port);
    return client_socket;
}

int acceptHcclSocket(std::shared_ptr<hccl::HcclSocket> &hccl_socket,
                     std::string baseTag_, hccl::HcclIpAddress rempoteDevIp,
                     bool is_cross_hccs) {
    int ret = 0;
    std::vector<SocketWlistInfo> wlistInfoVec;
    SocketWlistInfo wlistInfo = {};
    wlistInfo.connLimit = 1;
    memcpy(&wlistInfo.tag[0], baseTag_.c_str(), baseTag_.size() + 1);
    wlistInfo.remoteIp.addr = rempoteDevIp.GetBinaryAddress().addr;
    wlistInfo.remoteIp.addr6 = rempoteDevIp.GetBinaryAddress().addr6;
    wlistInfoVec.emplace_back(wlistInfo);
    auto serverSocket = is_cross_hccs ? nicServerSocket_ : vnicServerSocket_;
    ret = serverSocket->AddWhiteList(wlistInfoVec);
    if (ret) {
        LOG(ERROR) << "serverSocket AddWhiteList failed, ret: " << ret;
        return ret;
    }
    // Before using the device-side network card for communication, it is
    // necessary to add the client device address to the whitelist.
    LOG(INFO) << "Add the client's Device IP address to the whitelist success.";

    ret = serverSocket->Accept(baseTag_, hccl_socket);
    if (ret) {
        LOG(ERROR) << "serverSocket transportMemAccept ctrl socket failed ret: "
                   << ret;
        return ret;
    }
    return 0;
}

#ifdef __cplusplus
}
#endif  // __cplusplus