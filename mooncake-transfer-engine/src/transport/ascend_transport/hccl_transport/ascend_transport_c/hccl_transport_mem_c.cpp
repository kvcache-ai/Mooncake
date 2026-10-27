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
#include <net/if.h>
#include <netdb.h>
#include <random>
#include <ifaddrs.h>
#include "mpi.h"
#include "transport/ascend_transport/hccl_transport/hccl_transport_mem_c.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

#define READ 0
#define WRITE 1
#define MAX_EVENTS 32
#define CONNECT_MAX 1000
#define RETRY_TIMES 3
#define VECTOR_RESERVE_SIZE 200

HcclNetDevCtx vnicNetDevCtx_{nullptr};
HcclNetDevCtx nicNetDevCtx_{nullptr};
std::shared_ptr<hccl::HcclSocket> vnicServerSocket_{nullptr};
std::shared_ptr<hccl::HcclSocket> nicServerSocket_{nullptr};
std::unique_ptr<hccl::NotifyPool> notifyPool_;
HcclDispatcher dispatcher_{nullptr};
std::unordered_map<std::string, ConnectionInfo> target_key_to_connection_map_;
std::vector<MergeMem> g_localMergeMem;
int g_server_socket_ = 0;
int g_epoll_fd = 0;
struct epoll_event g_ev;
struct epoll_event g_events[MAX_EVENTS];

bool printEnabled() {
    char *env = getenv("ASCEND_TRANSPORT_PRINT");
    return env != nullptr && std::string(env) == "1";
}

static size_t getMaxRegMemoryNum() {
    static const size_t g_default_max_reg_memory_num = 8192;
    static char *env = getenv("ASCEND_TRANSPORT_MAX_REG_MEMORY_NUM");
    if (env != nullptr) {
        return std::stoi(env);
    }
    return g_default_max_reg_memory_num;
}

static int getEpollWaitTimeoutMs() {
    static const int g_default_epoll_wait_timeout_ms = 3000;
    static char *env = getenv("ASCEND_TRANSPORT_EPOLL_WAIT_TIMEOUT");
    if (env != nullptr) {
        return std::stoi(env);
    }
    return g_default_epoll_wait_timeout_ms;
}

static int getHcclRdmaTrafficClass() {
    static char *env = getenv("HCCL_RDMA_TC");
    return env ? std::stoi(env) : 132;
}

static int getHcclRdmaServiceLevel() {
    static char *env = getenv("HCCL_RDMA_SL");
    return env ? std::stoi(env) : 4;
}

uint16_t findAvailableTcpPort(int &sockfd, bool use_ipv6) {
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

static int initServerNetSocket(RankInfo *local_rank_info) {
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
static int initControlSocket(RankInfo *local_rank_info) {
    int ret = 0;
    g_server_socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (g_server_socket_ < 0) {
        LOG(ERROR) << "ascend transport out-of-band socket create failed";
        return g_server_socket_;
    }

    int optval = 1;
    ret = setsockopt(g_server_socket_, SOL_SOCKET, SO_REUSEADDR, &optval,
                     sizeof(optval));
    if (ret < 0) {
        LOG(ERROR) << "set sock opt failed, ret: " << ret;
        close(g_server_socket_);
        return ret;
    }

    struct sockaddr_in bind_address;
    memset(&bind_address, 0, sizeof(sockaddr_in));
    bind_address.sin_family = AF_INET;
    bind_address.sin_addr.s_addr = INADDR_ANY;
    bind_address.sin_port = htons(local_rank_info->hostPort);

    ret = bind(g_server_socket_, (struct sockaddr *)&bind_address,
               sizeof(bind_address));
    if (ret < 0) {
        LOG(INFO) << "bind failed on the default port, default port: "
                  << local_rank_info->hostPort << ", will find available port";
        uint16_t port = findAvailableTcpPort(g_server_socket_, false);
        if (port == 0) {
            LOG(ERROR) << "findAvailableTcpPort failed";
            close(g_server_socket_);
            return -1;
        }
        local_rank_info->hostPort = (uint64_t)port;
    }

    struct timeval timeout;
    timeout.tv_sec = 120;
    timeout.tv_usec = 0;
    ret = setsockopt(g_server_socket_, SOL_SOCKET, SO_RCVTIMEO,
                     (const char *)&timeout, sizeof(timeout));
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
    LOG(INFO) << "initControlSocket successful, listen on hostPort: "
              << local_rank_info->hostPort << "..." << " g_server_socket_"
              << g_server_socket_;
    g_epoll_fd = epoll_create1(0);
    if (g_epoll_fd == -1) {
        LOG(ERROR) << "epoll create Failed, ret: " << g_epoll_fd;
        close(g_server_socket_);
        return g_epoll_fd;
    }
    g_ev.events = EPOLLIN;
    g_ev.data.fd = g_server_socket_;
    ret = epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, g_server_socket_, &g_ev);
    if (ret < 0) {
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

    ret = initControlSocket(local_rank_info);
    if (ret) {
        LOG(ERROR) << "initControlSocket failed, ret: " << ret;
        return ret;
    }

    g_localMergeMem.reserve(VECTOR_RESERVE_SIZE);

    return 0;
}

void freeTransportMem() {
    clearTransportMems();
    if (vnicServerSocket_) {
        HcclNetCloseDev(vnicNetDevCtx_);
        vnicServerSocket_.reset();
    }
    if (nicServerSocket_) {
        HcclNetCloseDev(nicNetDevCtx_);
        nicServerSocket_.reset();
    }
    HcclDispatcherDestroy(dispatcher_);
    if (notifyPool_) {
        notifyPool_.reset();
    }
    if (g_server_socket_ > 0) {
        close(g_server_socket_);
    }
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
    LOG(INFO) << "transportMemTask local_rank_info rankId: "
              << local_rank_info->rankId
              << ", serverIdx: " << local_rank_info->serverIdx
              << ", deviceLogicId: " << local_rank_info->deviceLogicId
              << ", devicePhyId: " << local_rank_info->devicePhyId
              << ", deviceIp: " << inet_ntoa(local_rank_info->deviceIp)
              << ", devicePort: " << local_rank_info->devicePort
              << ", hostIp: " << inet_ntoa(local_rank_info->hostIp)
              << ", hostPort: " << local_rank_info->hostPort
              << ", device pid: " << local_rank_info->pid;

    LOG(INFO) << "transportMemTask remote_rank_info rankId: "
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
    ret = send(client_socket, &control_info, sizeof(RankControlInfo), 0);
    if (ret < 0) {
        LOG(ERROR) << "send control_info failed, ret: " << ret;
        close(client_socket);
        return ret;
    }
    target_key_to_connection_map_[key_str].tcp_socket = client_socket;
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
        remoteDevPhyId.push_back(remote_rank_info->devicePhyId);
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
                       std::shared_ptr<hccl::TransportMem> &transport_mem) {
    int ret = 0;
    bool same_host =
        local_rank_info->hostIp.s_addr == remote_rank_info->hostIp.s_addr;
    // For A2 series, internal communication among 8 cards does not cross HCCS,
    // such as communication among cards 0-7
    bool same_group = (local_rank_info->devicePhyId / 8) ==
                      (remote_rank_info->devicePhyId / 8);
    bool is_cross_hccs = !(same_host && same_group);
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    if (printEnabled()) {
        LOG(INFO) << "hccl transport is cross_hccs: "
                  << (is_cross_hccs ? "true (cross-hccs)"
                                    : "false (same-hccs)");
    }
    std::shared_ptr<hccl::HcclSocket> hccl_ctrl_socket;
    std::shared_ptr<hccl::HcclSocket> hccl_data_socket;
    ret = createClientSocket(hccl_ctrl_socket, local_rank_info,
                             remote_rank_info, is_cross_hccs, "ctrl");
    if (ret) {
        LOG(ERROR) << "createClientSocket hccl_ctrl_socket failed, ret: "
                   << ret;
        return ret;
    }
    target_key_to_connection_map_[key_str].hccl_ctrl_socket = hccl_ctrl_socket;
    ret = createClientSocket(hccl_data_socket, local_rank_info,
                             remote_rank_info, is_cross_hccs, "data");
    if (ret) {
        LOG(ERROR) << "createClientSocket hccl_data_socket failed, ret: "
                   << ret;
        return ret;
    }
    target_key_to_connection_map_[key_str].hccl_data_socket = hccl_data_socket;
    hccl::TransportMem::AttrInfo attrInfo;
    attrInfo.localRankId = local_rank_info->deviceLogicId;
    attrInfo.remoteRankId = remote_rank_info->deviceLogicId;
    attrInfo.sdid = 0xFFFFFFFF;
    attrInfo.serverId = local_rank_info->serverIdx;
    attrInfo.trafficClass = getHcclRdmaTrafficClass();
    attrInfo.serviceLevel = getHcclRdmaServiceLevel();
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
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp,
                  sizeof(deviceIp));
        LOG(ERROR) << "transport_mem SetDataSocket failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp
                   << ", remote port: " << remote_rank_info->devicePort
                   << ", ret: " << ret;
        return ret;
    }
    ret = transport_mem->SetSocket(hccl_ctrl_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &remote_rank_info->deviceIp, deviceIp,
                  sizeof(deviceIp));
        LOG(ERROR) << "transport_mem SetSocket failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp
                   << ", remote port: " << remote_rank_info->devicePort
                   << ", ret: " << ret;
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
        LOG(ERROR) << "transport_mem Connect failed, target devicePhyId: "
                   << remote_rank_info->devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp
                   << ", remote port: " << remote_rank_info->devicePort
                   << ", ret: " << ret;
        return ret;
    }
    LOG(INFO) << "transport_mem connect success";
    target_key_to_connection_map_[key_str].transport_mem = transport_mem;
    size_t m_num = g_localMergeMem.size();
    std::vector<hccl::TransportMem::RmaMemDesc> rmaMemDescs(m_num);

    for (size_t i = 0; i < m_num; ++i) {
        HcclMem mem;
        HcclBuf buf;
        mem.addr = g_localMergeMem[i].addr;
        mem.size = g_localMergeMem[i].len;
        mem.type = HCCL_MEM_TYPE_DEVICE;
        if (!is_cross_hccs) {
            ret = HcclMemReg(vnicNetDevCtx_, &mem, &buf);
        } else {
            ret = HcclMemReg(nicNetDevCtx_, &mem, &buf);
        }
        if (ret != 0 && ret != 20) {
            LOG(ERROR) << "HcclMemReg failed, ret: " << ret
                       << " addr: " << g_localMergeMem[i].addr
                       << " len: " << g_localMergeMem[i].len;
            return ret;
        }
        char *desc = nullptr;
        uint64_t desc_len = 0;
        ret = HcclMemExport(&buf, &desc, &desc_len);
        if (ret) {
            LOG(ERROR) << "HcclMemExport failed, ret: " << ret
                       << ", addr: " << g_localMergeMem[i].addr
                       << ", len: " << g_localMergeMem[i].len;
            return ret;
        }

        rmaMemDescs[i].localRankId = local_rank_info->deviceLogicId;
        rmaMemDescs[i].remoteRankId = remote_rank_info->deviceLogicId;
        memset_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, 0,
                 hccl::TRANSPORT_EMD_ESC_SIZE);
        if (memcpy_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, desc,
                     desc_len + 1) != EOK) {
            LOG(ERROR) << "memcpy_s failed, ret: " << ret
                       << ", addr: " << g_localMergeMem[i].addr
                       << ", len: " << g_localMergeMem[i].len;
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
                           << ", addr: " << g_localMergeMem[i].addr
                           << ", len: " << g_localMergeMem[i].len;
                return ret;
            }
        }
    }

    size_t max_m_num = getMaxRegMemoryNum();
    if (m_num >= max_m_num) {
        LOG(ERROR) << "The number of registered memory exceeds the expected "
                      "maximum size "
                   << max_m_num
                   << ". To resolve this issue, you can increase the maximum "
                      "size by setting the environment variable "
                      "ASCEND_TRANSPORT_MAX_REG_MEMORY_NUM.";
        return -1;
    }
    hccl::TransportMem::RmaMemDescs localRmaMemDescs;
    localRmaMemDescs.array = rmaMemDescs.data();
    localRmaMemDescs.arrayLength = rmaMemDescs.size();
    uint32_t actualNumOfRemote = 0;
    std::vector<hccl::TransportMem::RmaMemDesc> remoteRmaMemDescArray(
        max_m_num);
    hccl::TransportMem::RmaMemDescs remoteRmaMemDescs;
    remoteRmaMemDescs.array = remoteRmaMemDescArray.data();
    remoteRmaMemDescs.arrayLength = max_m_num;
    ret = transport_mem->ExchangeMemDesc(localRmaMemDescs, remoteRmaMemDescs,
                                         actualNumOfRemote);
    if (ret) {
        LOG(ERROR) << "transport_mem->ExchangeMemDesc failed, ret: " << ret
                   << ", local_rank: " << local_rank_info->devicePhyId
                   << ", remote_rank: " << remote_rank_info->devicePhyId;
        return ret;
    }
    std::vector<hccl::TransportMem::RmaMem> remoteRmaMemArray(
        actualNumOfRemote);
    for (uint32_t i = 0; i < actualNumOfRemote; ++i) {
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

int clearTransportMem(RankInfo *remote_rank_info) {
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    const auto &it = target_key_to_connection_map_.find(key_str);
    if (it != target_key_to_connection_map_.end()) {
        auto &hccl_ctrl_socket = it->second.hccl_ctrl_socket;
        if (hccl_ctrl_socket) {
            hccl_ctrl_socket->Close();
        }
        auto &hccl_data_socket = it->second.hccl_data_socket;
        if (hccl_data_socket) {
            hccl_data_socket->Close();
        }
        target_key_to_connection_map_.erase(key_str);
    }
    return 0;
}

int clearTransportMems() {
    for (auto &it : target_key_to_connection_map_) {
        auto &hccl_ctrl_socket = it.second.hccl_ctrl_socket;
        if (hccl_ctrl_socket) {
            hccl_ctrl_socket->Close();
        }
        auto &hccl_data_socket = it.second.hccl_data_socket;
        if (hccl_data_socket) {
            hccl_data_socket->Close();
        }
    }
    target_key_to_connection_map_.clear();
    return 0;
}

int transportMemAddOpFence(RankInfo *remote_rank_info, aclrtStream stream) {
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    int ret = target_key_to_connection_map_[key_str].transport_mem->AddOpFence(
        stream);
    if (ret) {
        LOG(ERROR) << "transport_mem AddOpFence failed, ret: " << ret;
        return ret;
    }

    return 0;
}

int transportMemTask(RankInfo *local_rank_info, RankInfo *remote_rank_info,
                     int op_code, uint64_t offset, uint64_t req_len,
                     void *local_mem, aclrtStream stream) {
    int ret = 0;
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    // Check if a connection has been established with the peer, and send local
    // information to the peer
    std::string key_str = inet_ntoa(remote_rank_info->hostIp) +
                          std::to_string(remote_rank_info->devicePhyId);
    auto iter = target_key_to_connection_map_.find(key_str);
    if (iter == target_key_to_connection_map_.end()) {
        ret = controlInfoSend(local_rank_info, remote_rank_info);
        if (ret) {
            LOG(ERROR) << "controlInfoSend failed, ret: " << ret;
            return ret;
        }
        ret = createTransportMem(local_rank_info, remote_rank_info,
                                 transport_mem);
        if (ret) {
            LOG(ERROR) << "createTransportMem failed, ret: " << ret;
            return ret;
        }
    } else {
        transport_mem = target_key_to_connection_map_[key_str].transport_mem;
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

static int acceptFromTarget() {
    int client_socket;
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    client_socket =
        accept(g_server_socket_, (struct sockaddr *)&client_addr, &client_len);
    if (client_socket < 0) {
        LOG(ERROR) << "Accept failed";
        return client_socket;
    }

    LOG(INFO) << "host client connected from "
              << inet_ntoa(client_addr.sin_addr) << ":"
              << ntohs(client_addr.sin_port);
    return client_socket;
}

int acceptSocket(std::shared_ptr<hccl::HcclSocket> &hccl_socket,
                 RankInfo *local_rank_info, RankControlInfo remote_control_info,
                 std::string baseTag_, hccl::HcclIpAddress rempoteDevIp,
                 bool is_cross_hccs) {
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

int transportMemAccept(RankInfo *local_rank_info) {
    static int epoll_wait_timeout = getEpollWaitTimeoutMs();
    // Self-built out-of-band, host socket for receiving control plane
    int ret = 0;
    int nfds = epoll_wait(g_epoll_fd, g_events, MAX_EVENTS, epoll_wait_timeout);
    if (nfds <= 0) {
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
        return -1;
    }

    LOG(INFO) << "Received remote_control_info, deviceLogicId: "
              << remote_control_info.deviceLogicId
              << ", devicePhyId: " << remote_control_info.devicePhyId
              << ", hostIp: " << inet_ntoa(remote_control_info.hostIp)
              << ", deviceIp: " << inet_ntoa(remote_control_info.deviceIp)
              << ", device pid: " << remote_control_info.pid;

    // Check if TransportMem has been established with the peer
    std::string key_str = inet_ntoa(remote_control_info.hostIp) +
                          std::to_string(remote_control_info.devicePhyId);
    auto iter = target_key_to_connection_map_.find(key_str);
    if (iter != target_key_to_connection_map_.end()) {
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
    if (printEnabled()) {
        LOG(INFO) << "transport is cross_hccs: "
                  << (is_cross_hccs ? "true (cross-hccs)"
                                    : "false (same-hccs)");
    }
    if (!is_cross_hccs) {
        std::vector<unsigned int> remoteDevPhyId;
        rempoteDevIp = hccl::HcclIpAddress(remote_control_info.devicePhyId);
        remoteDevPhyId.push_back(remote_control_info.devicePhyId);
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
        ret =
            acceptSocket(hccl_ctrl_socket, local_rank_info, remote_control_info,
                         baseTag_ + "ctrl", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket ctrl failed, ret: " << ret;
            return ret;
        }

        ret =
            acceptSocket(hccl_data_socket, local_rank_info, remote_control_info,
                         baseTag_ + "data", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket data failed, ret: " << ret;
            return ret;
        }
    } else {
        rempoteDevIp = hccl::HcclIpAddress(remote_control_info.deviceIp);
        ret =
            acceptSocket(hccl_ctrl_socket, local_rank_info, remote_control_info,
                         baseTag_ + "ctrl", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket ctrl failed, ret: " << ret;
            return ret;
        }

        ret =
            acceptSocket(hccl_data_socket, local_rank_info, remote_control_info,
                         baseTag_ + "data", rempoteDevIp, is_cross_hccs);
        if (ret) {
            LOG(ERROR) << "acceptSocket data failed, ret: " << ret;
            return ret;
        }
    }

    target_key_to_connection_map_[key_str].hccl_ctrl_socket = hccl_ctrl_socket;
    target_key_to_connection_map_[key_str].hccl_data_socket = hccl_data_socket;
    LOG(INFO) << "Creating transfer_mem on the accept side";
    std::shared_ptr<hccl::TransportMem> transport_mem{};
    hccl::TransportMem::AttrInfo attrInfo;
    attrInfo.localRankId = local_rank_info->deviceLogicId;
    attrInfo.remoteRankId = remote_control_info.deviceLogicId;
    attrInfo.sdid = 0xFFFFFFFF;
    attrInfo.serverId = local_rank_info->serverIdx;
    attrInfo.trafficClass = getHcclRdmaTrafficClass();
    attrInfo.serviceLevel = getHcclRdmaServiceLevel();
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
        LOG(ERROR) << "transport_mem SetDataSocket failed, target devicePhyId: "
                   << remote_control_info.devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp << ", ret: " << ret;
        return ret;
    }

    ret = transport_mem->SetSocket(hccl_ctrl_socket);
    if (ret) {
        char deviceIp[64];
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "transport_mem SetSocket failed, target devicePhyId: "
                   << remote_control_info.devicePhyId
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
        inet_ntop(AF_INET, &rempoteDevIp, deviceIp, sizeof(deviceIp));
        LOG(ERROR) << "transport_mem Connect failed, target devicePhyId: "
                   << remote_control_info.devicePhyId
                   << ", local devicePhyId: " << local_rank_info->devicePhyId
                   << ", rempoteDevIp: " << deviceIp << ", ret: " << ret;
        return ret;
    }

    target_key_to_connection_map_[key_str].transport_mem = transport_mem;

    size_t m_num = g_localMergeMem.size();
    std::vector<hccl::TransportMem::RmaMemDesc> rmaMemDescs(m_num);
    for (size_t i = 0; i < m_num; ++i) {
        HcclBuf buf;
        HcclMem mem;
        mem.addr = g_localMergeMem[i].addr;
        mem.size = g_localMergeMem[i].len;
        mem.type = HCCL_MEM_TYPE_DEVICE;
        if (!is_cross_hccs) {
            ret = HcclMemReg(vnicNetDevCtx_, &mem, &buf);
        } else {
            ret = HcclMemReg(nicNetDevCtx_, &mem, &buf);
        }
        if (ret != 0 && ret != 20) {
            LOG(ERROR) << "HcclMemReg failed, ret: " << ret
                       << ", addr: " << g_localMergeMem[i].addr
                       << ", len: " << g_localMergeMem[i].len;
            return ret;
        }
        char *desc = nullptr;
        uint64_t desc_len = 0;
        ret = HcclMemExport(&buf, &desc, &desc_len);
        if (ret) {
            LOG(ERROR) << "HcclMemExport failed, ret: " << ret
                       << ", addr: " << g_localMergeMem[i].addr
                       << ", len: " << g_localMergeMem[i].len;
            return ret;
        }

        rmaMemDescs[i].localRankId = local_rank_info->deviceLogicId;
        rmaMemDescs[i].remoteRankId = remote_control_info.deviceLogicId;
        memset_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, 0,
                 hccl::TRANSPORT_EMD_ESC_SIZE);
        if (memcpy_s(rmaMemDescs[i].memDesc, hccl::TRANSPORT_EMD_ESC_SIZE, desc,
                     desc_len + 1) != EOK) {
            LOG(ERROR) << "memcpy_s failed, ret: " << ret
                       << ", addr: " << g_localMergeMem[i].addr
                       << ", len: " << g_localMergeMem[i].len;
            return -1;
        }

        // In the scenario within HCCS, it is necessary to call HcclMemGrant to
        // authorize peer memory
        if (!is_cross_hccs) {
            HcclMemGrantInfo grant_info;
            grant_info.remotePid = (int32_t)remote_control_info.pid;
            grant_info.remoteSdid = 0xFFFFFFFF;
            ret = HcclMemGrant(&buf, &grant_info);
            if (ret) {
                LOG(ERROR) << "HcclMemGrant failed, ret: " << ret
                           << ", addr: " << g_localMergeMem[i].addr
                           << ", len: " << g_localMergeMem[i].len;
                return ret;
            }
        }
    }

    size_t max_m_num = getMaxRegMemoryNum();
    if (m_num >= max_m_num) {
        LOG(ERROR) << "The number of registered memory exceeds the expected "
                      "maximum size "
                   << max_m_num
                   << ". To resolve this issue, you can increase the maximum "
                      "size by setting the environment variable "
                      "ASCEND_TRANSPORT_MAX_REG_MEMORY_NUM.";
        return -1;
    }
    hccl::TransportMem::RmaMemDescs localRmaMemDescs;
    localRmaMemDescs.array = rmaMemDescs.data();
    localRmaMemDescs.arrayLength = rmaMemDescs.size();
    uint32_t actualNumOfRemote = 0;
    std::vector<hccl::TransportMem::RmaMemDesc> remoteRmaMemDescArray(
        max_m_num);
    hccl::TransportMem::RmaMemDescs remoteRmaMemDescs;
    remoteRmaMemDescs.array = remoteRmaMemDescArray.data();
    remoteRmaMemDescs.arrayLength = max_m_num;
    ret = transport_mem->ExchangeMemDesc(localRmaMemDescs, remoteRmaMemDescs,
                                         actualNumOfRemote);
    if (ret) {
        LOG(ERROR) << "transport_mem->ExchangeMemDesc failed, ret: " << ret
                   << ", local_rank: " << local_rank_info->devicePhyId
                   << ", remote_rank: " << remote_control_info.devicePhyId;
        return ret;
    }
    std::vector<hccl::TransportMem::RmaMem> remoteRmaMemArray(
        actualNumOfRemote);
    for (uint32_t i = 0; i < actualNumOfRemote; ++i) {
        ret = transport_mem->EnableMemAccess(remoteRmaMemDescArray[i],
                                             remoteRmaMemArray[i]);
        if (ret) {
            LOG(ERROR) << "transport_mem->EnableMemAccess failed, ret: " << ret
                       << ", i: " << i
                       << ", local_rank: " << local_rank_info->devicePhyId
                       << ", remote_rank: " << remote_control_info.devicePhyId;
            return ret;
        }
    }

    LOG(INFO) << "ExchangeMem and EnableMemAccess Success, local devicePhyId: "
              << local_rank_info->devicePhyId
              << ", target devicePhyId: " << remote_control_info.devicePhyId;
    return 0;
}

int regLocalRmaMem(void *addr, uint64_t length) {
    g_localMergeMem.push_back(MergeMem{addr, length});
    return 0;
}

int unregLocalRmaMems() {
    g_localMergeMem.clear();
    return 0;
}

#ifdef __cplusplus
}
#endif  // __cplusplus
