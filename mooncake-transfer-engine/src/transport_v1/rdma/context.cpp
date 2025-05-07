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

#include "transport_v1/rdma/context.h"

#include <fcntl.h>
#include <sys/epoll.h>

#include <atomic>
#include <cassert>
#include <fstream>
#include <memory>
#include <thread>

#include "common/config.h"
#include "transport_v1/rdma/endpoint_store.h"

namespace mooncake {
namespace v1 {
static inline int isNullGid(union ibv_gid *gid) {
    for (int i = 0; i < 16; ++i) {
        if (gid->raw[i] != 0) return 0;
    }
    return 1;
}

static inline int querySocketID(const std::string &device_name) {
    std::string path =
        "/sys/class/infiniband/" + device_name + "/device/numa_node";
    std::ifstream file(path);
    if (file.is_open()) {
        int socket_id;
        file >> socket_id;
        file.close();
        return socket_id;
    } else {
        return 0;
    }
}

static inline int joinNonblockingPollList(int event_fd, int data_fd) {
    epoll_event event;
    memset(&event, 0, sizeof(epoll_event));

    int flags = fcntl(data_fd, F_GETFL, 0);
    if (flags == -1) {
        PLOG(ERROR) << "Failed to get file descriptor flags";
        return ERR_CONTEXT;
    }
    if (fcntl(data_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        PLOG(ERROR) << "Failed to set file descriptor nonblocking";
        return ERR_CONTEXT;
    }

    event.events = EPOLLIN | EPOLLET;
    event.data.fd = data_fd;
    if (epoll_ctl(event_fd, EPOLL_CTL_ADD, event.data.fd, &event)) {
        PLOG(ERROR) << "Failed to register file descriptor to epoll";
        return ERR_CONTEXT;
    }

    return 0;
}

static inline int ipv6_addr_v4mapped(const struct in6_addr *a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) |
            (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL ||
           /* IPv4 encoded multicast addresses */
           (a->s6_addr32[0] == htonl(0xff0e0000) &&
            ((a->s6_addr32[1] | (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL));
}

static inline int getBestGidIndex(const std::string &device_name,
                                  struct ibv_context *context,
                                  ibv_port_attr &port_attr, uint8_t port) {
    int gid_index = 0, i;
    struct ibv_gid_entry gid_entry;

    for (i = 0; i < port_attr.gid_tbl_len; i++) {
        if (ibv_query_gid_ex(context, port, i, &gid_entry, 0)) {
            PLOG(ERROR) << "Failed to query GID " << i << " on " << device_name
                        << "/" << port;
            continue;  // if gid is invalid ibv_query_gid_ex() will return !0
        }
        if ((ipv6_addr_v4mapped((struct in6_addr *)gid_entry.gid.raw) &&
             gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2) ||
            gid_entry.gid_type == IBV_GID_TYPE_IB) {
            gid_index = i;
            break;
        }
    }
    return gid_index;
}

RdmaContext::RdmaContext() : status_(kDeviceUninitialized), next_mr_index_(0) {
    static std::once_flag g_once_flag;
    auto fork_init = []() {
        int ret = ibv_fork_init();
        if (ret) PLOG(FATAL) << "RDMA context setup failed: fork compatibility";
    };
    std::call_once(g_once_flag, fork_init);
}

RdmaContext::~RdmaContext() {
    if (status_ == kDeviceEnabled) disable();
}

int RdmaContext::construct(const std::string &device_name,
                           std::shared_ptr<EndpointStore> endpoint_store,
                           const DeviceParams &params) {
    if (status_ != kDeviceUninitialized) {
        LOG(ERROR) << "RdmaContext object " << this << " has been constructed";
        return ERR_CONTEXT;
    }
    device_name_ = device_name;
    endpoint_store_ = endpoint_store;
    params_ = params;
    status_ = kDeviceDisabled;
    return enable();
}

int RdmaContext::enable() {
    if (status_ == kDeviceUninitialized) {
        LOG(ERROR) << "RdmaContext object " << this << " is not initialized";
        return ERR_CONTEXT;
    }

    if (status_ == kDeviceEnabled) return 0;

    if (openDevice(device_name_, params_.port, params_.gid_index)) {
        LOG(ERROR) << "Failed to open device " << device_name_ << " on port "
                   << params_.port << " with GID " << params_.gid_index;
        disable();
        return ERR_CONTEXT;
    }

    pd_ = ibv_alloc_pd(context_);
    if (!pd_) {
        PLOG(ERROR) << "Failed to allocate new protection domain on device "
                    << device_name_;
        disable();
        return ERR_CONTEXT;
    }

    num_comp_channel_ = params_.num_comp_channels;
    comp_channel_ = new ibv_comp_channel *[params_.num_comp_channels];
    for (size_t i = 0; i < params_.num_comp_channels; ++i) {
        comp_channel_[i] = ibv_create_comp_channel(context_);
        if (!comp_channel_[i]) {
            PLOG(ERROR) << "Failed to create completion channel on device "
                        << device_name_;
            disable();
            return ERR_CONTEXT;
        }
    }

    event_fd_ = epoll_create1(0);
    if (event_fd_ < 0) {
        PLOG(ERROR) << "Failed to create epoll";
        disable();
        return ERR_CONTEXT;
    }

    if (joinNonblockingPollList(event_fd_, context_->async_fd)) {
        LOG(ERROR) << "Failed to register context async fd to epoll";
        disable();
        return ERR_CONTEXT;
    }

    for (size_t i = 0; i < num_comp_channel_; ++i)
        if (joinNonblockingPollList(event_fd_, comp_channel_[i]->fd)) {
            LOG(ERROR) << "Failed to register completion channel " << i
                       << " to epoll";
            disable();
            return ERR_CONTEXT;
        }

    cq_list_.resize(params_.num_cq_list);
    for (int i = 0; i < params_.num_cq_list; ++i) {
        auto cq = ibv_create_cq(context_, params_.max_cqe, nullptr,
                                comp_channel_[i % num_comp_channel_],
                                i % context_->num_comp_vectors);
        if (!cq) {
            PLOG(ERROR) << "Failed to create completion queue";
            disable();
            return ERR_CONTEXT;
        }
        cq_list_[i].cq = cq;
        cq_list_[i].index = i;
        cq_list_[i].context = this;
    }

    status_ = kDeviceEnabled;
    LOG(INFO) << "RDMA Device " << context_->device->name << ", LID " << lid_
              << ", GID [" << gid_index_ << "] " << gid();

    return 0;
}

int RdmaContext::disable() {
    if (status_ == kDeviceUninitialized) {
        LOG(ERROR) << "RdmaContext object " << this << " is not initialized";
        return ERR_CONTEXT;
    }

    for (auto &entry : mr_map_) {
        int ret = ibv_dereg_mr(entry.second);
        if (ret) PLOG(ERROR) << "Failed to unregister memory region";
    }
    mr_map_.clear();

    for (size_t i = 0; i < cq_list_.size(); ++i) {
        int ret = ibv_destroy_cq(cq_list_[i].cq);
        if (ret) PLOG(ERROR) << "Failed to destroy completion queue";
    }
    cq_list_.clear();

    if (event_fd_ >= 0) {
        if (close(event_fd_)) LOG(ERROR) << "Failed to close epoll fd";
        event_fd_ = -1;
    }

    if (comp_channel_) {
        for (size_t i = 0; i < num_comp_channel_; ++i)
            if (comp_channel_[i])
                if (ibv_destroy_comp_channel(comp_channel_[i]))
                    LOG(ERROR) << "Failed to destroy completion channel";
        delete[] comp_channel_;
        comp_channel_ = nullptr;
    }

    if (pd_) {
        if (ibv_dealloc_pd(pd_))
            PLOG(ERROR) << "Failed to deallocate protection domain";
        pd_ = nullptr;
    }

    if (context_) {
        if (ibv_close_device(context_))
            PLOG(ERROR) << "Failed to close device context";
        context_ = nullptr;
    }

    status_ = kDeviceDisabled;
    return 0;
}

RdmaContext::MemRegIndex RdmaContext::registerMemReg(void *addr, size_t length,
                                                     int access) {
    if (status_ == kDeviceUninitialized) {
        LOG(ERROR) << "RdmaContext object " << this << " is not initialized";
        return ERR_CONTEXT;
    }

    ibv_mr *mr = ibv_reg_mr(pd_, addr, length, access);
    if (!mr) {
        PLOG(ERROR) << "failed to register memory from " << addr << " to "
                    << (char *)addr + length << " in context " << device_name_;
        return ERR_CONTEXT;
    }

    LOG(INFO) << "register ... " << addr << " " << length << " " << mr->lkey
              << " " << mr->rkey << " perm " << access;

    int index = next_mr_index_.fetch_add(1);
    RWSpinlock::WriteGuard guard(mr_lock_);
    mr_map_[index] = mr;
    return 0;
}

int RdmaContext::unregisterMemReg(MemRegIndex index) {
    if (status_ == kDeviceUninitialized) {
        LOG(ERROR) << "RdmaContext object " << this << " is not initialized";
        return ERR_CONTEXT;
    }

    RWSpinlock::WriteGuard guard(mr_lock_);
    if (!mr_map_.count(index)) {
        LOG(ERROR) << "memory region #" << index << " not found in context "
                   << device_name_;
        return 0;
    }
    auto &entry = mr_map_[index];
    if (ibv_dereg_mr(entry)) {
        LOG(ERROR) << "failed to unregister memory from " << entry->addr
                   << " to " << (char *)entry->addr + entry->length
                   << " in context " << device_name_;
    }
    mr_map_.erase(index);
    return 0;
}

std::pair<uint32_t, uint32_t> RdmaContext::queryMemRegKey(MemRegIndex index) {
    if (status_ == kDeviceUninitialized) {
        LOG(ERROR) << "RdmaContext object " << this << " is not initialized";
        return {0, 0};
    }

    RWSpinlock::ReadGuard guard(mr_lock_);
    if (!mr_map_.count(index)) {
        LOG(ERROR) << "memory region #" << index << " not found in context "
                   << device_name_;
        return {0, 0};
    }
    auto &entry = mr_map_[index];
    return {entry->lkey, entry->rkey};
}

std::string RdmaContext::gid() const {
    std::string gid_str;
    char buf[16] = {0};
    const static size_t kGidLength = 16;
    for (size_t i = 0; i < kGidLength; ++i) {
        sprintf(buf, "%02x", gid_.raw[i]);
        gid_str += i == 0 ? buf : std::string(":") + buf;
    }
    return gid_str;
}

RdmaContext::CompletionQueue *RdmaContext::cq(int index) {
    if (index < 0 || index >= params_.num_cq_list) return nullptr;
    return &cq_list_[index];
}

std::shared_ptr<RdmaEndPoint> RdmaContext::endpoint(
    const std::string &key, EndPointOperation operation) {
    if (status_ == kDeviceUninitialized) {
        LOG(ERROR) << "RdmaContext object " << this << " is not initialized";
        return nullptr;
    }

    if (key.empty()) {
        LOG(ERROR) << "Invalid peer NIC path";
        return nullptr;
    }

    auto full_key = device_name_ + "|" + key;
    auto endpoint = endpoint_store_->getEndpoint(full_key);
    switch (operation) {
        case EP_GET_OR_CREATE:
            if (!endpoint)
                endpoint = endpoint_store_->insertEndpoint(full_key, this);
            return endpoint;
        case EP_GET:
            return endpoint;
        case EP_DELETE:
            endpoint_store_->deleteEndpoint(full_key);
            return endpoint;
        case EP_RECREATE:
            endpoint_store_->deleteEndpoint(full_key);
            endpoint = endpoint_store_->insertEndpoint(full_key, this);
            return endpoint;
        default:
            return nullptr;
    }
}

int RdmaContext::openDevice(const std::string &device_name, uint8_t port,
                            int gid_index) {
    int num_devices = 0;
    struct ibv_context *context = nullptr;
    struct ibv_device **devices = ibv_get_device_list(&num_devices);
    if (!devices || num_devices <= 0) {
        LOG(ERROR) << "ibv_get_device_list failed";
        return ERR_DEVICE_NOT_FOUND;
    }

    for (int i = 0; i < num_devices; ++i) {
        if (device_name != ibv_get_device_name(devices[i])) continue;
        context = ibv_open_device(devices[i]);
        if (!context) {
            LOG(ERROR) << "ibv_open_device(" << device_name << ") failed";
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }
    }

    ibv_free_device_list(devices);
    if (!context) {
        LOG(ERROR) << "No matched device found: " << device_name;
        return ERR_DEVICE_NOT_FOUND;
    }

    ibv_port_attr port_attr;
    int ret = ibv_query_port(context, port, &port_attr);
    if (ret) {
        PLOG(ERROR) << "Failed to query port " << port << " on " << device_name;
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
        }
        return ERR_CONTEXT;
    }

    if (port_attr.state != IBV_PORT_ACTIVE) {
        LOG(WARNING) << "Device " << device_name << " port not active";
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
        }
        return ERR_CONTEXT;
    }

    ibv_device_attr device_attr;
    ret = ibv_query_device(context, &device_attr);
    if (ret) {
        PLOG(WARNING) << "Failed to query attributes on " << device_name;
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
        }
        return ERR_CONTEXT;
    }

    params_.max_cqe = std::min(params_.max_cqe, device_attr.max_cqe);
    if (gid_index == 0) {
        int ret = getBestGidIndex(device_name, context, port_attr, port);
        if (ret >= 0) {
            LOG(INFO) << "Find best GID " << ret << " on " << device_name << "/"
                      << port;
            gid_index = ret;
        }
    }

    ret = ibv_query_gid(context, port, gid_index, &gid_);
    if (ret) {
        PLOG(ERROR) << "Failed to query GID " << gid_index << " on "
                    << device_name << "/" << port;
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
        }
        return ERR_CONTEXT;
    }

    if (isNullGid(&gid_)) {
        PLOG(ERROR) << "Uninitialized GID " << gid_index << " on "
                    << device_name << "/" << port;
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
        }
        return ERR_CONTEXT;
    }

    context_ = context;
    lid_ = port_attr.lid;
    active_mtu_ = port_attr.active_mtu;
    active_speed_ = port_attr.active_speed;
    gid_index_ = gid_index;

    return 0;
}
}  // namespace v1
}  // namespace mooncake