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

#include "v1/transport/rdma/context.h"

#include <fcntl.h>
#include <sys/epoll.h>

#include <atomic>
#include <cassert>
#include <fstream>
#include <memory>
#include <thread>

#include "v1/common/status.h"
#include "v1/transport/rdma/endpoint_store.h"

#define MIN(lhs, rhs) lhs = std::min(lhs, rhs)

#define ASSERT_STATUS(status)                                              \
    do {                                                                   \
        if (status_ != (status)) {                                         \
            LOG(FATAL) << "Detected incorrect status in context ["         \
                       << device_name_ << "], expect " #status ", actual " \
                       << statusToString(status_);                         \
            exit(EXIT_FAILURE);                                            \
        }                                                                  \
    } while (0)

#define ASSERT_STATUS_NOT(status)                                              \
    do {                                                                       \
        if (status_ == (status)) {                                             \
            LOG(FATAL) << "Detected incorrect status in context ["             \
                       << device_name_ << "], expect NOT " #status ", actual " \
                       << statusToString(status_);                             \
            exit(EXIT_FAILURE);                                                \
        }                                                                      \
    } while (0)

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
        PLOG(ERROR) << "fcntl(F_GETFL)";
        return -1;
    }

    if (fcntl(data_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        PLOG(ERROR) << "fcntl(F_SETFL)";
        return -1;
    }

    event.events = EPOLLIN | EPOLLET;
    event.data.fd = data_fd;
    if (epoll_ctl(event_fd, EPOLL_CTL_ADD, event.data.fd, &event)) {
        PLOG(ERROR) << "epoll_ctl(EPOLL_CTL_ADD)";
        return -1;
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
            PLOG(WARNING) << "Unable to query GID " << i << " on device "
                          << device_name << " port " << port;
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

static inline const std::string statusToString(
    RdmaContext::DeviceStatus status) {
    switch (status) {
        case RdmaContext::DEVICE_UNINIT:
            return "DEVICE_UNINIT";
        case RdmaContext::DEVICE_ENABLED:
            return "DEVICE_ENABLED";
        case RdmaContext::DEVICE_DISABLED:
            return "DEVICE_DISABLED";
    }
    return "UNKNOWN";
}

RdmaContext::RdmaContext(RdmaTransport &transport)
    : transport_(transport), status_(DEVICE_UNINIT) {
    static std::once_flag g_once_flag;
    auto fork_init = []() {
        int ret = ibv_fork_init();
        if (ret) PLOG(FATAL) << "ibv_fork_init";
    };
    std::call_once(g_once_flag, fork_init);
}

RdmaContext::~RdmaContext() {
    if (status_ != DEVICE_UNINIT) disable();
}

int RdmaContext::construct(const std::string &device_name,
                           std::shared_ptr<RdmaParams> params) {
    ASSERT_STATUS(DEVICE_UNINIT);
    device_name_ = device_name;
    params_ = params;
    endpoint_store_ = std::make_shared<SIEVEEndpointStore>(
        *this, params_->endpoint.endpoint_store_cap);
    status_ = DEVICE_DISABLED;
    return enable();
}

int RdmaContext::enable() {
    ASSERT_STATUS(DEVICE_DISABLED);
    if (openDevice(device_name_, params_->device.port)) {
        LOG(ERROR) << "Failed to open device [" << device_name_ << "] on port ["
                   << params_->device.port << "] with GID index ["
                   << params_->device.gid_index << "]";
        disable();
        return -1;
    }

    native_pd_ = ibv_alloc_pd(native_context_);
    if (!native_pd_) {
        PLOG(ERROR) << "ibv_alloc_pd";
        disable();
        return -1;
    }

    event_fd_ = epoll_create1(0);
    if (event_fd_ < 0) {
        PLOG(ERROR) << "epoll_create1";
        disable();
        return -1;
    }

    if (joinNonblockingPollList(event_fd_, native_context_->async_fd)) {
        disable();
        return -1;
    }

    num_comp_channel_ = params_->device.num_comp_channels;
    comp_channel_.resize(num_comp_channel_, nullptr);

    for (size_t i = 0; i < num_comp_channel_; ++i) {
        comp_channel_[i] = ibv_create_comp_channel(native_context_);
        if (!comp_channel_[i]) {
            PLOG(ERROR) << "ibv_create_comp_channel";
            disable();
            return -1;
        }

        if (joinNonblockingPollList(event_fd_, comp_channel_[i]->fd)) {
            disable();
            return -1;
        }
    }

    for (int i = 0; i < params_->device.num_cq_list; ++i) {
        auto cq = new RdmaCQ();
        int ret = cq->construct(this, params_->device.max_cqe, i);
        if (ret) {
            disable();
            return ret;
        }
        cq_list_.push_back(cq);
    }

    status_ = DEVICE_ENABLED;

    LOG(INFO) << "Context " << device_name_ << " is enabled: "
              << "LID " << lid_ << ", GID [" << gid_index_ << "] " << gid();
    return 0;
}

int RdmaContext::disable() {
    ASSERT_STATUS_NOT(DEVICE_UNINIT);
    endpoint_store_->clear();

    for (auto &entry : mr_set_) {
        int ret = ibv_dereg_mr(entry);
        if (ret) PLOG(ERROR) << "ibv_dereg_mr";
    }
    mr_set_.clear();
    for (auto &entry : cq_list_) {
        delete entry;
    }
    cq_list_.clear();

    if (event_fd_ >= 0) {
        if (close(event_fd_)) PLOG(ERROR) << "close";
        event_fd_ = -1;
    }

    for (auto &item : comp_channel_)
        if (item && ibv_destroy_comp_channel(item))
            PLOG(ERROR) << "ibv_destroy_comp_channel";
    comp_channel_.clear();
    num_comp_channel_ = 0;

    if (native_pd_) {
        if (ibv_dealloc_pd(native_pd_)) PLOG(ERROR) << "ibv_dealloc_pd";
        native_pd_ = nullptr;
    }

    if (native_context_) {
        if (ibv_close_device(native_context_))
            PLOG(ERROR) << "ibv_close_device";
        native_context_ = nullptr;
    }

    status_ = DEVICE_DISABLED;
    return 0;
}

RdmaContext::MemReg RdmaContext::registerMemReg(void *addr, size_t length,
                                                int access) {
    ASSERT_STATUS(DEVICE_ENABLED);
    ibv_mr *entry = ibv_reg_mr(native_pd_, addr, length, access);
    if (!entry) {
        PLOG(ERROR) << "Failed to register memory from " << addr << " to "
                    << (char *)addr + length << " in RDMA device "
                    << device_name_;
        return nullptr;
    }
    mr_set_mutex_.lock();
    mr_set_.insert(entry);
    mr_set_mutex_.unlock();
    return entry;
}

int RdmaContext::unregisterMemReg(MemReg id) {
    ASSERT_STATUS(DEVICE_ENABLED);
    auto entry = (ibv_mr *)id;
    mr_set_mutex_.lock();
    mr_set_.erase(entry);
    mr_set_mutex_.unlock();

    if (ibv_dereg_mr(entry)) {
        LOG(ERROR) << "Failed to unregister memory from " << entry->addr
                   << " to " << (char *)entry->addr + entry->length
                   << " in RDMA device " << device_name_;
    }

    return 0;
}

std::string RdmaContext::gid() const {
    ASSERT_STATUS(DEVICE_ENABLED);
    std::string gid_str;
    char buf[16] = {0};
    const static size_t kGidLength = 16;
    for (size_t i = 0; i < kGidLength; ++i) {
        sprintf(buf, "%02x", gid_.raw[i]);
        gid_str += i == 0 ? buf : std::string(":") + buf;
    }
    return gid_str;
}

RdmaCQ *RdmaContext::cq(int index) {
    ASSERT_STATUS(DEVICE_ENABLED);
    if (index < 0 || index >= params_->device.num_cq_list) return nullptr;
    return cq_list_[index];
}

int RdmaContext::openDevice(const std::string &device_name, uint8_t port) {
    int num_devices = 0;
    struct ibv_context *context = nullptr;
    struct ibv_device **devices = ibv_get_device_list(&num_devices);
    if (!devices || num_devices <= 0) {
        PLOG(ERROR) << "ibv_get_device_list";
        return -1;
    }

    for (int i = 0; i < num_devices; ++i) {
        if (device_name != ibv_get_device_name(devices[i])) continue;
        context = ibv_open_device(devices[i]);
        if (!context) {
            PLOG(ERROR) << "ibv_open_device";
            ibv_free_device_list(devices);
            return -1;
        }
    }

    ibv_free_device_list(devices);
    if (!context) {
        LOG(ERROR) << "No matched device found in this server: " << device_name;
        return -1;
    }

    ibv_port_attr port_attr;
    int ret = ibv_query_port(context, port, &port_attr);
    if (ret) {
        PLOG(ERROR) << "Failed to query port " << port << " on " << device_name;
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device";
        }
        return -1;
    }

    if (port_attr.state != IBV_PORT_ACTIVE) {
        LOG(WARNING) << "Device " << device_name << " port " << port
                     << " not active";
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device";
        }
        return -1;
    }

    ibv_device_attr device_attr;
    ret = ibv_query_device(context, &device_attr);
    if (ret) {
        PLOG(WARNING) << "ibv_query_device";
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device";
        }
        return -1;
    }

    MIN(params_->device.max_cqe, device_attr.max_cqe);
    MIN(params_->device.num_cq_list, device_attr.max_cq);
    MIN(params_->endpoint.path_mtu, port_attr.active_mtu);
    MIN(params_->endpoint.max_sge, device_attr.max_sge);
    MIN(params_->endpoint.max_qp_wr, device_attr.max_qp_wr);

    gid_index_ = params_->device.gid_index;
    if (gid_index_ <= 0) {
        int ret = getBestGidIndex(device_name, context, port_attr, port);
        if (ret >= 0) {
            gid_index_ = ret;
        }
    }

    ret = ibv_query_gid(context, port, gid_index_, &gid_);
    if (ret) {
        PLOG(ERROR) << "Unable to query GID " << gid_index_ << " on device "
                    << device_name << " port " << port;
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
        }
        return -1;
    }

    if (isNullGid(&gid_)) {
        PLOG(ERROR) << "Uninitialized GID " << gid_index_ << " on "
                    << device_name << "/" << port;
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device";
        }
        return -1;
    }

    native_context_ = context;
    lid_ = port_attr.lid;
    return 0;
}
}  // namespace v1
}  // namespace mooncake