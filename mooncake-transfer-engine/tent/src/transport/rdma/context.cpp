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

#include "tent/transport/rdma/context.h"

#include <dirent.h>
#include <cstdlib>
#include <fcntl.h>
#include <limits.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <fstream>
#include <memory>
#include <thread>

#include "tent/common/status.h"
#include "tent/transport/rdma/endpoint_store.h"

#define MIN(lhs, rhs) lhs = std::min(lhs, rhs)
namespace mooncake {
namespace tent {
struct IbvContextDeleter {
    const IbvSymbols* verbs = nullptr;

    void operator()(ibv_context* ctx) const {
        if (ctx && verbs && verbs->ibv_close_device(ctx)) {
            PLOG(ERROR) << "ibv_close_device";
        }
    }
};
using IbvContextPtr = std::unique_ptr<ibv_context, IbvContextDeleter>;

static inline int isNullGid(union ibv_gid* gid) {
    for (int i = 0; i < 16; ++i) {
        if (gid->raw[i] != 0) return 0;
    }
    return 1;
}

static inline int querySocketID(const std::string& device_name) {
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

static inline int ipv6_addr_v4mapped(const struct in6_addr* a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) |
            (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL ||
           /* IPv4 encoded multicast addresses */
           (a->s6_addr32[0] == htonl(0xff0e0000) &&
            ((a->s6_addr32[1] | (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL));
}

// Reads the associated network device name for the given GID index from sysfs.
// Returns empty string if no network device is associated (e.g., overlay
// networks).
static inline std::string readGidNdev(const std::string& device_name,
                                      uint8_t port, int gid_index) {
    std::string sysfs_path = "/sys/class/infiniband/" + device_name +
                             "/ports/" + std::to_string(port) +
                             "/gid_attrs/ndevs/" + std::to_string(gid_index);
    std::ifstream file(sysfs_path);
    if (!file.is_open()) {
        return "";
    }

    std::string ndev;
    std::getline(file, ndev);
    return ndev;
}

// Check if the network device is an overlay network (e.g., flannel, calico,
// cni0)
static inline bool isOverlayNetwork(const std::string& ndev) {
    // Common overlay network device prefixes
    return ndev.find("flannel") == 0 || ndev.find("cni") == 0 ||
           ndev.find("calico") == 0 || ndev.find("vxlan") == 0 ||
           ndev.find("docker") == 0 || ndev == "tunl0";
}

// Check if an IPv4 address is in overlay network range
static inline bool isOverlayIPv4(const struct in6_addr* addr) {
    if (!ipv6_addr_v4mapped(addr)) return false;

    // Extract IPv4 address from IPv6-mapped format
    uint32_t ipv4 = ntohl(addr->s6_addr32[3]);
    uint8_t octet1 = (ipv4 >> 24) & 0xFF;
    uint8_t octet2 = (ipv4 >> 16) & 0xFF;

    // Common overlay network ranges:
    // 10.0.0.0/8 (flannel, etc.)
    // 172.16.0.0/12 (Docker default)
    // 100.64.0.0/10 (Calico default)
    if (octet1 == 10) return true;  // 10.0.0.0/8
    if (octet1 == 172 && octet2 >= 16 && octet2 <= 31)
        return true;  // 172.16.0.0/12
    if (octet1 == 100 && octet2 >= 64 && octet2 <= 127)
        return true;  // 100.64.0.0/10

    return false;
}

enum class GidNetworkState {
    GID_NOT_FOUND,             // No GID found
    GID_WITH_NETWORK,          // RoCEv2/IB with network device, not overlay
    GID_WITH_NETWORK_OVERLAY,  // RoCEv2/IB with network device, but overlay
    GID_WITHOUT_NETWORK,       // RoCEv2/IB without network device
    GID_FALLBACK_NONZERO       // First non-zero GID (any type)
};

static inline const char* gidTypeToString(uint32_t gid_type) {
    switch (gid_type) {
        case IBV_GID_TYPE_IB:
            return "IB";
        case IBV_GID_TYPE_ROCE_V1:
            return "RoCE v1";
        case IBV_GID_TYPE_ROCE_V2:
            return "RoCE v2";
        default:
            return "Unknown";
    }
}

static inline std::string gidBytesToString(const uint8_t* raw) {
    char buf[16] = {0};
    std::string gid_str;
    const static size_t kGidLength = 16;
    for (size_t i = 0; i < kGidLength; ++i) {
        sprintf(buf, "%02x", raw[i]);
        gid_str += i == 0 ? buf : std::string(":") + buf;
    }
    return gid_str;
}

// Finds the best GID index for RDMA communication.
// Priority:
// 1) RoCEv2/IB + network device + non-overlay
// 2) RoCEv2/IB + network device (overlay)
// 3) RoCEv2/IB + no network device
// 4) first non-zero GID (any type)
static inline GidNetworkState getBestGidIndex(const std::string& device_name,
                                              struct ibv_context* context,
                                              ibv_port_attr& port_attr,
                                              uint8_t port, int& gid_index) {
    gid_index = -1;
    int i;
    struct ibv_gid_entry gid_entry;
    int overlay_index = -1;        // priority-2 candidate
    int no_net_index = -1;         // priority-3 candidate
    int first_nonzero_index = -1;  // priority-4 candidate
    GidNetworkState state = GidNetworkState::GID_NOT_FOUND;

    VLOG(1) << "Scanning " << port_attr.gid_tbl_len << " GID entries for "
            << device_name << " port " << (int)port;

    for (i = 0; i < port_attr.gid_tbl_len; i++) {
        if (ibv_query_gid_ex(context, port, i, &gid_entry, 0)) {
            continue;  // Skip invalid GID entries
        }

        if (first_nonzero_index < 0 && !isNullGid(&gid_entry.gid)) {
            first_nonzero_index = i;
        }

        bool is_ipv4_mapped =
            ipv6_addr_v4mapped((struct in6_addr*)gid_entry.gid.raw);
        bool is_roce_v2 = gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2;
        bool is_ib = gid_entry.gid_type == IBV_GID_TYPE_IB;

        if (!((is_ipv4_mapped && is_roce_v2) || is_ib)) {
            continue;  // Skip non-RoCEv2/IB GIDs
        }

        std::string ndev = readGidNdev(device_name, port, i);
        bool has_ndev = !ndev.empty();
        bool is_overlay_dev = isOverlayNetwork(ndev);
        bool is_overlay_ip = is_ipv4_mapped &&
                             isOverlayIPv4((struct in6_addr*)gid_entry.gid.raw);
        bool is_overlay = has_ndev && (is_overlay_dev || is_overlay_ip);

        VLOG(1) << "GID[" << i << "]: " << gidBytesToString(gid_entry.gid.raw)
                << " ndev=" << (has_ndev ? ndev : "<none>")
                << (is_overlay_dev || is_overlay_ip ? " (overlay)" : "");

        if (has_ndev && !is_overlay) {
            gid_index = i;
            state = GidNetworkState::GID_WITH_NETWORK;
            LOG(INFO) << "Selected GID[" << i << "] on " << device_name
                      << " with network device: " << ndev;
            return state;
        }

        if (has_ndev) {
            if (overlay_index < 0) {
                overlay_index = i;
            }
        } else if (no_net_index < 0) {
            no_net_index = i;
        }
    }

    // Apply priority order after scan: overlay -> no-net -> first non-zero.
    if (overlay_index >= 0) {
        gid_index = overlay_index;
        state = GidNetworkState::GID_WITH_NETWORK_OVERLAY;
        LOG(WARNING) << "No non-overlay network GID found on " << device_name
                     << ", using overlay GID[" << overlay_index << "]";
    } else if (no_net_index >= 0) {
        gid_index = no_net_index;
        state = GidNetworkState::GID_WITHOUT_NETWORK;
        LOG(WARNING) << "No network-attached GID found on " << device_name
                     << ", using non-network GID[" << no_net_index << "]";
    } else if (first_nonzero_index >= 0) {
        gid_index = first_nonzero_index;
        state = GidNetworkState::GID_FALLBACK_NONZERO;
        LOG(WARNING) << "No RoCEv2/IB candidate found on " << device_name
                     << ", using first non-zero GID[" << first_nonzero_index
                     << "]";
    }
    return state;
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
        case RdmaContext::DEVICE_PAUSED:
            return "DEVICE_PAUSED";
    }
    return "UNKNOWN";
}

RdmaContext::RdmaContext(RdmaTransport& transport)
    : transport_(transport),
      status_(DEVICE_UNINIT),
      verbs_(IbvLoader::Instance().sym()) {
    static std::once_flag g_once_flag;
    auto fork_init = [&]() {
        int ret = verbs_.ibv_fork_init();
        if (ret) PLOG(FATAL) << "ibv_fork_init";
    };
    std::call_once(g_once_flag, fork_init);
}

RdmaContext::~RdmaContext() {
    if (status_ != DEVICE_UNINIT) disable();
}

int RdmaContext::construct(const std::string& device_name,
                           std::shared_ptr<RdmaParams> params) {
    if (status_ != DEVICE_UNINIT) {
        LOG(WARNING) << "RDMA context " << name() << " has been constructed";
        return 0;
    }
    device_name_ = device_name;
    params_ = params;
    endpoint_store_ = std::make_shared<SIEVEEndpointStore>(
        *this, params_->endpoint.endpoint_store_cap);
    status_ = DEVICE_DISABLED;
    return enable();
}

int RdmaContext::enable() {
    if (status_ != DEVICE_DISABLED) {
        LOG(WARNING) << "RDMA context " << name() << " has been enabled";
        return 0;
    }
    if (openDevice(device_name_, params_->device.port)) {
        LOG(ERROR) << "Failed to open device [" << device_name_ << "] on port ["
                   << params_->device.port << "] with GID index ["
                   << params_->device.gid_index << "]";
        disable();
        return -1;
    }

    native_pd_ = verbs_.ibv_alloc_pd(native_context_);
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
        comp_channel_[i] = verbs_.ibv_create_comp_channel(native_context_);
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

    // Create dedicated notification CQ
    notify_cq_ = new RdmaCQ();
    int notify_ret = notify_cq_->construct(this, params_->device.max_cqe,
                                           params_->device.num_cq_list);
    if (notify_ret) {
        LOG(ERROR) << "Failed to create notification CQ for " << device_name_;
        disable();
        return notify_ret;
    }

    ibv_port_attr port_attr;
    int ret = verbs_.ibv_query_port_default(native_context_,
                                            params_->device.port, &port_attr);
    if (ret) {
        PLOG(ERROR) << "Failed to query port " << params_->device.port << " on "
                    << device_name_;
        if (verbs_.ibv_close_device(native_context_)) {
            PLOG(ERROR) << "ibv_close_device";
        }
        return -1;
    }

    if (port_attr.state != IBV_PORT_ACTIVE) {
        status_ = DEVICE_PAUSED;
    } else {
        status_ = DEVICE_ENABLED;
    }

    if (params_->verbose) {
        LOG(INFO) << "Context " << device_name_ << " is enabled: "
                  << "LID " << lid_ << ", GID [" << gid_index_ << "] " << gid();
    }
    return 0;
}

int RdmaContext::disable() {
    if (status_ == DEVICE_UNINIT || status_ == DEVICE_DISABLED) {
        LOG(WARNING) << "RDMA context " << name() << " has been deconstructed";
        return 0;
    }
    endpoint_store_->clear();

    for (auto& entry : mr_set_) {
        int ret = verbs_.ibv_dereg_mr(entry);
        if (ret) PLOG(ERROR) << "ibv_dereg_mr";
    }
    mr_set_.clear();
    for (auto& entry : cq_list_) {
        delete entry;
    }
    cq_list_.clear();

    // Destroy notification CQ
    if (notify_cq_) {
        delete notify_cq_;
        notify_cq_ = nullptr;
    }

    if (event_fd_ >= 0) {
        if (close(event_fd_)) PLOG(ERROR) << "close";
        event_fd_ = -1;
    }

    for (auto& item : comp_channel_)
        if (item && verbs_.ibv_destroy_comp_channel(item))
            PLOG(ERROR) << "ibv_destroy_comp_channel";
    comp_channel_.clear();
    num_comp_channel_ = 0;

    if (native_pd_) {
        if (verbs_.ibv_dealloc_pd(native_pd_)) PLOG(ERROR) << "ibv_dealloc_pd";
        native_pd_ = nullptr;
    }

    if (native_context_) {
        if (verbs_.ibv_close_device(native_context_))
            PLOG(ERROR) << "ibv_close_device";
        native_context_ = nullptr;
    }

    status_ = DEVICE_DISABLED;
    return 0;
}

int RdmaContext::pause() {
    DeviceStatus expected = DEVICE_ENABLED;
    status_.compare_exchange_strong(expected, DEVICE_PAUSED);
    return (expected == DEVICE_PAUSED) ? 0 : -1;
}

int RdmaContext::resume() {
    DeviceStatus expected = DEVICE_PAUSED;
    status_.compare_exchange_strong(expected, DEVICE_ENABLED);
    return (expected == DEVICE_ENABLED) ? 0 : -1;
}

RdmaContext::MemReg RdmaContext::registerMemReg(void* addr, size_t length,
                                                int access) {
    if (status_ == DEVICE_DISABLED || status_ == DEVICE_UNINIT) {
        LOG(FATAL) << "RDMA context " << name() << " not constructed";
        return nullptr;
    }
    ibv_mr* entry = verbs_.ibv_reg_mr_default(native_pd_, addr, length, access);
    if (!entry) {
        PLOG(ERROR) << "Failed to register memory from " << addr << " to "
                    << (char*)addr + length << " in RDMA device "
                    << device_name_;
        return nullptr;
    }
    mr_set_mutex_.lock();
    mr_set_.insert(entry);
    mr_set_mutex_.unlock();
    return entry;
}

int RdmaContext::warmupMrRegistration(void* addr, size_t length) {
    if (status_ == DEVICE_DISABLED || status_ == DEVICE_UNINIT) {
        LOG(FATAL) << "RDMA context " << name() << " not constructed";
        return -1;
    }
    ibv_mr* entry = verbs_.ibv_reg_mr_default(native_pd_, addr, length,
                                              IBV_ACCESS_LOCAL_WRITE);
    if (!entry) {
        return -1;
    }
    if (verbs_.ibv_dereg_mr(entry)) {
        return -1;
    }
    return 0;
}

int RdmaContext::unregisterMemReg(MemReg id) {
    if (status_ == DEVICE_DISABLED || status_ == DEVICE_UNINIT) {
        LOG(FATAL) << "RDMA context " << name() << " not constructed";
        return -1;
    }
    auto entry = (ibv_mr*)id;
    mr_set_mutex_.lock();
    mr_set_.erase(entry);
    mr_set_mutex_.unlock();

    if (verbs_.ibv_dereg_mr(entry)) {
        LOG(ERROR) << "Failed to unregister memory from " << entry->addr
                   << " to " << (char*)entry->addr + entry->length
                   << " in RDMA device " << device_name_;
    }

    return 0;
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

RdmaCQ* RdmaContext::cq(int index) {
    if (index < 0 || index >= params_->device.num_cq_list) return nullptr;
    return cq_list_.empty() ? nullptr : cq_list_[index];
}

int RdmaContext::openDevice(const std::string& device_name, uint8_t port) {
    int num_devices = 0;
    struct ibv_device** devices = verbs_.ibv_get_device_list(&num_devices);
    if (!devices) {
        PLOG(ERROR) << "ibv_get_device_list";
        return -1;
    }
    if (num_devices <= 0) {
        LOG(WARNING) << "No RDMA devices found, check your device installation";
        verbs_.ibv_free_device_list(devices);
        return -1;
    }

    struct ibv_device* matched_device = nullptr;
    for (int i = 0; i < num_devices; ++i) {
        if (device_name == verbs_.ibv_get_device_name(devices[i])) {
            matched_device = devices[i];
            break;
        }
    }
    if (!matched_device) {
        verbs_.ibv_free_device_list(devices);
        LOG(ERROR) << "No matched device found in this server: " << device_name;
        return -1;
    }

    IbvContextPtr context(verbs_.ibv_open_device(matched_device),
                          IbvContextDeleter{&verbs_});
    verbs_.ibv_free_device_list(devices);
    if (!context) {
        PLOG(ERROR) << "ibv_open_device";
        return -1;
    }

    ibv_device_attr device_attr;
    int ret = verbs_.ibv_query_device(context.get(), &device_attr);
    if (ret) {
        PLOG(WARNING) << "ibv_query_device";
        return -1;
    }

    if (port == 0 || port > device_attr.phys_port_cnt) {
        LOG(WARNING) << "Device " << device_name << " port "
                     << static_cast<int>(port) << " is out of range (1-"
                     << static_cast<int>(device_attr.phys_port_cnt) << ")";
        return -1;
    }

    ibv_port_attr port_attr;
    ret = verbs_.ibv_query_port_default(context.get(), port, &port_attr);
    if (ret) {
        PLOG(ERROR) << "Failed to query port " << static_cast<int>(port)
                    << " on " << device_name;
        return -1;
    }

    MIN(params_->device.max_cqe, device_attr.max_cqe);
    MIN(params_->device.num_cq_list, device_attr.max_cq);
    MIN(params_->endpoint.path_mtu, port_attr.active_mtu);
    MIN(params_->endpoint.max_sge, device_attr.max_sge);
    MIN(params_->endpoint.max_qp_wr, device_attr.max_qp_wr);

    if (params_->verbose) {
        for (int i = 0; i < port_attr.gid_tbl_len; i++) {
            struct ibv_gid_entry entry;
            if (ibv_query_gid_ex(context.get(), port, i, &entry, 0)) {
                PLOG(WARNING)
                    << "Scan: Unable to query GID " << i << " on device "
                    << device_name << " port " << port;
                continue;
            }
            LOG(INFO) << "RDMA device " << device_name << " port " << (int)port
                      << " GID[" << i
                      << "]: " << gidBytesToString(entry.gid.raw)
                      << ", type=" << gidTypeToString(entry.gid_type);
        }
    }

    const int gid_tbl_len = static_cast<int>(port_attr.gid_tbl_len);
    gid_index_ = params_->device.gid_index;

    if (gid_index_ < 0) {
        // Auto-select GID
        int found_gid_index = -1;
        GidNetworkState gid_state = getBestGidIndex(
            device_name, context.get(), port_attr, port, found_gid_index);
        if (gid_state == GidNetworkState::GID_NOT_FOUND) {
            LOG(ERROR) << "No valid GID found for device " << device_name
                       << " port " << static_cast<int>(port);
            return -1;
        }
        gid_index_ = found_gid_index;
        if (gid_index_ < 0 || gid_index_ >= gid_tbl_len) {
            LOG(ERROR) << "Auto-selected GID index " << gid_index_
                       << " out of range [0, " << gid_tbl_len << ") for "
                       << device_name;
            return -1;
        }
    } else {
        // User-specified GID
        if (gid_index_ >= gid_tbl_len) {
            LOG(ERROR) << "Configured GID index " << gid_index_
                       << " out of range [0, " << gid_tbl_len << ") for "
                       << device_name;
            return -1;
        }
        // Validate user-specified GID
        std::string ndev = readGidNdev(device_name, port, gid_index_);
        struct ibv_gid_entry user_gid_entry;
        bool has_issues = false;
        std::string issues;

        if (ndev.empty()) {
            issues += "no network device; ";
            has_issues = true;
        } else if (isOverlayNetwork(ndev)) {
            issues += "overlay device (" + ndev + "); ";
            has_issues = true;
        }

        if (ibv_query_gid_ex(context.get(), port, gid_index_, &user_gid_entry,
                             0) == 0) {
            bool is_ipv4 =
                ipv6_addr_v4mapped((struct in6_addr*)user_gid_entry.gid.raw);
            bool is_roce_v2 = user_gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2;
            bool is_ib = user_gid_entry.gid_type == IBV_GID_TYPE_IB;
            if (!((is_ipv4 && is_roce_v2) || is_ib)) {
                issues += std::string("non-optimal type (") +
                          gidTypeToString(user_gid_entry.gid_type) + "); ";
                has_issues = true;
            }
            if (is_ipv4 &&
                isOverlayIPv4((struct in6_addr*)user_gid_entry.gid.raw)) {
                issues += "overlay IP range; ";
                has_issues = true;
            }
        }

        if (has_issues) {
            LOG(WARNING) << "User-specified GID[" << gid_index_ << "] on "
                         << device_name << ": " << issues;
        } else {
            LOG(INFO) << "Using configured GID[" << gid_index_ << "] on "
                      << device_name << " (ndev=" << ndev << ")";
        }
    }

    ret = verbs_.ibv_query_gid(context.get(), port, gid_index_, &gid_);
    if (ret) {
        PLOG(ERROR) << "Unable to query GID " << gid_index_ << " on device "
                    << device_name << " port " << static_cast<int>(port);
        return -1;
    }

    if (isNullGid(&gid_)) {
        PLOG(ERROR) << "Uninitialized GID " << gid_index_ << " on "
                    << device_name << "/" << static_cast<int>(port);
        return -1;
    }

    if (!params_->verbose) {
        LOG(INFO) << "Resolved GID index " << gid_index_ << " for device "
                  << device_name << ": " << gidBytesToString(gid_.raw);
    }

    native_context_ = context.release();
    lid_ = port_attr.lid;
    return 0;
}
}  // namespace tent
}  // namespace mooncake
