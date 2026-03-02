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

#include "transport/rdma_transport/rdma_context.h"

#include <fcntl.h>
#include <sys/epoll.h>

#include <atomic>
#include <cassert>
#include <fstream>
#include <memory>
#include <thread>

#include "config.h"
#include "cuda_alike.h"
#include "transport/rdma_transport/endpoint_store.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"
#include "transport/transport.h"

namespace mooncake {
static int isNullGid(union ibv_gid *gid) {
    for (int i = 0; i < 16; ++i) {
        if (gid->raw[i] != 0) return 0;
    }
    return 1;
}

RdmaContext::RdmaContext(RdmaTransport &engine, const std::string &device_name)
    : device_name_(device_name),
      engine_(engine),
      next_comp_channel_index_(0),
      next_comp_vector_index_(0),
      next_cq_list_index_(0),
      worker_pool_(nullptr),
      active_(true) {
    static std::once_flag g_once_flag;
    auto fork_init = []() {
        int ret = ibv_fork_init();
        if (ret) PLOG(ERROR) << "RDMA context setup failed: fork compatibility";
    };
    std::call_once(g_once_flag, fork_init);
}

RdmaContext::~RdmaContext() {
    if (context_) deconstruct();
}

int RdmaContext::construct(size_t num_cq_list, size_t num_comp_channels,
                           uint8_t port, int gid_index, size_t max_cqe,
                           int max_endpoints) {
    // Create endpoint store based on configuration
    auto &config = globalConfig();
    switch (config.endpoint_store_type) {
        case EndpointStoreType::FIFO:
            endpoint_store_ =
                std::make_shared<FIFOEndpointStore>(max_endpoints);
            LOG(INFO) << "Using FIFO endpoint store";
            break;
        case EndpointStoreType::SIEVE:
        default:
            endpoint_store_ =
                std::make_shared<SIEVEEndpointStore>(max_endpoints);
            LOG(INFO) << "Using SIEVE endpoint store";
            break;
    }

    // Set deletion callback to notify peer when endpoint is reclaimed
    endpoint_store_->setOnDeleteEndpointCallback(
        [this](const std::string &peer_nic_path, uint64_t endpoint_id) {
            notifyPeerEndpointDeletion(peer_nic_path, endpoint_id);
        });

    if (openRdmaDevice(device_name_, port, gid_index)) {
        LOG(ERROR) << "Failed to open device " << device_name_ << " on port "
                   << port << " with GID " << gid_index;
        return ERR_CONTEXT;
    }

    pd_ = ibv_alloc_pd(context_);
    if (!pd_) {
        PLOG(ERROR) << "Failed to allocate new protection domain on device "
                    << device_name_;
        return ERR_CONTEXT;
    }

    num_comp_channel_ = num_comp_channels;
    comp_channel_ = new ibv_comp_channel *[num_comp_channels];
    for (size_t i = 0; i < num_comp_channels; ++i) {
        comp_channel_[i] = ibv_create_comp_channel(context_);
        if (!comp_channel_[i]) {
            PLOG(ERROR) << "Failed to create completion channel on device "
                        << device_name_;
            return ERR_CONTEXT;
        }
    }

    event_fd_ = epoll_create1(0);
    if (event_fd_ < 0) {
        PLOG(ERROR) << "Failed to create epoll";
        return ERR_CONTEXT;
    }

    if (joinNonblockingPollList(event_fd_, context_->async_fd)) {
        LOG(ERROR) << "Failed to register context async fd to epoll";
        close(event_fd_);
        return ERR_CONTEXT;
    }

    for (size_t i = 0; i < num_comp_channel_; ++i)
        if (joinNonblockingPollList(event_fd_, comp_channel_[i]->fd)) {
            LOG(ERROR) << "Failed to register completion channel " << i
                       << " to epoll";
            close(event_fd_);
            return ERR_CONTEXT;
        }

    cq_list_.resize(num_cq_list);
    for (size_t i = 0; i < num_cq_list; ++i) {
        auto cq =
            ibv_create_cq(context_, max_cqe,
                          (void *)&cq_list_[i].outstanding /* CQ context */,
                          compChannel(), compVector());
        if (!cq) {
            PLOG(ERROR) << "Failed to create completion queue";
            close(event_fd_);
            return ERR_CONTEXT;
        }
        cq_list_[i].native = cq;
    }

    worker_pool_ = std::make_shared<WorkerPool>(*this, socketId());

    LOG(INFO) << "RDMA device: " << context_->device->name << ", LID: " << lid_
              << ", GID: (GID_Index " << gid_index_ << ") " << gid();

    return 0;
}

int RdmaContext::socketId() {
    std::string path =
        "/sys/class/infiniband/" + device_name_ + "/device/numa_node";
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

int RdmaContext::deconstruct() {
    worker_pool_.reset();

    endpoint_store_->destroyQPs();

    for (auto &entry : memory_region_list_) {
        int ret = ibv_dereg_mr(entry.mr);
        if (ret) {
            PLOG(ERROR) << "Failed to unregister memory region";
        }
    }
    memory_region_list_.clear();

    for (size_t i = 0; i < cq_list_.size(); ++i) {
        if (!cq_list_[i].native) continue;

        int ret = ibv_destroy_cq(cq_list_[i].native);
        if (ret) {
            PLOG(ERROR) << "Failed to destroy completion queue";
        }
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

    return 0;
}

int RdmaContext::registerMemoryRegionInternal(void *addr, size_t length,
                                              int access,
                                              MemoryRegionMeta &mrMeta) {
    if (length > (size_t)globalConfig().max_mr_size) {
        PLOG(WARNING) << "The buffer length exceeds device max_mr_size, "
                      << "shrink it to " << globalConfig().max_mr_size;
        length = (size_t)globalConfig().max_mr_size;
    }
#if !defined(WITH_NVIDIA_PEERMEM) && defined(USE_CUDA)
    // Implement register memory in a way that does not assume the presence of
    // nvidia-peermem. If memory is on CPU call ibv_reg_mr() as usual. If memory
    // is on GPU then use ibv_reg_dmabuf_mr() instead which does not require
    // nvidia-peermem.
    CUmemorytype memType;
    CUresult result = cuPointerGetAttribute(
        &memType, CU_POINTER_ATTRIBUTE_MEMORY_TYPE, (CUdeviceptr)addr);

    // Register memory depending on whether memory is on host or GPU.
    if (result != CUDA_SUCCESS || memType == CU_MEMORYTYPE_HOST) {
        mrMeta.addr = addr;
        mrMeta.mr = ibv_reg_mr(pd_, addr, length, access);
    } else if (memType == CU_MEMORYTYPE_DEVICE) {
        size_t allocSize;
        result = cuPointerGetAttribute(
            &allocSize, CU_POINTER_ATTRIBUTE_RANGE_SIZE, (CUdeviceptr)addr);
        if (result != CUDA_SUCCESS) {
            const char *errStr;
            cuGetErrorString(result, &errStr);
            LOG(ERROR) << "Failed to call cuPointerGetAttribute for "
                       << (uintptr_t)addr << " cuda error=" << errStr;
            return ERR_CONTEXT;
        }

        int dmabuf_fd;
        result = cuMemGetHandleForAddressRange(
            &dmabuf_fd, (CUdeviceptr)addr, allocSize,
            CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0);
        if (result != CUDA_SUCCESS) {
            const char *errStr;
            cuGetErrorString(result, &errStr);
            LOG(ERROR) << "Failed to retrieve dmabuf for " << (uintptr_t)addr
                       << " cuda error=" << errStr;
            return ERR_CONTEXT;
        }
        mrMeta.addr = addr;
        mrMeta.mr = ibv_reg_dmabuf_mr(pd_, 0 /* offset */, length,
                                      (uintptr_t)addr, dmabuf_fd, access);
    }
#else
    mrMeta.addr = addr;
    mrMeta.mr = ibv_reg_mr(pd_, addr, length, access);
#endif
    if (!mrMeta.mr) {
        PLOG(ERROR) << "Failed to register memory " << addr;
        return ERR_CONTEXT;
    }
    return 0;
}

int RdmaContext::registerMemoryRegion(void *addr, size_t length, int access) {
    MemoryRegionMeta mrMeta;
    int ret = registerMemoryRegionInternal(addr, length, access, mrMeta);
    if (ret != 0) {
        return ret;
    }
    RWSpinlock::WriteGuard guard(memory_regions_lock_);
    memory_region_list_.push_back(mrMeta);
    return 0;
}

int RdmaContext::unregisterMemoryRegion(void *addr) {
    RWSpinlock::WriteGuard guard(memory_regions_lock_);
    bool has_removed;
    do {
        has_removed = false;
        for (auto iter = memory_region_list_.begin();
             iter != memory_region_list_.end(); ++iter) {
            if (iter->addr <= addr &&
                addr < (char *)(iter->addr) + iter->mr->length) {
                if (ibv_dereg_mr(iter->mr)) {
                    LOG(ERROR) << "Failed to unregister memory " << addr;
                    return ERR_CONTEXT;
                }
                memory_region_list_.erase(iter);
                has_removed = true;
                break;
            }
        }
    } while (has_removed);
    return 0;
}

int RdmaContext::preTouchMemory(void *addr, size_t length) {
    MemoryRegionMeta mrMeta;
    int ret = registerMemoryRegionInternal(addr, length, IBV_ACCESS_LOCAL_WRITE,
                                           mrMeta);
    if (ret != 0) {
        return ret;
    }
    return ibv_dereg_mr(mrMeta.mr);
}

uint32_t RdmaContext::rkey(void *addr) {
    RWSpinlock::ReadGuard guard(memory_regions_lock_);
    for (auto iter = memory_region_list_.begin();
         iter != memory_region_list_.end(); ++iter)
        if (iter->addr <= addr &&
            addr < (char *)(iter->addr) + iter->mr->length)
            return iter->mr->rkey;

    LOG(ERROR) << "Address " << addr << " rkey not found for " << deviceName();
    return 0;
}

uint32_t RdmaContext::lkey(void *addr) {
    RWSpinlock::ReadGuard guard(memory_regions_lock_);
    for (auto iter = memory_region_list_.begin();
         iter != memory_region_list_.end(); ++iter)
        if (iter->addr <= addr &&
            addr < (char *)(iter->addr) + iter->mr->length)
            return iter->mr->lkey;

    LOG(ERROR) << "Address " << addr << " lkey not found for " << deviceName();
    return 0;
}

std::shared_ptr<RdmaEndPoint> RdmaContext::endpoint(
    const std::string &peer_nic_path) {
    if (!active_) {
        LOG(ERROR) << "Context is not active: " << deviceName();
        return nullptr;
    }

    if (peer_nic_path.empty()) {
        LOG(ERROR) << "Invalid peer NIC path: " << deviceName();
        return nullptr;
    }

    auto endpoint = endpoint_store_->getEndpoint(peer_nic_path);
    if (endpoint) {
        return endpoint;
    }

    endpoint = endpoint_store_->insertEndpoint(peer_nic_path, this);
    endpoint_store_->reclaimEndpoint();
    return endpoint;
}

int RdmaContext::disconnectAllEndpoints() {
    return endpoint_store_->disconnectQPs();
}

int RdmaContext::deleteEndpoint(const std::string &peer_nic_path) {
    return endpoint_store_->deleteEndpoint(peer_nic_path);
}

int RdmaContext::deleteEndpoint(const std::string &peer_nic_path,
                                uint64_t peer_endpoint_id) {
    return endpoint_store_->deleteEndpoint(peer_nic_path, peer_endpoint_id);
}

void RdmaContext::notifyPeerEndpointDeletion(const std::string &peer_nic_path,
                                             uint64_t endpoint_id) {
    auto peer_server_name = getServerNameFromNicPath(peer_nic_path);
    if (peer_server_name.empty()) {
        LOG(WARNING) << "Failed to parse peer server name from: "
                     << peer_nic_path;
        return;
    }

    TransferMetadata::DeleteEndpointDesc local_desc;
    local_desc.deleted_nic_path = nicPath();
    local_desc.target_nic_path = peer_nic_path;
    local_desc.endpoint_id = endpoint_id;

    engine_.sendDeleteEndpoint(peer_server_name, local_desc);
}

size_t RdmaContext::getTotalQPNumber() const {
    return endpoint_store_->getTotalQPNumber();
}

std::string RdmaContext::nicPath() const {
    return MakeNicPath(engine_.local_server_name_, device_name_);
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

ibv_cq *RdmaContext::cq() {
    int index = (next_cq_list_index_++) % cq_list_.size();
    return cq_list_[index].native;
}

ibv_comp_channel *RdmaContext::compChannel() {
    int index = (next_comp_channel_index_++) % num_comp_channel_;
    return comp_channel_[index];
}

int RdmaContext::compVector() {
    return (next_comp_vector_index_++) % context_->num_comp_vectors;
}

static inline int ipv6_addr_v4mapped(const struct in6_addr *a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) |
            (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL ||
           /* IPv4 encoded multicast addresses */
           (a->s6_addr32[0] == htonl(0xff0e0000) &&
            ((a->s6_addr32[1] | (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL));
}

// Reads the associated network device name for the given GID index from sysfs.
static std::string readGidNdev(const std::string &device_name, uint8_t port,
                               int gid_index) {
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

// Returns 1 if the GID has an associated network device, 0 otherwise.
static int hasNetworkDevice(const std::string &device_name, uint8_t port,
                            int gid_index) {
    return !readGidNdev(device_name, port, gid_index).empty() ? 1 : 0;
}

static const char *GidNetworkStateToString(GidNetworkState state) {
    return (state == GidNetworkState::GID_WITH_NETWORK)
               ? "with network device"
               : "without network device";
}

GidNetworkState RdmaContext::findBestGidIndex(const std::string &device_name,
                                              struct ibv_context *context,
                                              ibv_port_attr &port_attr,
                                              uint8_t port, int &gid_index) {
    gid_index = -1;
    int i;
    struct ibv_gid_entry gid_entry;
    bool fallback_found = false;
    GidNetworkState state = GidNetworkState::GID_NOT_FOUND;

    for (i = 0; i < port_attr.gid_tbl_len; i++) {
        if (ibv_query_gid_ex(context, port, i, &gid_entry, 0)) {
            PLOG(ERROR) << "Failed to query GID " << i << " on " << device_name
                        << "/" << port;
            continue;  // if gid is invalid ibv_query_gid_ex() will return !0
        }

        if ((ipv6_addr_v4mapped((struct in6_addr *)gid_entry.gid.raw) &&
             gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2) ||
            gid_entry.gid_type == IBV_GID_TYPE_IB) {
            // Check if this GID has an associated network device
            if (hasNetworkDevice(device_name, port, i)) {
                // Found a GID with network device, this is the best choice
                gid_index = i;
                state = GidNetworkState::GID_WITH_NETWORK;
                break;
            }
            // No network device, keep the first one as fallback candidate
            if (!fallback_found) {
                gid_index = i;
                fallback_found = true;
                state = GidNetworkState::GID_WITHOUT_NETWORK;
            }
        }
    }
    return state;
}

int RdmaContext::openRdmaDevice(const std::string &device_name, uint8_t port,
                                int gid_index) {
    int num_devices = 0;
    struct ibv_context *context = nullptr;
    struct ibv_device **devices = ibv_get_device_list(&num_devices);
    if (!devices) {
        LOG(ERROR) << "ibv_get_device_list failed";
        return ERR_DEVICE_NOT_FOUND;
    }
    if (devices && num_devices <= 0) {
        LOG(ERROR) << "ibv_get_device_list failed";
        ibv_free_device_list(devices);
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

        ibv_port_attr attr;
        int ret = ibv_query_port(context, port, &attr);
        if (ret) {
            PLOG(ERROR) << "Failed to query port " << port << " on "
                        << device_name;
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        if (attr.state != IBV_PORT_ACTIVE) {
            LOG(WARNING) << "Device " << device_name << " port not active";
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        ibv_device_attr device_attr;
        ret = ibv_query_device(context, &device_attr);
        if (ret) {
            PLOG(WARNING) << "Failed to query attributes on " << device_name;
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

#if !defined(WITH_NVIDIA_PEERMEM) && defined(USE_CUDA)
        // Verify dmabuf support which is required if not using nvidia-peermem.
        // Assume device index matches.
        CUdevice cuDevice;
        CUresult result = cuDeviceGet(&cuDevice, i);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR) << "Failed to query CUDA device";
            return ERR_CONTEXT;
        }
        int dmaBufSupported;
        result = cuDeviceGetAttribute(
            &dmaBufSupported, CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED, cuDevice);
        if (result != CUDA_SUCCESS) {
            LOG(ERROR) << "Failed to query CUDA device attributes";
            return ERR_CONTEXT;
        }
        if (!dmaBufSupported) {
            LOG(ERROR) << "DMA BUF supported required for GPU RDMA without "
                          "nvidia-peermem";
            return ERR_CONTEXT;
        }
#endif

        ibv_port_attr port_attr;
        ret = ibv_query_port(context, port, &port_attr);
        if (ret) {
            PLOG(WARNING) << "Failed to query port attributes on "
                          << device_name << "/" << port;
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        updateGlobalConfig(device_attr);
        GidNetworkState gid_state;
        if (gid_index < 0) {
            int found_gid_index = -1;
            gid_state = findBestGidIndex(device_name, context, port_attr, port,
                                         found_gid_index);
            if (gid_state != GidNetworkState::GID_NOT_FOUND) {
                LOG(INFO) << "Find best gid index: " << found_gid_index
                          << " on " << device_name << "/" << port
                          << " (network state: "
                          << GidNetworkStateToString(gid_state) << ")";
                gid_index = found_gid_index;
            } else {
                LOG(WARNING) << "No suitable GID found on " << device_name
                             << "/" << port;
                goto cleanup_context_and_devices;
            }
        } else {
            // Also check network state for user-specified GID
            if (!hasNetworkDevice(device_name, port, gid_index)) {
                LOG(WARNING) << "User-specified GID index " << gid_index
                             << " on " << device_name << "/" << port
                             << " has no associated network device, "
                             << "may not be optimal for RDMA operations";
                goto cleanup_context_and_devices;
            }
            LOG(INFO) << "Using user-specified GID index: " << gid_index
                      << " on " << device_name << "/" << port
                      << " (with network device)";
        }

        // Continue with GID validation
        ret = ibv_query_gid(context, port, gid_index, &gid_);
        if (ret) {
            PLOG(ERROR) << "Failed to query GID " << gid_index << " on "
                        << device_name << "/" << port;
            goto cleanup_context_and_devices;
        }

#ifndef CONFIG_SKIP_NULL_GID_CHECK
        if (isNullGid(&gid_)) {
            LOG(WARNING) << "GID is NULL, please check your GID index by "
                            "specifying MC_GID_INDEX";
            goto cleanup_context_and_devices;
        }
#endif  // CONFIG_SKIP_NULL_GID_CHECK

        // All checks passed, assign member variables
        context_ = context;
        port_ = port;
        lid_ = attr.lid;
        active_mtu_ = attr.active_mtu;
        active_speed_ = attr.active_speed;
        gid_index_ = gid_index;

        ibv_free_device_list(devices);
        return 0;

    cleanup_context_and_devices:
        if (ibv_close_device(context)) {
            PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
        }
        ibv_free_device_list(devices);
        return ERR_CONTEXT;
    }

    ibv_free_device_list(devices);
    LOG(ERROR) << "No matched device found: " << device_name;
    return ERR_DEVICE_NOT_FOUND;
}

int RdmaContext::joinNonblockingPollList(int event_fd, int data_fd) {
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

int RdmaContext::poll(int num_entries, ibv_wc *wc, int cq_index) {
    int nr_poll = ibv_poll_cq(cq_list_[cq_index].native, num_entries, wc);
    if (nr_poll < 0) {
        LOG(ERROR) << "Failed to poll CQ " << cq_index << " of device "
                   << device_name_;
        return ERR_CONTEXT;
    }
    return nr_poll;
}

int RdmaContext::submitPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
    return worker_pool_->submitPostSend(slice_list);
}
}  // namespace mooncake
