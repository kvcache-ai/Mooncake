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

#include "transport/efa_transport/efa_context.h"

#include <fcntl.h>
#include <sys/epoll.h>

#include <atomic>
#include <cassert>
#include <fstream>
#include <memory>
#include <thread>
#include <cstring>
#include <iomanip>
#include <sstream>

#include "config.h"
#include "cuda_alike.h"
#include "transport/efa_transport/efa_endpoint.h"
#include "transport/efa_transport/efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

// EfaEndpointStore implementation

EfaEndpointStore::EfaEndpointStore(size_t max_endpoints,
                                   double inactive_timeout_sec)
    : max_endpoints_(max_endpoints),
      inactive_timeout_sec_(inactive_timeout_sec) {}

std::shared_ptr<EfaEndPoint> EfaEndpointStore::get(
    const std::string& peer_nic_path) {
    RWSpinlock::ReadGuard guard(lock_);
    auto it = endpoints_.find(peer_nic_path);
    if (it != endpoints_.end()) {
        it->second->set_active(true);
        return it->second;
    }
    return nullptr;
}

std::shared_ptr<EfaEndPoint> EfaEndpointStore::getOrInsert(
    const std::string& peer_nic_path, std::shared_ptr<EfaEndPoint> new_ep) {
    RWSpinlock::WriteGuard guard(lock_);
    auto it = endpoints_.find(peer_nic_path);
    if (it != endpoints_.end()) {
        it->second->set_active(true);
        return it->second;  // Another thread already created it
    }
    // Evict stale endpoints if at capacity
    if (endpoints_.size() >= max_endpoints_) {
        size_t evicted = evictStaleLocked();
        if (evicted == 0 && endpoints_.size() >= max_endpoints_) {
            LOG(WARNING) << "EfaEndpointStore at capacity (" << max_endpoints_
                         << ") with no stale endpoints to evict";
        }
    }
    endpoints_[peer_nic_path] = new_ep;
    new_ep->set_active(true);
    return new_ep;
}

void EfaEndpointStore::add(const std::string& peer_nic_path,
                           std::shared_ptr<EfaEndPoint> endpoint) {
    RWSpinlock::WriteGuard guard(lock_);
    endpoints_[peer_nic_path] = endpoint;
}

void EfaEndpointStore::remove(const std::string& peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    endpoints_.erase(peer_nic_path);
}

int EfaEndpointStore::disconnectAll() {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto& entry : endpoints_) {
        if (entry.second) {
            entry.second->disconnect();
        }
    }
    return 0;
}

size_t EfaEndpointStore::size() const {
    RWSpinlock::ReadGuard guard(lock_);
    return endpoints_.size();
}

size_t EfaEndpointStore::evictStale() {
    RWSpinlock::WriteGuard guard(lock_);
    return evictStaleLocked();
}

size_t EfaEndpointStore::evictStaleLocked() {
    size_t evicted = 0;
    for (auto it = endpoints_.begin(); it != endpoints_.end();) {
        auto& ep = it->second;
        double age = ep ? ep->lastUsedAge() : 0;
        bool outstanding = ep ? ep->hasOutstandingSlice() : false;
        if (ep && !outstanding && age > inactive_timeout_sec_) {
            LOG(INFO) << "Evicting stale EFA endpoint: " << it->first
                      << " (idle " << age
                      << "s, timeout=" << inactive_timeout_sec_ << "s)";
            ep->disconnect();
            it = endpoints_.erase(it);
            ++evicted;
        } else {
            ++it;
        }
    }
    if (endpoints_.size() > 0) {
        VLOG(1) << "evictStale: " << endpoints_.size() << " endpoints, "
                << evicted << " evicted";
    }
    return evicted;
}

size_t EfaEndpointStore::removeDisconnected() {
    RWSpinlock::WriteGuard guard(lock_);
    size_t removed = 0;
    for (auto it = endpoints_.begin(); it != endpoints_.end();) {
        auto& ep = it->second;
        if (ep && !ep->connected() && !ep->hasOutstandingSlice()) {
            LOG(INFO) << "Removing disconnected EFA endpoint: " << it->first;
            it = endpoints_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    return removed;
}

// EfaContext implementation
EfaContext::EfaContext(EfaTransport& engine, const std::string& device_name)
    : engine_(engine),
      device_name_(device_name),
      fi_info_(nullptr),
      hints_(nullptr),
      fabric_(nullptr),
      domain_(nullptr),
      av_(nullptr),
      active_(true) {}

EfaContext::~EfaContext() {
    if (fabric_) deconstruct();
}

int EfaContext::construct(size_t num_cq_list, size_t num_comp_channels,
                          uint8_t port, int gid_index, size_t max_cqe,
                          int max_endpoints) {
    endpoint_store_ = std::make_shared<EfaEndpointStore>(max_endpoints);

#if !defined(USE_CUDA) && !defined(USE_HIP)
    // When built without GPU support, prevent libfabric's EFA provider from
    // dlopen-ing libcudart/libcuda at fi_getinfo/fi_domain time. That
    // initialization creates a CUDA primary context on GPU 0 and leaks
    // ~616 MiB of device memory even when no GPU memory is ever registered.
    // Only set if the user hasn't explicitly configured FI_HMEM.
    setenv("FI_HMEM", "system", 0);
#endif

    // Setup hints for EFA provider
    hints_ = fi_allocinfo();
    if (!hints_) {
        LOG(ERROR) << "Failed to allocate fi_info hints";
        return ERR_CONTEXT;
    }

    hints_->caps =
        FI_MSG | FI_RMA | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    hints_->mode = FI_CONTEXT;
    hints_->ep_attr->type = FI_EP_RDM;  // EFA uses RDM endpoints
    hints_->fabric_attr->prov_name = strdup("efa");

    // Specify the domain (device) name - append "-rdm" for RDM endpoint
    std::string domain_name = device_name_ + "-rdm";
    hints_->domain_attr->name = strdup(domain_name.c_str());
    hints_->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_VIRT_ADDR |
                                   FI_MR_ALLOCATED | FI_MR_PROV_KEY
#if defined(USE_CUDA) || defined(USE_HIP)
                                   | FI_MR_HMEM
#endif
        ;
    hints_->domain_attr->threading = FI_THREAD_SAFE;

    // Get fabric info
    int ret =
        fi_getinfo(FI_VERSION(1, 14), nullptr, nullptr, 0, hints_, &fi_info_);
    if (ret) {
        LOG(ERROR) << "fi_getinfo failed for device " << device_name_ << ": "
                   << fi_strerror(-ret);
        fi_freeinfo(hints_);
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Open fabric
    ret = fi_fabric(fi_info_->fabric_attr, &fabric_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_fabric failed: " << fi_strerror(-ret);
        fi_freeinfo(fi_info_);
        fi_freeinfo(hints_);
        fi_info_ = nullptr;
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Open domain
    ret = fi_domain(fabric_, fi_info_, &domain_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_domain failed: " << fi_strerror(-ret);
        fi_close(&fabric_->fid);
        fi_freeinfo(fi_info_);
        fi_freeinfo(hints_);
        fabric_ = nullptr;
        fi_info_ = nullptr;
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Create address vector
    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    av_attr.count = max_endpoints;

    ret = fi_av_open(domain_, &av_attr, &av_, nullptr);
    if (ret) {
        LOG(ERROR) << "fi_av_open failed: " << fi_strerror(-ret);
        fi_close(&domain_->fid);
        fi_close(&fabric_->fid);
        fi_freeinfo(fi_info_);
        fi_freeinfo(hints_);
        domain_ = nullptr;
        fabric_ = nullptr;
        fi_info_ = nullptr;
        hints_ = nullptr;
        return ERR_CONTEXT;
    }

    // Create completion queues
    cq_list_.resize(num_cq_list);
    for (size_t i = 0; i < num_cq_list; ++i) {
        auto cq = std::make_shared<EfaCq>();

        struct fi_cq_attr cq_attr = {};
        cq_attr.size = max_cqe;
        cq_attr.format = FI_CQ_FORMAT_DATA;
        cq_attr.wait_obj = FI_WAIT_NONE;

        ret = fi_cq_open(domain_, &cq_attr, &cq->cq, nullptr);
        if (ret) {
            LOG(ERROR) << "fi_cq_open failed: " << fi_strerror(-ret);
            return ERR_CONTEXT;
        }
        cq_list_[i] = cq;
    }

    LOG(INFO) << "EFA device (libfabric): " << device_name_
              << ", domain: " << fi_info_->domain_attr->name
              << ", provider: " << fi_info_->fabric_attr->prov_name;

    return 0;
}

int EfaContext::deconstruct() {
    // Destroy all endpoints before closing domain/fabric/AV.
    // Endpoints hold fi_ep handles that reference the domain, so they must
    // be closed first.
    endpoint_store_.reset();

    {
        RWSpinlock::WriteGuard guard(mr_lock_);
        for (auto& entry : mr_map_) {
            if (entry.second.mr) {
                fi_close(&entry.second.mr->fid);
            }
        }
        mr_map_.clear();
    }

    for (auto& cq : cq_list_) {
        if (cq && cq->cq) {
            fi_close(&cq->cq->fid);
            cq->cq = nullptr;
        }
    }
    cq_list_.clear();

    if (av_) {
        fi_close(&av_->fid);
        av_ = nullptr;
    }

    if (domain_) {
        fi_close(&domain_->fid);
        domain_ = nullptr;
    }

    if (fabric_) {
        fi_close(&fabric_->fid);
        fabric_ = nullptr;
    }

    if (fi_info_) {
        fi_freeinfo(fi_info_);
        fi_info_ = nullptr;
    }

    if (hints_) {
        fi_freeinfo(hints_);
        hints_ = nullptr;
    }

    return 0;
}

int EfaContext::registerMemoryRegionInternal(void* addr, size_t length,
                                             int access,
                                             EfaMemoryRegionMeta& mrMeta) {
    if (length > (size_t)globalConfig().max_mr_size) {
        LOG(ERROR) << "Buffer length " << length
                   << " exceeds device max_mr_size "
                   << globalConfig().max_mr_size
                   << ". Use EfaTransport::registerLocalMemory() which "
                      "auto-splits large buffers.";
        return ERR_CONTEXT;
    }

    mrMeta.addr = addr;
    mrMeta.length = length;

    // For EFA, we need local read/write and remote read/write
    uint64_t fi_access = FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;

    // Detect memory type and use fi_mr_regattr() for GPU memory.
    // The EFA provider's fi_mr_reg() hardcodes iface=FI_HMEM_SYSTEM,
    // so GPU memory must go through fi_mr_regattr() with explicit
    // iface/device fields (per libfabric spec and EFA provider impl).
    enum fi_hmem_iface iface = FI_HMEM_SYSTEM;
    int device_ordinal = 0;

#if defined(USE_CUDA)
    cudaPointerAttributes attributes;
    cudaError_t cuda_ret = cudaPointerGetAttributes(&attributes, addr);
    if (cuda_ret == cudaSuccess && attributes.type == cudaMemoryTypeDevice) {
        iface = FI_HMEM_CUDA;
        device_ordinal = attributes.device;
    }
#elif defined(USE_HIP)
    hipPointerAttribute_t attributes;
    hipError_t hip_ret = hipPointerGetAttributes(&attributes, addr);
    if (hip_ret == hipSuccess && attributes.type == hipMemoryTypeDevice) {
        iface = FI_HMEM_ROCR;
        device_ordinal = attributes.device;
    }
#endif

    int ret;
    if (iface != FI_HMEM_SYSTEM) {
        // GPU memory: use fi_mr_regattr with explicit iface and device
        struct iovec iov = {.iov_base = addr, .iov_len = length};
        struct fi_mr_attr attr = {};
        attr.mr_iov = &iov;
        attr.iov_count = 1;
        attr.access = fi_access;
        attr.iface = iface;
        attr.device.cuda = device_ordinal;

        ret = fi_mr_regattr(domain_, &attr, 0, &mrMeta.mr);
        if (ret) {
            LOG(ERROR) << "fi_mr_regattr failed for GPU memory " << addr
                       << " (device " << device_ordinal
                       << "): " << fi_strerror(-ret);
            return ERR_CONTEXT;
        }
    } else {
        // CPU memory: fi_mr_reg is sufficient
        ret = fi_mr_reg(domain_, addr, length, fi_access, 0, 0, 0, &mrMeta.mr,
                        nullptr);
        if (ret) {
            LOG(ERROR) << "fi_mr_reg failed for " << addr << ": "
                       << fi_strerror(-ret);
            return ERR_CONTEXT;
        }
    }

    mrMeta.key = fi_mr_key(mrMeta.mr);

    return 0;
}

int EfaContext::registerMemoryRegion(void* addr, size_t length, int access) {
    EfaMemoryRegionMeta mrMeta;
    int ret = registerMemoryRegionInternal(addr, length, access, mrMeta);
    if (ret != 0) {
        return ret;
    }
    RWSpinlock::WriteGuard guard(mr_lock_);
    mr_map_[(uint64_t)addr] = mrMeta;
    return 0;
}

int EfaContext::unregisterMemoryRegion(void* addr) {
    RWSpinlock::WriteGuard guard(mr_lock_);
    auto it = mr_map_.find((uint64_t)addr);
    if (it != mr_map_.end()) {
        if (it->second.mr) {
            int ret = fi_close(&it->second.mr->fid);
            if (ret) {
                LOG(ERROR) << "Failed to unregister memory " << addr << ": "
                           << fi_strerror(-ret);
                return ERR_CONTEXT;
            }
        }
        mr_map_.erase(it);
    }
    return 0;
}

int EfaContext::preTouchMemory(void* addr, size_t length) {
    volatile char* ptr = (volatile char*)addr;
    for (size_t i = 0; i < length; i += 4096) {
        ptr[i] = ptr[i];
    }
    return 0;
}

uint64_t EfaContext::rkey(void* addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.upper_bound((uint64_t)addr);
    if (it != mr_map_.begin()) {
        --it;
        if ((uint64_t)addr < it->first + it->second.length && it->second.mr) {
            return it->second.key;
        }
    }
    return 0;
}

uint64_t EfaContext::lkey(void* addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.upper_bound((uint64_t)addr);
    if (it != mr_map_.begin()) {
        --it;
        if ((uint64_t)addr < it->first + it->second.length && it->second.mr) {
            return fi_mr_key(it->second.mr);
        }
    }
    return 0;
}

void* EfaContext::mrDesc(void* addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.upper_bound((uint64_t)addr);
    if (it != mr_map_.begin()) {
        --it;
        if ((uint64_t)addr < it->first + it->second.length && it->second.mr) {
            return fi_mr_desc(it->second.mr);
        }
    }
    return nullptr;
}

std::shared_ptr<EfaEndPoint> EfaContext::endpoint(
    const std::string& peer_nic_path) {
    if (!endpoint_store_) return nullptr;

    // Use normalized key (strip port) so the same physical peer reuses its
    // endpoint across reconnections.  Each P2PHANDSHAKE run picks a random
    // port, producing a different peer_nic_path for the same peer host+NIC.
    std::string key = normalizeNicPath(peer_nic_path);

    // Fast path: endpoint already exists for this physical peer.
    // Update peer_nic_path in case the port changed (new initiator run).
    // setPeerNicPath disconnects old connection (fi_av_remove) if needed.
    auto ep = endpoint_store_->get(key);
    if (ep) {
        ep->setPeerNicPath(peer_nic_path);
        return ep;
    }

    // Slow path: create new endpoint, then atomically insert (or get existing
    // if another thread raced us).  getOrInsert prevents duplicate endpoints
    // and duplicate AV entries for the same peer.
    auto new_endpoint = std::make_shared<EfaEndPoint>(*this);
    auto cq = nextCq();
    if (cq) {
        int ret = new_endpoint->construct(cq->cq, &cq->outstanding, 1, 4,
                                          globalConfig().max_wr, 64);
        if (ret != 0) {
            LOG(ERROR) << "Failed to construct EFA endpoint";
            return nullptr;
        }
    }
    // Still set the full peer_nic_path (with port) for handshake routing
    new_endpoint->setPeerNicPath(peer_nic_path);
    ep = endpoint_store_->getOrInsert(key, new_endpoint);
    // If another thread won the race, new_endpoint is discarded (RAII cleanup)
    return ep;
}

int EfaContext::deleteEndpoint(const std::string& peer_nic_path) {
    if (endpoint_store_) {
        endpoint_store_->remove(peer_nic_path);
    }
    return 0;
}

int EfaContext::disconnectAllEndpoints() {
    if (endpoint_store_) {
        return endpoint_store_->disconnectAll();
    }
    return 0;
}

size_t EfaContext::getTotalQPNumber() const {
    return endpoint_store_ ? endpoint_store_->size() : 0;
}

std::string EfaContext::nicPath() const {
    return engine_.local_server_name() + "@" + device_name_;
}

std::string EfaContext::localAddr() const {
    // Return a hex string representation of the local address info
    if (!fi_info_ || !fi_info_->src_addr) {
        return "";
    }

    std::ostringstream oss;
    const uint8_t* addr = static_cast<const uint8_t*>(fi_info_->src_addr);
    for (size_t i = 0; i < fi_info_->src_addrlen; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)addr[i];
    }
    return oss.str();
}

int EfaContext::submitPostSend(
    const std::vector<Transport::Slice*>& slice_list) {
    // Route slices to appropriate endpoints for sending
    // Group slices by peer NIC path
    std::unordered_map<std::string, std::vector<Transport::Slice*>>
        slices_by_peer;
    std::vector<Transport::Slice*> failed_slices;

    for (auto* slice : slice_list) {
        if (!slice) continue;

        // Fast path: peer info already resolved by submitTransferTask's
        // striping path (dest_rkey and peer_nic_path pre-set on slice).
        // This eliminates per-slice metadata lookup, selectDevice(), and
        // string construction — the main bottleneck for multi-NIC striping.
        if (!slice->peer_nic_path.empty()) {
            slices_by_peer[slice->peer_nic_path].push_back(slice);
            continue;
        }

        // Slow path: resolve peer info per-slice (non-striped transfers)
        auto peer_segment_desc =
            engine_.meta()->getSegmentDescByID(slice->target_id);
        if (!peer_segment_desc) {
            LOG(ERROR) << "Cannot get segment descriptor for target "
                       << slice->target_id;
            slice->markFailed();
            continue;
        }

        // Find the buffer and device for this destination address
        int buffer_id = -1, device_id = -1;
        if (EfaTransport::selectDevice(peer_segment_desc.get(),
                                       slice->rdma.dest_addr, slice->length,
                                       buffer_id, device_id)) {
            LOG(ERROR) << "Cannot select device for dest_addr "
                       << (void*)slice->rdma.dest_addr;
            slice->markFailed();
            continue;
        }

        // Set the remote key from the peer's registered memory region
        slice->rdma.dest_rkey =
            peer_segment_desc->buffers[buffer_id].rkey[device_id];

        // Construct peer NIC path: "server_name@device_name"
        std::string peer_nic_path = peer_segment_desc->name + "@" +
                                    peer_segment_desc->devices[device_id].name;
        slice->peer_nic_path = peer_nic_path;

        slices_by_peer[peer_nic_path].push_back(slice);
    }

    // Now send to each peer endpoint
    for (auto& entry : slices_by_peer) {
        const std::string& peer_nic_path = entry.first;
        auto& peer_slices = entry.second;

        // Get or create endpoint for this peer
        auto ep = endpoint(peer_nic_path);
        if (!ep) {
            LOG(ERROR) << "Cannot create endpoint for peer " << peer_nic_path;
            for (auto* slice : peer_slices) {
                slice->markFailed();
            }
            continue;
        }

        // Submit to endpoint
        std::vector<Transport::Slice*> failed_slice_list;
        ep->submitPostSend(peer_slices, failed_slice_list);

        // Handle any slices that failed to post
        for (auto* slice : failed_slice_list) {
            slice->markFailed();
        }
    }

    return 0;
}

int EfaContext::pollCq(int max_entries, int cq_index) {
    if (cq_index < 0 || (size_t)cq_index >= cq_list_.size()) {
        return 0;
    }

    struct fid_cq* cq = cq_list_[cq_index]->cq;
    if (!cq) return 0;

    // Use fi_cq_data format for completions
    struct fi_cq_data_entry entries[64];
    int to_poll = std::min(max_entries, 64);

    ssize_t ret = fi_cq_read(cq, entries, to_poll);

    if (ret > 0) {
        // Process completions outside the lock (markSuccess / delete are safe)
        std::unordered_map<volatile int*, int> wr_depth_set;
        for (ssize_t i = 0; i < ret; i++) {
            EfaOpContext* op_ctx =
                reinterpret_cast<EfaOpContext*>(entries[i].op_context);
            if (op_ctx && op_ctx->slice) {
                op_ctx->slice->markSuccess();
                if (op_ctx->wr_depth) {
                    wr_depth_set[op_ctx->wr_depth]++;
                }
                delete op_ctx;
            }
        }
        for (auto& entry : wr_depth_set) {
            __sync_fetch_and_sub(entry.first, entry.second);
        }
        __sync_fetch_and_sub(&cq_list_[cq_index]->outstanding,
                             static_cast<int>(ret));
        return static_cast<int>(ret);
    } else if (ret == -FI_EAGAIN) {
        return 0;
    } else if (ret < 0) {
        // CQ error - drain all queued error entries under the domain lock
        int err_count = 0;
        struct fi_cq_err_entry err_entry;
        std::unordered_map<volatile int*, int> wr_depth_set;

        while ((ret = fi_cq_readerr(cq, &err_entry, 0)) > 0) {
            EfaOpContext* op_ctx =
                reinterpret_cast<EfaOpContext*>(err_entry.op_context);
            if (op_ctx && op_ctx->slice) {
                LOG(ERROR) << "EFA CQ error: "
                           << fi_cq_strerror(cq, err_entry.prov_errno,
                                             err_entry.err_data, nullptr, 0)
                           << " for slice at " << op_ctx->slice->source_addr;
                op_ctx->slice->markFailed();
                if (op_ctx->wr_depth) {
                    wr_depth_set[op_ctx->wr_depth]++;
                }
                delete op_ctx;
            }
            err_count++;
        }

        for (auto& entry : wr_depth_set) {
            __sync_fetch_and_sub(entry.first, entry.second);
        }
        if (err_count > 0) {
            __sync_fetch_and_sub(&cq_list_[cq_index]->outstanding, err_count);
        }
        return err_count;
    }

    return 0;
}

}  // namespace mooncake
