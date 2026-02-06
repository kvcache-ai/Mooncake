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
#include "transport/efa_transport/efa_endpoint.h"
#include "transport/efa_transport/efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

// EfaEndpointStore implementation
std::shared_ptr<EfaEndPoint> EfaEndpointStore::get(
    const std::string &peer_nic_path) {
    RWSpinlock::ReadGuard guard(lock_);
    auto it = endpoints_.find(peer_nic_path);
    if (it != endpoints_.end()) {
        return it->second;
    }
    return nullptr;
}

std::shared_ptr<EfaEndPoint> EfaEndpointStore::getOrInsert(
    const std::string &peer_nic_path, std::shared_ptr<EfaEndPoint> new_ep) {
    RWSpinlock::WriteGuard guard(lock_);
    auto it = endpoints_.find(peer_nic_path);
    if (it != endpoints_.end()) {
        return it->second;  // Another thread already created it
    }
    endpoints_[peer_nic_path] = new_ep;
    return new_ep;
}

void EfaEndpointStore::add(const std::string &peer_nic_path,
                           std::shared_ptr<EfaEndPoint> endpoint) {
    RWSpinlock::WriteGuard guard(lock_);
    endpoints_[peer_nic_path] = endpoint;
}

void EfaEndpointStore::remove(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    endpoints_.erase(peer_nic_path);
}

int EfaEndpointStore::disconnectAll() {
    RWSpinlock::WriteGuard guard(lock_);
    for (auto &entry : endpoints_) {
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

// EfaContext implementation
EfaContext::EfaContext(EfaTransport &engine, const std::string &device_name)
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
    endpoint_store_ = std::make_shared<EfaEndpointStore>();

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
    hints_->domain_attr->mr_mode =
        FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY;
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
        for (auto &entry : mr_map_) {
            if (entry.second.mr) {
                fi_close(&entry.second.mr->fid);
            }
        }
        mr_map_.clear();
    }

    for (auto &cq : cq_list_) {
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

int EfaContext::registerMemoryRegionInternal(void *addr, size_t length,
                                             int access,
                                             EfaMemoryRegionMeta &mrMeta) {
    if (length > (size_t)globalConfig().max_mr_size) {
        PLOG(WARNING) << "The buffer length exceeds device max_mr_size, "
                      << "shrink it to " << globalConfig().max_mr_size;
        length = (size_t)globalConfig().max_mr_size;
    }

    mrMeta.addr = addr;
    mrMeta.length = length;

    // Convert access flags to libfabric flags
    uint64_t fi_access = 0;
    if (access & FI_READ) fi_access |= FI_READ;
    if (access & FI_WRITE) fi_access |= FI_WRITE;
    if (access & FI_REMOTE_READ) fi_access |= FI_REMOTE_READ;
    if (access & FI_REMOTE_WRITE) fi_access |= FI_REMOTE_WRITE;

    // For EFA, we need local read/write and remote read/write
    fi_access = FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;

    int ret = fi_mr_reg(domain_, addr, length, fi_access, 0, 0, 0, &mrMeta.mr,
                        nullptr);
    if (ret) {
        LOG(ERROR) << "fi_mr_reg failed for " << addr << ": "
                   << fi_strerror(-ret);
        return ERR_CONTEXT;
    }

    mrMeta.key = fi_mr_key(mrMeta.mr);

    return 0;
}

int EfaContext::registerMemoryRegion(void *addr, size_t length, int access) {
    EfaMemoryRegionMeta mrMeta;
    int ret = registerMemoryRegionInternal(addr, length, access, mrMeta);
    if (ret != 0) {
        return ret;
    }
    RWSpinlock::WriteGuard guard(mr_lock_);
    mr_map_[(uint64_t)addr] = mrMeta;
    return 0;
}

int EfaContext::unregisterMemoryRegion(void *addr) {
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

int EfaContext::preTouchMemory(void *addr, size_t length) {
    volatile char *ptr = (volatile char *)addr;
    for (size_t i = 0; i < length; i += 4096) {
        ptr[i] = ptr[i];
    }
    return 0;
}

uint64_t EfaContext::rkey(void *addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.find((uint64_t)addr);
    if (it != mr_map_.end() && it->second.mr) {
        return it->second.key;
    }
    return 0;
}

uint64_t EfaContext::lkey(void *addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    auto it = mr_map_.find((uint64_t)addr);
    if (it != mr_map_.end() && it->second.mr) {
        return fi_mr_key(it->second.mr);
    }
    return 0;
}

void *EfaContext::mrDesc(void *addr) {
    RWSpinlock::ReadGuard guard(mr_lock_);
    // Find the MR that contains this address
    for (auto &entry : mr_map_) {
        if ((uint64_t)addr >= entry.first &&
            (uint64_t)addr < entry.first + entry.second.length) {
            if (entry.second.mr) {
                return fi_mr_desc(entry.second.mr);
            }
        }
    }
    return nullptr;
}

std::shared_ptr<EfaEndPoint> EfaContext::endpoint(
    const std::string &peer_nic_path) {
    if (!endpoint_store_) return nullptr;

    // Fast path: endpoint already exists
    auto ep = endpoint_store_->get(peer_nic_path);
    if (ep) return ep;

    // Slow path: create new endpoint, then atomically insert (or get existing
    // if another thread raced us).  getOrInsert prevents duplicate endpoints
    // and duplicate AV entries for the same peer.
    auto new_endpoint = std::make_shared<EfaEndPoint>(*this);
    if (!cq_list_.empty() && cq_list_[0]) {
        int ret = new_endpoint->construct(cq_list_[0]->cq);
        if (ret != 0) {
            LOG(ERROR) << "Failed to construct EFA endpoint";
            return nullptr;
        }
    }
    new_endpoint->setPeerNicPath(peer_nic_path);
    ep = endpoint_store_->getOrInsert(peer_nic_path, new_endpoint);
    // If another thread won the race, new_endpoint is discarded (RAII cleanup)
    return ep;
}

int EfaContext::deleteEndpoint(const std::string &peer_nic_path) {
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
    const uint8_t *addr = static_cast<const uint8_t *>(fi_info_->src_addr);
    for (size_t i = 0; i < fi_info_->src_addrlen; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)addr[i];
    }
    return oss.str();
}

int EfaContext::submitPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
    // Route slices to appropriate endpoints for sending
    // Group slices by peer NIC path
    std::unordered_map<std::string, std::vector<Transport::Slice *>>
        slices_by_peer;
    std::vector<Transport::Slice *> failed_slices;

    for (auto *slice : slice_list) {
        if (!slice) continue;

        // Get peer segment descriptor to find dest_rkey and peer device info
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
                       << (void *)slice->rdma.dest_addr;
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
    for (auto &entry : slices_by_peer) {
        const std::string &peer_nic_path = entry.first;
        auto &peer_slices = entry.second;

        // Get or create endpoint for this peer
        auto ep = endpoint(peer_nic_path);
        if (!ep) {
            LOG(ERROR) << "Cannot create endpoint for peer " << peer_nic_path;
            for (auto *slice : peer_slices) {
                slice->markFailed();
            }
            continue;
        }

        // Submit to endpoint
        std::vector<Transport::Slice *> failed_slice_list;
        ep->submitPostSend(peer_slices, failed_slice_list);

        // Handle any slices that failed to post
        for (auto *slice : failed_slice_list) {
            slice->markFailed();
        }
    }

    return 0;
}

int EfaContext::pollCq(int max_entries, int cq_index) {
    if (cq_index < 0 || (size_t)cq_index >= cq_list_.size()) {
        return 0;
    }

    struct fid_cq *cq = cq_list_[cq_index]->cq;
    if (!cq) return 0;

    // Use fi_cq_data format for completions
    struct fi_cq_data_entry entries[64];
    int to_poll = std::min(max_entries, 64);

    ssize_t ret = fi_cq_read(cq, entries, to_poll);

    if (ret > 0) {
        // Process completions outside the lock (markSuccess / delete are safe)
        std::unordered_map<volatile int *, int> wr_depth_set;
        for (ssize_t i = 0; i < ret; i++) {
            EfaOpContext *op_ctx =
                reinterpret_cast<EfaOpContext *>(entries[i].op_context);
            if (op_ctx && op_ctx->slice) {
                op_ctx->slice->markSuccess();
                if (op_ctx->wr_depth) {
                    wr_depth_set[op_ctx->wr_depth]++;
                }
                delete op_ctx;
            }
        }
        for (auto &entry : wr_depth_set) {
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
        std::unordered_map<volatile int *, int> wr_depth_set;

        while ((ret = fi_cq_readerr(cq, &err_entry, 0)) > 0) {
            EfaOpContext *op_ctx =
                reinterpret_cast<EfaOpContext *>(err_entry.op_context);
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

        for (auto &entry : wr_depth_set) {
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
