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

#include <glog/logging.h>
#include <sys/mman.h>

#include "transport/efa_transport/efa_endpoint.h"
#include "transport/efa_transport/efa_transport.h"
#include "transport/rdma_transport/endpoint_store.h"

namespace mooncake {

EfaContext::EfaContext(EfaTransport &engine, const std::string &device_name)
    : engine_(engine),
      device_name_(device_name),
      context_(nullptr),
      pd_(nullptr),
      port_(1),
      gid_index_(0),
      mtu_(4096),
      active_(false) {}

EfaContext::~EfaContext() { deconstruct(); }

int EfaContext::construct(size_t num_cq_list, size_t num_comp_channels,
                         uint8_t port, int gid_index, size_t max_cqe,
                         int max_endpoints) {
    struct ibv_device **device_list = ibv_get_device_list(nullptr);
    if (!device_list) {
        LOG(ERROR) << "Failed to get IB device list";
        return ERR_INVALID_ARGUMENT;
    }

    // Find the specified EFA device
    struct ibv_device *device = nullptr;
    for (int i = 0; device_list[i] != nullptr; ++i) {
        if (device_name_ == ibv_get_device_name(device_list[i])) {
            device = device_list[i];
            break;
        }
    }

    if (!device) {
        LOG(ERROR) << "EFA device " << device_name_ << " not found";
        ibv_free_device_list(device_list);
        return ERR_INVALID_ARGUMENT;
    }

    // Open device
    context_ = ibv_open_device(device);
    ibv_free_device_list(device_list);

    if (!context_) {
        LOG(ERROR) << "Failed to open EFA device " << device_name_;
        return ERR_INVALID_ARGUMENT;
    }

    // Allocate protection domain
    pd_ = ibv_alloc_pd(context_);
    if (!pd_) {
        LOG(ERROR) << "Failed to allocate protection domain";
        deconstruct();
        return ERR_INVALID_ARGUMENT;
    }

    port_ = port;
    gid_index_ = (gid_index >= 0) ? gid_index : 0;

    // Query GID
    if (ibv_query_gid(context_, port_, gid_index_, &gid_)) {
        LOG(ERROR) << "Failed to query GID";
        deconstruct();
        return ERR_INVALID_ARGUMENT;
    }

    // Create completion queues
    for (size_t i = 0; i < num_cq_list; ++i) {
        auto cq = std::make_shared<EfaCq>();
        cq->native = ibv_create_cq(context_, max_cqe, nullptr, nullptr, 0);
        if (!cq->native) {
            LOG(ERROR) << "Failed to create completion queue";
            deconstruct();
            return ERR_INVALID_ARGUMENT;
        }
        cq_list_.push_back(cq);
    }

    // Initialize worker pool and endpoint store
    // Note: Using simplified implementation without WorkerPool for now
    endpoint_store_ = std::make_shared<EndpointStore>();

    active_ = true;
    LOG(INFO) << "EFA context constructed for device: " << device_name_;
    return 0;
}

int EfaContext::deconstruct() {
    active_ = false;

    endpoint_store_.reset();

    for (auto &cq : cq_list_) {
        if (cq->native) {
            ibv_destroy_cq(cq->native);
            cq->native = nullptr;
        }
    }
    cq_list_.clear();

    if (pd_) {
        ibv_dealloc_pd(pd_);
        pd_ = nullptr;
    }

    if (context_) {
        ibv_close_device(context_);
        context_ = nullptr;
    }

    return 0;
}

int EfaContext::registerMemoryRegion(void *addr, size_t length, int access) {
    MemoryRegionMeta mr_meta;
    return registerMemoryRegionInternal(addr, length, access, mr_meta);
}

int EfaContext::registerMemoryRegionInternal(void *addr, size_t length,
                                            int access,
                                            MemoryRegionMeta &mr_meta) {
    if (!pd_) {
        LOG(ERROR) << "Protection domain not initialized";
        return ERR_INVALID_ARGUMENT;
    }

    // Register memory region
    struct ibv_mr *mr = ibv_reg_mr(pd_, addr, length, access);
    if (!mr) {
        LOG(ERROR) << "Failed to register memory region";
        return ERR_INVALID_ARGUMENT;
    }

    mr_meta.addr = addr;
    mr_meta.mr = mr;

    SpinLock::Guard guard(mr_lock_);
    mr_map_[(uint64_t)addr] = mr_meta;

    return 0;
}

int EfaContext::unregisterMemoryRegion(void *addr) {
    SpinLock::Guard guard(mr_lock_);

    auto it = mr_map_.find((uint64_t)addr);
    if (it == mr_map_.end()) {
        LOG(WARNING) << "Memory region not found for address: " << addr;
        return ERR_INVALID_ARGUMENT;
    }

    if (ibv_dereg_mr(it->second.mr)) {
        LOG(ERROR) << "Failed to deregister memory region";
        return ERR_INVALID_ARGUMENT;
    }

    mr_map_.erase(it);
    return 0;
}

int EfaContext::preTouchMemory(void *addr, size_t length) {
    // Touch memory to ensure it's resident
    volatile char *ptr = (volatile char *)addr;
    for (size_t i = 0; i < length; i += 4096) {
        ptr[i] = ptr[i];
    }
    return 0;
}

uint32_t EfaContext::rkey(void *addr) {
    SpinLock::Guard guard(mr_lock_);
    auto it = mr_map_.find((uint64_t)addr);
    if (it != mr_map_.end()) {
        return it->second.mr->rkey;
    }
    return 0;
}

uint32_t EfaContext::lkey(void *addr) {
    SpinLock::Guard guard(mr_lock_);
    auto it = mr_map_.find((uint64_t)addr);
    if (it != mr_map_.end()) {
        return it->second.mr->lkey;
    }
    return 0;
}

std::shared_ptr<EfaEndPoint> EfaContext::endpoint(
    const std::string &peer_nic_path) {
    if (!endpoint_store_) {
        return nullptr;
    }

    auto endpoint = endpoint_store_->get(peer_nic_path);
    if (!endpoint) {
        // Create new endpoint
        auto new_endpoint = std::make_shared<EfaEndPoint>(*this);
        if (!cq_list_.empty()) {
            new_endpoint->construct(cq_list_[0]->native);
        }
        endpoint_store_->add(peer_nic_path, new_endpoint);
        endpoint = new_endpoint;
    }

    return std::static_pointer_cast<EfaEndPoint>(endpoint);
}

int EfaContext::submitTask(Transport::TransferTask *task) {
    // Submit task for processing
    // This is a simplified implementation
    task->is_finished = true;
    task->transferred_bytes = task->total_bytes;

    return 0;
}

int EfaContext::postBatch(const std::vector<Transport::Slice *> &slice_list) {
    // Post batch of slices for transfer
    for (auto *slice : slice_list) {
        slice->markSuccess();
    }
    return 0;
}

}  // namespace mooncake
