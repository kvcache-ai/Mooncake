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

#ifndef EFA_CONTEXT_H
#define EFA_CONTEXT_H

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_errno.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <map>
#include <unordered_map>

#include "common.h"
#include "efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

class EfaEndPoint;
class EfaTransport;

struct EfaCq {
    EfaCq() : cq(nullptr), outstanding(0) {}
    struct fid_cq* cq;
    volatile int outstanding;
};

struct EfaMemoryRegionMeta {
    void* addr;
    size_t length;
    struct fid_mr* mr;
    uint64_t key;
};

// Endpoint store for EFA with LRU eviction support.
// When the store approaches the AV capacity (max_endpoints), stale endpoints
// that have been inactive beyond `inactive_timeout_sec` are evicted to free
// AV slots.  This prevents AV exhaustion in long-running services that
// communicate with many transient peers.
class EfaEndpointStore {
   public:
    static constexpr double kDefaultInactiveTimeoutSec = 5.0;  // 5 seconds

    explicit EfaEndpointStore(
        size_t max_endpoints = 65536,
        double inactive_timeout_sec = kDefaultInactiveTimeoutSec);

    std::shared_ptr<EfaEndPoint> get(const std::string& peer_nic_path);
    // Atomically get-or-insert: returns existing endpoint or inserts new_ep.
    // Prevents duplicate endpoint creation from concurrent callers.
    // Triggers eviction when the store is at or above max_endpoints.
    std::shared_ptr<EfaEndPoint> getOrInsert(
        const std::string& peer_nic_path, std::shared_ptr<EfaEndPoint> new_ep);
    void add(const std::string& peer_nic_path,
             std::shared_ptr<EfaEndPoint> endpoint);
    void remove(const std::string& peer_nic_path);
    int disconnectAll();
    size_t size() const;

    // Evict endpoints that have been inactive longer than the configured
    // timeout.  Returns the number of evicted endpoints.
    size_t evictStale();

    // Remove endpoints whose connections are broken (not connected and not
    // initializing).  Returns the number of removed endpoints.
    size_t removeDisconnected();

   private:
    // Must be called with write lock held.
    size_t evictStaleLocked();

    mutable RWSpinlock lock_;
    std::unordered_map<std::string, std::shared_ptr<EfaEndPoint>> endpoints_;
    size_t max_endpoints_;
    double inactive_timeout_sec_;
};

// EfaContext represents the set of resources controlled by each local EFA
// device, including Memory Region, CQ, EndPoint, etc. using libfabric
class EfaContext {
   public:
    EfaContext(EfaTransport& engine, const std::string& device_name);

    ~EfaContext();

    int construct(size_t num_cq_list = 1, size_t num_comp_channels = 1,
                  uint8_t port = 1, int gid_index = -1, size_t max_cqe = 4096,
                  int max_endpoints = 65536);

   private:
    int deconstruct();

   public:
    // Memory Region Management
    int registerMemoryRegion(void* addr, size_t length, int access);
    int unregisterMemoryRegion(void* addr);
    int preTouchMemory(void* addr, size_t length);
    uint64_t rkey(void* addr);
    uint64_t lkey(void* addr);
    void* mrDesc(void* addr);  // Get MR descriptor for fi_write local_desc

   private:
    int registerMemoryRegionInternal(void* addr, size_t length, int access,
                                     EfaMemoryRegionMeta& mrMeta);

   public:
    bool active() const { return active_; }
    void set_active(bool flag) { active_ = flag; }

   public:
    // EndPoint Management
    std::shared_ptr<EfaEndPoint> endpoint(const std::string& peer_nic_path);
    int deleteEndpoint(const std::string& peer_nic_path);
    int disconnectAllEndpoints();
    size_t getTotalQPNumber() const;

   public:
    // Access to engine for endpoint handshake
    EfaTransport& engine() { return engine_; }
    const EfaTransport& engine() const { return engine_; }

    // Submit slices for transfer
    int submitPostSend(const std::vector<Transport::Slice*>& slice_list);

    // Poll completion queue for completed operations
    int pollCq(int max_entries, int cq_index = 0);

    // Evict stale endpoints to free EFA resources
    size_t evictStaleEndpoints() {
        return endpoint_store_ ? endpoint_store_->evictStale() : 0;
    }

    // Get CQ count
    size_t cqCount() const { return cq_list_.size(); }

    // Round-robin CQ assignment for new endpoints (mirrors RDMA transport)
    std::shared_ptr<EfaCq> nextCq() {
        if (cq_list_.empty()) return nullptr;
        int index = (next_cq_index_++) % cq_list_.size();
        return cq_list_[index];
    }

    // Get CQ outstanding count pointer
    volatile int* cqOutstandingCount(int cq_index) {
        if (cq_index < 0 || (size_t)cq_index >= cq_list_.size()) return nullptr;
        return &cq_list_[cq_index]->outstanding;
    }

   public:
    // Device name, such as `rdmap0s2`
    std::string deviceName() const { return device_name_; }

    // NIC Path, such as `192.168.3.76@rdmap0s2`
    std::string nicPath() const;

   public:
    // Libfabric accessors
    struct fid_fabric* fabric() const { return fabric_; }
    struct fid_domain* domain() const { return domain_; }
    struct fid_av* av() const { return av_; }
    struct fi_info* info() const { return fi_info_; }
    std::string localAddr() const;

    // Compatibility methods (libfabric doesn't use lid/gid like ibverbs)
    uint16_t lid() const { return 0; }
    std::string gid() const { return localAddr(); }

   private:
    EfaTransport& engine_;
    std::string device_name_;

    // Libfabric objects
    struct fi_info* fi_info_;
    struct fi_info* hints_;
    struct fid_fabric* fabric_;
    struct fid_domain* domain_;
    struct fid_av* av_;  // Address vector for peer addressing

    bool active_;

    std::shared_ptr<EfaEndpointStore> endpoint_store_;
    std::vector<std::shared_ptr<EfaCq>> cq_list_;
    std::atomic<int> next_cq_index_{0};

    RWSpinlock mr_lock_;
    std::map<uint64_t, EfaMemoryRegionMeta> mr_map_;
};

}  // namespace mooncake

#endif  // EFA_CONTEXT_H
