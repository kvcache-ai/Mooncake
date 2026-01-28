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
#include <infiniband/verbs.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "common.h"
#include "efa_transport.h"
#include "transport/transport.h"

namespace mooncake {

class EfaEndPoint;
class EfaTransport;
class WorkerPool;
class EndpointStore;

// Enum to represent the network state of the GID found
enum class GidNetworkState {
    GID_WITH_NETWORK = 0,     // Found a GID with network device (best choice)
    GID_WITHOUT_NETWORK = 1,  // Found a GID without network device
    GID_NOT_FOUND = 2         // No suitable GID found
};

struct EfaCq {
    EfaCq() : native(nullptr), outstanding(0) {}
    ibv_cq *native;
    volatile int outstanding;
};

struct MemoryRegionMeta {
    // mr->addr is not set to starting address for iova based mr. Therefore we
    // track it ourselves.
    void *addr;
    struct ibv_mr *mr;
};

// EfaContext represents the set of resources controlled by each local EFA,
// including Memory Region, CQ, EndPoint (QPs), etc.
class EfaContext {
   public:
    EfaContext(EfaTransport &engine, const std::string &device_name);

    ~EfaContext();

    int construct(size_t num_cq_list = 1, size_t num_comp_channels = 1,
                  uint8_t port = 1, int gid_index = -1, size_t max_cqe = 4096,
                  int max_endpoints = 256);

   private:
    int deconstruct();

   public:
    // Memory Region Management
    int registerMemoryRegion(void *addr, size_t length, int access);

    int unregisterMemoryRegion(void *addr);

    int preTouchMemory(void *addr, size_t length);

    uint32_t rkey(void *addr);

    uint32_t lkey(void *addr);

   private:
    int registerMemoryRegionInternal(void *addr, size_t length, int access,
                                     MemoryRegionMeta &mrMeta);

   public:
    bool active() const { return active_; }

    void set_active(bool flag) { active_ = flag; }

   public:
    // EndPoint Management
    std::shared_ptr<EfaEndPoint> endpoint(const std::string &peer_nic_path);

    int submitTask(Transport::TransferTask *task);

    int postBatch(const std::vector<Transport::Slice *> &slice_list);

   public:
    std::string device_name() const { return device_name_; }

    struct ibv_context *context() { return context_; }

    uint8_t port() const { return port_; }

    int gid_index() const { return gid_index_; }

    union ibv_gid gid() const { return gid_; }

    int mtu() const { return mtu_; }

    std::shared_ptr<WorkerPool> workerPool() { return worker_pool_; }

   private:
    EfaTransport &engine_;
    std::string device_name_;
    struct ibv_context *context_;
    struct ibv_pd *pd_;
    uint8_t port_;
    int gid_index_;
    union ibv_gid gid_;
    int mtu_;
    bool active_;

    std::shared_ptr<EndpointStore> endpoint_store_;
    std::shared_ptr<WorkerPool> worker_pool_;

    std::vector<std::shared_ptr<EfaCq>> cq_list_;
    std::vector<struct ibv_comp_channel *> comp_channel_list_;

    SpinLock mr_lock_;
    std::unordered_map<uint64_t, MemoryRegionMeta> mr_map_;
};

}  // namespace mooncake

#endif  // EFA_CONTEXT_H
