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

#ifndef RDMA_CONTEXT_H
#define RDMA_CONTEXT_H

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
#include "rdma_transport.h"
#include "transport/transport.h"

namespace mooncake {

class RdmaEndPoint;
class RdmaTransport;
class WorkerPool;
class EndpointStore;

struct RdmaCq {
    RdmaCq() : native(nullptr), outstanding(0) {}
    ibv_cq *native;
    volatile int outstanding;
};

// RdmaContext represents the set of resources controlled by each local NIC,
// including Memory Region, CQ, EndPoint (QPs), etc.
class RdmaContext {
   public:
    RdmaContext(RdmaTransport &engine, const std::string &device_name);

    ~RdmaContext();

    int construct(size_t num_cq_list = 1, size_t num_comp_channels = 1,
                  uint8_t port = 1, int gid_index = 0, size_t max_cqe = 4096,
                  int max_endpoints = 256);

   private:
    int deconstruct();

   public:
    // Memory Region Management
    int registerMemoryRegion(void *addr, size_t length, int access);

    int unregisterMemoryRegion(void *addr);

    uint32_t rkey(void *addr);

    uint32_t lkey(void *addr);

   public:
    bool active() const { return active_; }

    void set_active(bool flag) { active_ = flag; }

   public:
    // EndPoint Management
    std::shared_ptr<RdmaEndPoint> endpoint(const std::string &peer_nic_path);

    int deleteEndpoint(const std::string &peer_nic_path);

    int disconnectAllEndpoints();

   public:
    // Device name, such as `mlx5_3`
    std::string deviceName() const { return device_name_; }

    // NIC Path, such as `192.168.3.76@mlx5_3`
    std::string nicPath() const;

   public:
    uint16_t lid() const { return lid_; }

    std::string gid() const;

    int gidIndex() const { return gid_index_; }

    ibv_context *context() const { return context_; }

    RdmaTransport &engine() const { return engine_; }

    ibv_pd *pd() const { return pd_; }

    uint8_t portNum() const { return port_; }

    int activeSpeed() const { return active_speed_; }

    ibv_mtu activeMTU() const { return active_mtu_; }

    ibv_comp_channel *compChannel();

    int compVector();

    int eventFd() const { return event_fd_; }

    ibv_cq *cq();

    volatile int *cqOutstandingCount(int cq_index) {
        return &cq_list_[cq_index].outstanding;
    }

    int cqCount() const { return cq_list_.size(); }

    int poll(int num_entries, ibv_wc *wc, int cq_index = 0);

    int socketId();

   private:
    int openRdmaDevice(const std::string &device_name, uint8_t port,
                       int gid_index);

    int joinNonblockingPollList(int event_fd, int data_fd);

    int getBestGidIndex(const std::string &device_name,
                        struct ibv_context *context, ibv_port_attr &port_attr,
                        uint8_t port);

   public:
    int submitPostSend(const std::vector<Transport::Slice *> &slice_list);

   private:
    const std::string device_name_;
    RdmaTransport &engine_;

    ibv_context *context_ = nullptr;
    ibv_pd *pd_ = nullptr;
    int event_fd_ = -1;

    size_t num_comp_channel_ = 0;
    ibv_comp_channel **comp_channel_ = nullptr;

    uint8_t port_ = 0;
    uint16_t lid_ = 0;
    int gid_index_ = -1;
    int active_speed_ = -1;
    ibv_mtu active_mtu_;
    ibv_gid gid_;

    RWSpinlock memory_regions_lock_;
    std::vector<ibv_mr *> memory_region_list_;
    std::vector<RdmaCq> cq_list_;

    std::shared_ptr<EndpointStore> endpoint_store_;

    std::vector<std::thread> background_thread_;
    std::atomic<bool> threads_running_;

    std::atomic<int> next_comp_channel_index_;
    std::atomic<int> next_comp_vector_index_;
    std::atomic<int> next_cq_list_index_;

    std::shared_ptr<WorkerPool> worker_pool_;

    volatile bool active_;
};

}  // namespace mooncake

#endif  // RDMA_CONTEXT_H
