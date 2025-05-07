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

#include "common/common.h"
#include "rdma_transport.h"
#include "transport/transport.h"

namespace mooncake {
namespace v1 {

class RdmaEndPoint;
class EndpointStore;

class RdmaContext {
   public:
    RdmaContext();

    ~RdmaContext();

    struct DeviceParams {
        int num_cq_list = 1;
        size_t num_comp_channels = 1;
        uint8_t port = 1;
        int gid_index = 0;
        int max_cqe = 4096;
    };

    struct CompletionQueue {
        CompletionQueue()
            : cq(nullptr), cqe_now(0), index(-1), context(nullptr) {}

        ibv_cq *cq;
        volatile int cqe_now;
        int index;
        RdmaContext *context;
    };

    int construct(const std::string &device_name,
                  std::shared_ptr<EndpointStore> endpoint_store,
                  const DeviceParams &params);

   public:
    // Enable, disable, restart
    enum DeviceStatus { kDeviceUninitialized, kDeviceDisabled, kDeviceEnabled };

    int enable();

    int disable();

    DeviceStatus status() const { return status_; }

   public:
    // Memory Region Management
    using MemRegIndex = int;
    MemRegIndex registerMemReg(void *addr, size_t length, int access);

    int unregisterMemReg(MemRegIndex index);

    std::pair<uint32_t, uint32_t> queryMemRegKey(MemRegIndex index);

    // helper APIs
    // device name, such as `mlx5_3`
    const std::string name() const { return device_name_; }

    uint16_t lid() const { return lid_; }

    std::string gid() const;

    int gidIndex() const { return gid_index_; }

    ibv_context *context() const { return context_; }

    ibv_pd *pd() const { return pd_; }

    uint8_t portNum() const { return params_.port; }

    int activeSpeed() const { return active_speed_; }

    ibv_mtu activeMTU() const { return active_mtu_; }

    int eventFd() const { return event_fd_; }

    CompletionQueue *cq(int index);

    int cqCount() const { return params_.num_cq_list; }

    const DeviceParams &params() const { return params_; }

    int socketId() const { return socket_id_; }

    enum EndPointOperation { EP_GET_OR_CREATE, EP_GET, EP_DELETE, EP_RECREATE };
    std::shared_ptr<RdmaEndPoint> endpoint(
        const std::string &key, EndPointOperation operation = EP_GET_OR_CREATE);

   private:
    int openDevice(const std::string &device_name, uint8_t port, int gid_index);

   private:
    // initialized during ctor, will never be changed during the context's
    // lifecycle
    std::string device_name_;
    DeviceParams params_;
    DeviceStatus status_;

    // initialized during enable() and destroyed during disable()
    ibv_context *context_ = nullptr;
    ibv_pd *pd_ = nullptr;
    int event_fd_ = -1;

    size_t num_comp_channel_ = 0;
    ibv_comp_channel **comp_channel_ = nullptr;

    uint16_t lid_ = 0;
    int gid_index_ = -1;
    int active_speed_ = -1;
    ibv_mtu active_mtu_;
    ibv_gid gid_;
    int socket_id_;

    RWSpinlock mr_lock_;
    std::unordered_map<int, ibv_mr *> mr_map_;
    std::atomic<int> next_mr_index_;

    std::vector<CompletionQueue> cq_list_;

    std::shared_ptr<EndpointStore> endpoint_store_;
};

using CompletionQueue = RdmaContext::CompletionQueue;
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_CONTEXT_H
