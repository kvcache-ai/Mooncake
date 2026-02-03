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

#ifndef TENT_CONTEXT_H
#define TENT_CONTEXT_H

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

#include "cq.h"
#include "params.h"
#include "ibv_loader.h"
#include "rdma_transport.h"
#include "tent/common/status.h"

namespace mooncake {
namespace tent {

class RdmaCQ;
class RdmaEndPoint;
class EndpointStore;
class RdmaTransport;

class RdmaContext {
    friend class RdmaCQ;
    friend class RdmaEndPoint;

   public:
    RdmaContext(RdmaTransport &transport);

    ~RdmaContext();

    int construct(const std::string &device_name,
                  std::shared_ptr<RdmaParams> params);

   public:
    int enable();

    int disable();

    int pause();

    int resume();

    enum DeviceStatus {
        DEVICE_UNINIT,
        DEVICE_DISABLED,
        DEVICE_ENABLED,
        DEVICE_PAUSED
    };

    DeviceStatus status() const { return status_; }

   public:
    using MemReg = void *;

    MemReg registerMemReg(void *addr, size_t length, int access);

    int unregisterMemReg(MemReg id);

    const std::pair<uint32_t, uint32_t> queryMemRegKey(MemReg id) const {
        auto entry = (ibv_mr *)id;
        return {entry->lkey, entry->rkey};
    }

    std::shared_ptr<EndpointStore> endpointStore() { return endpoint_store_; }

    const std::string name() const { return device_name_; }

   public:
    uint16_t lid() const { return lid_; }

    std::string gid() const;

    int gidIndex() const { return gid_index_; }

    ibv_context *nativeContext() const { return native_context_; }

    ibv_pd *nativePD() const { return native_pd_; }

    uint8_t portNum() const { return params_->device.port; }

    int eventFd() const { return event_fd_; }

    RdmaCQ *cq(int index);

    int cqCount() const { return params_->device.num_cq_list; }

    RdmaParams &params() const { return *params_.get(); }

    // Notification CQ (dedicated for notification QPs)
    RdmaCQ *notifyCq() { return notify_cq_; }

   private:
    int openDevice(const std::string &device_name, uint8_t port);

   private:
    // initialized during ctor, will never be changed during the context's
    // lifecycle
    RdmaTransport &transport_;
    std::string device_name_;
    std::shared_ptr<RdmaParams> params_;
    std::atomic<DeviceStatus> status_;

    // initialized during enable() and destroyed during disable()
    ibv_context *native_context_ = nullptr;
    ibv_pd *native_pd_ = nullptr;
    int event_fd_ = -1;

    size_t num_comp_channel_ = 0;
    std::vector<ibv_comp_channel *> comp_channel_;

    uint16_t lid_ = 0;
    int gid_index_ = -1;
    ibv_gid gid_;

    std::mutex mr_set_mutex_;
    std::unordered_set<ibv_mr *> mr_set_;

    std::shared_ptr<EndpointStore> endpoint_store_;
    std::vector<RdmaCQ *> cq_list_;

    // Dedicated CQ for notification QPs (one per device)
    RdmaCQ *notify_cq_ = nullptr;

    const IbvSymbols &verbs_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CONTEXT_H
