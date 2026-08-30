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
#include <mutex>
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
    friend class RdmaContextTestPeer;

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

    // Evict all cached endpoints so they are rebuilt with fresh QPs.
    // Called on port recovery (IBV_EVENT_PORT_ACTIVE): QPs that entered
    // IBV_QPS_ERR while the link was down are stale and must be torn down.
    void evictEndpoints();

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

    // Warm up RDMA MR registration by temporarily registering/deregistering.
    // This targets RDMA driver-side pinning/metadata and differs from CPU
    // prefault (madvise/mlock/touch) used before NUMA probing.
    int warmupMrRegistration(void *addr, size_t length);

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

    // The one port this context opened; 0 for a slot that never constructed.
    uint8_t portNum() const { return params_ ? params_->device.port : 0; }

    // Port speed in Gbps: the effective speed from ibv_query_port_speed()
    // where the library provides it (LAG-aware), otherwise the negotiated
    // link rate. 0 when neither could be determined.
    double linkSpeedGbps() const;

    // Re-read the port's negotiated speed and width from the hardware, so
    // linkSpeedGbps() reflects a renegotiated link. Called from the monitor
    // thread on IBV_EVENT_PORT_ACTIVE / IBV_EVENT_DEVICE_SPEED_CHANGE.
    // Returns -1 and leaves the cached values untouched if no device is
    // open or the query fails.
    int refreshPortAttributes();

    int eventFd() const { return event_fd_; }

    RdmaCQ *cq(int index);

    int cqCount() const { return params_->device.num_cq_list; }

    RdmaParams &params() const { return *params_.get(); }

    // PCIe Relaxed Ordering support
    bool isRelaxedOrderingEnabled() const { return relaxed_ordering_enabled_; }

    // Notification CQ (dedicated for notification QPs)
    RdmaCQ *notifyCq() { return notify_cq_; }

   private:
    int openDevice(const std::string &device_name, uint8_t port);
    // Decode one ibv_query_port result into active_speed_/active_width_.
    void recordPortSpeed(const ibv_port_attr &port_attr);
    // Ask ibv_query_port_speed() for the effective speed when the library
    // has it; records 0 otherwise so linkSpeedGbps() falls back.
    void queryEffectiveSpeed();

    // Release every resource currently owned by this context. This is
    // intentionally state-independent so it can clean up a partially completed
    // enable() and can safely be retried.
    void cleanupResources();

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
    // Set by openDevice() and refreshed by refreshPortAttributes() on the
    // monitor thread. Today every runtime reader is that same thread;
    // atomic so a reader added elsewhere stays well-defined.
    std::atomic<int> active_speed_{0};
    std::atomic<int> active_width_{0};
    // From ibv_query_port_speed(), converted to Mb/s; 0 = unavailable.
    std::atomic<uint64_t> effective_speed_mbps_{0};
    int gid_index_ = -1;
    ibv_gid gid_;

    std::mutex mr_set_mutex_;
    std::unordered_set<ibv_mr *> mr_set_;

    std::shared_ptr<EndpointStore> endpoint_store_;
    std::vector<RdmaCQ *> cq_list_;

    // Dedicated CQ for notification QPs (one per device)
    RdmaCQ *notify_cq_ = nullptr;

    // PCIe Relaxed Ordering support
    bool relaxed_ordering_enabled_ = false;

    // The context's own copy of the loader's verbs table (copied once at
    // construction, read-only afterwards). A copy rather than a reference so
    // tests can substitute individual entries and drive the port-attribute
    // and event paths without an RNIC.
    IbvSymbols verbs_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CONTEXT_H
