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

#ifndef TENT_RDMA_TRANSPORT_H
#define TENT_RDMA_TRANSPORT_H

#include <infiniband/verbs.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "buffers.h"
#include "context.h"
#include "slice.h"
#include "quota.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
class RdmaContext;
class RdmaEndPoint;
class Workers;
class EndpointStore;
class LocalBuffers;

using RdmaContextSet = std::vector<std::shared_ptr<RdmaContext>>;

struct RdmaSubBatch : public Transport::SubBatch {
    std::vector<RdmaTask*> task_list;
    std::vector<RdmaSlice*> slice_chain;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

class RdmaTransport : public Transport {
    friend class Workers;
    friend class RdmaEndPoint;
    friend class RdmaTransportTestPeer;

   public:
    RdmaTransport();

    ~RdmaTransport();

    virtual Status install(std::string& local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    Status quiesce() override;

    virtual Status allocateSubBatch(SubBatchRef& batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef& batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus& status);

    bool supportsCancellation() const override { return true; }

    Status cancelTransferTask(SubBatchRef batch, int task_id) override;

    virtual Status addMemoryBuffer(BufferDesc& desc,
                                   const MemoryOptions& options);

    virtual Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                   const MemoryOptions& options);

    virtual Status removeMemoryBuffer(BufferDesc& desc);

    bool warmupMemory(void* addr, size_t length) override;

    virtual const char* getName() const { return "rdma"; }

    double getEstimatedBandwidth() const override;
    Status getNicLoadStats(std::vector<NicLoadStats>& stats) const override;

    virtual bool supportNotification() const override { return true; }

    virtual Status sendNotification(SegmentID target_id,
                                    const Notification& notify) override;

    virtual Status receiveNotification(
        std::vector<Notification>& notify_list) override;

    // Process notification completions (call from worker threads)
    int processNotifyCompletions();

    // Add notification directly to queue (called from endpoint
    // handleNotifyRecv)
    void addNotificationToQueue(const std::string& name,
                                const std::string& msg);

   public:
    int onSetupRdmaConnections(const BootstrapDesc& peer_desc,
                               BootstrapDesc& local_desc);

   public:
    Status setupLocalSegment();

    // Update one RNIC's published address after a GID/LID change. The local
    // descriptor is rolled back if registry synchronization fails.
    Status refreshLocalDeviceDesc(const std::string& device_name, uint16_t lid,
                                  const std::string& gid);

    std::shared_ptr<Config> config() const { return conf_; }

   private:
    // Builds context_set_ with one slot per NicID; returns how many RNICs
    // came up. Remaining slots hold inert contexts.
    size_t initializeContexts();

    // Free orphaned slices whose completion has been handled, or whose
    // endpoint is gone. Driven by the monitor tick and, so a straggler is
    // not held for a whole second, by freeSubBatch(). Two callers at once
    // are fine: each takes the list whole, so they scan disjoint sets.
    // uninstall()'s drain runs after the monitor is joined and takes the
    // same mutex as a reap from a batch free. Only the monitor's call
    // passes `on_tick`: it alone ages an orphan toward its warning, since
    // batch frees come as fast as the caller likes.
    void reapOrphanSlices(bool on_tick);

   private:
    bool installed_;
    std::shared_ptr<Config> conf_;
    std::string local_segment_name_;
    // When MC_RDMA_BIND_ADDRESS is set in a dual-NIC environment,
    // rdma_server_name_ holds the RDMA-reachable address for NIC path
    // construction, while local_segment_name_ keeps the TCP-reachable
    // address for P2P routing.
    std::string rdma_server_name_;
    std::shared_ptr<Topology> local_topology_;
    std::shared_ptr<ControlService> metadata_;
    LocalBufferManager local_buffer_manager_;
    RdmaContextSet context_set_;
    std::unordered_map<std::string, int> context_name_lookup_;
    std::unique_ptr<Workers> workers_;
    std::shared_ptr<RdmaParams> params_;

    // Local notification queue for receiveNotification()
    std::mutex notify_mutex_;
    std::vector<Notification> notify_list_;
    std::condition_variable notify_cv_;

    // Slices outliving the batch they belonged to because the completion
    // queue can still name them. Owned here until that is no longer true.
    // `passes` counts reap ticks survived: an orphan is only ever held while
    // its endpoint is alive, so one that lingers means a queue pair is not
    // being destroyed, and that is reported once per slice.
    struct OrphanSlice {
        RdmaSlice* slice;
        uint32_t passes;
    };
    std::mutex orphan_slice_mutex_;
    std::vector<OrphanSlice> orphan_slices_;
    // Whether orphan_slices_ has anything in it, published under the mutex
    // above. freeSubBatch() reads it on every batch free and would put every
    // caller thread on that one mutex for what is almost always an empty
    // list.
    std::atomic<bool> orphans_pending_{false};
    uint32_t orphan_warn_after_passes_{60};  // ~60 s at the 1 Hz tick
    size_t orphan_warnings_{0};              // reaper-owned; test-visible

    // Map QP number to Endpoint for notification processing
    RWSpinlock notify_endpoint_map_lock_;
    std::unordered_map<uint32_t, std::weak_ptr<RdmaEndPoint>>
        notify_qp_to_endpoint_;

    enum class NotifyCompletionAction {
        SkipSilently,         // expected flush from a retiring or gone endpoint
        ReportOnly,           // no live endpoint left to act on
        DisableNotification,  // fault confined to the notify QP
        RetireEndpoint,       // the peer or the path may be gone
    };

    // Decides what a failed notification completion costs. Only defined for
    // error completions; endpoint_ready means the endpoint is still EP_READY
    // and its notifications are still connected (a retiring or disabled
    // notify QP only flushes from then on).
    static NotifyCompletionAction classifyNotifyCompletion(ibv_wc_status status,
                                                           bool endpoint_alive,
                                                           bool endpoint_ready);

    // Register/unregister notification QP (called by Endpoint)
    void registerNotifyQp(uint32_t qp_num,
                          const std::shared_ptr<RdmaEndPoint>& endpoint);
    void unregisterNotifyQp(uint32_t qp_num);
    std::shared_ptr<RdmaEndPoint> getEndpoint(SegmentID target_id,
                                              int device_id);

    // Notification worker thread
    void notifyWorkerThread();
    std::thread notify_worker_;
    std::atomic<bool> notify_worker_running_;
    int notify_poll_interval_us_;                   // Adaptive polling interval
    static constexpr int kNotifyMinPollUs = 100;    // 100us
    static constexpr int kNotifyMaxPollUs = 10000;  // 10ms
};
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_RDMA_TRANSPORT_H
