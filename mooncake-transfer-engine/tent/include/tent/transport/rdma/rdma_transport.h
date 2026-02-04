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
    std::vector<RdmaTask> task_list;
    std::vector<RdmaSlice*> slice_chain;
    size_t max_size;
    virtual size_t size() const { return task_list.size(); }
};

class RdmaTransport : public Transport {
    friend class Workers;
    friend class RdmaEndPoint;

   public:
    RdmaTransport();

    ~RdmaTransport();

    virtual Status install(std::string& local_segment_name,
                           std::shared_ptr<ControlService> metadata,
                           std::shared_ptr<Topology> local_topology,
                           std::shared_ptr<Config> conf = nullptr);

    virtual Status uninstall();

    virtual Status allocateSubBatch(SubBatchRef& batch, size_t max_size);

    virtual Status freeSubBatch(SubBatchRef& batch);

    virtual Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list);

    virtual Status getTransferStatus(SubBatchRef batch, int task_id,
                                     TransferStatus& status);

    virtual Status addMemoryBuffer(BufferDesc& desc,
                                   const MemoryOptions& options);

    virtual Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                   const MemoryOptions& options);

    virtual Status removeMemoryBuffer(BufferDesc& desc);

    virtual const char* getName() const { return "rdma"; }

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

   private:
    bool installed_;
    std::shared_ptr<Config> conf_;
    std::string local_segment_name_;
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

    // Map QP number to Endpoint for notification processing
    RWSpinlock notify_endpoint_map_lock_;
    std::unordered_map<uint32_t, RdmaEndPoint*> notify_qp_to_endpoint_;

    // Register/unregister notification QP (called by Endpoint)
    void registerNotifyQp(uint32_t qp_num, RdmaEndPoint* endpoint);
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