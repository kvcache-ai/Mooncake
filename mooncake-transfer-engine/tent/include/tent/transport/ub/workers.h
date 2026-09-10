// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_WORKERS_H_
#define TENT_TRANSPORT_UB_WORKERS_H_

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/segment_manager.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

class UbEndpoint;

struct EndpointResolveRequest {
    UbContextPtr local_context;
    SegmentID remote_segment_id{LOCAL_SEGMENT_ID};
    const SegmentDesc* remote_segment{nullptr};
    Topology::NicID remote_topology_id{-1};
    uint64_t segment_generation{0};
};

using EndpointResolver = std::function<Status(const EndpointResolveRequest&,
                                              std::shared_ptr<UbEndpoint>&)>;
using EndpointRetirer = std::function<void(const std::shared_ptr<UbEndpoint>&)>;

// Native UB scheduler. Posting lanes own request selection and URMA post;
// poller lanes own completion dispatch. A monotonic numeric token, never a raw
// UbSlice pointer, crosses the adapter boundary.
class UbWorkers final {
   public:
    UbWorkers(std::shared_ptr<UrmaAdapter> adapter,
              std::vector<UbContextPtr> contexts,
              std::shared_ptr<Topology> local_topology,
              SegmentManager* segment_manager, UbBufferManager* buffers,
              RailMonitor* rail_monitor, QuotaManager* quota, UbParams params,
              EndpointResolver endpoint_resolver,
              EndpointRetirer endpoint_retirer = {});
    ~UbWorkers();

    UbWorkers(const UbWorkers&) = delete;
    UbWorkers& operator=(const UbWorkers&) = delete;

    Status start();
    Status stop();
    Status submit(const UbTask::Ptr& task, uint64_t device_mask = ~0ULL);
    Status cancel(const UbTask::Ptr& task);

    [[nodiscard]] bool running() const noexcept {
        return accepting_.load(std::memory_order_acquire);
    }
    [[nodiscard]] size_t queuedCount() const;
    [[nodiscard]] size_t inflightCount() const;

   private:
    struct PendingSlice {
        // Keep the task alive until every queued/in-flight slice reaches a
        // terminal state. Callers may release the sub-batch handle before the
        // asynchronous data path has drained.
        UbTask::Ptr task;
        UbSlice::Ptr slice;
        uint64_t device_mask{~0ULL};
        int priority{PRIO_HIGH};
        SegmentID target_id{LOCAL_SEGMENT_ID};
        Request::OpCode opcode{Request::READ};
    };
    struct Route;
    struct Inflight;

    void postingLoop(size_t worker_index);
    void pollingLoop(size_t poller_index);
    bool popPending(PendingSlice& pending);
    void enqueueRetry(const PendingSlice& pending);
    void deferPending(const PendingSlice& pending);
    void processPending(const PendingSlice& pending, size_t worker_index);
    Status buildRoute(const PendingSlice& pending, Route& route);
    Status chooseAndResolveEndpoint(const PendingSlice& pending, Route& route,
                                    std::shared_ptr<UbEndpoint>& endpoint,
                                    UbPostPath& path);
    void handleCompletion(const Completion& completion);
    void scanTimeouts();
    void releaseInflight(const std::shared_ptr<Inflight>& inflight);
    void resolveInflight(const std::shared_ptr<Inflight>& inflight,
                         TransferStatusEnum outcome, size_t bytes,
                         bool retryable);
    void recordTimeoutOnce(const std::shared_ptr<Inflight>& inflight,
                           uint64_t now_ns);
    void rememberEndpointDrain(const std::shared_ptr<UbEndpoint>& endpoint);
    void forgetEndpointDrain(const std::shared_ptr<UbEndpoint>& endpoint);
    void failUnposted(const PendingSlice& pending,
                      TransferStatusEnum outcome = FAILED);
    uint64_t nextCompletionToken();
    std::vector<Topology::NicID> orderedLocalDevices(
        const PendingSlice& pending) const;
    static std::vector<Topology::NicID> orderedRemoteDevices(
        const SegmentDesc& segment, const BufferDesc& buffer);

    std::shared_ptr<UrmaAdapter> adapter_;
    std::vector<UbContextPtr> contexts_;
    std::unordered_map<Topology::NicID, UbContextPtr> context_by_topology_id_;
    std::shared_ptr<Topology> local_topology_;
    SegmentManager* segment_manager_;
    UbBufferManager* buffers_;
    RailMonitor* rail_monitor_;
    QuotaManager* quota_;
    const UbParams params_;
    EndpointResolver endpoint_resolver_;
    EndpointRetirer endpoint_retirer_;

    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::array<std::deque<PendingSlice>, PRIO_LOW + 1> queues_;

    mutable std::mutex inflight_mutex_;
    mutable std::mutex endpoint_drain_mutex_;
    std::condition_variable inflight_cv_;
    std::unordered_map<uint64_t, std::shared_ptr<Inflight>> inflight_;
    // Endpoints whose ERROR transition has not yet reached its native flush
    // fence remain owned here even if every logical token completes naturally.
    std::unordered_map<uint64_t, std::shared_ptr<UbEndpoint>>
        draining_endpoints_;

    std::vector<std::shared_ptr<UbJfc>> all_jfcs_;
    std::unordered_map<const UbJfc*, UbContextPtr> context_by_jfc_;
    std::vector<std::thread> posting_threads_;
    std::vector<std::thread> polling_threads_;
    std::atomic<bool> accepting_{false};
    std::atomic<bool> posting_{false};
    std::atomic<bool> polling_{false};
    std::atomic<bool> timeout_scans_enabled_{false};
    std::atomic<uint64_t> next_token_{1};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_WORKERS_H_
