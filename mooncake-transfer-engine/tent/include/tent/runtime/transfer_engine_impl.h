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

#ifndef TRANSFER_ENGINE_IMPL_H_
#define TRANSFER_ENGINE_IMPL_H_

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/common/concurrent/thread_local_storage.h"
#include "tent/runtime/transport_selector.h"

namespace mooncake {
namespace tent {

class Batch;
class BatchSet;
class Topology;
class Transport;
class SegmentDesc;
class AllocatedMemory;
class ControlService;
class SegmentTracker;
class Platform;
class ProxyManager;
class ProgressWorker;

struct TaskInfo {
    TransportType type{UNSPEC};
    int sub_task_id{-1};
    bool derived{false};          // merged by other tasks
    int xport_priority{0};        // transport priority (for fallback)
    int failover_count{0};        // number of failover attempts
    uint64_t device_mask{~0ULL};  // Device mask for quota allocation
    Request request;
    bool staging{false};
    TransferStatusEnum status{TransferStatusEnum::PENDING};
    volatile TransferStatusEnum staging_status{TransferStatusEnum::PENDING};
    std::chrono::steady_clock::time_point start_time{};  // For latency tracking
};

class TransferEngineImpl {
    friend class ProxyManager;

   public:
    TransferEngineImpl();

    TransferEngineImpl(std::shared_ptr<Config> config);

    ~TransferEngineImpl();

    TransferEngineImpl(const TransferEngineImpl&) = delete;

    TransferEngineImpl& operator=(const TransferEngineImpl&) = delete;

   public:
    bool available() const { return available_; }

    const std::string getSegmentName() const;

    const std::string getRpcServerAddress() const;

    uint16_t getRpcServerPort() const;

   public:
    Status exportLocalSegment(std::string& shared_handle);

    Status importRemoteSegment(SegmentID& handle,
                               const std::string& shared_handle);

    Status openSegment(SegmentID& handle, const std::string& segment_name);

    Status closeSegment(SegmentID handle);

    Status getSegmentInfo(SegmentID handle, SegmentInfo& info);

   public:
    Status allocateLocalMemory(void** addr, size_t size,
                               Location location = kWildcardLocation);

    Status allocateLocalMemory(void** addr, size_t size, Location location,
                               bool internal);

    Status freeLocalMemory(void* addr);

    Status registerLocalMemory(void* addr, size_t size,
                               Permission permission = kGlobalReadWrite);

    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               Permission permission = kGlobalReadWrite);

    Status unregisterLocalMemory(void* addr, size_t size = 0);

    Status unregisterLocalMemory(std::vector<void*> addr_list,
                                 std::vector<size_t> size_list = {});

    // advanced buffer allocate function
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions& options);

    // advanced buffer register function
    Status registerLocalMemory(std::vector<void*> addr_list,
                               std::vector<size_t> size_list,
                               MemoryOptions& options);

   public:
    BatchID allocateBatch(size_t batch_size);

    Status freeBatch(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<Request>& request_list,
                          const Notification& notifi);

    Status sendNotification(SegmentID target_id, const Notification& notifi);

    Status receiveNotification(std::vector<Notification>& notifi_list);

    Status probePeerAliveByID(SegmentID target_id);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status);

    Status getTransferStatus(BatchID batch_id,
                             std::vector<TransferStatus>& status_list);

    Status getTransferStatus(BatchID batch_id, TransferStatus& overall_status);

    Status progressBatch(BatchID batch_id, TransferStatus& overall_status);

    Status waitTransferCompletion(BatchID batch_id);

    Status transferSync(const std::vector<Request>& request_list);

    uint64_t lockStageBuffer(const std::string& location);

    Status unlockStageBuffer(uint64_t addr);

    // Test-only hook: replace the transport in a given slot after construct().
    // Production code never calls this. Used by failover integration tests to
    // inject a FaultProxyTransport without bypassing resubmitTransferTask,
    // resolveTransport, or any other engine state. Not thread-safe with any
    // in-flight transfer on that slot.
    void swapTransportForTest(TransportType type,
                              std::shared_ptr<Transport> xport) {
        if (type >= 0 && type < (TransportType)kSupportedTransportTypes) {
            transport_list_[type] = std::move(xport);
        }
    }

    // Wake the optional event-driven progress worker for `batch_id`. No-op if
    // enable_progress_worker is false. Currently used by test/integration
    // hooks; transports will be migrated to call this in a follow-up PR.
    void notifyBatchMaybeReady(BatchID batch_id);

   private:
    Status construct();

    Status deconstruct();

    Status setupLocalSegment();

    Status lazyFreeBatch();

    SelectionResult getTransportType(const Request& request,
                                     int transport_index = 0);

    std::vector<TransportType> getSupportedTransports(
        TransportType request_type);

    Status resubmitTransferTask(Batch* batch, size_t task_id);

    Status pollTaskStatus(Batch* batch, size_t task_id,
                          TransferStatus& task_status);

    void updateTaskStatusAfterPoll(Batch* batch, size_t task_id,
                                   TransferStatus& task_status,
                                   bool allow_failover);

    Status getBatchStatus(BatchID batch_id, TransferStatus& overall_status,
                          bool allow_failover);

    SelectionResult resolveTransport(const Request& req, int transport_index,
                                     bool invalidate_on_fail = true);

    // Verify that req.transport_hint is usable for this request
    Status validateTransportHint(const Request& req, size_t request_index);

    Status loadTransports();

    void findStagingPolicy(const Request& req,
                           std::vector<std::string>& policy);

    Status maybeFireSubmitHooks(Batch* batch, bool check = true);

    void recordTaskCompletionMetrics(TaskInfo& task,
                                     TransferStatusEnum prev_status,
                                     TransferStatusEnum new_status);

   private:
    struct AllocatedMemory {
        void* addr;
        size_t size;
        Transport* transport;
        MemoryOptions options;
    };

    struct BatchSet {
        std::unordered_set<Batch*> active;
        std::vector<Batch*> freelist;
    };

   private:
    std::shared_ptr<Config> conf_;
    std::shared_ptr<ControlService> metadata_;
    std::shared_ptr<Topology> topology_;
    std::unique_ptr<TransportSelector> transport_selector_;
    bool available_;

    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transport_list_;
    std::unique_ptr<SegmentTracker> local_segment_tracker_;

    ThreadLocalStorage<BatchSet> batch_set_;

    std::vector<AllocatedMemory> allocated_memory_;
    std::mutex mutex_;

    std::string hostname_;
    uint16_t port_;
    bool ipv6_;
    std::string local_segment_name_;

    std::unique_ptr<ProxyManager> staging_proxy_;
    bool merge_requests_;
    int max_failover_attempts_{3};
    bool enable_auto_failover_on_poll_{true};
    bool enable_progress_worker_{false};

    // Track RDMA failures to skip RDMA when threshold exceeded
    std::atomic<int> rdma_failure_count_{0};
    std::atomic<uint64_t> rdma_failure_window_start_ns_{0};
    int rdma_failure_threshold_{3};
    uint64_t rdma_failure_time_window_ns_{60000000000ULL};  // 60 seconds

    // Guards alive_batches_ and serializes pollTaskStatus /
    // updateTaskStatusAfterPoll / lazyFreeBatch against the optional
    // ProgressWorker thread. Recursive because freeBatch -> lazyFreeBatch ->
    // getTransferStatus can re-enter on the same thread. See issue #2116.
    std::recursive_mutex progress_mutex_;
    std::unordered_set<BatchID> alive_batches_;
    std::unique_ptr<ProgressWorker> progress_worker_;
};
}  // namespace tent
}  // namespace mooncake

#endif