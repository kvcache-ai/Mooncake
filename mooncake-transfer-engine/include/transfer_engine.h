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

#ifndef MULTI_TRANSFER_ENGINE_H_
#define MULTI_TRANSFER_ENGINE_H_

#include <asm-generic/errno-base.h>
#include <bits/stdint-uintn.h>
#include <limits.h>
#include <string.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "memory_location.h"
#include "multi_transport.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#ifdef WITH_METRICS
#include <chrono>
#include <unordered_map>
#include "ylt/metric/counter.hpp"
#include "ylt/metric/histogram.hpp"
#endif

namespace mooncake {
using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentHandle = Transport::SegmentHandle;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;
const static BatchID INVALID_BATCH_ID = UINT64_MAX;
using BufferEntry = Transport::BufferEntry;

class TransferEngine {
   public:
    TransferEngine(bool auto_discover = false)
        : metadata_(nullptr),
          local_topology_(std::make_shared<Topology>()),
          auto_discover_(auto_discover) {
#ifdef WITH_METRICS
        InitializeMetricsConfig();
        StartMetricsReportingThread();
#endif
    }

    TransferEngine(bool auto_discover, const std::vector<std::string> &filter)
        : metadata_(nullptr),
          local_topology_(std::make_shared<Topology>()),
          auto_discover_(auto_discover),
          filter_(filter) {
#ifdef WITH_METRICS
        InitializeMetricsConfig();
        StartMetricsReportingThread();
#endif
    }

    ~TransferEngine() {
#ifdef WITH_METRICS
        StopMetricsReportingThread();
#endif
        freeEngine();
    }

    int init(const std::string &metadata_conn_string,
             const std::string &local_server_name,
             const std::string &ip_or_host_name = "",
             uint64_t rpc_port = 12345);

    int freeEngine();

    // Only for testing.
    Transport *installTransport(const std::string &proto, void **args);

    int uninstallTransport(const std::string &proto);

    std::string getLocalIpAndPort();

    int getRpcPort();

    SegmentHandle openSegment(const std::string &segment_name);

    Status CheckSegmentStatus(SegmentID sid);

    int closeSegment(SegmentHandle handle);

    int removeLocalSegment(const std::string &segment_name);

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location = kWildcardLocation,
                            bool remote_accessible = true,
                            bool update_metadata = true);

    int unregisterLocalMemory(void *addr, bool update_metadata = true);

    int registerLocalMemoryBatch(const std::vector<BufferEntry> &buffer_list,
                                 const std::string &location);

    int unregisterLocalMemoryBatch(const std::vector<void *> &addr_list);

    BatchID allocateBatchID(size_t batch_size) {
        return multi_transports_->allocateBatchID(batch_size);
    }

    Status freeBatchID(BatchID batch_id) {
        return multi_transports_->freeBatchID(batch_id);
    }

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) {
        return multi_transports_->submitTransfer(batch_id, entries);
    }

    Status submitTransferWithNotify(BatchID batch_id,
                                    const std::vector<TransferRequest> &entries,
                                    TransferMetadata::NotifyDesc notify_msg) {
        auto target_id = entries[0].target_id;
        Status s = multi_transports_->submitTransfer(batch_id, entries);
        if (!s.ok()) {
            return s;
        }

        // store notify
        RWSpinlock::WriteGuard guard(send_notifies_lock_);
        notifies_to_send_[batch_id] = std::make_pair(target_id, notify_msg);

        return s;
    }

    int getNotifies(std::vector<TransferMetadata::NotifyDesc> &notifies);

    int sendNotifyByID(SegmentID target_id,
                       TransferMetadata::NotifyDesc notify_msg);

    int sendNotifyByName(std::string remote_agent,
                         TransferMetadata::NotifyDesc notify_msg);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) {
#ifdef WITH_METRICS
        // Record task start time on first query
        RecordTaskStart(batch_id, task_id);
#endif

        Status result =
            multi_transports_->getTransferStatus(batch_id, task_id, status);
#ifdef WITH_METRICS
        if (result.ok()) {
            // Check if task reached a terminal state
            bool is_terminal = (status.s == TransferStatusEnum::COMPLETED ||
                                status.s == TransferStatusEnum::FAILED ||
                                status.s == TransferStatusEnum::CANCELED ||
                                status.s == TransferStatusEnum::TIMEOUT);

            if (is_terminal) {
                // Record latency only for successfully completed tasks
                if (status.s == TransferStatusEnum::COMPLETED) {
                    if (status.transferred_bytes > 0) {
                        transferred_bytes_counter_.inc(
                            status.transferred_bytes);
                    }
                    RecordTaskCompletion(batch_id, task_id);
                } else {
                    // Clean up timing info for failed/canceled/timeout tasks
                    // without recording latency
                    uint64_t task_key = MakeTaskKey(batch_id, task_id);
                    std::lock_guard<std::mutex> lock(task_timing_mutex_);
                    task_start_times_.erase(task_key);
                }
            }
        }
#endif
#ifdef USE_ASCEND_DIRECT
        return result;
#endif
        if (result.ok() && status.s == TransferStatusEnum::COMPLETED) {
            // call getBatchTransferStatus to post notify message
            // when the overall status is COMPLETED
            TransferStatus dummy_status;
            auto status = getBatchTransferStatus(batch_id, dummy_status);
            if (!status.ok()) {
                LOG(ERROR) << status.ToString();
            }
        }
        return result;
    }

    Status getBatchTransferStatus(BatchID batch_id, TransferStatus &status) {
        Status result =
            multi_transports_->getBatchTransferStatus(batch_id, status);
#ifdef WITH_METRICS
        if (result.ok() && status.s == TransferStatusEnum::COMPLETED) {
            if (status.transferred_bytes > 0) {
                transferred_bytes_counter_.inc(status.transferred_bytes);
            }
        }
#endif
        if (result.ok() && status.s == TransferStatusEnum::COMPLETED) {
            // send notify
            RWSpinlock::WriteGuard guard(send_notifies_lock_);
            if (!notifies_to_send_.count(batch_id)) return result;
            auto value = notifies_to_send_[batch_id];
            auto rc = sendNotifyByID(value.first, value.second);
            if (rc) {
                LOG(ERROR) << "Failed to send notify message, error code: "
                           << rc;
            }
            notifies_to_send_.erase(batch_id);
        }
        return result;
    }

    Transport *getTransport(const std::string &proto) {
        return multi_transports_->getTransport(proto);
    }

    int syncSegmentCache(const std::string &segment_name = "") {
        return metadata_->syncSegmentCache(segment_name);
    }

    std::shared_ptr<TransferMetadata> getMetadata() { return metadata_; }

    bool checkOverlap(void *addr, uint64_t length);

    void setAutoDiscover(bool auto_discover) { auto_discover_ = auto_discover; }

    void setWhitelistFilters(std::vector<std::string> &&filters) {
        filter_ = std::move(filters);
    }

    int numContexts() const {
        return (int)local_topology_->getHcaList().size();
    }

    std::shared_ptr<Topology> getLocalTopology() const {
        return local_topology_;
    }

   private:
    struct MemoryRegion {
        void *addr;
        uint64_t length;
        std::string location;
        bool remote_accessible;
    };

    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::shared_ptr<MultiTransport> multi_transports_;
    std::shared_mutex mutex_;
    std::vector<MemoryRegion> local_memory_regions_;
    std::shared_ptr<Topology> local_topology_;

    RWSpinlock send_notifies_lock_;
    std::unordered_map<BatchID,
                       std::pair<SegmentID, TransferMetadata::NotifyDesc>>
        notifies_to_send_;

    // Discover topology and install transports automatically when it's true.
    // Set it to false only for testing.
    bool auto_discover_;
    std::vector<std::string> filter_;
    bool use_barex_ = false;

#ifdef WITH_METRICS
    // Latency bucket in microseconds
    // Fine-grained for sub-ms region, coarse for tail latencies
    inline static const std::vector<double> kTaskLatencyBuckets = {
        // sub-ms: 10μs to 1ms
        10, 20, 50, 100, 200, 500, 1000,
        // 1ms to 10ms
        2000, 5000, 10000,
        // 10ms to 100ms
        20000, 50000, 100000,
        // 100ms to 1s
        200000, 500000, 1000000,
        // > 1s
        2000000, 5000000, 10000000};

    struct TaskTimingInfo {
        std::chrono::steady_clock::time_point start_time;
        bool is_started{false};
    };

    ylt::metric::counter_t transferred_bytes_counter_{
        "transferred bytes", "Measure transferred bytes"};
    ylt::metric::histogram_t task_completion_latency_us_{
        "transfer_task_completion_latency",
        "Transfer task completion latency (us)", kTaskLatencyBuckets};

    // Track task start times: key = (batch_id, task_id)
    std::unordered_map<uint64_t, TaskTimingInfo> task_start_times_;
    std::mutex task_timing_mutex_;

    // Previous snapshot for computing interval statistics
    std::vector<int64_t> prev_bucket_counts_;
    std::mutex metrics_snapshot_mutex_;

    std::thread metrics_reporting_thread_;
    std::atomic<bool> should_stop_metrics_thread_{false};
    bool metrics_enabled_{false};
    uint64_t metrics_interval_seconds_{5};

    // Helper methods for metrics reporting thread management
    void InitializeMetricsConfig();
    void StartMetricsReportingThread();
    void StopMetricsReportingThread();

    // Helper methods for task timing
    uint64_t MakeTaskKey(BatchID batch_id, size_t task_id) const {
// task_id is expected to be the index within a batch (< UINT32_MAX)
// This encoding allows efficient key generation while supporting
// up to 2^32 concurrent batches and 2^32 tasks per batch
// Note: If task_id exceeds 32-bit, key collision may occur, but this
// only affects latency metrics accuracy, not transfer correctness
#ifndef NDEBUG
        if (task_id > 0xFFFFFFFF) {
            LOG(WARNING)
                << "task_id " << task_id
                << " exceeds 32-bit limit, potential key collision in metrics";
        }
#endif
        return (static_cast<uint64_t>(batch_id) << 32) | (task_id & 0xFFFFFFFF);
    }
    void RecordTaskStart(BatchID batch_id, size_t task_id);
    void RecordTaskCompletion(BatchID batch_id, size_t task_id);
#endif
};
}  // namespace mooncake

#endif
