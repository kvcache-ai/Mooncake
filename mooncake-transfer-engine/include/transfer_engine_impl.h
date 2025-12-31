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

#ifndef MULTI_TRANSFER_ENGINE_IMPL_H_
#define MULTI_TRANSFER_ENGINE_IMPL_H_

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
using BufferEntry = Transport::BufferEntry;

class TransferEngineImpl {
   public:
    TransferEngineImpl(bool auto_discover = false)
        : metadata_(nullptr),
          local_topology_(std::make_shared<Topology>()),
          auto_discover_(auto_discover) {
#ifdef WITH_METRICS
        InitializeMetricsConfig();
        StartMetricsReportingThread();
#endif
    }

    TransferEngineImpl(bool auto_discover,
                       const std::vector<std::string>& filter)
        : metadata_(nullptr),
          local_topology_(std::make_shared<Topology>()),
          auto_discover_(auto_discover),
          filter_(filter) {
#ifdef WITH_METRICS
        InitializeMetricsConfig();
        StartMetricsReportingThread();
#endif
    }

    ~TransferEngineImpl() {
#ifdef WITH_METRICS
        StopMetricsReportingThread();
#endif
        freeEngine();
    }

    int init(const std::string& metadata_conn_string,
             const std::string& local_server_name,
             const std::string& ip_or_host_name = "",
             uint64_t rpc_port = 12345);

    int freeEngine();

    // Only for testing.
    Transport* installTransport(const std::string& proto, void** args);

    int uninstallTransport(const std::string& proto);

    std::string getLocalIpAndPort();

    int getRpcPort();

    SegmentHandle openSegment(const std::string& segment_name);

    Status CheckSegmentStatus(SegmentID sid);

    int closeSegment(SegmentHandle handle);

    int removeLocalSegment(const std::string& segment_name);

    int registerLocalMemory(void* addr, size_t length,
                            const std::string& location = kWildcardLocation,
                            bool remote_accessible = true,
                            bool update_metadata = true);

    int unregisterLocalMemory(void* addr, bool update_metadata = true);

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location);

    int unregisterLocalMemoryBatch(const std::vector<void*>& addr_list);

    BatchID allocateBatchID(size_t batch_size) {
        return multi_transports_->allocateBatchID(batch_size);
    }

    Status freeBatchID(BatchID batch_id) {
        return multi_transports_->freeBatchID(batch_id);
    }

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries) {
        Status s = multi_transports_->submitTransfer(batch_id, entries);
#ifdef WITH_METRICS
        if (metrics_enabled_ && s.ok()) {
            auto& batch = Transport::toBatchDesc(batch_id);
            auto now = std::chrono::steady_clock::now();
            for (auto& task : batch.task_list) {
                if (task.start_time.time_since_epoch().count() == 0) {
                    task.start_time = now;
                }
            }
        }
#endif
        return s;
    }

    Status submitTransferWithNotify(BatchID batch_id,
                                    const std::vector<TransferRequest>& entries,
                                    TransferMetadata::NotifyDesc notify_msg) {
        auto target_id = entries[0].target_id;
        Status s = multi_transports_->submitTransfer(batch_id, entries);
        if (!s.ok()) {
            return s;
        }

#ifdef WITH_METRICS
        if (metrics_enabled_) {
            auto& batch = Transport::toBatchDesc(batch_id);
            auto now = std::chrono::steady_clock::now();
            for (auto& task : batch.task_list) {
                if (task.start_time.time_since_epoch().count() == 0) {
                    task.start_time = now;
                }
            }
        }
#endif

        // store notify
        RWSpinlock::WriteGuard guard(send_notifies_lock_);
        notifies_to_send_[batch_id] = std::make_pair(target_id, notify_msg);

        return s;
    }

    int getNotifies(std::vector<TransferMetadata::NotifyDesc>& notifies);

    int sendNotifyByID(SegmentID target_id,
                       TransferMetadata::NotifyDesc notify_msg);

    int sendNotifyByName(std::string remote_agent,
                         TransferMetadata::NotifyDesc notify_msg);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) {
        Status result =
            multi_transports_->getTransferStatus(batch_id, task_id, status);
#ifdef WITH_METRICS
        if (!metrics_enabled_ || !result.ok()) {
            goto metrics_done;
        }

        {
            bool is_terminal = (status.s == TransferStatusEnum::COMPLETED ||
                                status.s == TransferStatusEnum::FAILED ||
                                status.s == TransferStatusEnum::CANCELED ||
                                status.s == TransferStatusEnum::TIMEOUT);
            if (!is_terminal) {
                goto metrics_done;
            }

            auto& batch = Transport::toBatchDesc(batch_id);
            if (task_id >= batch.task_list.size()) {
                goto metrics_done;
            }

            auto& task = batch.task_list[task_id];
            auto start = task.start_time;
            if (start.time_since_epoch().count() == 0) {
                goto metrics_done;
            }

            // Only record metrics for successful completions
            if (status.s == TransferStatusEnum::COMPLETED) {
                if (status.transferred_bytes > 0) {
                    transferred_bytes_counter_.inc(status.transferred_bytes);
                }
                auto now = std::chrono::steady_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        now - start);
                task_completion_latency_us_.observe(duration.count());
            }

            // Reset start_time to prevent duplicate processing
            task.start_time = std::chrono::steady_clock::time_point();
        }
    metrics_done:
#endif
#ifdef USE_ASCEND_DIRECT
        return result;
#endif
        if (result.ok() && status.s == TransferStatusEnum::COMPLETED) {
            // call getBatchTransferStatus to post notify message
            // when the overall status is COMPLETED
            // skip_metrics=true to avoid double counting since we already
            // recorded metrics above
            TransferStatus dummy_status;
            auto status = getBatchTransferStatus(batch_id, dummy_status, true);
            if (!status.ok()) {
                LOG(ERROR) << status.ToString();
            }
        }
        return result;
    }

    Status getBatchTransferStatus(BatchID batch_id, TransferStatus& status,
                                  bool skip_metrics = false) {
        Status result =
            multi_transports_->getBatchTransferStatus(batch_id, status);
#ifdef WITH_METRICS
        if (metrics_enabled_ && !skip_metrics && result.ok() &&
            status.s == TransferStatusEnum::COMPLETED) {
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

    Transport* getTransport(const std::string& proto) {
        return multi_transports_->getTransport(proto);
    }

    int syncSegmentCache(const std::string& segment_name = "") {
        return metadata_->syncSegmentCache(segment_name);
    }

    std::shared_ptr<TransferMetadata> getMetadata() { return metadata_; }

    bool checkOverlap(void* addr, uint64_t length);

    void setAutoDiscover(bool auto_discover) { auto_discover_ = auto_discover; }

    void setWhitelistFilters(std::vector<std::string>&& filters) {
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
        void* addr;
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
    inline static const std::vector<double> kTaskLatencyBuckets = {
        10,     20,      50,      100,     200,     500,    1000,
        2000,   5000,    10000,   20000,   50000,   100000, 200000,
        500000, 1000000, 2000000, 5000000, 10000000};

    ylt::metric::counter_t transferred_bytes_counter_{
        "transferred bytes", "Measure transferred bytes"};
    ylt::metric::histogram_t task_completion_latency_us_{
        "transfer_task_completion_latency",
        "Transfer task completion latency (us)", kTaskLatencyBuckets};

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
#endif
};
}  // namespace mooncake

#endif
