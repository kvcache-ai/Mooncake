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

#include <limits.h>
#include <string.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "memory_location.h"
#include "multi_transport.h"
#include "transfer_metadata.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "trace_support.h"
#include "tracing_facade.h"
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

#ifdef ENABLE_MULTI_PROTOCOL
using RegisteredBuffer = TransferEngine::RegisteredBuffer;
#endif

inline const char* ToTransferStatusName(TransferStatusEnum status) {
    switch (status) {
        case TransferStatusEnum::WAITING:
            return "WAITING";
        case TransferStatusEnum::PENDING:
            return "PENDING";
        case TransferStatusEnum::INVALID:
            return "INVALID";
        case TransferStatusEnum::CANCELED:
            return "CANCELED";
        case TransferStatusEnum::COMPLETED:
            return "COMPLETED";
        case TransferStatusEnum::TIMEOUT:
            return "TIMEOUT";
        case TransferStatusEnum::FAILED:
            return "FAILED";
    }
    return "UNKNOWN";
}

class ActiveBatchTraceRegistry {
   public:
    struct EnsureResult {
        mooncake::tracing::TraceContext context;
        bool created{false};
    };

    EnsureResult EnsureRoot(mooncake::tracing::TracingFacade& tracing,
                            BatchID batch_id,
                            const mooncake::tracing::TraceContext* parent =
                                nullptr) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = spans_.find(batch_id);
        if (it != spans_.end()) {
            return {it->second.context, false};
        }

        auto root = tracing.StartSpan(
            "te.batch", parent && parent->valid() ? parent : nullptr,
                                      {{"te.batch_id",
                                        std::to_string(batch_id)}});
        if (!root.valid()) {
            return {};
        }
        root.AddEvent("batch trace root established");

        auto [inserted, ok] = spans_.try_emplace(batch_id);
        inserted->second.context = root.context();
        inserted->second.span = std::move(root);
        return {inserted->second.context, ok};
    }

    EnsureResult EnsureDataPlane(mooncake::tracing::TracingFacade& tracing,
                                 BatchID batch_id) {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = spans_.find(batch_id);
        if (it == spans_.end()) {
            return {};
        }
        if (it->second.data_plane_started) {
            return {it->second.data_plane_context, false};
        }

        auto span = tracing.StartSpan("te.batch.execute", &it->second.context,
                                      {{"te.batch_id",
                                        std::to_string(batch_id)}});
        if (!span.valid()) {
            return {};
        }

        it->second.data_plane_context = span.context();
        it->second.data_plane_span = std::move(span);
        it->second.data_plane_started = true;
        return {it->second.data_plane_context, true};
    }

    std::optional<mooncake::tracing::TraceContext> LookupContext(
        BatchID batch_id) const {
        std::lock_guard<std::mutex> guard(mutex_);
        auto it = spans_.find(batch_id);
        if (it == spans_.end()) {
            return std::nullopt;
        }
        return it->second.context;
    }

    bool Finish(BatchID batch_id, const std::string& event_name,
                const mooncake::tracing::TraceAttrs& attrs = {},
                bool error = false) {
        std::optional<ActiveBatchSpan> active;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            auto it = spans_.find(batch_id);
            if (it == spans_.end()) {
                return false;
            }
            active.emplace(std::move(it->second));
            spans_.erase(it);
        }

        for (const auto& attr : attrs) {
            active->span.SetAttribute(attr.first, attr.second);
        }
        if (active->data_plane_started) {
            for (const auto& attr : attrs) {
                active->data_plane_span.SetAttribute(attr.first, attr.second);
            }
            active->data_plane_span.AddEvent("batch data plane finished", attrs);
            if (error) {
                active->data_plane_span.SetStatus("ERROR");
            }
            active->data_plane_span.End();
        }
        active->span.AddEvent(event_name, attrs);
        if (error) {
            active->span.SetStatus("ERROR");
        }
        active->span.End();
        return true;
    }

   private:
    struct ActiveBatchSpan {
        mooncake::tracing::Span span;
        mooncake::tracing::TraceContext context;
        mooncake::tracing::Span data_plane_span;
        mooncake::tracing::TraceContext data_plane_context;
        bool data_plane_started{false};
    };

    mutable std::mutex mutex_;
    std::unordered_map<BatchID, ActiveBatchSpan> spans_;
};

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

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest>& entries,
                          const mooncake::tracing::TraceContext* trace_context =
                              nullptr) {
        auto span = StartBatchOperationSpan(
            "submitTransfer", batch_id, trace_context,
            {{"te.batch_id", std::to_string(batch_id)},
             {"transfer.count", std::to_string(entries.size())}});
        Status s = multi_transports_->submitTransfer(batch_id, entries);
        if (!s.ok()) span.SetStatus("ERROR");
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
                                    TransferMetadata::NotifyDesc notify_msg,
                                    const mooncake::tracing::TraceContext* trace_context =
                                        nullptr) {
        auto span = StartBatchOperationSpan(
            "submitTransferWithNotify", batch_id, trace_context,
            {{"te.batch_id", std::to_string(batch_id)}});
        auto target_id = entries[0].target_id;
        notify_msg.trace_carrier = mooncake::tracing::EncodeCarrierString(
            mooncake::tracing::ToCarrier(span.context()));
        span.AddEvent("carrier encode",
                      {{"trace_carrier.present",
                        notify_msg.trace_carrier.empty() ? "false" : "true"}});
        Status s = multi_transports_->submitTransfer(batch_id, entries);
        if (!s.ok()) span.SetStatus("ERROR");
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
        span.AddEvent("notify queued",
                      {{"target.id", std::to_string(target_id)}});

        return s;
    }

#ifdef ENABLE_MULTI_PROTOCOL
    // Multi-protocol API
    // Supports registering memory for multiple protocols (CXL, TCP / RDMA)
    int mp_registerLocalMemory(
        std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
            buffer_map);

    int mp_unregisterLocalMemory(
        std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
            buffer_map);

    Status mp_submitTransfer(BatchID batch_id,
                             const std::vector<TransferRequest>& entries,
                             std::string& proto) {
        Status s =
            multi_transports_->mp_submitTransfer(batch_id, entries, proto);
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

    Status mp_submitTransferWithNotify(
        BatchID batch_id, const std::vector<TransferRequest>& entries,
        TransferMetadata::NotifyDesc notify_msg, std::string& proto) {
        auto target_id = entries[0].target_id;
        Status s =
            multi_transports_->mp_submitTransfer(batch_id, entries, proto);
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
#endif

    int registerLocalMemoryBatch(const std::vector<BufferEntry>& buffer_list,
                                 const std::string& location);

    int unregisterLocalMemoryBatch(const std::vector<void*>& addr_list);

    BatchID allocateBatchID(size_t batch_size) {
        return multi_transports_->allocateBatchID(batch_size);
    }

    Status freeBatchID(BatchID batch_id) {
        TransferStatus batch_status;
        const Status status_before_free =
            multi_transports_->getBatchTransferStatus(batch_id, batch_status);
        const Status free_status = multi_transports_->freeBatchID(batch_id);
        if (!free_status.ok()) {
            return free_status;
        }
        if (status_before_free.ok() &&
            (batch_status.s == TransferStatusEnum::COMPLETED ||
             batch_status.s == TransferStatusEnum::FAILED ||
             batch_status.s == TransferStatusEnum::TIMEOUT ||
             batch_status.s == TransferStatusEnum::CANCELED)) {
            const bool error = batch_status.s != TransferStatusEnum::COMPLETED;
            active_batch_traces_.Finish(
                batch_id, "batch terminal status",
                {{"status", ToTransferStatusName(batch_status.s)},
                 {"transferred.bytes",
                  std::to_string(batch_status.transferred_bytes)}},
                error);
        } else {
            active_batch_traces_.Finish(
                batch_id, "batch released before terminal",
                {{"status", "UNKNOWN"}}, true);
        }
        return free_status;
    }

    int getNotifies(std::vector<TransferMetadata::NotifyDesc>& notifies);

    int sendNotifyByID(SegmentID target_id,
                       TransferMetadata::NotifyDesc notify_msg);

    int sendNotifyByName(std::string remote_agent,
                         TransferMetadata::NotifyDesc notify_msg);

    int probePeerAliveByID(SegmentID target_id);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) {
        Status result =
            multi_transports_->getTransferStatus(batch_id, task_id, status);
        const bool is_terminal =
            result.ok() &&
            (status.s == TransferStatusEnum::COMPLETED ||
             status.s == TransferStatusEnum::FAILED ||
             status.s == TransferStatusEnum::TIMEOUT ||
             status.s == TransferStatusEnum::CANCELED);
        if (!result.ok() || is_terminal) {
            auto span = StartBatchOperationSpan(
                "getTransferStatus", batch_id, nullptr,
                {{"te.batch_id", std::to_string(batch_id)},
                 {"task.id", std::to_string(task_id)},
                 {"status",
                  result.ok() ? ToTransferStatusName(status.s) : "ERROR"}});
            if (!result.ok()) {
                span.SetStatus("ERROR");
                span.AddEvent("status query failed",
                              {{"task.id", std::to_string(task_id)},
                               {"error.message", std::string(result.message())}});
            } else {
                span.AddEvent("task terminal status",
                              {{"task.id", std::to_string(task_id)},
                               {"status", ToTransferStatusName(status.s)},
                               {"transferred.bytes",
                                std::to_string(status.transferred_bytes)}});
                if (status.s != TransferStatusEnum::COMPLETED) {
                    span.SetStatus("ERROR");
                }
            }
        }
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
        if (result.ok() &&
            (status.s == TransferStatusEnum::COMPLETED ||
             status.s == TransferStatusEnum::FAILED ||
             status.s == TransferStatusEnum::TIMEOUT ||
             status.s == TransferStatusEnum::CANCELED)) {
            auto batch_context = active_batch_traces_.LookupContext(batch_id);
            auto& tracing = mooncake::tracing::TracingFacade::Instance(
                "mooncake-transfer-engine", "transfer-engine");
            mooncake::tracing::Span span;
            if (batch_context.has_value()) {
                span = tracing.StartSpan(
                    "getBatchTransferStatus", &batch_context.value(),
                    {{"te.batch_id", std::to_string(batch_id)},
                     {"status", ToTransferStatusName(status.s)}});
                span.AddEvent("batch terminal status");
            }
            if (status.s == TransferStatusEnum::COMPLETED) {
                RWSpinlock::WriteGuard guard(send_notifies_lock_);
                auto it = notifies_to_send_.find(batch_id);
                if (it != notifies_to_send_.end()) {
                    auto rc = sendNotifyByID(it->second.first, it->second.second);
                    if (rc) {
                        LOG(ERROR) << "Failed to send notify message, error code: "
                                   << rc;
                        span.SetStatus("ERROR");
                    }
                    notifies_to_send_.erase(it);
                }
            }
            const bool error = status.s != TransferStatusEnum::COMPLETED;
            active_batch_traces_.Finish(
                batch_id, "batch terminal status",
                {{"status", ToTransferStatusName(status.s)},
                 {"transferred.bytes",
                  std::to_string(status.transferred_bytes)}},
                error);
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

#ifdef ENABLE_MULTI_PROTOCOL
    struct RegisteredRecord {
        Transport* transport;
        void* addr;
        uint64_t length;
        std::string location;
        bool remote_accessible;
    };
    void rollbackAllRegistrations(const std::vector<RegisteredRecord>& records);
#endif

    void setAutoDiscover(bool auto_discover) { auto_discover_ = auto_discover; }

    void* getBaseAddr() { return multi_transports_->getBaseAddr(); }

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
    mooncake::tracing::Span StartBatchOperationSpan(
        const std::string& span_name, BatchID batch_id,
        const mooncake::tracing::TraceContext* trace_context,
        const mooncake::tracing::TraceAttrs& attrs) {
        auto& tracing = mooncake::tracing::TracingFacade::Instance(
            "mooncake-transfer-engine", "transfer-engine");
        auto ensured =
            active_batch_traces_.EnsureRoot(tracing, batch_id, trace_context);
        if (span_name == "submitTransfer" ||
            span_name == "submitTransferWithNotify") {
            auto data_plane = active_batch_traces_.EnsureDataPlane(tracing, batch_id);
            auto transport_context =
                data_plane.context.valid() ? data_plane.context : ensured.context;
            if (ensured.created && transport_context.valid()) {
                multi_transports_->SetBatchTraceContext(batch_id, transport_context);
            }
        } else if (ensured.created && ensured.context.valid()) {
            multi_transports_->SetBatchTraceContext(batch_id, ensured.context);
        }
        return tracing.StartSpan(span_name, &ensured.context, attrs);
    }

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

    ActiveBatchTraceRegistry active_batch_traces_;

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
