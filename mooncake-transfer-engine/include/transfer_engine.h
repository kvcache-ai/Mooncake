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
#include <mutex>
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
#endif

namespace mooncake {
using TransferRequest = Transport::TransferRequest;
using TransferStatus = Transport::TransferStatus;
using TransferStatusEnum = Transport::TransferStatusEnum;
using SegmentHandle = Transport::SegmentHandle;
using SegmentID = Transport::SegmentID;
using BatchID = Transport::BatchID;
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

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) {
        Status result =
            multi_transports_->getTransferStatus(batch_id, task_id, status);
#ifdef WITH_METRICS
        if (result.ok() && status.s == TransferStatusEnum::COMPLETED) {
            if (status.transferred_bytes > 0) {
                transferred_bytes_counter_.inc(status.transferred_bytes);
            }
        }
#endif
        return result;
    }

    int syncSegmentCache(const std::string &segment_name = "") {
        return metadata_->syncSegmentCache(segment_name);
    }

    std::shared_ptr<TransferMetadata> getMetadata() { return metadata_; }

    bool checkOverlap(void *addr, uint64_t length);

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
    // Discover topology and install transports automatically when it's true.
    // Set it to false only for testing.
    bool auto_discover_;
    std::vector<std::string> filter_;

#ifdef WITH_METRICS
    ylt::metric::counter_t transferred_bytes_counter_{
        "transferred bytes", "Measure transferred bytes"};
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
