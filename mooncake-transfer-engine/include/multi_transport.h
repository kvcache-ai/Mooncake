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

#ifndef MULTI_TRANSPORT_H_
#define MULTI_TRANSPORT_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <unordered_map>

#include "transport/transport.h"

namespace mooncake {

// Transport health tracking for automatic failover
struct TransportHealth {
    bool healthy = true;
    int consecutive_failures = 0;
    std::chrono::steady_clock::time_point cooldown_until;

    bool isCoolingDown() const {
        return !healthy &&
               std::chrono::steady_clock::now() < cooldown_until;
    }

    void markUnhealthy(int recovery_secs) {
        healthy = false;
        consecutive_failures++;
        cooldown_until =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(recovery_secs);
    }

    void markHealthy() {
        healthy = true;
        consecutive_failures = 0;
    }
};

class MultiTransport {
   public:
    using BatchID = Transport::BatchID;
    using TransferRequest = Transport::TransferRequest;
    using TransferStatus = Transport::TransferStatus;
    using BatchDesc = Transport::BatchDesc;

    MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                   std::string &local_server_name);

    ~MultiTransport();

    BatchID allocateBatchID(size_t batch_size);

    Status freeBatchID(BatchID batch_id);

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries);

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_submitTransfer(BatchID batch_id,
                             const std::vector<TransferRequest> &entries,
                             std::string &proto);
#endif

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status);

    Status getBatchTransferStatus(BatchID batch_id, TransferStatus &status);

    Transport *installTransport(const std::string &proto,
                                std::shared_ptr<Topology> topo);

    Transport *getTransport(const std::string &proto);

    /**
     * @brief Mark a transport as unhealthy (triggers failover).
     */
    void markTransportUnhealthy(const std::string &proto, int recovery_secs);

    /**
     * @brief Mark a transport as healthy again.
     */
    void markTransportHealthy(const std::string &proto);

    /**
     * @brief Check if a specific transport is currently healthy.
     */
    bool isTransportHealthy(const std::string &proto) const;

    /**
     * @brief Check if TCP is the only installed transport.
     *
     * When only TCP transport is available (no RDMA, NVLink, etc.),
     * local memcpy is preferred over TCP loopback for same-host transfers.
     */
    bool isTcpOnly() const;

    std::vector<Transport *> listTransports();

    void *getBaseAddr();

   private:
    Status selectTransport(const TransferRequest &entry, Transport *&transport);

#ifdef ENABLE_MULTI_PROTOCOL
    Status mp_selectTransport(const TransferRequest &entry,
                              Transport *&transport,
                              std::string &preferred_proto);
#endif

    /// Try to recover transports whose cooldown has expired.
    void tryRecoverTransports();

    /// Background health monitor thread.
    void healthMonitorLoop();

   private:
    std::shared_ptr<TransferMetadata> metadata_;
    std::string local_server_name_;
    std::map<std::string, std::shared_ptr<Transport>> transport_map_;
    RWSpinlock batch_desc_lock_;
    std::unordered_map<BatchID, std::shared_ptr<BatchDesc>> batch_desc_set_;

    /// Per-transport health tracking for automatic failover.
    std::map<std::string, TransportHealth> transport_health_;
    mutable std::mutex health_mutex_;

    /// Health monitor thread control.
    std::atomic<bool> health_monitor_running_{false};
    std::thread health_monitor_thread_;

    /// Failover configuration.
    int failover_threshold_ = 3;
    int recovery_secs_ = 30;
};  // class MultiTransport

}  // namespace mooncake

#endif  // MULTI_TRANSPORT_H_