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

#include "multi_transport.h"
#include <algorithm>
#include <sstream>
#include <string>

#include "config.h"
#include "transport/rdma_transport/rdma_transport.h"
#ifdef USE_BAREX
#include "transport/barex_transport/barex_transport.h"
#endif
#ifdef USE_TCP
#include "transport/tcp_transport/tcp_transport.h"
#include "transport/rdma_transport/mock_rdma_transport.h"
#endif
#include "transport/transport.h"
#ifdef USE_NVMEOF
#include "transport/nvmeof_transport/nvmeof_transport.h"
#endif
#ifdef USE_ASCEND_DIRECT
#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"
#endif
#if defined(USE_ASCEND) && !defined(USE_ASCEND_DIRECT)
#include "transport/ascend_transport/hccl_transport/hccl_transport.h"
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
#include "transport/ascend_transport/heterogeneous_rdma_transport.h"
#endif
#ifdef USE_INTRA_NVLINK
#include "transport/intranode_nvlink_transport/intranode_nvlink_transport.h"
#endif
#ifdef USE_HIP
#include "transport/hip_transport/hip_transport.h"
#endif
#ifdef USE_MACA
#include "transport/maca_transport/maca_transport.h"
#endif
#ifdef USE_MNNVL
#include "transport/nvlink_transport/nvlink_transport.h"
#endif
#ifdef USE_CXL
#include "transport/cxl_transport/cxl_transport.h"
#endif
#ifdef USE_UBSHMEM
#include "transport/ascend_transport/ubshmem_transport/ubshmem_transport.h"
#endif
#ifdef USE_EFA
#include "transport/efa_transport/efa_transport.h"
#endif
#ifdef USE_UB
#include "transport/kunpeng_transport/ub_transport.h"
#endif

#include <cassert>

namespace mooncake {
MultiTransport::MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                               std::string& local_server_name)
    : metadata_(metadata), local_server_name_(local_server_name) {
    // Load failover configuration from environment variables
    const char* threshold = std::getenv("MC_FAILOVER_THRESHOLD");
    if (threshold) {
        failover_threshold_ = std::stoi(threshold);
        if (failover_threshold_ < 1) failover_threshold_ = 1;
    }
    const char* recovery = std::getenv("MC_FAILOVER_RECOVERY_SECS");
    if (recovery) {
        recovery_secs_ = std::stoi(recovery);
        if (recovery_secs_ < 1) recovery_secs_ = 1;
    }

    // Start health monitor background thread
    const char* failover_enabled = std::getenv("MC_FAILOVER_ENABLED");
    bool enable_monitor = true;  // Enabled by default
    if (failover_enabled) {
        std::string val(failover_enabled);
        enable_monitor = (val == "1" || val == "true" || val == "yes");
    }

    if (enable_monitor) {
        health_monitor_running_.store(true, std::memory_order_release);
        health_monitor_thread_ = std::thread(&MultiTransport::healthMonitorLoop, this);
        LOG(INFO) << "[MultiTransport] Failover monitor started "
                  << "(threshold=" << failover_threshold_
                  << ", recovery=" << recovery_secs_ << "s)";
    }
}

MultiTransport::~MultiTransport() {
    // Stop health monitor thread
    health_monitor_running_.store(false, std::memory_order_release);
    if (health_monitor_thread_.joinable()) {
        health_monitor_thread_.join();
    }
}

MultiTransport::BatchID MultiTransport::allocateBatchID(size_t batch_size) {
    auto batch_desc = new BatchDesc();
    if (!batch_desc) return ERR_MEMORY;
    batch_desc->id = BatchID(batch_desc);
    batch_desc->batch_size = batch_size;
    batch_desc->task_list.reserve(batch_size);
    batch_desc->context = NULL;
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_lock_.lock();
    batch_desc_set_[batch_desc->id] = batch_desc;
    batch_desc_lock_.unlock();
#endif
    return batch_desc->id;
}

Status MultiTransport::freeBatchID(BatchID batch_id) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        if (!batch_desc.task_list[task_id].is_finished) {
            return Status::BatchBusy(
                "BatchID cannot be freed until all tasks are done");
        }
    }
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_.erase(batch_id);
#endif
    return Status::OK();
}

Status MultiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests(
            "Exceed the limitation of batch capacity");
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    std::unordered_map<Transport*, std::vector<Transport::TransferTask*> >
        submit_tasks;
    for (auto& request : entries) {
        Transport* transport = nullptr;
        auto status = selectTransport(request, transport);
        if (!status.ok()) return status;
        assert(transport);
        auto& task = batch_desc.task_list[task_id];
        task.batch_id = batch_id;
        task.transport_ = transport;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<Transport::TransferRequest*>(&request);
#else
        task.request = &request;
#endif
        ++task_id;
        submit_tasks[transport].push_back(&task);
    }
    Status overall_status = Status::OK();
    for (auto& entry : submit_tasks) {
        auto* transport = entry.first;
        auto* proto_name = transport->getName();
        auto status = transport->submitTransferTask(entry.second);
        if (!status.ok()) {
            // Check if this is an RDMA transport that should trigger failover
            std::string proto_str(proto_name);
            if (proto_str == "rdma" || proto_str == "mock_rdma" ||
                proto_str == "ascend") {
                std::lock_guard<std::mutex> lock(health_mutex_);
                auto it = transport_health_.find(proto_str);
                if (it != transport_health_.end()) {
                    it->second.consecutive_failures++;
                    if (it->second.consecutive_failures >= failover_threshold_) {
                        it->second.markUnhealthy(recovery_secs_);
                        LOG(WARNING) << "[MultiTransport] " << proto_str
                                    << " marked unhealthy after "
                                    << it->second.consecutive_failures
                                    << " consecutive failures. "
                                    << "Cooldown for " << recovery_secs_ << "s";
                    }
                } else {
                    TransportHealth health;
                    health.consecutive_failures = 1;
                    transport_health_[proto_str] = health;
                }
            }

            // Attempt TCP fallback for this batch
            if (transport_map_.count("tcp")) {
                auto* tcp = transport_map_["tcp"].get();
                if (tcp != transport) {
                    LOG(WARNING) << "[MultiTransport] " << proto_str
                                << " submit failed, retrying with TCP";
                    auto retry_status = tcp->submitTransferTask(entry.second);
                    if (retry_status.ok()) {
                        overall_status = Status::OK();
                        continue;
                    }
                }
            }
            overall_status = status;
        } else {
            // Success: reset consecutive failure counter for RDMA
            std::string proto_str(proto_name);
            if (proto_str == "rdma" || proto_str == "mock_rdma" ||
                proto_str == "ascend") {
                std::lock_guard<std::mutex> lock(health_mutex_);
                auto it = transport_health_.find(proto_str);
                if (it != transport_health_.end()) {
                    if (it->second.consecutive_failures > 0) {
                        it->second.consecutive_failures = 0;
                        it->second.markHealthy();
                        LOG(INFO) << "[MultiTransport] " << proto_str
                                 << " recovered after successful transfer";
                    }
                }
            }
        }
    }
    return overall_status;
}

#ifdef ENABLE_MULTI_PROTOCOL
Status MultiTransport::mp_submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    std::string& proto) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests(
            "Exceed the limitation of batch capacity");
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    std::unordered_map<Transport*, std::vector<Transport::TransferTask*> >
        submit_tasks;
    for (auto& request : entries) {
        Transport* transport = nullptr;
        auto status = mp_selectTransport(request, transport, proto);
        if (!status.ok()) return status;
        assert(transport);
        auto& task = batch_desc.task_list[task_id];
        task.batch_id = batch_id;
#ifdef USE_ASCEND_HETEROGENEOUS
        task.request = const_cast<Transport::TransferRequest*>(&request);
#else
        task.request = &request;
#endif
        ++task_id;
        submit_tasks[transport].push_back(&task);
    }
    Status overall_status = Status::OK();
    for (auto& entry : submit_tasks) {
        auto status = entry.first->submitTransferTask(entry.second);
        if (!status.ok()) {
            // LOG(ERROR) << "Failed to submit transfer task to "
            //            << entry.first->getName();
            overall_status = status;
        }
    }
    return overall_status;
}
#endif

Status MultiTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument("Task ID out of range");
    }
    auto& task = batch_desc.task_list[task_id];

    // Helper: check if any slice has exceeded the configured timeout.
    // Returns true if a timeout was detected (and logs it).
    auto checkSliceTimeout = [&](const Transport::TransferTask& t) -> bool {
        if (globalConfig().slice_timeout <= 0) return false;
        auto current_ts = getCurrentTimeInNano();
        const int64_t kPacketDeliveryTimeout =
            globalConfig().slice_timeout * 1000000000;
        for (auto& slice : t.slice_list) {
            auto ts = slice->ts;
            if (ts > 0 && current_ts > ts &&
                current_ts - ts > kPacketDeliveryTimeout) {
                LOG(INFO) << "Slice timeout detected";
                return true;
            }
        }
        return false;
    };

    // If the task has an associated transport, delegate to its
    // getTransferStatus() to trigger transport-specific completion
    // polling. For example, the NVLink async transport polls CUDA
    // streams via cudaStreamQuery() here; without this call the
    // slice statuses (and therefore success/failed_slice_count)
    // would never be updated.
    if (task.transport_) {
        auto ret =
            task.transport_->getTransferStatus(batch_id, task_id, status);
        if (!ret.ok()) return ret;

        // Apply timeout check on top of the transport's result.
        if (status.s == Transport::TransferStatusEnum::WAITING &&
            checkSliceTimeout(task)) {
            status.s = Transport::TransferStatusEnum::TIMEOUT;
        }
        return Status::OK();
    }

    // Fallback for tasks without a transport pointer (legacy path)
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    assert(task.slice_count);
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = Transport::TransferStatusEnum::FAILED;
        } else {
            status.s = Transport::TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        if (checkSliceTimeout(task)) {
            status.s = Transport::TransferStatusEnum::TIMEOUT;
        } else {
            status.s = Transport::TransferStatusEnum::WAITING;
        }
    }
    return Status::OK();
}

Status MultiTransport::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    status.transferred_bytes = 0;

    if (batch_desc.is_finished.load(std::memory_order_acquire) ||
        task_count == 0) {
        status.s = Transport::TransferStatusEnum::COMPLETED;
        status.transferred_bytes =
            batch_desc.finished_transfer_bytes.load(std::memory_order_relaxed);
        return Status::OK();
    }

    size_t success_count = 0;
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        TransferStatus task_status;
        auto ret = getTransferStatus(batch_id, task_id, task_status);

        if (!ret.ok()) {
            status.s = Transport::TransferStatusEnum::FAILED;
            return Status::OK();
        }

        if (task_status.s == Transport::TransferStatusEnum::COMPLETED) {
            status.transferred_bytes += task_status.transferred_bytes;
            success_count++;
        } else if (task_status.s == Transport::TransferStatusEnum::FAILED) {
            status.s = Transport::TransferStatusEnum::FAILED;
            return Status::OK();
        }
    }

    status.s = (success_count == task_count)
                   ? Transport::TransferStatusEnum::COMPLETED
                   : Transport::TransferStatusEnum::WAITING;
    if (status.s == Transport::TransferStatusEnum::COMPLETED) {
        batch_desc.is_finished.store(true, std::memory_order_release);
        batch_desc.finished_transfer_bytes.store(status.transferred_bytes,
                                                 std::memory_order_release);
    } else if (status.s == Transport::TransferStatusEnum::FAILED) {
        batch_desc.has_failure.store(true, std::memory_order_release);
    }
    return Status::OK();
}

Transport* MultiTransport::installTransport(const std::string& proto,
                                            std::shared_ptr<Topology> topo) {
    Transport* transport = nullptr;
    if (std::string(proto) == "rdma") {
        if (isMockRdmaEnabled()) {
            transport = new MockRdmaTransport();
        } else {
            transport = new RdmaTransport();
        }
    }
#ifdef USE_UB
    else if (std::string(proto) == "ub") {
        transport = new UbTransport();
    }
#endif
#ifdef USE_BAREX
    else if (std::string(proto) == "barex") {
        transport = new BarexTransport();
    }
#endif
#ifdef USE_TCP
    else if (std::string(proto) == "tcp") {
        transport = new TcpTransport();
    }
#endif
#ifdef USE_NVMEOF
    else if (std::string(proto) == "nvmeof") {
        transport = new NVMeoFTransport();
    }
#endif
#ifdef USE_ASCEND_DIRECT
    else if (std::string(proto) == "ascend") {
        transport = new AscendDirectTransport();
    }
#endif
#ifdef USE_ASCEND
    else if (std::string(proto) == "ascend") {
        transport = new HcclTransport();
    }
#endif
#ifdef USE_ASCEND_HETEROGENEOUS
    else if (std::string(proto) == "ascend") {
        transport = new HeterogeneousRdmaTransport();
    }
#endif

#ifdef USE_INTRA_NVLINK
    else if (std::string(proto) == "nvlink_intra") {
        transport = new IntraNodeNvlinkTransport();
    }
#endif

#ifdef USE_HIP
    else if (std::string(proto) == "hip") {
        transport = new HipTransport();
    }
#endif
#ifdef USE_MACA
    else if (std::string(proto) == "maca") {
        transport = new MacaTransport();
    }
#endif
#ifdef USE_MNNVL
    else if (std::string(proto) == "nvlink") {
        transport = new NvlinkTransport();
    }
#endif  // USE_MNNVL
#ifdef USE_CXL
    else if (std::string(proto) == "cxl") {
        transport = new CxlTransport();
    }
#endif
#ifdef USE_UBSHMEM
    else if (std::string(proto) == "ubshmem") {
        transport = new UBShmemTransport();
    }
#endif
#ifdef USE_EFA
    else if (std::string(proto) == "efa") {
        transport = new EfaTransport();
    }
#endif

    if (!transport) {
        LOG(ERROR) << "Unsupported transport " << proto
                   << ", please rebuild Mooncake";
        return nullptr;
    }

#ifdef USE_BAREX
    bool use_eic = false;
    for (auto& dev : topo->getHcaList()) {
        if (dev.find("soe") != std::string::npos ||
            dev.find("solar") != std::string::npos) {
            use_eic = true;
        }
    }

    if (std::string(proto) == "barex") {
        std::string nics;
        for (auto& dev : topo->getHcaList()) {
            if (use_eic) {
                if (dev.find("soe") == std::string::npos &&
                    dev.find("solar") == std::string::npos) {
                    // ignore no eic nics
                    continue;
                }
            }
            nics += dev;
            nics += ",";
        }

        // Remove the last extra comma
        if (!nics.empty()) {
            nics.pop_back();
        }

        if (!nics.empty()) {
            LOG(INFO) << "ACCL_USE_NICS is set to " << nics;
            setenv("ACCL_USE_NICS", nics.c_str(), 1);
        }
    }
#endif
    if (transport->install(local_server_name_, metadata_, topo)) {
        return nullptr;
    }

    transport_map_[proto] = std::shared_ptr<Transport>(transport);

    // When installing RDMA transport, also register TCP as fallback if available.
    // TCP transport is installed via installTransport("tcp", topo) so it goes
    // through the normal install path (which calls the virtual install method).
    if ((proto == "rdma") && !transport_map_.count("tcp")) {
#ifdef USE_TCP
        LOG(INFO) << "[MultiTransport] Auto-installing TCP transport as "
                 << "RDMA failover fallback";
        Transport* tcp = installTransport("tcp", topo);
        if (!tcp) {
            LOG(WARNING) << "[MultiTransport] Failed to install TCP fallback";
        }
#endif
    }

    return transport;
}

Status MultiTransport::selectTransport(const TransferRequest& entry,
                                       Transport*& transport) {
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        return Status::InvalidArgument("Invalid target segment ID " +
                                       std::to_string(entry.target_id));
    }
    auto proto = target_segment_desc->protocol;
#ifdef USE_ASCEND_HETEROGENEOUS
    // When USE_ASCEND_HETEROGENEOUS is enabled:
    // - Target side directly reuses RDMA Transport
    // - Initiator side uses heterogeneous_rdma_transport
    if (target_segment_desc->protocol == "rdma") {
        proto = "ascend";
    }
#endif
    // Failover: if RDMA transport is unhealthy, try TCP fallback
    if (proto == "rdma" || proto == "ascend") {
        std::lock_guard<std::mutex> lock(health_mutex_);
        auto it = transport_health_.find(proto);
        if (it != transport_health_.end() && !it->second.healthy) {
            if (it->second.isCoolingDown()) {
                // RDMA is in cooldown, try TCP fallback
                if (transport_map_.count("tcp")) {
                    LOG(WARNING) << "[MultiTransport] RDMA unhealthy, "
                                << "falling back to TCP";
                    transport = transport_map_["tcp"].get();
                    return Status::OK();
                }
            } else {
                // Cooldown expired, allow retry but clear health
                it->second.markHealthy();
                LOG(INFO) << "[MultiTransport] RDMA cooldown expired, "
                         << "retrying RDMA";
            }
        }
    }

    if (!transport_map_.count(proto)) {
        return Status::NotSupportedTransport("Transport " + proto +
                                             " not installed");
    }
    transport = transport_map_[proto].get();
    return Status::OK();
}

#ifdef ENABLE_MULTI_PROTOCOL
Status MultiTransport::mp_selectTransport(const TransferRequest& entry,
                                          Transport*& transport,
                                          std::string& preferred_proto) {
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        return Status::InvalidArgument("Invalid target segment ID " +
                                       std::to_string(entry.target_id));
    }
    // Parse comma-separated protocols
    std::vector<std::string> protos;
    std::stringstream ss(target_segment_desc->protocol);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (!item.empty()) protos.push_back(item);
    }

#ifdef USE_ASCEND_HETEROGENEOUS
    // When USE_ASCEND_HETEROGENEOUS is enabled:
    // - Target side directly reuses RDMA Transport
    // - Initiator side uses heterogeneous_rdma_transport
    if (preferred_proto == "rdma" &&
        std::find(protos.begin(), protos.end(), "rdma") != protos.end()) {
        preferred_proto = "ascend";
    }
#endif

    // Failover: if preferred transport is unhealthy, try next in list
    {
        std::lock_guard<std::mutex> lock(health_mutex_);
        auto it = transport_health_.find(preferred_proto);
        if (it != transport_health_.end() && !it->second.healthy) {
            if (it->second.isCoolingDown()) {
                // Try next transport in the protocol list
                for (const auto& fallback_proto : protos) {
                    if (fallback_proto == preferred_proto) continue;
                    if (!transport_map_.count(fallback_proto)) continue;
                    auto fallback_it = transport_health_.find(fallback_proto);
                    if (fallback_it != transport_health_.end() &&
                        !fallback_it->second.healthy) {
                        continue;
                    }
                    LOG(WARNING) << "[MultiTransport] " << preferred_proto
                                << " unhealthy, failover to " << fallback_proto;
                    preferred_proto = fallback_proto;
                    break;
                }
            } else {
                it->second.markHealthy();
                LOG(INFO) << "[MultiTransport] " << preferred_proto
                         << " cooldown expired, retrying";
            }
        }
    }

    if (!transport_map_.count(preferred_proto)) {
        return Status::NotSupportedTransport("Transport " + preferred_proto +
                                             " not installed");
    }
    if (std::find(protos.begin(), protos.end(), preferred_proto) ==
        protos.end()) {
        return Status::NotSupportedTransport(
            "Transport " + preferred_proto +
            " not supported by target segment");
    }
    transport = transport_map_[preferred_proto].get();
    return Status::OK();
}
#endif

Transport* MultiTransport::getTransport(const std::string& proto) {
    if (!transport_map_.count(proto)) return nullptr;
    return transport_map_[proto].get();
}

bool MultiTransport::isTcpOnly() const {
    return transport_map_.size() == 1 && transport_map_.count("tcp") == 1;
}

std::vector<Transport*> MultiTransport::listTransports() {
    std::vector<Transport*> transport_list;
    for (auto& entry : transport_map_)
        transport_list.push_back(entry.second.get());
    return transport_list;
}

void* MultiTransport::getBaseAddr() {
#ifdef USE_CXL
    Transport* transport = getTransport("cxl");
    if (transport) {
        auto* cxl_transport = dynamic_cast<CxlTransport*>(transport);
        return cxl_transport ? cxl_transport->getCxlBaseAddr() : 0;
    }
#endif
    return 0;
}

void MultiTransport::markTransportUnhealthy(const std::string& proto,
                                             int recovery_secs) {
    std::lock_guard<std::mutex> lock(health_mutex_);
    transport_health_[proto].markUnhealthy(recovery_secs);
    LOG(WARNING) << "[MultiTransport] " << proto << " marked unhealthy "
                 << "(cooldown=" << recovery_secs << "s)";
}

void MultiTransport::markTransportHealthy(const std::string& proto) {
    std::lock_guard<std::mutex> lock(health_mutex_);
    auto it = transport_health_.find(proto);
    if (it != transport_health_.end()) {
        it->second.markHealthy();
        LOG(INFO) << "[MultiTransport] " << proto << " marked healthy";
    }
}

bool MultiTransport::isTransportHealthy(const std::string& proto) const {
    std::lock_guard<std::mutex> lock(health_mutex_);
    auto it = transport_health_.find(proto);
    if (it == transport_health_.end()) return true;  // No tracking = healthy
    return it->second.healthy;
}

void MultiTransport::tryRecoverTransports() {
    std::lock_guard<std::mutex> lock(health_mutex_);
    for (auto& entry : transport_health_) {
        if (!entry.second.healthy && !entry.second.isCoolingDown()) {
            entry.second.markHealthy();
            LOG(INFO) << "[MultiTransport] " << entry.first
                     << " cooldown expired, recovering";
        }
    }
}

void MultiTransport::healthMonitorLoop() {
    while (health_monitor_running_.load(std::memory_order_acquire)) {
        tryRecoverTransports();
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

}  // namespace mooncake
