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

#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include <utility>

#include <algorithm>
#include <cctype>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <unistd.h>

TransferEngine::TransferEngine(bool auto_discover)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover)) {}

TransferEngine::TransferEngine(bool auto_discover,
                               const std::vector<std::string>& filter)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover, filter)) {}

TransferEngine::~TransferEngine() = default;

int TransferEngine::init(const std::string& metadata_conn_string,
                         const std::string& local_server_name,
                         const std::string& ip_or_host_name,
                         uint64_t rpc_port) {
    return impl_->init(metadata_conn_string, local_server_name, ip_or_host_name,
                       rpc_port);
}

int TransferEngine::freeEngine() { return impl_->freeEngine(); }

Transport* TransferEngine::installTransport(const std::string& proto,
                                            void** args) {
    return impl_->installTransport(proto, args);
}

int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name,
                         const std::string &ip_or_host_name,
                         uint64_t rpc_port) {
    TransferMetadata::RpcMetaDesc desc;
    std::string rpc_binding_method;

    if (!setFilesLimit()) {
        LOG(WARNING) << "Failed to set file descriptor limit. Continuing "
                        "initialization, but this may cause issues if too many "
                        "files are opened.";
    }
    // Set resources to the maximum value
#ifdef USE_BAREX
    const char *use_barex_env = std::getenv("USE_BAREX");
    if (use_barex_env) {
        int val = atoi(use_barex_env);
        if (val != 0) {
            use_barex_ = true;
        }
    }
#endif

#ifdef USE_ASCEND
    // The only difference in initializing the Ascend Transport is that the
    // `local_server_name` must include the physical NPU card ID. The format
    // changes from `ip:port` to `ip:port:npu_x`, e.g., `"0.0.0.0:12345:npu_2"`.
    // While the desc_name stored in the metadata remains in the format of
    // ip:port.
    int devicePhyId = -1;
    auto [host_name, port] =
        parseHostNameWithPortAscend(local_server_name, &devicePhyId);
    LOG(INFO) << "Transfer Engine parseHostNameWithPortAscend. server_name: "
              << host_name << " port: " << port
              << " devicePhyId: " << devicePhyId;
    local_server_name_ = host_name + ":" + std::to_string(port);
#else
    auto [host_name, port] = parseHostNameWithPort(local_server_name);
    LOG(INFO) << "Transfer Engine parseHostNameWithPort. server_name: "
              << host_name << " port: " << port;
    local_server_name_ = local_server_name;
#endif

    if (getenv("MC_LEGACY_RPC_PORT_BINDING") ||
        metadata_conn_string == P2PHANDSHAKE) {
        rpc_binding_method = "legacy/P2P";
        desc.ip_or_host_name = host_name;
        desc.rpc_port = port;
        desc.sockfd = -1;
#ifdef USE_BAREX
        if (use_barex_) {
            int tmp_fd = -1;
            desc.barex_port = findAvailableTcpPort(tmp_fd, true);
            if (desc.barex_port == 0) {
                LOG(ERROR)
                    << "Barex: No valid port found for local barex service.";
                return -1;
            }
            close(tmp_fd);
            tmp_fd = -1;
        }
#endif
        if (metadata_conn_string == P2PHANDSHAKE) {
            rpc_binding_method = "P2P handshake";
            desc.rpc_port = findAvailableTcpPort(desc.sockfd);
            if (desc.rpc_port == 0) {
                LOG(ERROR) << "P2P: No valid port found for local TCP service.";
                return -1;
            }
#if defined(USE_ASCEND)
            // The current version of Ascend Transport does not support IPv6,
            // but it will be added in a future release.
            local_server_name_ =
                desc.ip_or_host_name + ":" + std::to_string(desc.rpc_port);
#else
            local_server_name_ = maybeWrapIpV6(desc.ip_or_host_name) + ":" +
                                 std::to_string(desc.rpc_port);
#endif
        }
    } else {
        rpc_binding_method = "new RPC mapping";
        (void)(ip_or_host_name);
        auto *ip_address = getenv("MC_TCP_BIND_ADDRESS");
        if (ip_address)
            desc.ip_or_host_name = ip_address;
        else {
            auto ip_list = findLocalIpAddresses();
            if (ip_list.empty()) {
                LOG(ERROR) << "not valid LAN address found";
                return -1;
            } else {
                desc.ip_or_host_name = ip_list[0];
            }
        }

        // In the new rpc port mapping, it is randomly selected to prevent
        // port conflict
        (void)(rpc_port);
        desc.rpc_port = findAvailableTcpPort(desc.sockfd);
        if (desc.rpc_port == 0) {
            LOG(ERROR) << "not valid port for serving local TCP service";
            return -1;
        }
    }

    LOG(INFO) << "Transfer Engine RPC using " << rpc_binding_method
              << ", listening on " << desc.ip_or_host_name << ":"
              << desc.rpc_port
#ifdef USE_BAREX
              << (use_barex_
                      ? ", barex use port:" + std::to_string(desc.barex_port)
                      : "")
#endif
              << "";

    metadata_ = std::make_shared<TransferMetadata>(metadata_conn_string);
#ifdef USE_ASCEND
    std::string mutable_server_name =
        local_server_name_ + ":npu_" + std::to_string(devicePhyId);
    multi_transports_ =
        std::make_shared<MultiTransport>(metadata_, mutable_server_name);
#else
    multi_transports_ =
        std::make_shared<MultiTransport>(metadata_, local_server_name_);
#endif
    int ret = metadata_->addRpcMetaEntry(local_server_name_, desc);
    if (ret) return ret;

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
    Transport *ascend_transport =
        multi_transports_->installTransport("ascend", local_topology_);
    if (!ascend_transport) {
        LOG(ERROR) << "Failed to install Ascend transport";
        return -1;
    }
#else

#if defined(USE_CXL) && !defined(USE_ASCEND) && \
    !defined(USE_ASCEND_HETEROGENEOUS)
    if (std::getenv("MC_CXL_DEV_PATH") != nullptr) {
        Transport *cxl_transport =
            multi_transports_->installTransport("cxl", local_topology_);
        if (!cxl_transport) {
            LOG(ERROR) << "Failed to install CXL transport";
            return -1;
        }
    }
#endif

    if (auto_discover_) {
        LOG(INFO) << "Auto-discovering topology...";
        if (getenv("MC_CUSTOM_TOPO_JSON")) {
            auto path = getenv("MC_CUSTOM_TOPO_JSON");
            LOG(INFO) << "Using custom topology from: " << path;
            auto topo_json = loadTopologyJsonFile(path);
            if (!topo_json.empty()) {
                local_topology_->parse(topo_json);
            } else {
                LOG(WARNING) << "Failed to load custom topology from " << path
                             << ", falling back to auto-detect.";
                local_topology_->discover(filter_);
            }
        } else {
            local_topology_->discover(filter_);
        }
        LOG(INFO) << "Topology discovery complete. Found "
                  << local_topology_->getHcaList().size() << " HCAs.";

#ifdef USE_ASCEND_HETEROGENEOUS
        Transport *ascend_transport =
            multi_transports_->installTransport("ascend", local_topology_);
        if (!ascend_transport) {
            LOG(ERROR) << "Failed to install Ascend transport";
            return -1;
        }
#elif defined(USE_MNNVL)
        if (local_topology_->getHcaList().size() > 0 &&
            !getenv("MC_FORCE_MNNVL")) {
            Transport *rdma_transport =
                multi_transports_->installTransport("rdma", local_topology_);
            if (!rdma_transport) {
                LOG(ERROR) << "Failed to install RDMA transport";
                return -1;
            }
        } else {
#ifdef USE_HIP
            Transport *hip_transport =
                multi_transports_->installTransport("hip", nullptr);
            if (!hip_transport) {
                LOG(ERROR) << "Failed to install HIP transport";
                return -1;
            }
#else
            Transport *nvlink_transport =
                multi_transports_->installTransport("nvlink", nullptr);
            if (!nvlink_transport) {
                LOG(ERROR) << "Failed to install NVLink transport";
                return -1;
            }
#endif
        }
#else
        if (local_topology_->getHcaList().size() > 0 &&
            !getenv("MC_FORCE_TCP")) {
            // only install RDMA transport when there is at least one HCA
            Transport *rdma_transport = nullptr;
            if (use_barex_) {
#ifdef USE_BAREX
                rdma_transport = multi_transports_->installTransport(
                    "barex", local_topology_);
#else
                LOG(ERROR) << "Set USE BAREX while barex not compiled";
                return -1;
#endif
            } else {
                rdma_transport = multi_transports_->installTransport(
                    "rdma", local_topology_);
            }
            if (rdma_transport == nullptr) {
                LOG(ERROR) << "Failed to install RDMA transport, type="
                           << (use_barex_ ? "barex" : "rdma");
                return -1;
            } else {
                LOG(INFO) << "installTransport, type="
                          << (use_barex_ ? "barex" : "rdma");
            }
        } else {
            Transport *tcp_transport =
                multi_transports_->installTransport("tcp", nullptr);
            if (!tcp_transport) {
                LOG(ERROR) << "Failed to install TCP transport";
                return -1;
            }
        }
#endif
        // TODO: install other transports automatically
    }
#endif

    return 0;
}

std::string TransferEngine::getLocalIpAndPort() {
    return impl_->getLocalIpAndPort();
}

int TransferEngine::getRpcPort() { return impl_->getRpcPort(); }

SegmentHandle TransferEngine::openSegment(const std::string& segment_name) {
    return impl_->openSegment(segment_name);
}

Status TransferEngine::CheckSegmentStatus(SegmentID sid) {
    return impl_->CheckSegmentStatus(sid);
}

int TransferEngine::closeSegment(SegmentHandle handle) {
    return impl_->closeSegment(handle);
}

int TransferEngine::removeLocalSegment(const std::string& segment_name) {
    return impl_->removeLocalSegment(segment_name);
}

int TransferEngine::registerLocalMemory(void* addr, size_t length,
                                        const std::string& location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    return impl_->registerLocalMemory(addr, length, location, remote_accessible,
                                      update_metadata);
}

int TransferEngine::unregisterLocalMemory(void* addr, bool update_metadata) {
    return impl_->unregisterLocalMemory(addr, update_metadata);
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    return impl_->registerLocalMemoryBatch(buffer_list, location);
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    return impl_->unregisterLocalMemoryBatch(addr_list);
}

BatchID TransferEngine::allocateBatchID(size_t batch_size) {
    return impl_->allocateBatchID(batch_size);
}

Status TransferEngine::freeBatchID(BatchID batch_id) {
    return impl_->freeBatchID(batch_id);
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    return impl_->submitTransfer(batch_id, entries);
}

Status TransferEngine::submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg) {
    return impl_->submitTransferWithNotify(batch_id, entries, notify_msg);
}

int TransferEngine::getNotifies(
    std::vector<TransferMetadata::NotifyDesc>& notifies) {
    return impl_->getNotifies(notifies);
}

int TransferEngine::sendNotifyByID(SegmentID target_id,
                                   TransferMetadata::NotifyDesc notify_msg) {
    return impl_->sendNotifyByID(target_id, notify_msg);
}

int TransferEngine::sendNotifyByName(std::string remote_agent,
                                     TransferMetadata::NotifyDesc notify_msg) {
    return impl_->sendNotifyByName(std::move(remote_agent), notify_msg);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    return impl_->getTransferStatus(batch_id, task_id, status);
}

Status TransferEngine::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    return impl_->getBatchTransferStatus(batch_id, status);
}

Transport* TransferEngine::getTransport(const std::string& proto) {
    return impl_->getTransport(proto);
}

int TransferEngine::syncSegmentCache(const std::string& segment_name) {
    return impl_->syncSegmentCache(segment_name);
}

std::shared_ptr<TransferMetadata> TransferEngine::getMetadata() {
    return impl_->getMetadata();
}

void TransferEngine::RecordTaskStart(BatchID batch_id, size_t task_id) {
    uint64_t task_key = MakeTaskKey(batch_id, task_id);
    std::lock_guard<std::mutex> lock(task_timing_mutex_);
    auto it = task_start_times_.find(task_key);
    if (it == task_start_times_.end()) {
        TaskTimingInfo info;
        info.start_time = std::chrono::steady_clock::now();
        info.is_started = true;
        task_start_times_[task_key] = info;
    }
}

void TransferEngine::RecordTaskCompletion(BatchID batch_id, size_t task_id) {
    uint64_t task_key = MakeTaskKey(batch_id, task_id);
    auto completion_time = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(task_timing_mutex_);
    auto it = task_start_times_.find(task_key);
    if (it != task_start_times_.end() && it->second.is_started) {
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            completion_time - it->second.start_time);
        int64_t latency_us = duration.count();

        task_completion_latency_us_.observe(latency_us);

        task_start_times_.erase(it);
    }
}

void TransferEngine::InitializeMetricsConfig() {
    // Check if metrics reporting is enabled via environment variable
    const char *metric_env = getenv("MC_TE_METRIC");
    if (metric_env) {
        std::string value = toLower(metric_env);
        metrics_enabled_ = (value == "1" || value == "true" || value == "yes" ||
                            value == "on");
    }

    // Check for custom reporting interval
    const char *interval_env = getenv("MC_TE_METRIC_INTERVAL_SECONDS");
    if (interval_env) {
        try {
            int interval = std::stoi(interval_env);
            if (interval > 0) {
                metrics_interval_seconds_ = static_cast<uint64_t>(interval);
                LOG(INFO) << "Metrics reporting interval set to "
                          << metrics_interval_seconds_ << " seconds";
            } else {
                LOG(WARNING)
                    << "Invalid MC_TE_METRIC_INTERVAL_SECONDS value: "
                    << interval_env << ", must be positive. Using default: "
                    << metrics_interval_seconds_;
            }
        } catch (const std::exception &e) {
            LOG(WARNING) << "Failed to parse MC_TE_METRIC_INTERVAL_SECONDS: "
                         << interval_env
                         << ", using default: " << metrics_interval_seconds_;
        }
    }
}

void TransferEngine::StartMetricsReportingThread() {
    // Only start the metrics thread if metrics are enabled
    if (!metrics_enabled_) {
        LOG(INFO)
            << "Metrics reporting is disabled (set MC_TE_METRIC=1 to enable)";
        return;
    }

    should_stop_metrics_thread_ = false;

    // Initialize previous bucket counts
    {
        std::lock_guard<std::mutex> lock(metrics_snapshot_mutex_);
        auto bucket_counts = task_completion_latency_us_.get_bucket_counts();
        prev_bucket_counts_.resize(bucket_counts.size(), 0);
    }

    metrics_reporting_thread_ = std::thread([this]() {
        LOG(INFO) << "Metrics reporting thread started (interval: "
                  << metrics_interval_seconds_ << "s)";
        constexpr double kBytesPerMegabyte = 1024.0 * 1024.0;

        while (!should_stop_metrics_thread_) {
            // Sleep for the interval, checking periodically for stop signal
            for (uint64_t i = 0;
                 i < metrics_interval_seconds_ && !should_stop_metrics_thread_;
                 ++i) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            if (should_stop_metrics_thread_) {
                break;  // Exit if stopped during sleep
            }

            auto bytes_transferred_in_interval =
                transferred_bytes_counter_.value();
            transferred_bytes_counter_
                .reset();  // Reset counter for the next interval

            // Calculate throughput
            bool has_throughput = (bytes_transferred_in_interval > 0);
            double throughput_megabytes_per_second = 0.0;
            if (has_throughput) {
                throughput_megabytes_per_second =
                    static_cast<double>(bytes_transferred_in_interval) /
                    (metrics_interval_seconds_ * kBytesPerMegabyte);
            }

            // Calculate task completion latency statistics for this interval
            auto bucket_counts =
                task_completion_latency_us_.get_bucket_counts();

            // Compute interval counts (delta from previous snapshot)
            std::vector<int64_t> interval_counts;
            int64_t total_task_count = 0;
            {
                std::lock_guard<std::mutex> lock(metrics_snapshot_mutex_);
                interval_counts.resize(bucket_counts.size());

                for (size_t i = 0; i < bucket_counts.size(); ++i) {
                    int64_t current_count = bucket_counts[i]->value();
                    int64_t prev_count = (i < prev_bucket_counts_.size())
                                             ? prev_bucket_counts_[i]
                                             : 0;
                    interval_counts[i] = current_count - prev_count;
                    total_task_count += interval_counts[i];

                    // Update previous snapshot
                    if (i < prev_bucket_counts_.size()) {
                        prev_bucket_counts_[i] = current_count;
                    } else {
                        prev_bucket_counts_.push_back(current_count);
                    }
                }
            }

            bool has_latency = (total_task_count > 0);

            // Skip if no data to report
            if (!has_throughput && !has_latency) {
                continue;
            }

            // Build metrics log message
            std::stringstream log_msg;
            log_msg << "[Metrics] Transfer Engine Stats (over last "
                    << metrics_interval_seconds_ << "s):";

            if (has_throughput) {
                log_msg << " Throughput: " << std::fixed << std::setprecision(2)
                        << throughput_megabytes_per_second << " MB/s";
            }

            if (!has_latency) {
                LOG(INFO) << log_msg.str();
                continue;
            }

            // Append latency distribution
            log_msg << " | Latency Distribution (count=" << total_task_count
                    << "): ";

            bool first = true;
            for (size_t i = 0; i < interval_counts.size(); ++i) {
                int64_t count = interval_counts[i];
                if (count <= 0) continue;

                double percentage = (count * 100.0) / total_task_count;
                constexpr double kMinPercentageThreshold = 0.1;
                if (percentage < kMinPercentageThreshold) continue;

                // Add separator between entries
                if (!first) {
                    log_msg << ", ";
                }
                first = false;

                // Format and append bucket range with percentage
                bool is_overflow_bucket = (i >= kTaskLatencyBuckets.size());
                if (is_overflow_bucket) {
                    int threshold =
                        static_cast<int>(kTaskLatencyBuckets.back());
                    log_msg << ">" << threshold << "μs";
                } else {
                    int lower = 0;
                    if (i > 0) {
                        lower = static_cast<int>(kTaskLatencyBuckets[i - 1]);
                    }
                    int upper = static_cast<int>(kTaskLatencyBuckets[i]);
                    log_msg << lower << "-" << upper << "μs";
                }

                log_msg << ":" << std::fixed << std::setprecision(1)
                        << percentage << "%";
            }

            LOG(INFO) << log_msg.str();
        }
        LOG(INFO) << "Metrics reporting thread stopped";
    });
}

void TransferEngine::setWhitelistFilters(std::vector<std::string>&& filters) {
    impl_->setWhitelistFilters(std::move(filters));
}

int TransferEngine::numContexts() const { return impl_->numContexts(); }

std::shared_ptr<Topology> TransferEngine::getLocalTopology() {
    return impl_->getLocalTopology();
}
}  // namespace mooncake
