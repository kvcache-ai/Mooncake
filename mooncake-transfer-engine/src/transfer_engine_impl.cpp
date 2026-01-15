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

#include "transfer_engine_impl.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <sys/resource.h>
#include <unistd.h>
#ifdef WITH_METRICS
#include <iomanip>
#include <sstream>
#endif

#include "transfer_metadata_plugin.h"
#include "transport/transport.h"
#include "transport/barex_transport/barex_transport.h"

namespace mooncake {

static bool setFilesLimit() {
    struct rlimit filesLimit;
    if (getrlimit(RLIMIT_NOFILE, &filesLimit) != 0) {
        LOG(ERROR) << "getrlimit failed: " << strerror(errno);
        return false;
    }
    rlim_t target_limit = filesLimit.rlim_max;
    // Skip if already sufficient
    if (filesLimit.rlim_cur >= target_limit) {
        return true;
    }
    filesLimit.rlim_cur = target_limit;
    if (setrlimit(RLIMIT_NOFILE, &filesLimit) != 0) {
        LOG(ERROR) << "setrlimit failed: " << strerror(errno);
        return false;
    }
    return true;
}

static std::string loadTopologyJsonFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return "";
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    file.close();
    return content;
}

int TransferEngineImpl::init(const std::string& metadata_conn_string,
                             const std::string& local_server_name,
                             const std::string& ip_or_host_name,
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
    const char* use_barex_env = std::getenv("USE_BAREX");
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
        auto* ip_address = getenv("MC_TCP_BIND_ADDRESS");
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
    Transport* ascend_transport =
        multi_transports_->installTransport("ascend", local_topology_);
    if (!ascend_transport) {
        LOG(ERROR) << "Failed to install Ascend transport";
        return -1;
    }
#else

#if defined(USE_CXL) && !defined(USE_ASCEND) && \
    !defined(USE_ASCEND_HETEROGENEOUS)
    if (std::getenv("MC_CXL_DEV_PATH") != nullptr) {
        Transport* cxl_transport =
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
        Transport* ascend_transport =
            multi_transports_->installTransport("ascend", local_topology_);
        if (!ascend_transport) {
            LOG(ERROR) << "Failed to install Ascend transport";
            return -1;
        }
#elif defined(USE_MNNVL) || defined(USE_INTRA_NVLINK)

        const char* intra_env = getenv("MC_INTRANODE_NVLINK");
        const char* force_mnnvl = getenv("MC_FORCE_MNNVL");

        if (intra_env) {
            Transport* t =
                multi_transports_->installTransport("nvlink_intra", nullptr);
            if (!t) {
                LOG(ERROR) << "Failed to install Intra-Node NVLink transport";
                return -1;
            }
            LOG(INFO) << "Using Intra-Node NVLink transport "
                         "(MC_INTRANODE_NVLINK set)";
        } else if (force_mnnvl || local_topology_->getHcaList().empty()) {
            Transport* t =
                multi_transports_->installTransport("nvlink", nullptr);
            if (!t) {
                LOG(ERROR) << "Failed to install NVLink transport";
                return -1;
            }
            LOG(INFO) << "Using cross-node NVLink transport "
                      << "(MC_FORCE_MNNVL or no HCA detected)";
        } else {
            Transport* t =
                multi_transports_->installTransport("rdma", local_topology_);
            if (!t) {
                LOG(ERROR) << "Failed to install RDMA transport";
                return -1;
            }
            LOG(INFO) << "Using RDMA transport (RoCE/iWARP)";
        }

#else
        if (local_topology_->getHcaList().size() > 0 &&
                !getenv("MC_FORCE_TCP") ||
            getenv("MC_FORCE_HCA")) {
            // only install RDMA transport when there is at least one HCA
            Transport* rdma_transport = nullptr;
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
            Transport* tcp_transport =
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

int TransferEngineImpl::freeEngine() {
    if (metadata_) {
        metadata_->removeRpcMetaEntry(local_server_name_);
        metadata_.reset();
    }
    return 0;
}

// Only for testing
Transport* TransferEngineImpl::installTransport(const std::string& proto,
                                                void** args) {
    Transport* transport = multi_transports_->getTransport(proto);
    if (transport) {
        LOG(WARNING) << "Transport " << proto << " already installed";
        return transport;
    }

    if (args != nullptr && args[0] != nullptr) {
        const std::string nic_priority_matrix = static_cast<char*>(args[0]);
        int ret = local_topology_->parse(nic_priority_matrix);
        if (ret) {
            LOG(ERROR) << "Failed to parse NIC priority matrix";
            return nullptr;
        }
    }

    transport = multi_transports_->installTransport(proto, local_topology_);
    if (!transport) return nullptr;

    // Since installTransport() is only called once during initialization
    // and is not expected to be executed concurrently, we do not acquire a
    // shared lock here. If future modifications allow installTransport() to be
    // invoked concurrently, a std::shared_lock<std::shared_mutex> should be
    // added to ensure thread safety.
    for (auto& entry : local_memory_regions_) {
        int ret = transport->registerLocalMemory(
            entry.addr, entry.length, entry.location, entry.remote_accessible);
        if (ret < 0) return nullptr;
    }
    return transport;
}

int TransferEngineImpl::uninstallTransport(const std::string& proto) {
    return 0;
}

int TransferEngineImpl::getRpcPort() {
    return metadata_->localRpcMeta().rpc_port;
}

std::string TransferEngineImpl::getLocalIpAndPort() {
    return metadata_->localRpcMeta().ip_or_host_name + ":" +
           std::to_string(metadata_->localRpcMeta().rpc_port);
}

int TransferEngineImpl::getNotifies(
    std::vector<TransferMetadata::NotifyDesc>& notifies) {
    return metadata_->getNotifies(notifies);
}

int TransferEngineImpl::sendNotifyByID(
    SegmentID target_id, TransferMetadata::NotifyDesc notify_msg) {
    auto desc = metadata_->getSegmentDescByID(target_id);
    Transport::NotifyDesc peer_desc;
    int ret = metadata_->sendNotify(desc->name, notify_msg, peer_desc);
    return ret;
}

int TransferEngineImpl::sendNotifyByName(
    std::string remote_agent, TransferMetadata::NotifyDesc notify_msg) {
    Transport::NotifyDesc peer_desc;
    int ret = metadata_->sendNotify(remote_agent, notify_msg, peer_desc);
    return ret;
}

Transport::SegmentHandle TransferEngineImpl::openSegment(
    const std::string& segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    SegmentID sid = metadata_->getSegmentID(trimmed_segment_name);
#ifdef USE_BAREX
    if (use_barex_) {
        Transport* transport = multi_transports_->getTransport("barex");
        if (!transport) {
            LOG(ERROR) << "Barex proto not installed";
            return (Transport::SegmentHandle)-1;
        }
        Status s = transport->OpenChannel(segment_name, sid);
        if (!s.ok()) {
            LOG(ERROR) << "openSegment, OpenChannel failed";
            return (Transport::SegmentHandle)-1;
        }
    }
#endif
    return sid;
}

Status TransferEngineImpl::CheckSegmentStatus(SegmentID sid) {
#ifdef USE_BAREX
    if (use_barex_) {
        Transport* transport = multi_transports_->getTransport("barex");
        BarexTransport* barex_transport =
            dynamic_cast<BarexTransport*>(transport);
        return barex_transport->CheckStatus(sid);
    } else {
        return Status::OK();
    }
#else
    return Status::OK();
#endif
}

int TransferEngineImpl::closeSegment(Transport::SegmentHandle handle) {
    return 0;
}

int TransferEngineImpl::removeLocalSegment(const std::string& segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    return metadata_->removeLocalSegment(trimmed_segment_name);
}

bool TransferEngineImpl::checkOverlap(void* addr, uint64_t length) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (auto& local_memory_region : local_memory_regions_) {
        if (overlap(addr, length, local_memory_region.addr,
                    local_memory_region.length)) {
            return true;
        }
    }
    return false;
}

int TransferEngineImpl::registerLocalMemory(void* addr, size_t length,
                                            const std::string& location,
                                            bool remote_accessible,
                                            bool update_metadata) {
    if (checkOverlap(addr, length)) {
        LOG(ERROR)
            << "Transfer Engine does not support overlapped memory region";
        return ERR_ADDRESS_OVERLAPPED;
    }
    if (length == 0) {
        LOG(ERROR)
            << "Transfer Engine does not support zero length memory region";
        return ERR_INVALID_ARGUMENT;
    }
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata);
        if (ret < 0) return ret;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    local_memory_regions_.push_back(
        {addr, length, location, remote_accessible});
    return 0;
}

int TransferEngineImpl::unregisterLocalMemory(void* addr,
                                              bool update_metadata) {
    for (auto& transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemory(addr, update_metadata);
        if (ret) return ret;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto it = local_memory_regions_.begin();
         it != local_memory_regions_.end(); ++it) {
        if (it->addr == addr) {
            local_memory_regions_.erase(it);
            break;
        }
    }
    return 0;
}

int TransferEngineImpl::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    for (auto& buffer : buffer_list) {
        if (checkOverlap(buffer.addr, buffer.length)) {
            LOG(ERROR)
                << "Transfer Engine does not support overlapped memory region";
            return ERR_ADDRESS_OVERLAPPED;
        }
    }
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->registerLocalMemoryBatch(buffer_list, location);
        if (ret < 0) return ret;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto& buffer : buffer_list) {
        local_memory_regions_.push_back(
            {buffer.addr, buffer.length, location, true});
    }
    return 0;
}

int TransferEngineImpl::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemoryBatch(addr_list);
        if (ret < 0) return ret;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto& addr : addr_list) {
        for (auto it = local_memory_regions_.begin();
             it != local_memory_regions_.end(); ++it) {
            if (it->addr == addr) {
                local_memory_regions_.erase(it);
                break;
            }
        }
    }
    return 0;
}

#ifdef WITH_METRICS
// Helper function to convert string to lowercase for case-insensitive
// comparison
static std::string toLower(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

void TransferEngineImpl::InitializeMetricsConfig() {
    // Check if metrics reporting is enabled via environment variable
    const char* metric_env = getenv("MC_TE_METRIC");
    if (metric_env) {
        std::string value = toLower(metric_env);
        metrics_enabled_ = (value == "1" || value == "true" || value == "yes" ||
                            value == "on");
    }

    // Check for custom reporting interval
    const char* interval_env = getenv("MC_TE_METRIC_INTERVAL_SECONDS");
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
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to parse MC_TE_METRIC_INTERVAL_SECONDS: "
                         << interval_env
                         << ", using default: " << metrics_interval_seconds_;
        }
    }
}

void TransferEngineImpl::StartMetricsReportingThread() {
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
                    interval_counts[i] = current_count - prev_bucket_counts_[i];
                    total_task_count += interval_counts[i];
                    prev_bucket_counts_[i] = current_count;
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

void TransferEngineImpl::StopMetricsReportingThread() {
    should_stop_metrics_thread_ = true;  // Signal the thread to stop
    if (metrics_reporting_thread_.joinable()) {
        LOG(INFO) << "Waiting for metrics reporting thread to join...";
        metrics_reporting_thread_.join();  // Wait for the thread to finish
        LOG(INFO) << "Metrics reporting thread joined";
    }
}
#endif

}  // namespace mooncake
