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

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <string>

#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

namespace mooncake {

static std::string loadTopologyJsonFile(const std::string &path) {
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

int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name,
                         const std::string &ip_or_host_name,
                         uint64_t rpc_port) {
    LOG(INFO) << "Transfer Engine starting. Server: " << local_server_name
              << ", Metadata: " << metadata_conn_string
              << ", ip_or_host_name: " << ip_or_host_name
              << ", rpc_port: " << rpc_port;

    local_server_name_ = local_server_name;
    TransferMetadata::RpcMetaDesc desc;
    std::string rpc_binding_method;

    if (getenv("MC_LEGACY_RPC_PORT_BINDING") ||
        metadata_conn_string == P2PHANDSHAKE) {
        rpc_binding_method = "legacy/P2P";
        auto [host_name, port] = parseHostNameWithPort(local_server_name);
        desc.ip_or_host_name = host_name;
        desc.rpc_port = port;
        desc.sockfd = -1;

        if (metadata_conn_string == P2PHANDSHAKE) {
            rpc_binding_method = "P2P handshake";
            if (port == getDefaultHandshakePort()) {
                desc.rpc_port = findAvailableTcpPort(desc.sockfd);
                if (desc.rpc_port == 0) {
                    LOG(ERROR)
                        << "P2P: No valid port found for local TCP service.";
                    return -1;
                }
            }
            local_server_name_ =
                desc.ip_or_host_name + ":" + std::to_string(desc.rpc_port);
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
              << desc.rpc_port;

    metadata_ = std::make_shared<TransferMetadata>(metadata_conn_string);
    multi_transports_ =
        std::make_shared<MultiTransport>(metadata_, local_server_name_);

    int ret = metadata_->addRpcMetaEntry(local_server_name_, desc);
    if (ret) return ret;

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

        if (local_topology_->getHcaList().size() > 0) {
            // only install RDMA transport when there is at least one HCA
            multi_transports_->installTransport("rdma", local_topology_);
        } else {
            multi_transports_->installTransport("tcp", nullptr);
        }
        // TODO: install other transports automatically
    }

    return 0;
}

int TransferEngine::freeEngine() {
    if (metadata_) {
        metadata_->removeRpcMetaEntry(local_server_name_);
        metadata_.reset();
    }
    return 0;
}

// Only for testing
Transport *TransferEngine::installTransport(const std::string &proto,
                                            void **args) {
    Transport *transport = multi_transports_->getTransport(proto);
    if (transport) {
        LOG(WARNING) << "Transport " << proto << " already installed";
        return transport;
    }

    if (args != nullptr && args[0] != nullptr) {
        const std::string nic_priority_matrix = static_cast<char *>(args[0]);
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
    for (auto &entry : local_memory_regions_) {
        int ret = transport->registerLocalMemory(
            entry.addr, entry.length, entry.location, entry.remote_accessible);
        if (ret < 0) return nullptr;
    }
    return transport;
}

int TransferEngine::uninstallTransport(const std::string &proto) { return 0; }

int TransferEngine::getRpcPort() { return metadata_->localRpcMeta().rpc_port; }

std::string TransferEngine::getLocalIpAndPort() {
    return metadata_->localRpcMeta().ip_or_host_name + ":" +
           std::to_string(metadata_->localRpcMeta().rpc_port);
}

Transport::SegmentHandle TransferEngine::openSegment(
    const std::string &segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    return metadata_->getSegmentID(trimmed_segment_name);
}

int TransferEngine::closeSegment(Transport::SegmentHandle handle) { return 0; }

int TransferEngine::removeLocalSegment(const std::string &segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    return metadata_->removeLocalSegment(trimmed_segment_name);
}

bool TransferEngine::checkOverlap(void *addr, uint64_t length) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (auto &local_memory_region : local_memory_regions_) {
        if (overlap(addr, length, local_memory_region.addr,
                    local_memory_region.length)) {
            return true;
        }
    }
    return false;
}

int TransferEngine::registerLocalMemory(void *addr, size_t length,
                                        const std::string &location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    if (checkOverlap(addr, length)) {
        LOG(ERROR)
            << "Transfer Engine does not support overlapped memory region";
        return ERR_ADDRESS_OVERLAPPED;
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

int TransferEngine::unregisterLocalMemory(void *addr, bool update_metadata) {
    for (auto &transport : multi_transports_->listTransports()) {
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

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list, const std::string &location) {
    for (auto &buffer : buffer_list) {
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
    for (auto &buffer : buffer_list) {
        local_memory_regions_.push_back(
            {buffer.addr, buffer.length, location, true});
    }
    return 0;
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemoryBatch(addr_list);
        if (ret < 0) return ret;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto &addr : addr_list) {
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
static std::string toLower(const std::string &s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
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

            if (bytes_transferred_in_interval == 0) {
                continue;
            }

            // Calculate throughput in MB/s for better readability
            double throughput_megabytes_per_second =
                static_cast<double>(bytes_transferred_in_interval) /
                (metrics_interval_seconds_ * kBytesPerMegabyte);

            LOG(INFO) << "[Metrics] Transfer Engine Throughput: " << std::fixed
                      << std::setprecision(2) << throughput_megabytes_per_second
                      << " MB/s (over last " << metrics_interval_seconds_
                      << "s)";
        }
        LOG(INFO) << "Metrics reporting thread stopped";
    });
}

void TransferEngine::StopMetricsReportingThread() {
    should_stop_metrics_thread_ = true;  // Signal the thread to stop
    if (metrics_reporting_thread_.joinable()) {
        LOG(INFO) << "Waiting for metrics reporting thread to join...";
        metrics_reporting_thread_.join();  // Wait for the thread to finish
        LOG(INFO) << "Metrics reporting thread joined";
    }
}
#endif

}  // namespace mooncake
