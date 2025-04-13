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

#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

namespace mooncake {

int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name,
                         const std::string &ip_or_host_name,
                         uint64_t rpc_port) {
    local_server_name_ = local_server_name;
    metadata_ = std::make_shared<TransferMetadata>(metadata_conn_string);
    multi_transports_ =
        std::make_shared<MultiTransport>(metadata_, local_server_name_);

    TransferMetadata::RpcMetaDesc desc;
    if (getenv("MC_LEGACY_RPC_PORT_BINDING")) {
        desc.ip_or_host_name = ip_or_host_name;
        desc.rpc_port = rpc_port;
        desc.sockfd = -1;
    } else {
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

    LOG(INFO) << "Transfer Engine uses address " << desc.ip_or_host_name
              << " and port " << desc.rpc_port
              << " for serving local TCP service";

    int ret = metadata_->addRpcMetaEntry(local_server_name_, desc);
    if (ret) return ret;

    if (auto_discover_) {
        // discover topology automatically
        local_topology_->discover();

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
        LOG(INFO) << "Transport " << proto << " already installed";
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

bool TransferEngine::checkOverlap(void *addr, uint64_t length) {
    for (auto &entry : local_memory_regions_) {
        if (overlap(addr, length, entry.addr, entry.length)) {
            LOG(WARNING) << "Transfer Engine found overlapped memory region" 
                         << addr << "--" << (char *) addr + length << " with "
                         << entry.addr << "--" << (char *) entry.addr + entry.length;
            if (addr == entry.addr && length == entry.length) {
                // Identical memory region, add ref count
                entry.ref_count++;
                return true;
            }
        }
    }
    return false;
}

int TransferEngine::registerLocalMemory(void *addr, size_t length,
                                        const std::string &location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (checkOverlap(addr, length)) return 0;
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata);
        if (ret < 0) return ret;
    }
    local_memory_regions_.push_back(
        {addr, length, location, remote_accessible, 1});
    return 0;
}

int TransferEngine::unregisterLocalMemory(void *addr, bool update_metadata) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto memory_region_iter = local_memory_regions_.begin();
    for (auto it = local_memory_regions_.begin();
         it != local_memory_regions_.end(); ++it) {
        if (it->addr == addr) {
            if (it->ref_count > 1) {
                it->ref_count--;
                return 0;
            }
            if (memory_region_iter->addr != it->addr || memory_region_iter->length > it->length)
                memory_region_iter = it; // if base address is identical, remove the smallest one
        }
    }

    for (auto &transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemory(addr, update_metadata);
        if (ret) return ret;
    }

    local_memory_regions_.erase(memory_region_iter);
    return 0;
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list, const std::string &location) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<BufferEntry> real_buffer_list;
    for (auto &buffer : buffer_list) {
        if (!checkOverlap(buffer.addr, buffer.length)) {
            real_buffer_list.push_back(buffer);
        }
    }
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->registerLocalMemoryBatch(real_buffer_list, location);
        if (ret < 0) return ret;
    }

    for (auto &buffer : real_buffer_list) {
        local_memory_regions_.push_back(
            {buffer.addr, buffer.length, location, true, 1});
    }
    return 0;
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<void *> real_addr_list;
    std::vector<std::vector<MemoryRegion>::iterator> memory_region_iter_list;

    for (auto &addr : addr_list) {
        auto memory_region_iter = local_memory_regions_.begin();
        bool further_process = true;
        for (auto it = local_memory_regions_.begin();
            it != local_memory_regions_.end(); ++it) {
            if (it->addr == addr) {
                if (it->ref_count > 1) {
                    it->ref_count--;
                    further_process = false;
                    break;
                }
                if (memory_region_iter->addr != it->addr || memory_region_iter->length > it->length)
                    memory_region_iter = it; // if base address is identical, remove the smallest one
            }
        }
        if (further_process) {
            real_addr_list.push_back(addr);
            memory_region_iter_list.push_back(memory_region_iter);
        }
    }

    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemoryBatch(real_addr_list);
        if (ret < 0) return ret;
    }

    for (auto &memory_region_iter : memory_region_iter_list) {
        local_memory_regions_.erase(memory_region_iter);
    }
    return 0;
}
}  // namespace mooncake
