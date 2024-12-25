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

#include "transport/rdma_transport/rdma_transport.h"
#include "transport/tcp_transport/tcp_transport.h"
#include "transport/transport.h"
#ifdef USE_CUDA
#include "transport/nvmeof_transport/nvmeof_transport.h"
#endif

namespace mooncake {

int TransferEngine::init(const char *server_name, const char *connectable_name,
                         uint64_t rpc_port) {
    local_server_name_ = server_name;
    assert(metadata_);
    TransferMetadata::RpcMetaDesc desc;
    desc.ip_or_host_name = connectable_name;
    desc.rpc_port = rpc_port;
    return metadata_->addRpcMetaEntry(server_name, desc);
}

int TransferEngine::freeEngine() {
    while (!installed_transports_.empty()) {
        auto proto = installed_transports_.back()->getName();
        if (uninstallTransport(proto) < 0)
            LOG(ERROR) << "Failed to uninstall transport " << proto;
    }
    metadata_->removeRpcMetaEntry(local_server_name_);
    return 0;
}

Transport *TransferEngine::installOrGetTransport(const char *proto,
                                                 void **args) {
    Transport *xport = initTransport(proto);
    if (!xport) {
        LOG(ERROR) << "Failed to initialize transport " << proto;
        return nullptr;
    }

    if (xport->install(local_server_name_, metadata_, args) < 0) goto fail;

    installed_transports_.emplace_back(xport);
    for (const auto &mem : local_memory_regions_)
        if (xport->registerLocalMemory(mem.addr, mem.length, mem.location,
                                       mem.remote_accessible) < 0)
            goto fail;

    return xport;

fail:
    delete xport;
    return NULL;
}

int TransferEngine::uninstallTransport(const char *proto) {
    for (auto it = installed_transports_.begin();
         it != installed_transports_.end(); ++it) {
        if (strcmp((*it)->getName(), proto) == 0) {
            delete *it;
            installed_transports_.erase(it);
            return 0;
        }
    }
    return ERR_INVALID_ARGUMENT;
}

Transport::SegmentHandle TransferEngine::openSegment(const char *segment_name) {
    if (!segment_name)
        return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty())
        return ERR_INVALID_ARGUMENT;
    return metadata_->getSegmentID(trimmed_segment_name);
}

int TransferEngine::closeSegment(Transport::SegmentHandle seg_id) {
    // Not used
    return 0;
}

int TransferEngine::registerLocalMemory(void *addr, size_t length,
                                        const std::string &location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    for (auto &local_memory_region : local_memory_regions_) {
        if (overlap(addr, length, local_memory_region.addr,
                    local_memory_region.length)) {
            LOG(ERROR) << "Transfer Engine does not support overlapped memory region";
            return ERR_ADDRESS_OVERLAPPED;
        }
    }
    for (auto &xport : installed_transports_) {
        int ret = xport->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata);
        if (ret < 0) return ret;
    }
    local_memory_regions_.push_back(
        {addr, length, location.c_str(), remote_accessible});
    return 0;
}

int TransferEngine::unregisterLocalMemory(void *addr, bool update_metadata) {
    for (auto it = local_memory_regions_.begin();
         it != local_memory_regions_.end(); ++it) {
        if (it->addr == addr) {
            for (auto &xport : installed_transports_) {
                if (xport->unregisterLocalMemory(addr, update_metadata) < 0)
                    return ERR_MEMORY;
            }
            local_memory_regions_.erase(it);
            break;
        }
    }
    return 0;
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list, const std::string &location) {
    for (auto &xport : installed_transports_) {
        int ret = xport->registerLocalMemoryBatch(buffer_list, location);
        if (ret < 0) return ret;
    }
    return 0;
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &xport : installed_transports_) {
        int ret = xport->unregisterLocalMemoryBatch(addr_list);
        if (ret < 0) return ret;
    }
    return 0;
}

Transport *TransferEngine::findName(const char *name, size_t n) {
    for (const auto &xport : installed_transports_) {
        if (strncmp(xport->getName(), name, n) == 0) return xport;
    }
    return nullptr;
}

Transport *TransferEngine::initTransport(const char *proto) {
    if (std::string(proto) == "rdma") {
        return new RdmaTransport();
    } else if (std::string(proto) == "tcp") {
        return new TcpTransport();
    }
#ifdef USE_CUDA
    else if (std::string(proto) == "nvmeof") {
        return new NVMeoFTransport();
    }
#endif
    else {
        LOG(ERROR) << "Unsupported transport " << proto;
        return NULL;
    }
}
}  // namespace mooncake
