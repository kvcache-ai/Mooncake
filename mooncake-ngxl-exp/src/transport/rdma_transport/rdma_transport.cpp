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

#include "transport/rdma_transport/rdma_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <set>

#include "common.h"
#include "config.h"
#include "memory_location.h"
#include "topology.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"

namespace mooncake {
RdmaTransport::RdmaTransport() {}

RdmaTransport::~RdmaTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    for (auto &entry : batch_desc_set_) delete entry.second;
    batch_desc_set_.clear();
#endif
    batch_desc_set_.clear();
    context_list_.clear();
}

int RdmaTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> metadata,
                           std::shared_ptr<Topology> topology) {
    if (topology == nullptr) {
        LOG(ERROR) << "RdmaTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    metadata_ = metadata;
    local_server_name_ = local_server_name;
    local_topology_ = topology;

    auto ret = initializeRdmaResources();
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot initialize RDMA resources";
        return ret;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "Transfer engine cannot be initialized: cannot "
                      "allocate local segment";
        return ret;
    }

    ret = startHandshakeDaemon(local_server_name);
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot start handshake daemon";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot publish segments";
        return ret;
    }

    return 0;
}

int RdmaTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &location,
                                       bool remote_accessible,
                                       bool update_metadata) {
    (void)remote_accessible;
    BufferAttr buffer_desc;
    const static int access_rights = IBV_ACCESS_LOCAL_WRITE |
                                     IBV_ACCESS_REMOTE_WRITE |
                                     IBV_ACCESS_REMOTE_READ;
    for (auto &context : context_list_) {
        int ret = context->registerMemoryRegion(addr, length, access_rights);
        if (ret) return ret;
        buffer_desc.lkey.push_back(context->lkey(addr));
        buffer_desc.rkey.push_back(context->rkey(addr));
    }

    // Get the memory location automatically after registered MR(pinned),
    // when the name is "*".
    if (location == "*") {
        const std::vector<MemoryLocationEntry> entries =
            getMemoryLocation(addr, length);
        for (auto &entry : entries) {
            buffer_desc.location = entry.location;
            buffer_desc.addr = entry.start;
            buffer_desc.length = entry.len;
            int rc =
                metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
            if (rc) return rc;
        }
    } else {
        buffer_desc.location = location;
        buffer_desc.addr = (uint64_t)addr;
        buffer_desc.length = length;
        int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);

        if (rc) return rc;
    }

    return 0;
}

int RdmaTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;

    for (auto &context : context_list_) context->unregisterMemoryRegion(addr);

    return 0;
}

int RdmaTransport::allocateLocalSegmentID() {
    std::shared_ptr<SegmentDesc> desc;
    desc = metadata_->getLocalSegment();
    if (!desc) {
        desc = std::make_shared<SegmentDesc>();
        if (!desc) return ERR_MEMORY;
        desc->name = local_server_name_;
        desc->type = MemoryKind;
    }
    if (!desc->memory.rdma.empty()) return ERR_INVALID_ARGUMENT;
    for (auto &entry : context_list_) {
        RdmaAttr rdma;
        rdma.name = entry->deviceName();
        rdma.lid = entry->lid();
        rdma.gid = entry->gid();
        desc->memory.rdma.push_back(rdma);
    }
    desc->memory.topology = *(local_topology_.get());
    return metadata_->setLocalSegment(std::move(desc));
}

int RdmaTransport::registerLocalMemoryBatch(
    const std::vector<RdmaTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    std::vector<std::future<int>> results;
    for (auto &buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                return registerLocalMemory(buffer.addr, buffer.length, location,
                                           true, false);
            }));
    }

    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "RdmaTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
        }
    }

    return metadata_->updateLocalSegmentDesc();
}

int RdmaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    std::vector<std::future<int>> results;
    for (auto &addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterLocalMemory(addr, false);
            }));
    }

    for (size_t i = 0; i < addr_list.size(); ++i) {
        if (results[i].get())
            LOG(WARNING) << "RdmaTransport: Failed to unregister memory: addr "
                         << addr_list[i];
    }

    return metadata_->updateLocalSegmentDesc();
}

Status RdmaTransport::submitTransferTask(
    const std::vector<TransferRequest *> &request_list,
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<std::shared_ptr<RdmaContext>, std::vector<Slice *>>
        slices_to_post;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    for (size_t index = 0; index < request_list.size(); ++index) {
        auto &request = *request_list[index];
        auto &task = *task_list[index];
        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            auto slice = new Slice();
            slice->source_addr = (char *)request.source + offset;
            slice->length = std::min(request.length - offset, kBlockSize);
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = 0;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;

            int buffer_id = -1, device_id = -1, retry_cnt = 0;
            while (retry_cnt < kMaxRetryCount) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                auto &context = context_list_[device_id];
                if (!context->active()) continue;
                slice->rdma.source_lkey =
                    local_segment_desc->memory.buffers[buffer_id]
                        .lkey[device_id];
                slices_to_post[context].push_back(slice);
                task.total_bytes += slice->length;
                // task.slices.push_back(slice);
                task.slice_count += 1;
                break;
            }
            if (device_id < 0) {
                LOG(ERROR)
                    << "RdmaTransport: Address not registered by any device(s) "
                    << slice->source_addr;
                return Status::AddressNotRegistered(
                    "RdmaTransport: not registered by any device(s), address: "
                    + std::to_string(
                        reinterpret_cast<uintptr_t>(slice->source_addr)));
            }
        }
    }
    for (auto &entry : slices_to_post)
        entry.first->submitPostSend(entry.second);
    return Status::OK();
}

RdmaTransport::SegmentID RdmaTransport::getSegmentID(
    const std::string &segment_name) {
    return metadata_->getSegmentID(segment_name);
}

int RdmaTransport::onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                          HandShakeDesc &local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    std::shared_ptr<RdmaContext> context;
    int index = 0;
    for (auto &entry : local_topology_->getHcaList()) {
        if (entry == local_nic_name) {
            context = context_list_[index];
            break;
        }
        index++;
    }
    if (!context) return ERR_INVALID_ARGUMENT;

#ifdef CONFIG_ERDMA
    if (context->deleteEndpoint(peer_desc.local_nic_path)) return ERR_ENDPOINT;
#endif

    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) return ERR_ENDPOINT;
    return endpoint->setupConnectionsByPassive(peer_desc, local_desc);
}

int RdmaTransport::initializeRdmaResources() {
    if (local_topology_->empty()) {
        LOG(ERROR) << "RdmaTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }

    std::vector<int> device_speed_list;
    for (auto &device_name : local_topology_->getHcaList()) {
        auto context = std::make_shared<RdmaContext>(*this, device_name);
        if (!context) return ERR_MEMORY;

        auto &config = globalConfig();
        int ret = context->construct(config.num_cq_per_ctx,
                                     config.num_comp_channels_per_ctx,
                                     config.port, config.gid_index,
                                     config.max_cqe, config.max_ep_per_ctx);
        if (ret) return ret;
        device_speed_list.push_back(context->activeSpeed());
        context_list_.push_back(context);
    }

    return 0;
}

int RdmaTransport::startHandshakeDaemon(std::string &local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port);
}

// According to the request desc, offset and length information, find proper
// buffer_id and device_id as output.
// Return 0 if successful, ERR_ADDRESS_NOT_REGISTERED otherwise.
int RdmaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, int &buffer_id, int &device_id,
                                int retry_count) {
    for (buffer_id = 0; buffer_id < (int)desc->memory.buffers.size();
         ++buffer_id) {
        auto &buffer_desc = desc->memory.buffers[buffer_id];
        if (buffer_desc.addr > offset ||
            offset + length > buffer_desc.addr + buffer_desc.length)
            continue;
        device_id = desc->memory.topology.selectDevice(buffer_desc.location,
                                                       retry_count);
        if (device_id >= 0) return 0;
    }

    return ERR_ADDRESS_NOT_REGISTERED;
}
}  // namespace mooncake
