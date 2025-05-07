// Copyright 2025 KVCache.AI
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

#include "transport_v1/rdma/rdma_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <set>

#include "common/common.h"
#include "common/config.h"
#include "transport_v1/rdma/buffers.h"
#include "transport_v1/rdma/endpoint_store.h"
#include "transport_v1/rdma/workers.h"
#include "utility/memory_location.h"
#include "utility/topology.h"

namespace mooncake {
namespace v1 {
RdmaTask::~RdmaTask() { delete[] slices; }

RdmaTransport::RdmaTransport() : installed_(false) {}

RdmaTransport::~RdmaTransport() { uninstall(); }

Status RdmaTransport::install(
    std::string &local_segment_name,
    std::shared_ptr<mooncake::TransferMetadata> metadata_manager,
    std::shared_ptr<Topology> local_topology) {
    if (installed_) {
        return Status::InvalidArgument("cannot install for multiple times");
    }

    if (local_topology == nullptr || local_topology->getHcaList().empty()) {
        return Status::DeviceNotFound("no RDMA device found in topology");
    }

    resources_ = std::make_shared<RdmaResources>();
    resources_->metadata_manager = metadata_manager;
    resources_->local_segment_name = local_segment_name;
    resources_->local_topology = local_topology;
    auto endpoint_store = std::make_shared<SIEVEEndpointStore>(256);
    auto hca_list = resources_->local_topology->getHcaList();
    for (auto &device_name : hca_list) {
        auto context = std::make_shared<RdmaContext>();
        int ret = context->construct(device_name, endpoint_store,
                                     RdmaContext::DeviceParams{});
        if (ret) {
            resources_->local_topology->disableDevice(device_name);
            LOG(WARNING) << "Disable device " << device_name;
            continue;
        }
        resources_->context_group[device_name].context = context;
        resources_->context_group[device_name].local_buffers =
            std::make_shared<LocalBuffers>(context);
    }
    if (resources_->local_topology->empty()) {
        uninstall();
        return Status::DeviceNotFound("no RDMA device detected in active");
    }

    allocateLocalSegmentID();

    if (startHandshakeDaemon()) {
        uninstall();
        return Status::Metadata("failed to start handshake daemon thread");
    }

    if (resources_->metadata_manager->updateLocalSegmentDesc()) {
        uninstall();
        return Status::Metadata("failed to upload local segment descriptor");
    }

    installed_ = true;
    return Status::OK();
}

Status RdmaTransport::uninstall() {
    if (installed_) {
        resources_->metadata_manager->removeSegmentDesc(
            resources_->local_segment_name);
        resources_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status RdmaTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    batch = std::make_shared<RdmaSubBatch>(max_size);
    return Status::OK();
}

Status RdmaTransport::freeSubBatch(SubBatchRef &batch) {
    auto rdma_batch = std::dynamic_pointer_cast<RdmaSubBatch>(batch);
    if (!rdma_batch) return Status::InvalidArgument("invalid rdma sub batch");
    rdma_batch.reset();
    return Status::OK();
}

Status RdmaTransport::submitTransferTasks(
    SubBatchRef &batch, const std::vector<Request> &request_list) {
    auto rdma_batch = std::dynamic_pointer_cast<RdmaSubBatch>(batch);
    if (!rdma_batch) return Status::InvalidArgument("invalid rdma sub batch");
    if (request_list.size() + rdma_batch->task_list.size() >
        rdma_batch->max_size)
        return Status::InvalidArgument("too many requests");
    const static size_t kBlockSize = 65536;
    for (auto &request : request_list) {
        rdma_batch->task_list.push_back(RdmaTask{});
        auto &task = rdma_batch->task_list[rdma_batch->task_list.size() - 1];
        task.request = request;
        task.num_slices = (request.length + kBlockSize - 1) / kBlockSize;
        task.slices = new RdmaSlice[task.num_slices];
        RdmaSlice *slice = task.slices;
        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            slice->source_addr = (char *)request.source + offset;
            slice->target_addr = request.target_offset + offset;
            slice->length = std::min(request.length - offset, kBlockSize);
            slice->task = &task;
            slice++;
        }
        workers_->submit(slice, task.num_slices);
    }
    return Status::OK();
}

Transport::TransferStatus RdmaTransport::getTransferStatus(
    SubBatchRef &batch, int request_index) {
    auto rdma_batch = std::dynamic_pointer_cast<RdmaSubBatch>(batch);
    if (!rdma_batch) return TransferStatus{INVALID, 0};
    if (request_index < 0 ||
        request_index >= (int)rdma_batch->task_list.size()) {
        return TransferStatus{INVALID, 0};
    }
    return rdma_batch->task_list[request_index].status;
}

Status RdmaTransport::registerLocalMemory(
    const std::vector<BufferEntry> &buffer_list) {
    if (buffer_list.empty()) return Status::OK();
    if (buffer_list.size() == 1) {
        int ret = registerSingleLocalMemory(buffer_list[0], true);
        return ret == 0 ? Status::OK()
                        : Status::Context("register local memory error");
    }
    std::vector<std::future<int>> results;
    for (auto &buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer]() -> int {
                return registerSingleLocalMemory(buffer, false);
            }));
    }
    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "register local memory error: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
        }
    }
    int ret = resources_->metadata_manager->updateLocalSegmentDesc();
    return ret == 0 ? Status::OK()
                    : Status::Context("update local segment descriptor error");
}

Status RdmaTransport::unregisterLocalMemory(
    const std::vector<void *> &addr_list) {
    if (addr_list.empty()) return Status::OK();
    if (addr_list.size() == 1) {
        int ret = unregisterSingleLocalMemory(addr_list[0], true);
        return ret == 0 ? Status::OK()
                        : Status::Context("register local memory error");
    }
    std::vector<std::future<int>> results;
    for (auto &addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterSingleLocalMemory(addr, false);
            }));
    }
    for (size_t i = 0; i < addr_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "unregister local memory error: addr "
                         << addr_list[i];
        }
    }
    int ret = resources_->metadata_manager->updateLocalSegmentDesc();
    return ret == 0 ? Status::OK()
                    : Status::Context("update local segment descriptor error");
}

void RdmaTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    desc->name = resources_->local_segment_name;
    desc->protocol = "rdma";
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &entry : resources_->context_group) {
        DeviceDesc device_desc;
        auto &context = entry.second.context;
        device_desc.name = context->name();
        device_desc.lid = context->lid();
        device_desc.gid = context->gid();
        detail.devices.push_back(device_desc);
    }
    detail.topology = *(resources_->local_topology.get());
    resources_->metadata_manager->addLocalSegment(
        LOCAL_SEGMENT_ID, resources_->local_segment_name, std::move(desc));
}

int RdmaTransport::registerSingleLocalMemory(const BufferEntry &buffer,
                                               bool update_meta) {
    BufferDesc buffer_desc;
    for (auto &entry : resources_->context_group) {
        auto &local_buffers = entry.second.local_buffers;
        uint32_t lkey, rkey;
        int ret = local_buffers->add(buffer, lkey, rkey);
        if (ret) return ret;
        buffer_desc.lkey.push_back(lkey);
        buffer_desc.rkey.push_back(rkey);
    }

    // Get the memory location automatically after registered MR(pinned),
    // when the name is kWildcardLocation ("*").
    if (buffer.location == kWildcardLocation) {
        const std::vector<MemoryLocationEntry> entries =
            getMemoryLocation(buffer.addr, buffer.length);
        for (auto &entry : entries) {
            buffer_desc.location = entry.location;
            buffer_desc.addr = entry.start;
            buffer_desc.length = entry.len;
            int rc = resources_->metadata_manager->addLocalMemoryBuffer(
                buffer_desc, update_meta);
            if (rc) return rc;
        }
    } else {
        buffer_desc.location = buffer.location;
        buffer_desc.addr = (uint64_t)buffer.addr;
        buffer_desc.length = buffer.length;
        int rc = resources_->metadata_manager->addLocalMemoryBuffer(
            buffer_desc, update_meta);
        if (rc) return rc;
    }
    return 0;
}

int RdmaTransport::unregisterSingleLocalMemory(void *addr, bool update_meta) {
    int rc = resources_->metadata_manager->removeLocalMemoryBuffer(addr,
                                                                   update_meta);
    if (rc) return rc;
    for (auto &entry : resources_->context_group) {
        entry.second.local_buffers->remove(addr);
    }
    return 0;
}

int RdmaTransport::onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                            HandShakeDesc &local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty() ||
        !resources_->context_group.count(local_nic_name))
        return ERR_INVALID_ARGUMENT;
    auto context = resources_->context_group[local_nic_name].context;
    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) return ERR_ENDPOINT;
    auto peer_nic_path_ = peer_desc.local_nic_path;
    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_INVALID_ARGUMENT;
    }

    local_desc.local_nic_path =
        MakeNicPath(resources_->local_segment_name, context->name());
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = endpoint->qpNum();

    auto segment_desc =
        resources_->metadata_manager->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        auto &detail = std::get<MemorySegmentDesc>(segment_desc->detail);
        for (auto &nic : detail.devices)
            if (nic.name == peer_nic_name)
                return endpoint->configurePeer(
                    nic.gid, nic.lid, peer_desc.qp_num, &local_desc.reply_msg);
    }
    local_desc.reply_msg =
        "Peer nic not found in that server: " + peer_nic_path_;
    LOG(ERROR) << local_desc.reply_msg;
    return ERR_DEVICE_NOT_FOUND;
}

int RdmaTransport::startHandshakeDaemon() {
    auto local_rpc_meta = resources_->metadata_manager->localRpcMeta();
    return resources_->metadata_manager->startHandshakeDaemon(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        local_rpc_meta.rpc_port, local_rpc_meta.sockfd);
}

int RdmaTransport::sendHandshake(const std::string &peer_server_name,
                                   const HandShakeDesc &local_desc,
                                   HandShakeDesc &peer_desc) {
    return resources_->metadata_manager->sendHandshake(peer_server_name,
                                                       local_desc, peer_desc);
}
}  // namespace v1
}  // namespace mooncake
