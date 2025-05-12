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

    params_ = std::make_shared<RdmaParams>();
    metadata_manager_ = metadata_manager;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    auto endpoint_store = std::make_shared<SIEVEEndpointStore>(
        params_->endpoint.endpoint_store_cap);
    auto hca_list = local_topology_->getHcaList();
    for (auto &device_name : hca_list) {
        auto context = std::make_shared<RdmaContext>();
        int ret = context->construct(device_name, endpoint_store, params_);
        if (ret) {
            local_topology_->disableDevice(device_name);
            LOG(WARNING) << "Disable device " << device_name;
            continue;
        }
        context_name_lookup_[device_name] = context_set_.size();
        context_set_.push_back(context);
        local_buffer_set_.addDevice(context.get());
    }
    if (local_topology_->empty()) {
        uninstall();
        return Status::DeviceNotFound("no RDMA device detected in active");
    }

    allocateLocalSegmentID();

    if (startHandshakeDaemon()) {
        uninstall();
        return Status::Metadata("failed to start handshake daemon thread");
    }

    if (metadata_manager_->updateLocalSegmentDesc()) {
        uninstall();
        return Status::Metadata("failed to upload local segment descriptor");
    }

    workers_ = std::make_shared<Workers>(this);
    workers_->start();

    installed_ = true;
    return Status::OK();
}

Status RdmaTransport::uninstall() {
    if (installed_) {
        workers_.reset();
        metadata_manager_->removeSegmentDesc(local_segment_name_);
        metadata_manager_.reset();
        local_buffer_set_.clear();
        context_set_.clear();
        context_name_lookup_.clear();
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
    const size_t block_size = params_->workers.block_size;
    for (auto &request : request_list) {
        rdma_batch->task_list.push_back(RdmaTask{});
        auto &task = rdma_batch->task_list[rdma_batch->task_list.size() - 1];
        task.request = request;
        task.slice_list.num_slices =
            (request.length + block_size - 1) / block_size;
        RdmaSlice *first_slice = nullptr, *prev_slice = nullptr;
        for (uint64_t offset = 0; offset < request.length;
             offset += block_size) {
            auto slice = RdmaSliceStorage::Get().allocate();
            slice->source_addr = (char *)request.source + offset;
            slice->target_addr = request.target_offset + offset;
            slice->length = std::min(request.length - offset, block_size);
            slice->task = &task;
            slice->next = nullptr;
            slice->retry_count = 0;
            slice->endpoint_quota = nullptr;
            if (prev_slice) {
                prev_slice->next = slice;
            } else {
                first_slice = slice;
            }
            prev_slice = slice;
        }
        task.slice_list.first = first_slice;
        task.slice_list.last = prev_slice;
        workers_->submit(task.slice_list);
    }
    return Status::OK();
}

Transport::TransferStatus RdmaTransport::getTransferStatus(SubBatchRef &batch,
                                                           int request_index) {
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
    int ret = metadata_manager_->updateLocalSegmentDesc();
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
    int ret = metadata_manager_->updateLocalSegmentDesc();
    return ret == 0 ? Status::OK()
                    : Status::Context("update local segment descriptor error");
}

void RdmaTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    desc->name = local_segment_name_;
    desc->protocol = "rdma";
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &context : context_set_) {
        DeviceDesc device_desc;
        device_desc.name = context->name();
        device_desc.lid = context->lid();
        device_desc.gid = context->gid();
        detail.devices.push_back(device_desc);
    }
    detail.topology = *(local_topology_.get());
    metadata_manager_->addLocalSegment(LOCAL_SEGMENT_ID, local_segment_name_,
                                       std::move(desc));
}

int RdmaTransport::registerSingleLocalMemory(const BufferEntry &buffer,
                                             bool update_meta) {
    BufferDesc buffer_desc;
    int ret = local_buffer_set_.addBuffer(buffer);
    if (ret) return ret;
    for (auto &context : context_set_) {
        uint32_t lkey, rkey;
        local_buffer_set_.findBufferLegacy(buffer.addr, context.get(), lkey,
                                           rkey);
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
            int rc = metadata_manager_->addLocalMemoryBuffer(buffer_desc,
                                                             update_meta);
            if (rc) return rc;
        }
    } else {
        buffer_desc.location = buffer.location;
        buffer_desc.addr = (uint64_t)buffer.addr;
        buffer_desc.length = buffer.length;
        int rc =
            metadata_manager_->addLocalMemoryBuffer(buffer_desc, update_meta);
        if (rc) return rc;
    }
    return 0;
}

int RdmaTransport::unregisterSingleLocalMemory(void *addr, bool update_meta) {
    int rc = metadata_manager_->removeLocalMemoryBuffer(addr, update_meta);
    if (rc) return rc;
    local_buffer_set_.removeBufferLegacy(addr);
    return 0;
}

int RdmaTransport::onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                          HandShakeDesc &local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty() || !context_name_lookup_.count(local_nic_name))
        return ERR_INVALID_ARGUMENT;
    auto index = context_name_lookup_[local_nic_name];
    auto context = context_set_[index];
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
        MakeNicPath(local_segment_name_, context->name());
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = endpoint->qpNum();

    auto segment_desc =
        metadata_manager_->getSegmentDescByName(peer_server_name);
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
    auto local_rpc_meta = metadata_manager_->localRpcMeta();
    return metadata_manager_->startHandshakeDaemon(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        local_rpc_meta.rpc_port, local_rpc_meta.sockfd);
}

int RdmaTransport::sendHandshake(const std::string &peer_server_name,
                                 const HandShakeDesc &local_desc,
                                 HandShakeDesc &peer_desc) {
    return metadata_manager_->sendHandshake(peer_server_name, local_desc,
                                            peer_desc);
}
}  // namespace v1
}  // namespace mooncake
