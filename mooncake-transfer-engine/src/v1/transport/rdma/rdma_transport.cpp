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

#include "v1/transport/rdma/rdma_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <set>

#include "v1/common.h"
#include "v1/transport/rdma/buffers.h"
#include "v1/transport/rdma/endpoint_store.h"
#include "v1/transport/rdma/workers.h"
#include "v1/utility/memory_location.h"
#include "v1/utility/topology.h"

namespace mooncake {
namespace v1 {
RdmaTransport::RdmaTransport() : installed_(false) {}

RdmaTransport::~RdmaTransport() { uninstall(); }

Status RdmaTransport::install(
    std::string &local_segment_name,
    std::shared_ptr<TransferMetadata> metadata_manager,
    std::shared_ptr<Topology> local_topology) {
    if (installed_) {
        return Status::InvalidArgument(
            "RDMA transport has been installed" MSG_TAIL);
    }

    if (local_topology == nullptr || local_topology->getHcaList().empty()) {
        return Status::DeviceNotFound(
            "No RDMA device found in topology" MSG_TAIL);
    }

    params_ = std::make_shared<RdmaParams>();
    metadata_manager_ = metadata_manager;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;
    local_buffer_manager_.setTopology(local_topology);
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
        local_buffer_manager_.addDevice(context.get());
    }
    if (local_topology_->empty()) {
        uninstall();
        return Status::DeviceNotFound(
            "No RDMA device detected in active" MSG_TAIL);
    }

    allocateLocalSegmentID();

    metadata_manager_->setBootstrapRdmaCallback(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2));

    workers_ = std::make_unique<Workers>(this);
    workers_->start();

    installed_ = true;
    return Status::OK();
}

Status RdmaTransport::uninstall() {
    if (installed_) {
        workers_.reset();
        // metadata_manager_->removeSegmentDesc(local_segment_name_);
        metadata_manager_.reset();
        local_buffer_manager_.clear();
        context_set_.clear();
        context_name_lookup_.clear();
        installed_ = false;
    }
    return Status::OK();
}

Status RdmaTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto rdma_batch = new RdmaSubBatch();
    batch = rdma_batch;
    rdma_batch->task_list.reserve(max_size);
    rdma_batch->max_size = max_size;
    return Status::OK();
}

Status RdmaTransport::freeSubBatch(SubBatchRef &batch) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch *>(batch);
    if (!rdma_batch)
        return Status::InvalidArgument("Invalid RDMA sub-batch" MSG_TAIL);
    for (auto &slice : rdma_batch->slice_chain) {
        while (slice) {
            auto next = slice->next;
            RdmaSliceStorage::Get().deallocate(slice);
            slice = next;
        }
    }
    delete rdma_batch;
    batch = nullptr;
    return Status::OK();
}

Status RdmaTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch *>(batch);
    if (!rdma_batch)
        return Status::InvalidArgument("Invalid RDMA sub-batch" MSG_TAIL);
    if (request_list.size() + rdma_batch->task_list.size() >
        rdma_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" MSG_TAIL);
    const size_t block_size = params_->workers.block_size;
    RdmaSliceList slice_list;
    RdmaSlice *slice_tail = nullptr;
    for (auto &request : request_list) {
        rdma_batch->task_list.push_back(RdmaTask{});
        auto &task = rdma_batch->task_list[rdma_batch->task_list.size() - 1];
        task.request = request;
        task.num_slices = 0;
        task.status_word = WAITING;
        task.transferred_bytes = 0;
        for (uint64_t offset = 0; offset < request.length;
             offset += block_size) {
            auto slice = RdmaSliceStorage::Get().allocate();
            slice->source_addr = (char *)request.source + offset;
            slice->target_addr = request.target_offset + offset;
            slice->length = std::min(request.length - offset, block_size);
            slice->task = &task;
            slice->retry_count = 0;
            slice->endpoint_quota = nullptr;
            slice->next = nullptr;
            task.num_slices++;
            slice_list.num_slices++;
            if (slice_list.first) {
                assert(slice_tail);
                slice_tail->next = slice;
                slice_tail = slice;
            } else {
                slice_list.first = slice_tail = slice;
            }
        }
    }
    rdma_batch->slice_chain.push_back(slice_list.first);
    workers_->submit(slice_list);
    return Status::OK();
}

Status RdmaTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                        TransferStatus &status) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)rdma_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task ID" MSG_TAIL);
    }
    auto &task = rdma_batch->task_list[task_id];
    status = TransferStatus{task.status_word, task.transferred_bytes};
    return Status::OK();
}

void RdmaTransport::queryOutstandingTasks(SubBatchRef batch,
                                          std::vector<int> &task_id_list) {
    auto rdma_batch = dynamic_cast<RdmaSubBatch *>(batch);
    if (!rdma_batch) return;
    for (int task_id = 0; task_id < (int)rdma_batch->task_list.size();
         ++task_id) {
        auto &task = rdma_batch->task_list[task_id];
        if (task.success_slices + task.failed_slices < task.num_slices) {
            task_id_list.push_back(task_id);
        }
    }
}

Status RdmaTransport::registerLocalMemory(
    const std::vector<BufferEntry> &buffer_list) {
    if (buffer_list.empty()) return Status::OK();
    if (buffer_list.size() == 1) {
        return registerSingleLocalMemory(buffer_list[0], true);
    }
    std::vector<std::future<Status>> results;
    for (auto &buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer]() -> Status {
                return registerSingleLocalMemory(buffer, false);
            }));
    }
    for (size_t i = 0; i < buffer_list.size(); ++i) {
        auto status = results[i].get();
        if (!status.ok()) return status;
    }
    auto segment_desc = metadata_manager_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    local_buffer_manager_.fillBufferDesc(segment_desc);
    int ret = metadata_manager_->updateSegmentDesc(segment_desc);
    return ret == 0
               ? Status::OK()
               : Status::InternalError("update local segment descriptor error");
}

Status RdmaTransport::unregisterLocalMemory(
    const std::vector<BufferEntry> &buffer_list) {
    if (buffer_list.empty()) return Status::OK();
    if (buffer_list.size() == 1) {
        return unregisterSingleLocalMemory(buffer_list[0], true);
    }
    std::vector<std::future<Status>> results;
    for (auto &buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer]() -> Status {
                return unregisterSingleLocalMemory(buffer, false);
            }));
    }
    for (size_t i = 0; i < buffer_list.size(); ++i) {
        auto status = results[i].get();
        if (!status.ok()) return status;
    }
    auto segment_desc = metadata_manager_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    local_buffer_manager_.fillBufferDesc(segment_desc);
    int ret = metadata_manager_->updateSegmentDesc(segment_desc);
    return ret == 0
               ? Status::OK()
               : Status::InternalError("update local segment descriptor error");
}

void RdmaTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    desc->name = local_segment_name_;
    desc->type = SegmentType::Memory;
    auto &detail = std::get<MemorySegmentDesc>(desc->detail);
    for (auto &context : context_set_) {
        DeviceDesc device_desc;
        device_desc.name = context->name();
        device_desc.lid = context->lid();
        device_desc.gid = context->gid();
        detail.devices.push_back(device_desc);
    }
    detail.topology = *(local_topology_.get());
    metadata_manager_->updateSegmentDesc(desc);
}

Status RdmaTransport::registerSingleLocalMemory(const BufferEntry &buffer,
                                                bool update_meta) {
    Status status = local_buffer_manager_.addBuffer(buffer);
    if (!status.ok()) return status;
    if (!update_meta) return Status::OK();
    auto segment_desc = metadata_manager_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    local_buffer_manager_.fillBufferDesc(segment_desc);
    metadata_manager_->updateSegmentDesc(segment_desc);
    return Status::OK();
}

Status RdmaTransport::unregisterSingleLocalMemory(const BufferEntry &buffer,
                                                  bool update_meta) {
    AddressRange range;
    range.addr = buffer.addr;
    range.length = buffer.length;
    local_buffer_manager_.removeBuffer(range);
    if (!update_meta) return Status::OK();
    auto segment_desc = metadata_manager_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    local_buffer_manager_.fillBufferDesc(segment_desc);
    metadata_manager_->updateSegmentDesc(segment_desc);
    return Status::OK();
}

int RdmaTransport::onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                          HandShakeDesc &local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty() || !context_name_lookup_.count(local_nic_name)) {
        local_desc.reply_msg =
            "Unable to find RDMA device " + peer_desc.peer_nic_path;
        return ERR_ENDPOINT;
    }

    auto index = context_name_lookup_[local_nic_name];
    auto context = context_set_[index];
    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) {
        local_desc.reply_msg = "Unable to create endpoint object";
        return ERR_ENDPOINT;
    }

    auto peer_nic_path_ = peer_desc.local_nic_path;
    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        return ERR_ENDPOINT;
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
    local_desc.reply_msg = "Unable to find RDMA device " + peer_nic_path_;
    return ERR_ENDPOINT;
}

}  // namespace v1
}  // namespace mooncake
