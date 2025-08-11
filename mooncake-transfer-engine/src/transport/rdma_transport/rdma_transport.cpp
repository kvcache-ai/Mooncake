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
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

int RdmaTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> meta,
                           std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "RdmaTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;

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
                                       const std::string &name,
                                       bool remote_accessible,
                                       bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
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
    // when the name is kWildcardLocation("*").
    if (name == kWildcardLocation) {
        const std::vector<MemoryLocationEntry> entries =
            getMemoryLocation(addr, length);
        if (entries.empty()) return -1;
        buffer_desc.name = entries[0].location;
        buffer_desc.addr = (uint64_t)addr;
        buffer_desc.length = length;
        int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
        if (rc) return rc;
    } else {
        buffer_desc.name = name;
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
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "rdma";
    for (auto &entry : context_list_) {
        TransferMetadata::DeviceDesc device_desc;
        device_desc.name = entry->deviceName();
        device_desc.lid = entry->lid();
        device_desc.gid = entry->gid();
        desc->devices.push_back(device_desc);
    }
    desc->topology = *(local_topology_.get());
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
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

Status RdmaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "RdmaTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "RdmaTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    std::unordered_map<std::shared_ptr<RdmaContext>, std::vector<Slice *>>
        slices_to_post;
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            Slice *slice = getSliceCache().allocate();
            slice->source_addr = (char *)request.source + offset;
            slice->length = std::min(request.length - offset, kBlockSize);
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = 0;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->ts = 0;
            slice->status = Slice::PENDING;
            task.slice_list.push_back(slice);

            int buffer_id = -1, device_id = -1, retry_cnt = 0;
            while (retry_cnt < kMaxRetryCount) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                auto &context = context_list_[device_id];
                if (!context->active()) continue;
                slice->rdma.source_lkey =
                    local_segment_desc->buffers[buffer_id].lkey[device_id];
                slices_to_post[context].push_back(slice);
                task.total_bytes += slice->length;
                __sync_fetch_and_add(&task.slice_count, 1);
                break;
            }
            if (device_id < 0) {
                auto source_addr = slice->source_addr;
                for (auto &entry : slices_to_post)
                    for (auto s : entry.second) delete s;
                LOG(ERROR)
                    << "RdmaTransport: Address not registered by any device(s) "
                    << source_addr;
                return Status::AddressNotRegistered(
                    "RdmaTransport: not registered by any device(s), "
                    "address: " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            }
        }
    }
    for (auto &entry : slices_to_post)
        entry.first->submitPostSend(entry.second);
    return Status::OK();
}

Status RdmaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<std::shared_ptr<RdmaContext>, std::vector<Slice *>>
        slices_to_post;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    assert(local_segment_desc.get());
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    const size_t kFragmentSize = globalConfig().fragment_limit;
    const size_t kSubmitWatermark =
        globalConfig().max_wr * globalConfig().num_qp_per_ep;
    uint64_t nr_slices;
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        nr_slices = 0;
        assert(task.request);
        auto &request = *task.request;
        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            Slice *slice = getSliceCache().allocate();
            assert(slice);
            if (!slice->from_cache) {
                nr_slices++;
            }

            bool merge_final_slice =
                request.length - offset <= kBlockSize + kFragmentSize;

            slice->source_addr = (char *)request.source + offset;
            slice->length =
                merge_final_slice ? request.length - offset : kBlockSize;
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = request.advise_retry_cnt;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;
            slice->ts = 0;
            task.slice_list.push_back(slice);

            int buffer_id = -1, device_id = -1,
                retry_cnt = request.advise_retry_cnt;
            bool found_device = false;
            while (retry_cnt < kMaxRetryCount) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                assert(device_id >= 0 && device_id < context_list_.size());
                auto &context = context_list_[device_id];
                assert(context.get());
                if (!context->active()) continue;
                assert(buffer_id >= 0 &&
                       buffer_id < local_segment_desc->buffers.size());
                assert(local_segment_desc->buffers[buffer_id].lkey.size() ==
                       context_list_.size());
                slice->rdma.source_lkey =
                    local_segment_desc->buffers[buffer_id].lkey[device_id];
                slices_to_post[context].push_back(slice);
                task.total_bytes += slice->length;
                // task.slices.push_back(slice);
                __sync_fetch_and_add(&task.slice_count, 1);
                found_device = true;
                break;
            }
            if (!found_device) {
                auto source_addr = slice->source_addr;
                for (auto &entry : slices_to_post)
                    for (auto s : entry.second) getSliceCache().deallocate(s);
                LOG(ERROR)
                    << "Memory region not registered by any active device(s): "
                    << source_addr;
                return Status::AddressNotRegistered(
                    "Memory region not registered by any active device(s): " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            }

            if (nr_slices >= kSubmitWatermark) {
                for (auto &entry : slices_to_post)
                    entry.first->submitPostSend(entry.second);
                slices_to_post.clear();
                nr_slices = 0;
            }

            if (merge_final_slice) {
                break;
            }
        }
    }

    for (auto &entry : slices_to_post)
        if (!entry.second.empty()) entry.first->submitPostSend(entry.second);
    return Status::OK();
}

Status RdmaTransport::getTransferStatus(BatchID batch_id,
                                        std::vector<TransferStatus> &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    status.resize(task_count);
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        auto &task = batch_desc.task_list[task_id];
        status[task_id].transferred_bytes = task.transferred_bytes;
        uint64_t success_slice_count = task.success_slice_count;
        uint64_t failed_slice_count = task.failed_slice_count;
        if (success_slice_count + failed_slice_count == task.slice_count) {
            if (failed_slice_count)
                status[task_id].s = TransferStatusEnum::FAILED;
            else
                status[task_id].s = TransferStatusEnum::COMPLETED;
            task.is_finished = true;
        } else {
            status[task_id].s = TransferStatusEnum::WAITING;
        }
    }
    return Status::OK();
}

Status RdmaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                        TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "RdmaTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count)
            status.s = TransferStatusEnum::FAILED;
        else
            status.s = TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
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
    auto hca_list = local_topology_->getHcaList();
    for (auto &device_name : hca_list) {
        auto context = std::make_shared<RdmaContext>(*this, device_name);
        auto &config = globalConfig();
        int ret = context->construct(config.num_cq_per_ctx,
                                     config.num_comp_channels_per_ctx,
                                     config.port, config.gid_index,
                                     config.max_cqe, config.max_ep_per_ctx);
        if (ret) {
            local_topology_->disableDevice(device_name);
            LOG(WARNING) << "Disable device " << device_name;
        } else {
            context_list_.push_back(context);
        }
    }
    if (local_topology_->empty()) {
        LOG(ERROR) << "RdmaTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }
    return 0;
}

int RdmaTransport::startHandshakeDaemon(std::string &local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

// According to the request desc, offset and length information, find proper
// buffer_id and device_id as output.
// Return 0 if successful, ERR_ADDRESS_NOT_REGISTERED otherwise.
int RdmaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, std::string_view hint,
                                int &buffer_id, int &device_id,
                                int retry_count) {
    if (desc == nullptr) return ERR_ADDRESS_NOT_REGISTERED;
    const auto &buffers = desc->buffers;
    for (buffer_id = 0; buffer_id < static_cast<int>(buffers.size());
         ++buffer_id) {
        const auto &buffer = buffers[buffer_id];

        // Check if offset is within buffer range
        if (offset < buffer.addr || length > buffer.length ||
            offset - buffer.addr > buffer.length - length) {
            continue;
        }

        device_id =
            hint.empty()
                ? desc->topology.selectDevice(buffer.name, retry_count)
                : desc->topology.selectDevice(buffer.name, hint, retry_count);
        if (device_id >= 0) return 0;
        device_id = hint.empty() ? desc->topology.selectDevice(
                                       kWildcardLocation, retry_count)
                                 : desc->topology.selectDevice(
                                       kWildcardLocation, hint, retry_count);
        if (device_id >= 0) return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int RdmaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, int &buffer_id, int &device_id,
                                int retry_count) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                        retry_count);
}
}  // namespace mooncake
