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

#include <future>
#include "config.h"
#include "memory_location.h"
#include <cassert>
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_transport.h"
#include "transport/kunpeng_transport/ub_endpoint.h"
#include "transport/kunpeng_transport/urma_endpoint.h"

namespace mooncake {
UbTransport::UbTransport(UB_ENDPOINT_TYPE endpoint_type)
    : endpoint_type_(endpoint_type) {}

UbTransport::~UbTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_set_.clear();
#endif
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

int UbTransport::install(std::string& local_server_name,
                         std::shared_ptr<TransferMetadata> meta,
                         std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "UbTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }
    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;
    auto ret = initializeUbResources(this);
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot initialize Ub resources";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: initialize Ub resources done";

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "Transfer engine cannot be initialized: cannot "
                      "allocate local segment";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "Transfer engine allocate local segment done";

    ret = startHandshakeDaemon(local_server_name);
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot start handshake daemon";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: start handshake daemon done";

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "UbTransport: cannot publish segments";
        uninit(this);
        return ret;
    }
    LOG(INFO) << "UbTransport: publish segments done";

    return 0;
}

int UbTransport::registerLocalMemory(void* addr, size_t length,
                                     const std::string& name,
                                     bool remote_accessible,
                                     bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    for (auto& context : context_list_) {
        int ret = context->registerMemoryRegion((uint64_t)addr, length);
        if (ret) {
            LOG(ERROR) << "UbTransport: cannot register LocalMemory";
            return ret;
        }
        ret = context->buildLocalBufferDesc((uint64_t)addr, buffer_desc);
        if (ret) {
            LOG(ERROR) << "UbTransport: build buffer description failed";
            return ret;
        }
    }

    // Get the memory location automatically after registered MR(pinned),
    // when the name is kWildcardLocation("*").
    if (name == kWildcardLocation) {
        bool only_first_page = true;
        const std::vector<MemoryLocationEntry> entries =
            getMemoryLocation(addr, length, only_first_page);
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

int UbTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;
    for (auto& context : context_list_)
        context->unregisterMemoryRegion((uint64_t)addr);
    return 0;
}

int UbTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    std::vector<std::future<int>> results;
    results.reserve(buffer_list.size());
    for (auto& buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                return registerLocalMemory(buffer.addr, buffer.length, location,
                                           true, false);
            }));
    }

    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "UbTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
        }
    }

    return metadata_->updateLocalSegmentDesc();
}

int UbTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    std::vector<std::future<int>> results;
    results.reserve(addr_list.size());
    for (auto& addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterLocalMemory(addr, false);
            }));
    }

    for (size_t i = 0; i < addr_list.size(); ++i) {
        if (results[i].get())
            LOG(WARNING) << "UbTransport: Failed to unregister memory: addr "
                         << addr_list[i];
    }

    return metadata_->updateLocalSegmentDesc();
}

Status UbTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "UbTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "UbTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<TransferTask*> task_list;
    task_list.reserve(batch_desc.task_list.size());
    for (auto& task : batch_desc.task_list) task_list.push_back(&task);
    return submitTransferTask(task_list);
}

Status UbTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    std::unordered_map<std::shared_ptr<UbContext>, std::vector<Slice*>>
        slices_to_post;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    const size_t kFragmentSize = globalConfig().fragment_limit;
    const size_t kSubmitWatermark =
        globalConfig().max_wr * globalConfig().num_qp_per_ep;
    uint64_t nr_slices;
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto& task = *task_list[index];
        nr_slices = 0;
        assert(task.request);
        auto& request = *task.request;
        auto request_buffer_id = -1, request_device_id = -1;

        if (selectDevice(local_segment_desc.get(), (uint64_t)request.source,
                         request.length, request_buffer_id,
                         request_device_id)) {
            request_buffer_id = -1;
            request_device_id = -1;
        }

        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            Slice* slice = getSliceCache().allocate();
            assert(slice);
            if (!slice->from_cache) {
                nr_slices++;
            }
            bool merge_final_slice =
                request.length - offset <= kBlockSize + kFragmentSize;
            slice->source_addr = (char*)request.source + offset;
            slice->length =
                merge_final_slice ? request.length - offset : kBlockSize;
            slice->opcode = request.opcode;
            // LOG(INFO) << "target_offset : " << request.target_offset << ",
            // offset : " << offset;
            slice->ub.dest_addr = request.target_offset + offset;
            slice->ub.retry_cnt = 0;
            slice->ub.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->ts = 0;
            slice->status = Slice::PENDING;
            task.slice_list.push_back(slice);

            int buffer_id = -1, device_id = -1,
                retry_cnt = request.advise_retry_cnt;
            bool found_device = false;
            if (request_buffer_id >= 0 && request_device_id >= 0) {
                found_device = true;
                buffer_id = request_buffer_id;
                device_id = request_device_id;
            }
            while (retry_cnt < kMaxRetryCount && !found_device) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                assert(device_id >= 0 &&
                       static_cast<size_t>(device_id) < context_list_.size());
                auto& context = context_list_[device_id];
                assert(context.get());
                if (!context->active()) continue;
                assert(buffer_id >= 0 &&
                       static_cast<size_t>(buffer_id) <
                           local_segment_desc->buffers.size());
                assert(local_segment_desc->buffers[buffer_id].tseg.size() ==
                       context_list_.size());
                found_device = true;
                break;
            }
            if (device_id < 0) {
                auto source_addr = slice->source_addr;
                for (auto& entry : slices_to_post)
                    for (auto s : entry.second) getSliceCache().deallocate(s);
                LOG(ERROR)
                    << "UbTransport: Address not registered by any device(s) "
                    << source_addr;
                return Status::AddressNotRegistered(
                    "UbTransport: not registered by any device(s), "
                    "address: " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            }
            // start to submit batch request task
            auto& context = context_list_[device_id];
            if (!context->active()) {
                LOG(ERROR) << "Device " << device_id << " is not active";
                return Status::InvalidArgument(
                    "Device " + std::to_string(device_id) + " is not active");
            }
            auto local_tseg_index =
                local_segment_desc->buffers[buffer_id].l_seg_index[device_id];
            slice->ub.l_seg = context->localSegWithIndex(local_tseg_index);
            slices_to_post[context].push_back(slice);
            task.total_bytes += slice->length;
            __sync_fetch_and_add(&task.slice_count, 1);
            if (nr_slices >= kSubmitWatermark) {
                for (auto& entry : slices_to_post)
                    entry.first->submitPostSend(entry.second);
                slices_to_post.clear();
                nr_slices = 0;
            }

            if (merge_final_slice) {
                break;
            }
        }
    }
    for (auto& entry : slices_to_post)
        entry.first->submitPostSend(entry.second);
    return Status::OK();
}

Status UbTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                      TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "UbTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count)
            status.s = FAILED;
        else
            status.s = COMPLETED;
        task.is_finished = true;
    } else {
        status.s = WAITING;
    }
    return Status::OK();
}

Transport::SegmentID UbTransport::getSegmentID(
    const std::string& segment_name) {
    return metadata_->getSegmentID(segment_name);
}

int UbTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "ub";
    for (auto& context : context_list_) {
        TransferMetadata::DeviceDesc device_desc;
        device_desc.name = context->deviceName();
        device_desc.eid = context->getEid();
        desc->devices.push_back(device_desc);
    }
    desc->topology = *(local_topology_);
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int UbTransport::onSetupConnections(const HandShakeDesc& peer_desc,
                                    HandShakeDesc& local_desc) {
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    std::shared_ptr<UbContext> context;
    int index = 0;
    for (auto& entry : local_topology_->getHcaList()) {
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

int UbTransport::startHandshakeDaemon(std::string& local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&UbTransport::onSetupConnections, this, std::placeholders::_1,
                  std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

int UbTransport::selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                              int& buffer_id, int& device_id, int retry_cnt) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                        retry_cnt);
}

int UbTransport::selectDevice(SegmentDesc* desc, uint64_t offset, size_t length,
                              std::string_view hint, int& buffer_id,
                              int& device_id, int retry_cnt) {
    if (desc == nullptr) {
        LOG(ERROR) << "UbTransport Get Segment Desc failed";
        return ERR_ADDRESS_NOT_REGISTERED;
    }

    const auto& buffers = desc->buffers;
    for (buffer_id = 0; buffer_id < static_cast<int>(buffers.size());
         ++buffer_id) {
        const auto& buffer = buffers[buffer_id];
        // Check if offset is within buffer range
        if (offset < buffer.addr || length > buffer.length ||
            offset - buffer.addr > buffer.length - length) {
            continue;
        }

        device_id =
            hint.empty()
                ? desc->topology.selectDevice(buffer.name, retry_cnt)
                : desc->topology.selectDevice(buffer.name, hint, retry_cnt);
        if (device_id >= 0) return 0;
        device_id = hint.empty() ? desc->topology.selectDevice(
                                       kWildcardLocation, retry_cnt)
                                 : desc->topology.selectDevice(
                                       kWildcardLocation, hint, retry_cnt);
        if (device_id >= 0) return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int UbTransport::initializeUbResources(UbTransport* t) {
    auto ret = init(t);
    if (ret != 0) {
        LOG(ERROR) << "Failed to init, ret = " << ret;
        return -1;
    }
    auto hca_list = t->local_topology_->getHcaList();
    for (auto& device_name : hca_list) {
        auto& config = globalConfig();
        auto max_endpoints = config.max_ep_per_ctx;
        auto context = buildContext(t, device_name, max_endpoints);
        ret = context->doConstruct(config);
        if (ret) {
            t->local_topology_->disableDevice(device_name);
            LOG(WARNING) << "Disable device " << device_name;
        } else {
            t->context_list_.push_back(context);
            LOG(INFO) << "device " << context->deviceName() << " add to list";
        }
    }
    if (t->local_topology_->empty()) {
        LOG(ERROR) << "UbTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }
    LOG(INFO) << "ub resources init success";
    return 0;
}

int UbTransport::init(UbTransport* transport) {
    if (transport->endpoint_type_ == URMA_ENDPOINT) {
        if (!UrmaContext::init()) {
            LOG(ERROR) << "UrmaContext init failed";
            return -1;
        }
    } else if (transport->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
        return -1;
    } else {
        LOG(ERROR) << "invalid endpoint type : " << transport->endpoint_type_;
        return -1;
    }
    return 0;
}

void UbTransport::uninit(UbTransport* transport) {
    if (transport->endpoint_type_ == URMA_ENDPOINT) {
        if (!UrmaContext::uninit()) {
            LOG(ERROR) << "UrmaContext uninit failed";
        }
    } else if (transport->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
    } else {
        LOG(ERROR) << "invalid endpoint type : " << transport->endpoint_type_;
    }
}

std::shared_ptr<UbContext> UbTransport::buildContext(
    UbTransport* t, const std::string& device_name, int max_endpoints) {
    if (t->endpoint_type_ == URMA_ENDPOINT) {
        auto context =
            std::make_shared<UrmaContext>(*t, device_name, max_endpoints);
        if (!context) {
            LOG(ERROR) << "UrmaContext build failed";
            return nullptr;
        }
        return context;
    } else if (t->endpoint_type_ == OBMM_ENDPOINT) {
        LOG(ERROR) << "ObmmContext not support now.";
        return nullptr;
    } else {
        LOG(ERROR) << "invalid endpoint type : " << t->endpoint_type_;
        return nullptr;
    }
}
}  // namespace mooncake
