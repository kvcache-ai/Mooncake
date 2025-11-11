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

#include "transport/barex_transport/barex_transport.h"

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
// #include "transport/rdma_transport/rdma_context.h"
// #include "transport/rdma_transport/rdma_endpoint.h"

namespace mooncake {
using namespace accl::barex;

class EmptyCallback : public XChannelCallback {
public:
    void OnRecvCall(XChannel *channel, char *buf, size_t len, x_msg_header header) {}
};

BarexTransport::BarexTransport() {}

BarexTransport::~BarexTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    for (auto &entry : batch_desc_set_) delete entry.second;
    batch_desc_set_.clear();
#endif
    for (auto ctx : client_context_list_) {
        std::vector<XChannel*> chs = ctx->getAllChannel();
        for (auto ch : chs) {
            BarexResult ret = connector_->CloseChannel(ch, [&, ch](accl::barex::Status s) {
                LOG(INFO) << "CloseChannel() finished, s.IsOk=" << s.IsOk();
                ch->Destroy();
            });
            if (ret != accl::barex::BAREX_SUCCESS) {
                LOG(ERROR) << "CloseChannel() failed, ret " << ret;
            }
        }
    }
    client_context_list_.clear();
    server_context_list_.clear();
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    connector_->Shutdown();
    connector_->WaitStop();
    listener_->Shutdown();
    listener_->WaitStop();
    server_threadpool_->Shutdown();
    server_threadpool_->WaitStop();
    client_threadpool_->Shutdown();
    client_threadpool_->WaitStop();
    mempool_->Shutdown();
    mempool_->WaitStop();
}

int BarexTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> meta,
                           std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "BarexTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;

    const char *barex_random_dev_env = std::getenv("BAREX_USE_RANDOM_DEV");
    if (barex_random_dev_env) {
        int val = atoi(barex_random_dev_env);
        if (val != 0) {
            LOG(INFO) << "BarexTransport: use random rdma device";
            use_random_dev_ = true;
        }
    }

    const char *barex_use_cpu_env = std::getenv("ACCL_USE_CPU");
    if (barex_use_cpu_env) {
        int val = atoi(barex_use_cpu_env);
        if (val != 0) {
            LOG(INFO) << "BarexTransport: use_cpu";
            barex_use_cpu_ = true;            
        }
    }

    const char *barex_local_device_env = std::getenv("ACCL_LOCAL_DEVICE");
    if (barex_local_device_env) {
        int val = atoi(barex_local_device_env);
        LOG(INFO) << "BarexTransport: set local device id " << val;
        barex_local_device_ = val;
    }

    auto ret = initializeRdmaResources();
    if (ret) {
        LOG(ERROR) << "BarexTransport: cannot initialize RDMA resources";
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
        LOG(ERROR) << "BarexTransport: cannot start handshake daemon";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "BarexTransport: cannot publish segments";
        return ret;
    }

    return 0;
}

int BarexTransport::registerLocalMemory(void *addr, size_t length,
                                        const std::string &name,
                                        bool remote_accessible,
                                        bool update_metadata) {
    auto &config = globalConfig();
    size_t buffer_size = config.eic_max_block_size;
    size_t remaining = length;
    void *current_ptr = addr;
    device_type dtype;

    if (name.find("cuda") != std::string::npos || name == kWildcardLocation) {
       dtype = GPU;
    } else if (name.find("cpu") != std::string::npos) {
       dtype = CPU;
    } else {
        LOG(ERROR) << "BarexTransport: registerLocalMemory, cannot recognize: name " << name
                   << ", need include cpu or cuda in name";
        return ERR_INVALID_ARGUMENT;
    }

    bool is_gpu = dtype == GPU ? true : false;

    while (remaining > 0) {
        size_t buffer_len = std::min(buffer_size, remaining);
        int ret = registerLocalMemoryBase(current_ptr, buffer_len, name, remote_accessible, update_metadata, is_gpu);
        if (ret) {
            LOG(ERROR) << "registerLocalMemoryBase failed, ret " << ret;
            return -1;
        }
        current_ptr = static_cast<char*>(current_ptr) + buffer_len;
        remaining -= buffer_len;
    }

    std::lock_guard<std::mutex> guard(buf_mutex_);
    if (dtype == CPU) {
        buf_length_map_.emplace(addr, std::make_pair(length, 0));
    } else {
        buf_length_map_.emplace(addr, std::make_pair(length, 1));
    }

    return 0;
}

int BarexTransport::registerLocalMemoryBase(void *addr, size_t length,
                                       const std::string &name,
                                       bool remote_accessible,
                                       bool update_metadata,
                                       bool is_gpu) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    memp_t mem;
    BarexResult result;
    device_type dtype = is_gpu ? GPU : CPU;
    result = mempool_->RegUserMr(mem, addr, length, dtype);
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: registerLocalMemory failed"
                   << ", result " << result << ", addr " << addr
                   << ", length " << length << ", name "<< name;
        return ERR_ADDRESS_NOT_REGISTERED;
    } else {
        for (auto &mr : mem.mrs) {
            buffer_desc.lkey.push_back(mr.second->lkey);
            buffer_desc.rkey.push_back(mr.second->rkey);
        }
    }
    
    // Get the memory location automatically after registered MR(pinned),
    // when the name is kWildcardLocation("*").
    if (name == kWildcardLocation) {
        bool only_first_page = true;
        const std::vector<MemoryLocationEntry> entries =
            getMemoryLocation(addr, length, only_first_page);
        for (auto &entry : entries) {
            buffer_desc.name = entry.location;
            buffer_desc.addr = entry.start;
            buffer_desc.length = entry.len;
            int rc =
                metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
            if (rc) return rc;
        }
    } else {
        buffer_desc.name = name;
        buffer_desc.addr = (uint64_t)addr;
        buffer_desc.length = length;
        int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
        if (rc) return rc;
    }

    return 0;
}

int BarexTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;

    auto &config = globalConfig();
    size_t buffer_size = config.eic_max_block_size;
    void *current_ptr = addr;
    device_type dtype;
    BarexResult result;
    size_t remaining = 0;
    memp_t mem;
    {
        std::lock_guard<std::mutex> guard(buf_mutex_);
        auto iter = buf_length_map_.find(addr);
        if (iter != buf_length_map_.end()) {
            remaining = iter->second.first;
            dtype = iter->second.second ? GPU : CPU;
            buf_length_map_.erase(iter);
        }
    }

    while (remaining > 0) {
        size_t buffer_len = std::min(buffer_size, remaining);
        if (current_ptr > addr) {
           int rc = metadata_->removeLocalMemoryBuffer(current_ptr, update_metadata);
            if (rc) {
                LOG(WARNING) << "unregisterLocalMemory, removeLocalMemoryBuffer failed, addr " << addr;
            } 
        }
        result = mempool_->DeregUserMr(current_ptr, dtype);
        if (result != BAREX_SUCCESS) {
            LOG(ERROR) << "unregisterLocalMemory, DeregUserMr, failed, ret " << result << ", addr " << current_ptr;
            return -1;
        }
        current_ptr = static_cast<char*>(current_ptr) + buffer_len;
        remaining -= buffer_len;
    }

    return 0;
}

int BarexTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "barex";
    for (auto &entry : server_context_list_) {
        TransferMetadata::DeviceDesc device_desc;
        device_desc.name = entry->getCtx()->GetXDevice()->GetName();
        // TODO is barex need this?
        device_desc.lid = 0; //entry->lid();
        device_desc.gid = "ignore"; //entry->gid();
        desc->devices.push_back(device_desc);
    }
    desc->topology = *(local_topology_.get());
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int BarexTransport::registerLocalMemoryBatch(
    const std::vector<BarexTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location, true, false);
        if (ret) {
            LOG(ERROR) << "BarexTransport: Failed to register memory: addr "
                         << buffer.addr << " length "
                         << buffer.length;
            return ERR_ADDRESS_NOT_REGISTERED;
        }
    }

    return metadata_->updateLocalSegmentDesc();
}

int BarexTransport::unregisterLocalMemoryBatch(
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
            LOG(WARNING) << "BarexTransport: Failed to unregister memory: addr "
                         << addr_list[i];
    }

    return metadata_->updateLocalSegmentDesc();
}

Status BarexTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "BarexTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "BarexTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    std::unordered_map<std::shared_ptr<BarexContext>, std::vector<Slice *>>
        slices_to_post;
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    // const size_t kBlockSize = globalConfig().slice_size;
    const size_t kBlockMaxSize = globalConfig().eic_max_block_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>> segment_desc_map;
    for (auto &request : entries) {
        auto target_id = request.target_id;
        if (!segment_desc_map.count(target_id))
            segment_desc_map[target_id] = metadata_->getSegmentDescByID(target_id);
    }
    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        SegmentID target_id = request.target_id;
        auto peer_segment_desc = segment_desc_map[target_id];
        if (!peer_segment_desc) {
            LOG(ERROR) << "peer_segment_desc not found for target_id " << target_id;
            return Status::InvalidArgument(
                "BarexTransport: peer_segment_desc not found, batch id: " +
                std::to_string(batch_id));
        }
        size_t kBlockSize = std::min(request.length, kBlockMaxSize);
        for (uint64_t offset = 0; offset < request.length; offset += kBlockSize) {
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

            int peer_buffer_id = -1, extra_peer_buffer_id = 0, peer_device_id = -1;
            int local_buffer_id = -1, extra_local_buffer_id = 0, device_id = -1, retry_cnt = 0;
            while (retry_cnt < kMaxRetryCount) {
                int ret = selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 local_buffer_id, device_id, retry_cnt++);
                if (ret) {
                    if (ret == ERR_ADDRESS_NOT_REGISTERED) {
                        LOG(WARNING) << "local_segment_desc selectDevice failed";
                        continue;
                    } else {
                        // need 2 blocks
                        extra_local_buffer_id = local_buffer_id + 1;
                    }
                }
                ret = selectDevice(peer_segment_desc.get(),
                                            slice->rdma.dest_addr,
                                            slice->length, peer_buffer_id, peer_device_id,
                                            slice->rdma.retry_cnt);
                if (ret) {
                    if (ret == ERR_ADDRESS_NOT_REGISTERED) {
                        LOG(WARNING) << "peer_segment_desc selectDevice failed";
                        continue;
                    } else {
                        // need 2 blocks
                        extra_peer_buffer_id = peer_buffer_id + 1;
                    }
                }
                assert(device_id >= 0);
                if (device_id >= static_cast<int>(client_context_list_.size()) || use_random_dev_) {
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> dis(0, client_context_list_.size() - 1);
                    device_id = dis(gen);
                }
                auto &context = client_context_list_[device_id];
                if (!context->active()) continue;
                assert(context->getCtx()->GetXDevice()->GetId() ==  device_id);
                // 4 types, local:peer = 1:1, 1:2, 2:1, 2:2
                if (!extra_local_buffer_id && !extra_peer_buffer_id) { // 1:1
                    slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    slice->rdma.lkey_index = device_id;
                    slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(slice);
                    task.total_bytes += slice->length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    break;
                } else if (!extra_local_buffer_id && extra_peer_buffer_id) { // 1:2
                    auto &first_peer_buffer_desc = peer_segment_desc.get()->buffers[peer_buffer_id];
                    auto &last_peer_buffer_desc = peer_segment_desc.get()->buffers[extra_peer_buffer_id];
                    size_t first_length = first_peer_buffer_desc.addr + first_peer_buffer_desc.length - slice->rdma.dest_addr;
                    size_t last_length = slice->rdma.dest_addr + slice->length - last_peer_buffer_desc.addr;
                    assert(first_length + last_length == slice->length);
                    // add first part
                    slice->length = first_length;
                    slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    slice->rdma.lkey_index = device_id;
                    slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(slice);
                    task.total_bytes += first_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    // add last part
                    Slice *last_slice = getSliceCache().allocate();
                    last_slice->source_addr = (char *)request.source + offset + first_length;
                    last_slice->length = last_length;
                    last_slice->opcode = request.opcode;
                    last_slice->rdma.dest_addr = request.target_offset + offset + first_length;
                    last_slice->rdma.retry_cnt = 0;
                    last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                    last_slice->task = &task;
                    last_slice->target_id = request.target_id;
                    last_slice->ts = 0;
                    last_slice->status = Slice::PENDING;
                    task.slice_list.push_back(last_slice);
                    last_slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    last_slice->rdma.lkey_index = device_id;
                    last_slice->dest_rkeys =
                        peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                    slices_to_post[context].push_back(last_slice);
                    task.total_bytes += last_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    break;
                } else if (extra_local_buffer_id && !extra_peer_buffer_id) { // 2:1
                    auto &first_local_buffer_desc = local_segment_desc.get()->buffers[local_buffer_id];
                    auto &last_local_buffer_desc = local_segment_desc.get()->buffers[extra_local_buffer_id];
                    size_t first_length = first_local_buffer_desc.addr + first_local_buffer_desc.length - (size_t)slice->source_addr;
                    size_t last_length = (size_t)slice->source_addr + slice->length - last_local_buffer_desc.addr;
                    assert(first_length + last_length == slice->length);
                    // add first part
                    slice->length = first_length;
                    slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    slice->rdma.lkey_index = device_id;
                    slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(slice);
                    task.total_bytes += first_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    // add last part
                    Slice *last_slice = getSliceCache().allocate();
                    last_slice->source_addr = (char *)request.source + offset + first_length;
                    last_slice->length = last_length;
                    last_slice->opcode = request.opcode;
                    last_slice->rdma.dest_addr = request.target_offset + offset + first_length;
                    last_slice->rdma.retry_cnt = 0;
                    last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                    last_slice->task = &task;
                    last_slice->target_id = request.target_id;
                    last_slice->ts = 0;
                    last_slice->status = Slice::PENDING;
                    task.slice_list.push_back(last_slice);
                    last_slice->rdma.source_lkey =
                        local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                    last_slice->rdma.lkey_index = device_id;
                    last_slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(last_slice);
                    task.total_bytes += last_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    break;
                } else { // 2:2
                    auto &first_local_buffer_desc = local_segment_desc.get()->buffers[local_buffer_id];
                    auto &last_local_buffer_desc = local_segment_desc.get()->buffers[extra_local_buffer_id];
                    size_t first_local_length = first_local_buffer_desc.addr + first_local_buffer_desc.length - (size_t)slice->source_addr;
                    size_t last_local_length = (size_t)slice->source_addr + slice->length - last_local_buffer_desc.addr;
                    assert(first_local_length + last_local_length == slice->length);
                    auto &first_peer_buffer_desc = peer_segment_desc.get()->buffers[peer_buffer_id];
                    auto &last_peer_buffer_desc = peer_segment_desc.get()->buffers[extra_peer_buffer_id];
                    size_t first_peer_length = first_peer_buffer_desc.addr + first_peer_buffer_desc.length - slice->rdma.dest_addr;
                    size_t last_peer_length = slice->rdma.dest_addr + slice->length - last_peer_buffer_desc.addr;
                    assert(first_peer_length + last_peer_length == slice->length);
                    if (first_local_length == first_peer_length) {
                        // add first part
                        slice->length = first_local_length;
                        slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        slice->rdma.lkey_index = device_id;
                        slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(slice);
                        task.total_bytes += first_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add last part
                        Slice *last_slice = getSliceCache().allocate();
                        last_slice->source_addr = (char *)request.source + offset + first_local_length;
                        last_slice->length = last_local_length;
                        last_slice->opcode = request.opcode;
                        last_slice->rdma.dest_addr = request.target_offset + offset + first_local_length;
                        last_slice->rdma.retry_cnt = 0;
                        last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        last_slice->task = &task;
                        last_slice->target_id = request.target_id;
                        last_slice->ts = 0;
                        last_slice->status = Slice::PENDING;
                        task.slice_list.push_back(last_slice);
                        last_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        last_slice->rdma.lkey_index = device_id;
                        last_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(last_slice);
                        task.total_bytes += last_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                    } else if (first_local_length > first_peer_length) {
                        // add first part
                        slice->length = first_peer_length;
                        slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        slice->rdma.lkey_index = device_id;
                        slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(slice);
                        task.total_bytes += first_peer_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add second part
                        Slice *second_slice = getSliceCache().allocate();
                        second_slice->source_addr = (char *)request.source + offset + first_peer_length;
                        second_slice->length = first_local_length - first_peer_length;
                        second_slice->opcode = request.opcode;
                        second_slice->rdma.dest_addr = request.target_offset + offset + first_peer_length;
                        second_slice->rdma.retry_cnt = 0;
                        second_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        second_slice->task = &task;
                        second_slice->target_id = request.target_id;
                        second_slice->ts = 0;
                        second_slice->status = Slice::PENDING;
                        task.slice_list.push_back(second_slice);
                        second_slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        second_slice->rdma.lkey_index = device_id;
                        second_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(second_slice);
                        task.total_bytes += first_local_length - first_peer_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add last part
                        Slice *last_slice = getSliceCache().allocate();
                        last_slice->source_addr = (char *)request.source + offset + first_local_length;
                        last_slice->length = last_local_length;
                        last_slice->opcode = request.opcode;
                        last_slice->rdma.dest_addr = request.target_offset + offset + first_local_length;
                        last_slice->rdma.retry_cnt = 0;
                        last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        last_slice->task = &task;
                        last_slice->target_id = request.target_id;
                        last_slice->ts = 0;
                        last_slice->status = Slice::PENDING;
                        task.slice_list.push_back(last_slice);
                        last_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        last_slice->rdma.lkey_index = device_id;
                        last_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(last_slice);
                        task.total_bytes += last_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                    } else { 
                        // first_local_length < first_peer_length
                        // add first part
                        slice->length = first_local_length;
                        slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        slice->rdma.lkey_index = device_id;
                        slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(slice);
                        task.total_bytes += first_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add second part
                        Slice *second_slice = getSliceCache().allocate();
                        second_slice->source_addr = (char *)request.source + offset + first_local_length;
                        second_slice->length = first_peer_length - first_local_length;
                        second_slice->opcode = request.opcode;
                        second_slice->rdma.dest_addr = request.target_offset + offset + first_local_length;
                        second_slice->rdma.retry_cnt = 0;
                        second_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        second_slice->task = &task;
                        second_slice->target_id = request.target_id;
                        second_slice->ts = 0;
                        second_slice->status = Slice::PENDING;
                        task.slice_list.push_back(second_slice);
                        second_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        second_slice->rdma.lkey_index = device_id;
                        second_slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(second_slice);
                        task.total_bytes += first_peer_length - first_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add last part
                        Slice *last_slice = getSliceCache().allocate();
                        last_slice->source_addr = (char *)request.source + offset + first_peer_length;
                        last_slice->length = last_peer_length;
                        last_slice->opcode = request.opcode;
                        last_slice->rdma.dest_addr = request.target_offset + offset + first_peer_length;
                        last_slice->rdma.retry_cnt = 0;
                        last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        last_slice->task = &task;
                        last_slice->target_id = request.target_id;
                        last_slice->ts = 0;
                        last_slice->status = Slice::PENDING;
                        task.slice_list.push_back(last_slice);
                        last_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        last_slice->rdma.lkey_index = device_id;
                        last_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(last_slice);
                        task.total_bytes += last_peer_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                    }
                    break;
                }
            }
            if (device_id < 0) {
                auto source_addr = slice->source_addr;
                for (auto &entry : slices_to_post)
                    for (auto s : entry.second) delete s;
                LOG(ERROR)
                    << "BarexTransport: Address not registered by any device(s) "
                    << source_addr;
                return Status::AddressNotRegistered(
                    "BarexTransport: not registered by any device(s), "
                    "address: " +
                    std::to_string(reinterpret_cast<uintptr_t>(source_addr)));
            }
        }
    }
    for (auto &entry : slices_to_post) {
        int ret = entry.first->submitPostSend(entry.second);
        if (ret) {
            return Status::InvalidArgument("submitPostSend failed");
        }
    }
    return Status::OK();
}

Status BarexTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<std::shared_ptr<BarexContext>, std::vector<Slice *>>
        slices_to_post;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    assert(local_segment_desc.get());
    // const size_t kBlockSize = globalConfig().slice_size;
    const size_t kBlockMaxSize = globalConfig().eic_max_block_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>> segment_desc_map;
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        auto target_id = request.target_id;
        if (!segment_desc_map.count(target_id))
            segment_desc_map[target_id] = metadata_->getSegmentDescByID(target_id);
    }
    for (size_t index = 0; index < task_list.size(); ++index) {
        auto &task = *task_list[index];
        auto &request = *task.request;
        SegmentID target_id = request.target_id;
        auto peer_segment_desc = segment_desc_map[target_id];
        if (!peer_segment_desc) {
            LOG(ERROR) << "peer_segment_desc not found for target_id " << target_id;
            return Status::InvalidArgument(
                "BarexTransport: peer_segment_desc not found");
        }
        size_t kBlockSize = std::min(request.length, kBlockMaxSize);
        for (uint64_t offset = 0; offset < request.length; offset += kBlockSize) {
            Slice *slice = getSliceCache().allocate();
            assert(slice);
            slice->source_addr = (char *)request.source + offset;
            slice->length = std::min(request.length - offset, kBlockSize);
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = request.advise_retry_cnt;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;
            slice->ts = 0;
            task.slice_list.push_back(slice);

            int peer_buffer_id = -1, extra_peer_buffer_id = 0, peer_device_id = -1;
            int local_buffer_id = -1, extra_local_buffer_id = 0, device_id = -1, retry_cnt = request.advise_retry_cnt;
            bool found_device = false;
            while (retry_cnt < kMaxRetryCount) {
                int ret = selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 local_buffer_id, device_id, retry_cnt++);
                if (ret) {
                    if (ret == ERR_ADDRESS_NOT_REGISTERED) {
                        LOG(WARNING) << "local_segment_desc selectDevice failed";
                        continue;
                    } else {
                        // need 2 blocks
                        extra_local_buffer_id = local_buffer_id + 1;
                    }
                }
                ret = selectDevice(peer_segment_desc.get(),
                                            slice->rdma.dest_addr,
                                            slice->length, peer_buffer_id, peer_device_id,
                                            slice->rdma.retry_cnt);
                if (ret) {
                    if (ret == ERR_ADDRESS_NOT_REGISTERED) {
                        LOG(WARNING) << "peer_segment_desc selectDevice failed";
                        continue;
                    } else {
                        // need 2 blocks
                        extra_peer_buffer_id = peer_buffer_id + 1;
                    }
                }
                assert(device_id >= 0);
                if (device_id >= static_cast<int>(client_context_list_.size()) || use_random_dev_) {
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> dis(0, client_context_list_.size() - 1);
                    device_id = dis(gen);
                }
                auto &context = client_context_list_[device_id];
                assert(context.get());
                if (!context->active()) continue;
                assert(context->getCtx()->GetXDevice()->GetId() ==  device_id);
                assert(local_buffer_id >= 0 && local_buffer_id < local_segment_desc->buffers.size());
                assert(local_segment_desc->buffers[local_buffer_id].lkey.size() == client_context_list_.size());
                // 4 types, local:peer = 1:1, 1:2, 2:1, 2:2
                if (!extra_local_buffer_id && !extra_peer_buffer_id) { // 1:1
                    slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    slice->rdma.lkey_index = device_id;
                    slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(slice);
                    task.total_bytes += slice->length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    found_device = true;
                    break;
                } else if (!extra_local_buffer_id && extra_peer_buffer_id) { // 1:2
                    auto &first_peer_buffer_desc = peer_segment_desc.get()->buffers[peer_buffer_id];
                    auto &last_peer_buffer_desc = peer_segment_desc.get()->buffers[extra_peer_buffer_id];
                    size_t first_length = first_peer_buffer_desc.addr + first_peer_buffer_desc.length - slice->rdma.dest_addr;
                    size_t last_length = slice->rdma.dest_addr + slice->length - last_peer_buffer_desc.addr;
                    assert(first_length + last_length == slice->length);
                    // add first part
                    slice->length = first_length;
                    slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    slice->rdma.lkey_index = device_id;
                    slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(slice);
                    task.total_bytes += first_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    // add last part
                    Slice *last_slice = getSliceCache().allocate();
                    last_slice->source_addr = (char *)request.source + offset + first_length;
                    last_slice->length = last_length;
                    last_slice->opcode = request.opcode;
                    last_slice->rdma.dest_addr = request.target_offset + offset + first_length;
                    last_slice->rdma.retry_cnt = 0;
                    last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                    last_slice->task = &task;
                    last_slice->target_id = request.target_id;
                    last_slice->ts = 0;
                    last_slice->status = Slice::PENDING;
                    task.slice_list.push_back(last_slice);
                    last_slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    last_slice->rdma.lkey_index = device_id;
                    last_slice->dest_rkeys =
                        peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                    slices_to_post[context].push_back(last_slice);
                    task.total_bytes += last_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    found_device = true;
                    break;
                } else if (extra_local_buffer_id && !extra_peer_buffer_id) { // 2:1
                    auto &first_local_buffer_desc = local_segment_desc.get()->buffers[local_buffer_id];
                    auto &last_local_buffer_desc = local_segment_desc.get()->buffers[extra_local_buffer_id];
                    size_t first_length = first_local_buffer_desc.addr + first_local_buffer_desc.length - (size_t)slice->source_addr;
                    size_t last_length = (size_t)slice->source_addr + slice->length - last_local_buffer_desc.addr;
                    assert(first_length + last_length == slice->length);
                    // add first part
                    slice->length = first_length;
                    slice->rdma.source_lkey =
                        local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                    slice->rdma.lkey_index = device_id;
                    slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(slice);
                    task.total_bytes += first_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    // add last part
                    Slice *last_slice = getSliceCache().allocate();
                    last_slice->source_addr = (char *)request.source + offset + first_length;
                    last_slice->length = last_length;
                    last_slice->opcode = request.opcode;
                    last_slice->rdma.dest_addr = request.target_offset + offset + first_length;
                    last_slice->rdma.retry_cnt = 0;
                    last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                    last_slice->task = &task;
                    last_slice->target_id = request.target_id;
                    last_slice->ts = 0;
                    last_slice->status = Slice::PENDING;
                    task.slice_list.push_back(last_slice);
                    last_slice->rdma.source_lkey =
                        local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                    last_slice->rdma.lkey_index = device_id;
                    last_slice->dest_rkeys =
                        peer_segment_desc->buffers[peer_buffer_id].rkey;
                    slices_to_post[context].push_back(last_slice);
                    task.total_bytes += last_length;
                    __sync_fetch_and_add(&task.slice_count, 1);
                    found_device = true;
                    break;
                } else { // 2:2
                    auto &first_local_buffer_desc = local_segment_desc.get()->buffers[local_buffer_id];
                    auto &last_local_buffer_desc = local_segment_desc.get()->buffers[extra_local_buffer_id];
                    size_t first_local_length = first_local_buffer_desc.addr + first_local_buffer_desc.length - (size_t)slice->source_addr;
                    size_t last_local_length = (size_t)slice->source_addr + slice->length - last_local_buffer_desc.addr;
                    assert(first_local_length + last_local_length == slice->length);
                    auto &first_peer_buffer_desc = peer_segment_desc.get()->buffers[peer_buffer_id];
                    auto &last_peer_buffer_desc = peer_segment_desc.get()->buffers[extra_peer_buffer_id];
                    size_t first_peer_length = first_peer_buffer_desc.addr + first_peer_buffer_desc.length - slice->rdma.dest_addr;
                    size_t last_peer_length = slice->rdma.dest_addr + slice->length - last_peer_buffer_desc.addr;
                    assert(first_peer_length + last_peer_length == slice->length);
                    if (first_local_length == first_peer_length) {
                        // add first part
                        slice->length = first_local_length;
                        slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        slice->rdma.lkey_index = device_id;
                        slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(slice);
                        task.total_bytes += first_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add last part
                        Slice *last_slice = getSliceCache().allocate();
                        last_slice->source_addr = (char *)request.source + offset + first_local_length;
                        last_slice->length = last_local_length;
                        last_slice->opcode = request.opcode;
                        last_slice->rdma.dest_addr = request.target_offset + offset + first_local_length;
                        last_slice->rdma.retry_cnt = 0;
                        last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        last_slice->task = &task;
                        last_slice->target_id = request.target_id;
                        last_slice->ts = 0;
                        last_slice->status = Slice::PENDING;
                        task.slice_list.push_back(last_slice);
                        last_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        last_slice->rdma.lkey_index = device_id;
                        last_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(last_slice);
                        task.total_bytes += last_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                    } else if (first_local_length > first_peer_length) {
                        // add first part
                        slice->length = first_peer_length;
                        slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        slice->rdma.lkey_index = device_id;
                        slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(slice);
                        task.total_bytes += first_peer_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add second part
                        Slice *second_slice = getSliceCache().allocate();
                        second_slice->source_addr = (char *)request.source + offset + first_peer_length;
                        second_slice->length = first_local_length - first_peer_length;
                        second_slice->opcode = request.opcode;
                        second_slice->rdma.dest_addr = request.target_offset + offset + first_peer_length;
                        second_slice->rdma.retry_cnt = 0;
                        second_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        second_slice->task = &task;
                        second_slice->target_id = request.target_id;
                        second_slice->ts = 0;
                        second_slice->status = Slice::PENDING;
                        task.slice_list.push_back(second_slice);
                        second_slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        second_slice->rdma.lkey_index = device_id;
                        second_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(second_slice);
                        task.total_bytes += first_local_length - first_peer_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add last part
                        Slice *last_slice = getSliceCache().allocate();
                        last_slice->source_addr = (char *)request.source + offset + first_local_length;
                        last_slice->length = last_local_length;
                        last_slice->opcode = request.opcode;
                        last_slice->rdma.dest_addr = request.target_offset + offset + first_local_length;
                        last_slice->rdma.retry_cnt = 0;
                        last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        last_slice->task = &task;
                        last_slice->target_id = request.target_id;
                        last_slice->ts = 0;
                        last_slice->status = Slice::PENDING;
                        task.slice_list.push_back(last_slice);
                        last_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        last_slice->rdma.lkey_index = device_id;
                        last_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(last_slice);
                        task.total_bytes += last_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                    } else { 
                        // first_local_length < first_peer_length
                        // add first part
                        slice->length = first_local_length;
                        slice->rdma.source_lkey =
                            local_segment_desc->buffers[local_buffer_id].lkey[device_id];
                        slice->rdma.lkey_index = device_id;
                        slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(slice);
                        task.total_bytes += first_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add second part
                        Slice *second_slice = getSliceCache().allocate();
                        second_slice->source_addr = (char *)request.source + offset + first_local_length;
                        second_slice->length = first_peer_length - first_local_length;
                        second_slice->opcode = request.opcode;
                        second_slice->rdma.dest_addr = request.target_offset + offset + first_local_length;
                        second_slice->rdma.retry_cnt = 0;
                        second_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        second_slice->task = &task;
                        second_slice->target_id = request.target_id;
                        second_slice->ts = 0;
                        second_slice->status = Slice::PENDING;
                        task.slice_list.push_back(second_slice);
                        second_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        second_slice->rdma.lkey_index = device_id;
                        second_slice->dest_rkeys =
                            peer_segment_desc->buffers[peer_buffer_id].rkey;
                        slices_to_post[context].push_back(second_slice);
                        task.total_bytes += first_peer_length - first_local_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                        // add last part
                        Slice *last_slice = getSliceCache().allocate();
                        last_slice->source_addr = (char *)request.source + offset + first_peer_length;
                        last_slice->length = last_peer_length;
                        last_slice->opcode = request.opcode;
                        last_slice->rdma.dest_addr = request.target_offset + offset + first_peer_length;
                        last_slice->rdma.retry_cnt = 0;
                        last_slice->rdma.max_retry_cnt = kMaxRetryCount;
                        last_slice->task = &task;
                        last_slice->target_id = request.target_id;
                        last_slice->ts = 0;
                        last_slice->status = Slice::PENDING;
                        task.slice_list.push_back(last_slice);
                        last_slice->rdma.source_lkey =
                            local_segment_desc->buffers[extra_local_buffer_id].lkey[device_id];
                        last_slice->rdma.lkey_index = device_id;
                        last_slice->dest_rkeys =
                            peer_segment_desc->buffers[extra_peer_buffer_id].rkey;
                        slices_to_post[context].push_back(last_slice);
                        task.total_bytes += last_peer_length;
                        __sync_fetch_and_add(&task.slice_count, 1);
                    }
                    found_device = true;
                    break;
                }
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
        }
    }
    for (auto &entry : slices_to_post) {
        int ret = entry.first->submitPostSend(entry.second);
        if (ret) {
            return Status::InvalidArgument("submitPostSend failed");
        }
    }
    return Status::OK();
}

Status BarexTransport::getTransferStatus(BatchID batch_id,
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
            if (failed_slice_count) {
                status[task_id].s = TransferStatusEnum::FAILED;
            } else {
                status[task_id].s = TransferStatusEnum::COMPLETED;
            }
            task.is_finished = true;
        } else {
            status[task_id].s = TransferStatusEnum::WAITING;
        }
    }
    return Status::OK();
}

Status BarexTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                        TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "BarexTransport::getTransportStatus invalid argument, batch id: " +
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

BarexTransport::SegmentID BarexTransport::getSegmentID(
    const std::string &segment_name) {
    return metadata_->getSegmentID(segment_name);
}

Status BarexTransport::OpenChannel(const std::string &segment_name, SegmentID sid) {
    auto [ip, port] = parseHostNameWithPort(segment_name);
    
    HandShakeDesc local_desc, peer_desc;
    local_desc.barex_port = getLocalPort();

    int rc = metadata_->sendHandshake(segment_name, local_desc, peer_desc);
    if (rc) return Status::Socket("sendHandshake failed");;
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Reject the handshake request by peer "
                   << segment_name;
        return Status::Socket("empty peer_desc");
    } else {
        LOG(INFO) << "Handshake finish, get peer_server " << segment_name << ":" << peer_desc.barex_port;
        setPeerPort(peer_desc.barex_port);
    }

    int client_ctx_cnt = client_context_list_.size();
    int total_channels = client_ctx_cnt * client_context_list_[0]->getQpNum();
    CountDownLatch connect_latch(total_channels);
    std::vector<XChannel *> channels;
    static std::mutex push_channel_mtx;
    for (int i = 0; i < total_channels; i++) {
        BarexResult result = connector_->Connect(ip, getPeerPort(), [=, &channels, &connect_latch](XChannel *channel, accl::barex::Status s) {
            if (!s.IsOk()) {
                LOG(ERROR) << "BarexTransport::OpenChannel failed, " << s.ErrMsg();
            } else {
                std::unique_lock<std::mutex> lk(push_channel_mtx);
                channels.push_back(channel);
                LOG(INFO) << "Open channel " << i+1 << "/" << total_channels;
            }
            connect_latch.CountDown();
        });
        if (result != BAREX_SUCCESS) {
            LOG(ERROR) << "BarexTransport::OpenChannel failed, result=" << result;
            connect_latch.CountDown();
        }
    }
    connect_latch.Wait();
    if ((int)channels.size() != total_channels) {
        LOG(ERROR) << "open channel failed, need " << total_channels << " but got " << channels.size();
        return Status::InvalidArgument("connect failed");
    }
    for (auto channel : channels) {
        int idx = channel->GetContext()->GetXDevice()->GetId();
        assert(client_context_list_[idx]->getCtx()->GetXDevice()->GetId() == idx);
        client_context_list_[idx]->addChannel(sid, idx, channel);
    }
    return Status::OK();
}

Status BarexTransport::CheckStatus(SegmentID sid) {
    bool status = 0;
    for (auto ctx : client_context_list_) {
        int ret = ctx->checkStatus(sid);
        if (ret) {
            LOG(INFO) << "checkStatus failed in ctx" << ctx << ", bad channel cnt=" << ret;
            status = 1;
        }
    }
    if (!status) {
        LOG(ERROR) << "CheckStatus for sid " << sid << " failed";
        return Status::InvalidArgument("sid status error");        
    }
    return Status::OK();
}

int BarexTransport::onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                          HandShakeDesc &local_desc) {
    local_desc.barex_port = getLocalPort();
    return 0;
}

int BarexTransport::initializeRdmaResources() {
    auto hca_list = local_topology_->getHcaList();
    BarexResult result;
    XDeviceManager *manager = nullptr;
    XThreadpool *server_threadpool = nullptr;
    XThreadpool *client_threadpool = nullptr;
    XSimpleMempool *mempool = nullptr;
    result = XDeviceManager::Singleton(manager);
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: Create XDeviceManager failed";
        return ERR_DEVICE_NOT_FOUND;
    }
    std::vector<XDevice *> devices = manager->AllDevices();
    if (devices.size() <= 0) {
        LOG(ERROR) << "BarexTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    } else {
        LOG(INFO) << devices.size() << " rdma devices found";
    }
    result = XSimpleMempool::NewInstance(mempool, "barex-mempool", devices);
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: Create XSimpleMempool failed";
        return ERR_INVALID_ARGUMENT;
    }
    mempool_ = std::shared_ptr<XSimpleMempool>(mempool);
    result = XThreadpool::NewInstance(server_threadpool, 10, "barex-server-threadpool");
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: Create Server XThreadpool failed";
        return ERR_INVALID_ARGUMENT;
    }
    server_threadpool_ = std::shared_ptr<XThreadpool>(server_threadpool);
    result = XThreadpool::NewInstance(client_threadpool, 10, "barex-client-threadpool");
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: Create Client XThreadpool failed";
        return ERR_INVALID_ARGUMENT;
    }
    client_threadpool_ = std::shared_ptr<XThreadpool>(client_threadpool);
    auto &config = globalConfig();
    for (auto &dev : devices) {
        if (std::find(hca_list.begin(), hca_list.end(), dev->GetName()) == hca_list.end()) {
            LOG(WARNING) << "BarexTransport: device " << dev->GetName() << " not found in hca_list, ignore ";
            continue;
        }
        ContextConfig server_config = XConfigUtil::DefaultContextConfig();
        XContext *raw_server_context = nullptr;
        result = XContext::NewInstance(raw_server_context, server_config, new EmptyCallback(), dev, mempool, server_threadpool);
        if (result != BAREX_SUCCESS) {
            local_topology_->disableDevice(dev->GetName());
            LOG(WARNING) << "BarexTransport: Create XContext failed, Disable device " << dev->GetName();
        } else {
            raw_server_context->Start();
            auto server_context = std::make_shared<BarexContext>(raw_server_context, barex_use_cpu_, barex_local_device_);
            server_context->setQpNum(config.num_qp_per_ep);
            server_context_list_.push_back(server_context);
        }
        ContextConfig client_config = XConfigUtil::DefaultContextConfig();
        XContext *raw_client_context = nullptr;
        result = XContext::NewInstance(raw_client_context, client_config, new EmptyCallback(), dev, mempool, client_threadpool);
        if (result != BAREX_SUCCESS) {
            local_topology_->disableDevice(dev->GetName());
            LOG(WARNING) << "BarexTransport: Create XContext failed, Disable device " << dev->GetName();
        } else {
            raw_client_context->Start();
            auto client_context = std::make_shared<BarexContext>(raw_client_context, barex_use_cpu_, barex_local_device_);
            client_context->setQpNum(config.num_qp_per_ep);
            client_context_list_.push_back(client_context);
        }
    }

    if (local_topology_->empty()) {
        LOG(ERROR) << "BarexTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }
    return 0;
}

int BarexTransport::startHandshakeDaemon(std::string &local_server_name) {
    std::vector<XContext*> raw_server_contexts;
    std::vector<XContext*> raw_client_contexts;
    for (auto ctx : server_context_list_) {
        raw_server_contexts.emplace_back(ctx->getCtx());
    }
    for (auto ctx : client_context_list_) {
        raw_client_contexts.emplace_back(ctx->getCtx());
    }
    XListener* listener = nullptr;

    int port = metadata_->localRpcMeta().barex_port;
    setLocalPort(port);
    BarexResult result = XListener::NewInstance(listener, 2, getLocalPort(), TIMER_3S, raw_server_contexts);
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: startHandshakeDaemon, create listener failed, result " << result;
        return ERR_INVALID_ARGUMENT;
    }
    result = listener->Listen();
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: startHandshakeDaemon, Listen failed, result " << result;
        return ERR_INVALID_ARGUMENT;
    }
    listener_ = std::shared_ptr<XListener>(listener);
    XConnector* connector = nullptr;
    result = XConnector::NewInstance(connector, 2, TIMER_3S, raw_client_contexts);
    if (result != BAREX_SUCCESS) {
        LOG(ERROR) << "BarexTransport: startHandshakeDaemon, create connector failed, result " << result;
        return ERR_INVALID_ARGUMENT;
    }
    connector_ = std::shared_ptr<XConnector>(connector);
    return metadata_->startHandshakeDaemon(
        std::bind(&BarexTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port, metadata_->localRpcMeta().sockfd);
}

// According to the request desc, offset and length information, find proper
// buffer_id and device_id as output.
// Return 0 if successful, ERR_ADDRESS_NOT_REGISTERED otherwise.
int BarexTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, int &buffer_id, int &device_id,
                                int retry_count) {
    if (!desc) return ERR_ADDRESS_NOT_REGISTERED;
    int ret = 0;
    for (buffer_id = 0; buffer_id < (int)desc->buffers.size(); ++buffer_id) {
        auto &buffer_desc = desc->buffers[buffer_id];
        if (buffer_desc.addr > offset || offset >= buffer_desc.addr + buffer_desc.length) {
            continue;
        } else {
            if (offset + length > buffer_desc.addr + buffer_desc.length) {
                // mr cross two buffers, need separate into two parts
                if (buffer_id + 1 < (int)desc->buffers.size()) {
                    auto &next_buffer_desc = desc->buffers[buffer_id+1];
                    if (offset + length > next_buffer_desc.addr && offset + length <= next_buffer_desc.addr + next_buffer_desc.length) {
                        ret = 1;
                    } else {
                        LOG(ERROR) << "selectDevice failed, 2 buffers in need but next buffer not fit,"
                                   << " offset " << offset
                                   << " length " << length
                                   << " buffer_id " << buffer_id
                                   << " buffer_desc.addr " << buffer_desc.addr
                                   << " buffer_desc.length " << buffer_desc.length
                                   << " buffer_id " << buffer_id+1
                                   << " next_buffer_desc.addr " << next_buffer_desc.addr
                                   << " next_buffer_desc.length " << next_buffer_desc.length;
                        return ERR_ADDRESS_NOT_REGISTERED;
                    }
                } else {
                    LOG(ERROR) << "selectDevice failed, last buffer overflow,"
                               << " offset " << offset
                               << " length " << length
                               << " buffer_id " << buffer_id
                               << " buffer_desc.addr " << buffer_desc.addr
                               << " buffer_desc.length " << buffer_desc.length;
                    return ERR_ADDRESS_NOT_REGISTERED;
                }
            }
            device_id = desc->topology.selectDevice(buffer_desc.name, retry_count);
            if (device_id >= 0) return ret;
            device_id = desc->topology.selectDevice(kWildcardLocation, retry_count);
            if (device_id >= 0) return ret;
        }
    }

    return ERR_ADDRESS_NOT_REGISTERED;
}
}  // namespace mooncake
