// Copyright 2025 Huawei Technologies Co., Ltd
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

#include "transport/ascend_transport/ascend_direct_transport/ascend_direct_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <exception>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"
#include "transport/transport.h"

namespace mooncake {
AscendDirectTransport::AscendDirectTransport() : running_(false) {}

AscendDirectTransport::~AscendDirectTransport() {
    LOG(INFO) << "AscendDirectTransport destructor called";

    // Stop worker thread
    running_ = false;
    queue_cv_.notify_all();

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    // Disconnect all connections
    std::lock_guard<std::mutex> lock(connection_mutex_);
    if (!connected_segments_.empty()) {
        for (auto &connected_segment : connected_segments_) {
            auto status =
                adxl_->Disconnect(connected_segment.c_str(), connect_timeout_);
            if (status != adxl::SUCCESS) {
                LOG(ERROR) << "Failed to disconnect AdxlEngine:"
                           << connected_segment;
            } else {
                LOG(INFO) << "Success to disconnect AdxlEngine:"
                          << connected_segment;
            }
        }
        connected_segments_.clear();
    }

    // Deregister all memory
    std::lock_guard<std::mutex> mem_handle_lock(mem_handle_mutex_);
    for (const auto &[addr, mem_handle] : addr_to_mem_handle_) {
        auto status = adxl_->DeregisterMem(mem_handle);
        if (status != adxl::SUCCESS) {
            LOG(ERROR) << "Failed to deregister memory at address " << addr;
        } else {
            LOG(INFO) << "Deregistered memory at address " << addr;
        }
    }
    addr_to_mem_handle_.clear();
}

int AscendDirectTransport::install(std::string &local_server_name,
                                   std::shared_ptr<TransferMetadata> meta,
                                   std::shared_ptr<Topology> topo) {
    LOG(INFO) << "install AscendDirectTransport for: " << local_server_name;
    // Call base class install method
    int ret = Transport::install(local_server_name, meta, topo);
    if (ret != 0) {
        LOG(ERROR) << "Failed to install base transport";
        return ret;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR)
            << "AscendDirectTransport: cannot allocate local segment, ret: "
            << ret;
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "HcclTransport: cannot publish segments, "
                      "check the availability of metadata storage, ret: "
                   << ret;
        return ret;
    }

    ret = InitAdxlEngine();
    if (ret) {
        LOG(ERROR) << "AscendDirectTransport: InitAdxlEngine failed, ret: "
                   << ret;
        return ret;
    }
    ret = aclrtCreateStreamWithConfig(
        &stream_, 0, ACL_STREAM_FAST_LAUNCH | ACL_STREAM_FAST_SYNC);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "AscendDirectTransport: cannot create stream, ret: "
                   << ret;
        return FAILED;
    }
    // Start worker thread
    running_ = true;
    worker_thread_ = std::thread(&AscendDirectTransport::workerThread, this);
    return 0;
}

int AscendDirectTransport::InitAdxlEngine() {
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    std::string host_ip = local_segment_desc->rank_info.hostIp;
    uint16_t host_port = local_segment_desc->rank_info.hostPort;
    adxl_ = std::make_unique<adxl::AdxlEngine>();
    if (!adxl_) return ERR_MEMORY;
    std::map<adxl::AscendString, adxl::AscendString> options;
    char *rdma_tc = std::getenv("ASCEND_RDMA_TC");
    if (rdma_tc) {
        options["adxl.RdmaTrafficClass"] = rdma_tc;
        LOG(INFO) << "Set RdmaTrafficClass to:" << rdma_tc;
    } else {
        rdma_tc = std::getenv("HCCL_RDMA_TC");
        if (rdma_tc) {
            options["adxl.RdmaTrafficClass"] = rdma_tc;
            LOG(INFO) << "Set RdmaTrafficClass to:" << rdma_tc;
        }
    }
    char *rdma_sl = std::getenv("ASCEND_RDMA_SL");
    if (rdma_sl) {
        options["adxl.RdmaServiceLevel"] = rdma_sl;
        LOG(INFO) << "Set RdmaServiceLevel to:" << rdma_sl;
    } else {
        rdma_sl = std::getenv("HCCL_RDMA_SL");
        if (rdma_sl) {
            options["adxl.RdmaServiceLevel"] = rdma_sl;
            LOG(INFO) << "Set RdmaServiceLevel to:" << rdma_sl;
        }
    }
    char *buffer_pool = std::getenv("ASCEND_BUFFER_POOL");
    if (buffer_pool) {
        options["adxl.BufferPool"] = buffer_pool;
        LOG(INFO) << "Set adxl.BufferPool to:" << buffer_pool;
        use_buffer_pool_ = true;
    }
    auto adxl_engine_name =
        adxl::AscendString((host_ip + ":" + std::to_string(host_port)).c_str());
    auto status = adxl_->Initialize(adxl_engine_name, options);
    if (status != adxl::SUCCESS) {
        LOG(ERROR) << "Failed to initialize AdxlEngine, status: " << status;
        return -1;
    }
    LOG(INFO) << "Success to initialize adxl engine:"
              << adxl_engine_name.GetString()
              << " with device_id:" << device_logic_id_;
    char *connect_timeout_str = std::getenv("ASCEND_CONNECT_TIMEOUT");
    if (connect_timeout_str) {
        std::optional<int32_t> connect_timeout =
            parseFromString<int32_t>(connect_timeout_str);
        if (connect_timeout.has_value()) {
            connect_timeout_ = connect_timeout.value();
            LOG(INFO) << "Set connection timeout to:" << connect_timeout_;
        }
    }
    char *connect_transfer_str = std::getenv("ASCEND_TRANSFER_TIMEOUT");
    if (connect_transfer_str) {
        std::optional<int32_t> transfer_timeout =
            parseFromString<int32_t>(connect_transfer_str);
        if (transfer_timeout.has_value()) {
            transfer_timeout_ = transfer_timeout.value();
            LOG(INFO) << "Set transfer timeout to:" << transfer_timeout_;
        }
    }
    return 0;
}

Status AscendDirectTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "AscendDirectTransport: Exceed the limitation of current "
                      "batch's capacity";
        return Status::InvalidArgument(
            "AscendDirectTransport: Exceed the limitation of capacity, batch "
            "id: " +
            std::to_string(batch_id));
    }

    auto cur_task_size = batch_desc.task_list.size();
    batch_desc.task_list.resize(cur_task_size + entries.size());
    std::vector<Slice *> slice_list;
    slice_list.reserve(entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[cur_task_size];
        ++cur_task_size;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->ascend_direct.dest_addr = request.target_offset;
        slice->task = &task;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }

    std::unique_lock<std::mutex> lock(queue_mutex_);
    slice_queue_.push(slice_list);
    lock.unlock();
    queue_cv_.notify_one();

    return Status::OK();
}

Status AscendDirectTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::vector<Slice *> slice_list;
    slice_list.reserve(task_list.size());

    for (auto index : task_list) {
        assert(index);
        auto &task = *index;
        assert(task.request);
        auto &request = *task.request;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->ascend_direct.dest_addr = request.target_offset;
        slice->task = &task;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }

    std::unique_lock<std::mutex> lock(queue_mutex_);
    slice_queue_.push(slice_list);
    lock.unlock();
    queue_cv_.notify_one();

    return Status::OK();
}

Status AscendDirectTransport::getTransferStatus(BatchID batch_id,
                                                size_t task_id,
                                                TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "HcclTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

int AscendDirectTransport::registerLocalMemory(void *addr, size_t length,
                                               const std::string &location,
                                               bool remote_accessible,
                                               bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = location;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = (uint64_t)length;

    int ret;
    adxl::MemDesc mem_desc{};
    mem_desc.addr = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(addr));
    mem_desc.len = length;
    adxl::MemType mem_type;
    if (location.starts_with("cpu")) {
        mem_type = adxl::MEM_HOST;
    } else if (location.starts_with("npu")) {
        mem_type = adxl::MEM_DEVICE;
    } else if (location == kWildcardLocation) {
        aclrtPtrAttributes attributes;
        ret = aclrtPointerGetAttributes(addr, &attributes);
        if (ret != ACL_SUCCESS) {
            LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
            return -1;
        }
        if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
            mem_type = adxl::MEM_HOST;
        } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
            mem_type = adxl::MEM_DEVICE;
        } else {
            LOG(ERROR) << "location:" << location << " is not supported.";
            return ERR_INVALID_ARGUMENT;
        }
    } else {
        LOG(ERROR) << "location:" << location << " is not supported.";
        return ERR_INVALID_ARGUMENT;
    }
    LOG(INFO) << "AscendDirectTransport register mem addr:" << addr
              << ", length:" << length << ", location:" << location
              << ", mem type:" << mem_type;
    ret = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (ret) {
        LOG(ERROR) << "HcclTransport: addLocalMemoryBuffer failed, ret: "
                   << ret;
        return ret;
    }
    // memory type is HOST and use buffer pool, do not register to ADXL
    if (mem_type == adxl::MEM_HOST && use_buffer_pool_) {
        return 0;
    }
    adxl::MemHandle mem_handle;
    auto adxl_ret = adxl_->RegisterMem(mem_desc, mem_type, mem_handle);
    if (adxl_ret != adxl::SUCCESS) {
        LOG(ERROR) << "adxl_ret:" << adxl_ret << ".";
        return -1;
    }
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    addr_to_mem_handle_[addr] = mem_handle;
    return 0;
}

int AscendDirectTransport::unregisterLocalMemory(void *addr,
                                                 bool update_metadata) {
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    if (addr_to_mem_handle_.find(addr) != addr_to_mem_handle_.end()) {
        (void)adxl_->DeregisterMem(addr_to_mem_handle_[addr]);
        addr_to_mem_handle_.erase(addr);
    }
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int AscendDirectTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    LOG(INFO) << "AscendDirectTransport::registerLocalMemoryBatch called with "
                 "buffer count: "
              << buffer_list.size() << ", location: " << location;

    for (const auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret != 0) {
            LOG(ERROR) << "Failed to register memory in batch, addr: "
                       << buffer.addr;
            return ret;
        }
    }

    // Update metadata once for the entire batch
    return metadata_->updateLocalSegmentDesc();
}

int AscendDirectTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    LOG(INFO) << "AscendDirectTransport::unregisterLocalMemoryBatch called "
                 "with addr count: "
              << addr_list.size();

    for (void *addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret != 0) {
            LOG(ERROR) << "Failed to unregister memory in batch, addr: "
                       << addr;
            return ret;
        }
    }

    // Update metadata once for the entire batch
    return metadata_->updateLocalSegmentDesc();
}

int AscendDirectTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "ascend";

    // Parse local server name to get host IP and port
    auto [host_ip, host_port] = parseHostNameWithPort(local_server_name_);
    auto ret = aclrtGetDevice(&device_logic_id_);
    if (ret) {
        LOG(ERROR) << "Call aclrtGetDevice failed, ret: " << ret;
        return ret;
    }
    ret = aclrtGetCurrentContext(&rt_context_);
    if (ret) {
        LOG(ERROR) << "Call aclrtGetCurrentContext failed, ret: " << ret;
        return ret;
    }
    desc->rank_info.hostIp = host_ip;
    int sockfd;
    desc->rank_info.hostPort = findAvailableTcpPort(sockfd);
    if (desc->rank_info.hostPort == 0) {
        LOG(ERROR) << "Find available port failed.";
        return FAILED;
    }
    close(sockfd);
    local_adxl_engine_name_ =
        host_ip + ":" + std::to_string(desc->rank_info.hostPort);

    LOG(INFO) << "AscendDirectTransport set segment desc: host_ip=" << host_ip
              << ", host_port=" << desc->rank_info.hostPort
              << ", deviceLogicId=" << device_logic_id_;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

void AscendDirectTransport::workerThread() {
    LOG(INFO) << "AscendDirectTransport worker thread started";
    auto ret = aclrtSetCurrentContext(rt_context_);
    if (ret) {
        LOG(ERROR) << "Call aclrtSetCurrentContext failed, ret: " << ret;
        return;
    }
    while (running_) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock,
                       [this] { return !running_ || !slice_queue_.empty(); });
        if (!running_) {
            break;
        }

        if (!slice_queue_.empty()) {
            auto slice_list = std::move(slice_queue_.front());
            slice_queue_.pop();
            lock.unlock();

            if (slice_list.empty()) {
                LOG(ERROR)
                    << "AscendDirectTransport: empty transfer request batch";
                continue;
            }

            processSliceList(slice_list);
        }
    }
    LOG(INFO) << "AscendDirectTransport worker thread stopped";
}

void AscendDirectTransport::processSliceList(
    const std::vector<Slice *> &slice_list) {
    if (slice_list.empty()) {
        return;
    }
    auto target_segment_desc =
        metadata_->getSegmentDescByID(slice_list[0]->target_id);
    if (!target_segment_desc) {
        LOG(ERROR) << "Cannot find segment descriptor for target_id: "
                   << slice_list[0]->target_id;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    auto target_adxl_engine_name =
        (target_segment_desc->rank_info.hostIp + ":" +
         std::to_string(target_segment_desc->rank_info.hostPort));
    adxl::TransferOp operation;
    if (slice_list[0]->opcode == TransferRequest::WRITE) {
        operation = adxl::WRITE;
    } else if (slice_list[0]->opcode == TransferRequest::READ) {
        operation = adxl::READ;
    } else {
        LOG(ERROR) << "Unsupported opcode: " << slice_list[0]->opcode;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    if (target_adxl_engine_name == local_adxl_engine_name_) {
        VLOG(1) << "Target is local, use memory copy.";
        return localCopy(slice_list[0]->opcode, slice_list);
    }
    int ret = checkAndConnect(target_adxl_engine_name);
    if (ret != 0) {
        LOG(ERROR) << "Failed to connect to segment: "
                   << target_segment_desc->name;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        return;
    }
    std::vector<adxl::TransferOpDesc> op_descs;
    op_descs.reserve(slice_list.size());
    for (auto &slice : slice_list) {
        adxl::TransferOpDesc op_desc{};
        op_desc.local_addr = reinterpret_cast<uintptr_t>(slice->source_addr);
        op_desc.remote_addr =
            reinterpret_cast<uintptr_t>(slice->ascend_direct.dest_addr);
        op_desc.len = slice->length;
        op_descs.emplace_back(op_desc);
    }
    auto status = adxl_->TransferSync(target_adxl_engine_name.c_str(),
                                      operation, op_descs, transfer_timeout_);
    if (status == adxl::SUCCESS) {
        for (auto &slice : slice_list) {
            slice->markSuccess();
        }
    } else {
        LOG(ERROR) << "Transfer slice failed with status: " << status;
        for (auto &slice : slice_list) {
            slice->markFailed();
        }
        // the connection is probably broken.
        // set small timeout to just release local res.
        disconnect(target_adxl_engine_name, 10);
    }
}

void AscendDirectTransport::localCopy(TransferRequest::OpCode opcode,
                                      const std::vector<Slice *> &slice_list) {
    std::vector<Slice *> async_list;
    for (auto &slice : slice_list) {
        auto local_ptr = slice->source_addr;
        auto remote_ptr =
            reinterpret_cast<void *>(slice->ascend_direct.dest_addr);
        aclrtPtrAttributes attributes;
        auto ret = aclrtPointerGetAttributes(slice->source_addr, &attributes);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
            slice->markFailed();
            continue;
        }
        aclrtPtrAttributes dst_attributes;
        ret = aclrtPointerGetAttributes(remote_ptr, &dst_attributes);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtPointerGetAttributes failed, ret:" << ret;
            slice->markFailed();
            continue;
        }
        if (attributes.location.type != ACL_MEM_LOCATION_TYPE_HOST &&
            attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE) {
            LOG(ERROR) << "location of local addr is not supported.";
            slice->markFailed();
            continue;
        }
        if (dst_attributes.location.type != ACL_MEM_LOCATION_TYPE_HOST &&
            dst_attributes.location.type != ACL_MEM_LOCATION_TYPE_DEVICE) {
            LOG(ERROR) << "location of remote addr is not supported.";
            slice->markFailed();
            continue;
        }
        aclrtMemcpyKind kind;
        auto len = slice->length;
        if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST &&
            dst_attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
            ret = aclrtMemcpy(remote_ptr, len, local_ptr, len,
                              ACL_MEMCPY_HOST_TO_HOST);
            if (ret == ACL_ERROR_NONE) {
                slice->markSuccess();
            } else {
                LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret;
                slice->markFailed();
            }
            continue;
        } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_DEVICE &&
                   dst_attributes.location.type ==
                       ACL_MEM_LOCATION_TYPE_DEVICE) {
            kind = ACL_MEMCPY_DEVICE_TO_DEVICE;
        } else if (attributes.location.type == ACL_MEM_LOCATION_TYPE_HOST) {
            kind = (opcode == TransferRequest::WRITE)
                       ? ACL_MEMCPY_HOST_TO_DEVICE
                       : ACL_MEMCPY_DEVICE_TO_HOST;
        } else {
            kind = (opcode == TransferRequest::WRITE)
                       ? ACL_MEMCPY_DEVICE_TO_HOST
                       : ACL_MEMCPY_HOST_TO_DEVICE;
        }
        if (opcode == TransferRequest::WRITE) {
            ret = aclrtMemcpyAsync(remote_ptr, len, local_ptr, len, kind,
                                   stream_);
        } else {
            ret = aclrtMemcpyAsync(local_ptr, len, remote_ptr, len, kind,
                                   stream_);
        }
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "aclrtMemcpyAsync failed, ret:" << ret;
            slice->markFailed();
            continue;
        }
        async_list.emplace_back(slice);
    }
    auto ret = aclrtSynchronizeStreamWithTimeout(stream_, transfer_timeout_);
    if (ret == ACL_ERROR_NONE) {
        for (auto &slice : async_list) {
            slice->markSuccess();
        }
    } else {
        LOG(ERROR) << "Memory copy timeout.";
        ret = aclrtStreamAbort(stream_);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to abort stream, ret:" << ret;
        }
        for (auto &slice : async_list) {
            slice->markFailed();
        }
    }
}

int AscendDirectTransport::checkAndConnect(
    const std::string &target_adxl_engine_name) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto it = connected_segments_.find(target_adxl_engine_name);
    if (it != connected_segments_.end()) {
        LOG(INFO) << "Already connected to target adxl engine: "
                  << target_adxl_engine_name;
        return 0;
    }
    auto status =
        adxl_->Connect(target_adxl_engine_name.c_str(), connect_timeout_);
    if (status != adxl::SUCCESS) {
        LOG(ERROR) << "Failed to connect to target: " << target_adxl_engine_name
                   << ", status: " << status;
        return -1;
    }
    connected_segments_.emplace(target_adxl_engine_name);
    LOG(INFO) << "Connected to segment: " << target_adxl_engine_name;
    return 0;
}

int AscendDirectTransport::disconnect(
    const std::string &target_adxl_engine_name, int32_t timeout_in_millis) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto it = connected_segments_.find(target_adxl_engine_name);
    if (it == connected_segments_.end()) {
        LOG(INFO) << "Target adxl engine: " << target_adxl_engine_name
                  << " is not connected.";
        return 0;
    }
    auto status =
        adxl_->Disconnect(target_adxl_engine_name.c_str(), timeout_in_millis);
    if (status != adxl::SUCCESS) {
        LOG(ERROR) << "Failed to disconnect to: " << target_adxl_engine_name
                   << ", status: " << status;
        connected_segments_.erase(target_adxl_engine_name);
        return -1;
    }
    connected_segments_.erase(target_adxl_engine_name);
    return 0;
}

}  // namespace mooncake