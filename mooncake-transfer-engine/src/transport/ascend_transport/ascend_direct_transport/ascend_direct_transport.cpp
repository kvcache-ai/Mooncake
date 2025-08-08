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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <acl/acl.h>
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
AscendDirectTransport::AscendDirectTransport() : running_(false) {
    LOG(INFO) << "AscendDirectTransport constructor called";
}

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
        auto status = data_dist_->DisConnect(1000);
        if (status != adxl::ADXL_SUCCESS) {
            LOG(ERROR) << "Failed to disconnect AdxlEngine";
        } else {
            LOG(INFO) << "Disconnected AdxlEngine";
        }
        connected_segments_.clear();
    }
    
    // Deregister all memory
    for (const auto& [addr, mem_handle] : addr_to_mem_handle_) {
        auto status = data_dist_->DeregisterGlobalMem(mem_handle);
        if (status != adxl::ADXL_SUCCESS) {
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

    ret = InitAdxlEngine(local_server_name);
    if (ret) {
        LOG(ERROR) << "AscendDirectTransport: InitAdxlEngine failed, ret: " << ret;
        return ret;
    }

    // Start worker thread
    running_ = true;
    worker_thread_ = std::thread(&AscendDirectTransport::workerThread, this);

    return 0;
}

int AscendDirectTransport::InitAdxlEngine(const std::string &local_server_name) {
    std::map<adxl::AscendString, adxl::AscendString> options;
    
    // Get local segment descriptor to extract IP and port info
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    std::string host_ip = local_segment_desc->rank_info.hostIp;
    uint16_t host_port = local_segment_desc->rank_info.hostPort;
    options["llm.ListenIpInfo"] = (host_ip + ":" + std::to_string(host_port)).c_str();
    options["llm.DeviceId"] = std::to_string(local_segment_desc->rank_info.deviceLogicId).c_str();
    
    data_dist_ = std::make_unique<adxl::AdxlEngine>(local_server_name.c_str());
    if (!data_dist_) return ERR_MEMORY;

    auto status =  data_dist_->Initialize(options);
    if (status != adxl::ADXL_SUCCESS) {
        LOG(ERROR) << "Failed to initialize AdxlEngine, status: " << status;
        return -1;
    }
    return 0;
}

Status AscendDirectTransport::submitTransfer(BatchID batch_id,
                                            const std::vector<TransferRequest> &entries) {
    LOG(INFO) << "AscendDirectTransport::submitTransfer called with batch_id: " << batch_id
              << ", entries count: " << entries.size();

    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "AscendDirectTransport: Exceed the limitation of current batch's capacity";
        return Status::InvalidArgument(
            "AscendDirectTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    int task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    std::vector<Slice *> slice_list;
    slice_list.reserve(entries.size());
    
    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
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
    LOG(INFO) << "AscendDirectTransport::submitTransferTask called with task count: "
              << task_list.size();

    std::vector<Slice *> slice_list;
    slice_list.reserve(task_list.size());
    
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
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

Status AscendDirectTransport::getTransferStatus(BatchID batch_id, size_t task_id,
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
    mem_desc.size = length;
    adxl::MemType mem_type;
    if (location.starts_with("cpu:")) {
        mem_type = adxl::MEM_HOST;
    } else if (location.starts_with("npu:")) {
        mem_type = adxl::MEM_DEVICE;
    } else {
        LOG(ERROR) << "location:" << location << " is not supported.";
        return ERR_INVALID_ARGUMENT;
    }
    ret = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (ret) {
        LOG(ERROR) << "HcclTransport: addLocalMemoryBuffer failed, ret: "
                   << ret;
        return ret;
    }
    adxl::MemHandle mem_handle;
    auto data_dist_ret = data_dist_->RegisterGlobalMem(mem_desc, mem_type, mem_handle);
    if (data_dist_ret != adxl::ADXL_SUCCESS) {
        LOG(ERROR) << "data_dist_ret:" << data_dist_ret << ".";
        return -1;
    }
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    addr_to_mem_handle_[addr] = mem_handle;
    return 0;
}

int AscendDirectTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    std::lock_guard<std::mutex> lock(mem_handle_mutex_);
    if (addr_to_mem_handle_.find(addr) != addr_to_mem_handle_.end()) {
        (void)data_dist_->DeregisterGlobalMem(addr_to_mem_handle_[addr]);
        addr_to_mem_handle_.erase(addr);
    }
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int AscendDirectTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    LOG(INFO) << "AscendDirectTransport::registerLocalMemoryBatch called with buffer count: "
              << buffer_list.size() << ", location: " << location;

    for (const auto& buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location, true, false);
        if (ret != 0) {
            LOG(ERROR) << "Failed to register memory in batch, addr: " << buffer.addr;
            return ret;
        }
    }
    
    // Update metadata once for the entire batch
    return metadata_->updateLocalSegmentDesc();
}

int AscendDirectTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    LOG(INFO) << "AscendDirectTransport::unregisterLocalMemoryBatch called with addr count: "
              << addr_list.size();

    for (void* addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret != 0) {
            LOG(ERROR) << "Failed to unregister memory in batch, addr: " << addr;
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
    // Get device information
    int deviceLogicId;
    auto ret = aclrtGetDevice(&deviceLogicId);
    if (ret) {
        LOG(ERROR) << "AscendDirectTransport: aclrtGetDevice failed, ret: " << ret;
        return ret;
    }
    desc->rank_info.hostIp = host_ip;
    char *env = getenv("ASCEND_DIRECT_LISTEN_PORT");
    if (env != nullptr) {
        std::stringstream ss(env);
        ss >> desc->rank_info.hostIp;
        if (!ss.fail()) {
            LOG(ERROR) << "Failed to parse env: " << env <<" to number";
            return ERR_INVALID_ARGUMENT;
        }
        if (!ss.eof()) {
            LOG(ERROR) << "Failed to parse env: " << env <<" to number";
            return ERR_INVALID_ARGUMENT;
        }
    } else {
        // set default hostPort
        desc->rank_info.hostPort = ASCEND_DIRECT_DEFAULT_HOST_PORT + deviceLogicId;
    }
    desc->rank_info.deviceLogicId = deviceLogicId;
    
    LOG(INFO) << "AscendDirectTransport rank_info set: hostIp=" << host_ip 
              << ", hostPort=" << host_port 
              << ", deviceLogicId=" << deviceLogicId;
    
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc)); 
    return 0;
}

void AscendDirectTransport::workerThread() {
    LOG(INFO) << "AscendDirectTransport worker thread started";
    
    while (running_) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { return !running_ || !slice_queue_.empty(); });
        if (!running_) {
            break;
        }
        
        if (!slice_queue_.empty()) {
            auto slice_list = std::move(slice_queue_.front());
            slice_queue_.pop();
            lock.unlock();
            
            if (slice_list.empty()) {
                LOG(ERROR) << "AscendDirectTransport: empty transfer request batch";
                continue;
            }
            
            processSliceList(slice_list);
        }
    }
    
    LOG(INFO) << "AscendDirectTransport worker thread stopped";
}

void AscendDirectTransport::processSliceList(const std::vector<Slice*>& slice_list) {
    for (auto slice : slice_list) {
        try {
            auto target_segment_desc = metadata_->getSegmentDescByID(slice->target_id);
            if (!target_segment_desc) {
                LOG(ERROR) << "Cannot find segment descriptor for target_id: " << slice->target_id;
                slice->markFailed();
                continue;
            }
            int ret = connectToSegment(target_segment_desc);
            if (ret != 0) {
                LOG(ERROR) << "Failed to connect to segment: " << target_segment_desc->name;
                slice->markFailed();
                continue;
            }
            
            std::vector<adxl::TransferOpDesc> op_descs;
            adxl::TransferOpDesc op_desc;
            op_desc.local_addr = slice->source_addr;
            op_desc.remote_addr = reinterpret_cast<void*>(slice->ascend_direct.dest_addr);
            op_desc.len = slice->length;
            op_descs.push_back(op_desc);
            adxl::Status status;
            adxl::TransferOp operation;
            if (slice->opcode == TransferRequest::WRITE) {
                operation = adxl::WRITE;
            } else if (slice->opcode == TransferRequest::READ) {
                operation = adxl::READ;
            } else {
                LOG(ERROR) << "Unsupported opcode: " << slice->opcode;
                slice->markFailed();
                continue;
            }
            status = data_dist_->TransferSync(target_segment_desc->name.c_str(), operation, op_descs, 1000);
            if (status == adxl::ADXL_SUCCESS) {
                slice->markSuccess();
                LOG(INFO) << "Slice completed successfully";
            } else {
                LOG(ERROR) << "Slice failed with status: " << status;
                slice->markFailed();
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception in processSliceList: " << e.what();
            slice->markFailed();
        }
    }
}

int AscendDirectTransport::connectToSegment(std::shared_ptr<SegmentDesc> target_segment_desc) {
    std::lock_guard<std::mutex> lock(connection_mutex_);
    auto key = target_segment_desc->name;
    auto it = connected_segments_.find(key);
    if (it != connected_segments_.end()) {
        LOG(INFO) << "Already connected to segment: " << key;
        return 0;
    }
    
    auto status = data_dist_->Connect(target_segment_desc->name.c_str(), 1000);
    if (status != adxl::ADXL_SUCCESS) {
        LOG(ERROR) << "Failed to connect to target: " << key << ", status: " << status;
        return -1;
    }
    
    connected_segments_.insert(key);
    LOG(INFO) << "Connected to segment: " << key;
    return 0;
}

}  // namespace mooncake