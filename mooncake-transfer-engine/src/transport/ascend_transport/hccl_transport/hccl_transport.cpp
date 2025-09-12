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

#include <cassert>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <cstdlib>
#include <string>
#include "transport/ascend_transport/hccl_transport/hccl_transport.h"

namespace mooncake {
HcclTransport::HcclTransport() : running_(false) {
    const char *aggregateEnv = std::getenv("ASCEND_AGGREGATE_ENABLE");
    aggregateEnabled_ =
        (aggregateEnv != nullptr && std::string(aggregateEnv) == "1");
}

HcclTransport::~HcclTransport() {
    if (running_) {
        running_ = false;
    }

    if (aggregateEnabled_) {
        if (aggInitiatorThread_.joinable()) {
            aggInitiatorThread_.join();
        }
        if (aggInitiatorTransferThread_.joinable()) {
            aggInitiatorTransferThread_.join();
        }
        if (aggTargetAcceptThread_.joinable()) {
            aggTargetAcceptThread_.join();
        }
        if (aggTargetThread_.joinable()) {
            aggTargetThread_.join();
        }
    } else {
        if (initiatorThread_.joinable()) {
            initiatorThread_.join();
        }
        if (targetAcceptThread_.joinable()) {
            targetAcceptThread_.join();
        }
        if (targetThread_.joinable()) {
            targetThread_.join();
        }
    }
}

int HcclTransport::prepareTransport(std::vector<Slice *> &slice_list) {
    auto segment_desc = metadata_->getSegmentDescByID(slice_list[0]->target_id);
    if (!segment_desc) {
        LOG(ERROR)
            << "Unable to get target segment ID, please recheck, segment ID: "
            << slice_list[0]->target_id;
        for (auto slice : slice_list) {
            slice->markFailed();
        }
        return -1;
    }

    remote_rank_info_.rankId = segment_desc->rank_info.rankId;
    inet_pton(AF_INET, segment_desc->rank_info.hostIp.c_str(),
              &remote_rank_info_.hostIp);
    remote_rank_info_.hostPort = segment_desc->rank_info.hostPort;
    remote_rank_info_.deviceLogicId = segment_desc->rank_info.deviceLogicId;
    remote_rank_info_.devicePhyId = segment_desc->rank_info.devicePhyId;
    inet_pton(AF_INET, segment_desc->rank_info.deviceIp.c_str(),
              &remote_rank_info_.deviceIp);
    remote_rank_info_.devicePort = segment_desc->rank_info.devicePort;
    remote_rank_info_.serverIdx = 0;
    remote_rank_info_.pid = segment_desc->rank_info.pid;

    return 0;
}

int HcclTransport::nonAggTransport(std::vector<Slice *> &slice_list,
                                   aclrtStream stream) {
    auto start = std::chrono::high_resolution_clock::now();
    int ret = prepareTransport(slice_list);
    if (ret) {
        LOG(ERROR) << "HcclTransport: prepareTransport failed" << ret;
        return ret;
    }

    for (auto slice : slice_list) {
        LOG(ERROR) << "slice->hccl.dest_addr_type" << slice->hccl.dest_addr_type;
        ret = nonAggTransportMemTask(&local_rank_info_, &remote_rank_info_,
                                     slice->opcode, slice->hccl.dest_addr,
                                     slice->length, slice->source_addr, slice->hccl.dest_addr_type, stream);
        if (ret) {
            LOG(ERROR) << "HcclTransport: nonAggTransportMemTask error, local "
                          "devicePhyId: "
                       << local_rank_info_.devicePhyId
                       << ", remote devicePhyId: "
                       << remote_rank_info_.devicePhyId
                       << ", source_addr: " << slice->source_addr
                       << ", dest_addr: " << slice->hccl.dest_addr
                       << ", dest_addr_type: " << slice->hccl.dest_addr_type
                       << ", ret: " << ret;
            slice->markFailed();
            slice->status = Slice::SliceStatus::FAILED;
            return ret;
        }
    }

    if (inet_ntoa(local_rank_info_.hostIp) + std::to_string(local_rank_info_.devicePhyId) ==
        inet_ntoa(remote_rank_info_.hostIp) + std::to_string(remote_rank_info_.devicePhyId)) {
        //D2H
        for (auto slice : slice_list) {
            if (slice->status != Slice::SliceStatus::FAILED) {
                slice->markSuccess();
                slice->task->transferred_bytes = slice->length;
            }
        }
        return 0;
    }
    auto taskDispatch = std::chrono::high_resolution_clock::now();
    ret = transportMemAddOpFence(&remote_rank_info_, stream);
    if (ret) {
        LOG(ERROR) << "transportMemAddOpFence failed, local devicePhyId: "
                   << local_rank_info_.devicePhyId
                   << ", remote devicePhyId: " << remote_rank_info_.devicePhyId
                   << ", ret: " << ret;
        for (auto slice : slice_list) {
            slice->markFailed();
        }
        return ret;
    }

    ret = aclrtSynchronizeStream(stream);
    if (ret) {
        LOG(ERROR) << "aclrtSynchronizeStream failed, local devicePhyId: "
                   << local_rank_info_.devicePhyId
                   << ", remote devicePhyId: " << remote_rank_info_.devicePhyId
                   << ", ret: " << ret;
        for (auto slice : slice_list) {
            slice->markFailed();
        }
        return ret;
    }

    for (auto slice : slice_list) {
        if (slice->hccl.dest_addr_type == DDR) {
            ret = transportMemIntegrate(&local_rank_info_, &remote_rank_info_,
                                        slice->opcode, slice->hccl.dest_addr,
                                        slice->length, slice->source_addr,
                                        stream);
            if (ret) {
                LOG(ERROR) << "HcclTransport: transportMemIntegrate error, local devicePhyId: "
                        << local_rank_info_.devicePhyId
                        << ", remote devicePhyId: "
                        << remote_rank_info_.devicePhyId 
                        << ", source_addr: "
                        << slice->source_addr
                        << ", dest_addr: "
                        << slice->hccl.dest_addr
                        << ", ret: " << ret;
                slice->markFailed();
                slice->status = Slice::SliceStatus::FAILED;
            }
        }
    }

    for (auto slice : slice_list) {
        if (slice->status != Slice::SliceStatus::FAILED) {
            slice->markSuccess();
            slice->task->transferred_bytes = slice->length;
        }
    }

    auto stop = std::chrono::high_resolution_clock::now();
    pid_t pid = getpid();
    char remoteIp[64];
    inet_ntop(AF_INET, &remote_rank_info_.hostIp, remoteIp,
                sizeof(remoteIp));
    if (enableAscendLogging()) {
        auto duration_call =
            std::chrono::duration_cast<std::chrono::microseconds>(taskDispatch -
                                                                  start);
        auto duration_sync =
            std::chrono::duration_cast<std::chrono::microseconds>(stop -
                                                                  taskDispatch);
        LOG(INFO) << "pid: " << pid << ", target hostIp: " << remoteIp
                  << ", local devicePhyId: " << local_rank_info_.devicePhyId
                  << ", target devicePhyId: " << remote_rank_info_.devicePhyId
                  << ", batch call spent: " << duration_call.count() << "us"
                  << ", batch sync spent: " << duration_sync.count() << "us";
    } else {
        (void)start;
        (void)taskDispatch;
        (void)stop;
    }
    LOG(INFO) << "pid: " << pid << ", target hostIp: " << remoteIp
            << ", local devicePhyId: " << local_rank_info_.devicePhyId
            << ", target devicePhyId: " << remote_rank_info_.devicePhyId
            << ", slice end";
    return 0;
}

void HcclTransport::initiatorLoop(int deviceLogicId) {
    aclrtStream stream;
    int ret = aclrtSetDevice(deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice error, ret: " << ret;
    }

    ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtCreateStream error, ret: " << ret;
    }

    while (running_) {
        std::unique_lock<std::mutex> lock(initiator_mutex_);
        if (allReqQueues_.empty()) {
            initiator_cond_.wait(lock);
        }
        auto slice_list = std::move(allReqQueues_.front());
        allReqQueues_.pop();
        lock.unlock();
        if (slice_list.empty()) {
            LOG(ERROR) << "HcclTransport: empty transfer request batch";
        }
        ret = nonAggTransport(slice_list, stream);
        if (ret) {
            LOG(ERROR) << "HcclTransport: nonAggTransport error, ret: " << ret;
        }
    }
}

void HcclTransport::targetAcceptLoop(int deviceLogicId) {
    int ret = aclrtSetDevice(deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice failed ret: " << ret;
    }
    while (running_) {
        ret = transportMemAccept(&local_rank_info_, aggregateEnabled_);
        if (ret) {
            LOG(ERROR) << "HcclTransport: transportMemAccept failed ret: "
                       << ret;
        }
    }
}

void HcclTransport::targetLoop(int deviceLogicId) {
    aclrtStream stream;
    int ret = aclrtSetDevice(deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtSetDevice failed ret:" << ret;
    }

    ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtCreateStream error, ret:" << ret;
    }

    while (running_) {
        ret = transportMemTarget(stream);
        if (ret) {
            LOG(ERROR) << "HcclTransport: transportMemTarget failed ret:" << ret;
        }
    }
}

int HcclTransport::startNonAggThreads() {
    pid_t pid = getpid();
    int ret = 0;
    int deviceLogicId;
    ret = aclrtGetDevice(&deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtGetDevice failed, ret: " << ret;
        return ret;
    }

    initiatorThread_ =
        std::thread(&HcclTransport::initiatorLoop, this, deviceLogicId);
    targetAcceptThread_ =
        std::thread(&HcclTransport::targetAcceptLoop, this, deviceLogicId);
    targetThread_ = std::thread(&HcclTransport::targetLoop, this, deviceLogicId);

    LOG(INFO) << "HcclTransport: startNonAggThreads, pid: " << pid
              << ", deviceLogicId: " << deviceLogicId;
    return 0;
}

// Get HostIp\Port\DevicePhyId
int HcclTransport::getDevIdAndIpPortFromServerName(std::string &identifier,
                                                   std::string &hostIp,
                                                   int &port, int &npuId) {
    size_t firstColon = identifier.find(":");
    if (firstColon == std::string::npos) {
        LOG(ERROR) << "HcclTransport: getDevIdAndIpPortFromServerName failed, "
                      "identifier is empty";
        return -1;
    }

    size_t secondColon = identifier.find(":", firstColon + 1);
    if (secondColon == std::string::npos) {
        LOG(ERROR) << "HcclTransport: getDevIdAndIpPortFromServerName failed, "
                      "second colon missing";
        return -1;
    }

    hostIp = identifier.substr(0, firstColon);

    std::string portStr =
        identifier.substr(firstColon + 1, secondColon - firstColon - 1);
    try {
        port = std::stoi(portStr);
    } catch (const std::exception &e) {
        LOG(ERROR) << "Invalid Port Number";
        return -1;
    }

    std::string npuStr = identifier.substr(secondColon + 1);
    if (npuStr.find("npu_") != 0) {
        LOG(ERROR) << "Invalid npu number format - should start with 'npu_'";
        return -1;
    }

    try {
        npuId = std::stoi(npuStr.substr(4));
    } catch (const std::exception &e) {
        LOG(ERROR) << "Invalid device_id ID";
        return -1;
    }

    return 0;
}

int HcclTransport::rankInfoParse(int devicePhyId, std::string hostIp) {
    int ret = 0;
    int deviceLogicId = 0;
    ret = aclrtGetDevice(&deviceLogicId);
    if (ret) {
        LOG(ERROR) << "HcclTransport: aclrtGetDevice failed, ret: " << ret;
        return ret;
    }

    // Default configuration file path for HCCL
    std::ifstream fin("/etc/hccn.conf");
    if (!fin) {
        LOG(ERROR) << "can't open conf 文件：/etc/hccn.conf";
        return -1;
    }

    std::string line;
    while (std::getline(fin, line)) {
        if (line.rfind("address_", 0) == 0) {
            size_t equal_pos = line.find('=');
            if (equal_pos != std::string::npos) {
                std::string key = line.substr(8, equal_pos - 8);
                key.erase(key.begin(), std::find_if(key.begin(), key.end(),
                                                    [](unsigned char c) {
                                                        return !std::isspace(c);
                                                    }));
                if (key == std::to_string(devicePhyId)) {
                    std::string deviceIp = line.substr(equal_pos + 1);
                    deviceIp.erase(
                        deviceIp.begin(),
                        std::find_if(
                            deviceIp.begin(), deviceIp.end(),
                            [](unsigned char c) { return !std::isspace(c); }));
                    deviceIp.erase(
                        std::find_if(
                            deviceIp.rbegin(), deviceIp.rend(),
                            [](unsigned char c) { return !std::isspace(c); })
                            .base(),
                        deviceIp.end());

                    if (inet_pton(AF_INET, hostIp.c_str(),
                                  &local_rank_info_.hostIp) != 1) {
                        LOG(ERROR) << "HcclTransport: Invalid Host IP format: "
                                   << hostIp;
                        return -1;
                    }
                    local_rank_info_.rankId = devicePhyId;
                    local_rank_info_.serverIdx = 0;
                    local_rank_info_.devicePhyId = devicePhyId;
                    local_rank_info_.hostPort =
                        ASCEND_DEFAULT_HOST_PORT + devicePhyId;
                    local_rank_info_.deviceLogicId = deviceLogicId;
                    local_rank_info_.devicePort = ASCEND_DEFAULT_DEVICE_PORT;
                    local_rank_info_.pid = 0;
                    if (inet_pton(AF_INET, deviceIp.c_str(),
                                  &local_rank_info_.deviceIp) != 1) {
                        LOG(ERROR)
                            << "HcclTransport: Invalid Device IP format: "
                            << deviceIp;
                        return -1;
                    }
                    LOG(INFO)
                        << "rankInfoParse Success, hostIp: " << hostIp
                        << ", rankId: " << local_rank_info_.rankId
                        << ", serverIdx: " << local_rank_info_.serverIdx
                        << ", devicePhyId: " << local_rank_info_.devicePhyId
                        << ", hostPort: " << local_rank_info_.hostPort
                        << ", deviceLogicId: " << local_rank_info_.deviceLogicId
                        << ", devicePort: " << local_rank_info_.devicePort
                        << ", deviceIp: " << deviceIp
                        << ", device pid: " << local_rank_info_.pid;
                    // Exit after finishing rankInfoParse
                    return 0;
                }
            }
        }
    }
    // Not Found
    return -1;
}

int HcclTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> meta,
                           std::shared_ptr<Topology> topo) {
    int ret = 0;
    int port;
    std::string hostIp;
    int devicePhyId;
    metadata_ = meta;
    ret = getDevIdAndIpPortFromServerName(local_server_name, hostIp, port,
                                          devicePhyId);
    if (ret) {
        LOG(ERROR)
            << "HcclTransport: getDevIdAndIpPortFromServerName failed, ret: "
            << ret;
        return ret;
    }

    local_server_name_ = hostIp + ":" + std::to_string(port);
    LOG(INFO)
        << "HcclTransport: begin to install transport, local devicePhyId: "
        << devicePhyId << ", local_server_name: " << local_server_name;

    // add to local_rank_info_
    ret = rankInfoParse(devicePhyId, hostIp);
    if (ret) {
        LOG(ERROR) << "HcclTransport: rankInfoParse failed, ret: " << ret;
        return ret;
    }

    ret = initTransportMem(&local_rank_info_, aggregateEnabled_);
    if (ret) {
        LOG(ERROR) << "HcclTransport: initTransportMem failed, ret: " << ret;
        return ret;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "HcclTransport: cannot allocate local segment, ret: "
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

    running_ = true;

    if (aggregateEnabled_) {
        ret = startAggThreads();
        if (ret) {
            LOG(ERROR) << "HcclTransport: startAggThreads failed, ret: " << ret;
            return ret;
        }

        void *devAddr = nullptr;
        ret = aclrtMalloc(&devAddr, TOTAL_AGG_DEV_SIZE,
                          ACL_MEM_MALLOC_NORMAL_ONLY);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
            return ret;
        }

        const uint64_t alignment = 1 << 21;
        if ((uint64_t)devAddr % alignment != 0) {
            LOG(ERROR) << "The Merge malloc address is not 2M aligned.";
            return -1;
        }

        aggRegLocalMem((uint64_t)devAddr, TOTAL_AGG_DEV_SIZE, true);
    } else {
        ret = startNonAggThreads();
        if (ret) {
            LOG(ERROR) << "HcclTransport: startNonAggThreads failed, ret: "
                       << ret;
            return ret;
        }
    }

    return 0;
}

Status HcclTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "HcclTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "HcclTransport: Exceed the limitation of capacity, batch id: " +
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
        slice->hccl.dest_addr = request.target_offset;
        slice->hccl.dest_addr_type = request.target_offset_type;
        LOG(ERROR) << "HcclTransport: submitTransfer type: " << slice->hccl.dest_addr_type;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        std::unique_lock<std::mutex> lock(initiator_mutex_);
        slice_list.push_back(slice);
        lock.unlock();
        initiator_cond_.notify_one();
    }
    std::unique_lock<std::mutex> lock(initiator_mutex_);
    allReqQueues_.push(slice_list);
    lock.unlock();
    initiator_cond_.notify_one();

    return Status::OK();
}

Status HcclTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
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
        slice->hccl.dest_addr = request.target_offset;
        slice->hccl.dest_addr_type = request.target_offset_type;
        LOG(ERROR) << "HcclTransport: submitTransferTask type: " << slice->hccl.dest_addr_type;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        slice_list.push_back(slice);
    }
    std::unique_lock<std::mutex> lock(initiator_mutex_);
    allReqQueues_.push(slice_list);
    lock.unlock();
    initiator_cond_.notify_one();

    return Status::OK();
}

Status HcclTransport::getTransferStatus(BatchID batch_id, size_t task_id,
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

uint64_t g_transfer_dev_len = 0x4000000;
int HcclTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &location,
                                       bool remote_accessible,
                                       bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = location;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = (uint64_t)length;

    int ret;
    if (location == "cpu") {
        LOG(ERROR) << "HcclTransport: registerLocalMemory: " << addr << ", length:" << length;
        void *dev_addr = NULL;
        ret = aclrtMalloc(&dev_addr, g_transfer_dev_len, ACL_MEM_MALLOC_NORMAL_ONLY);
        if (ret != ACL_ERROR_NONE) {
            LOG(ERROR) << "failed to allocate device memory len:" << g_transfer_dev_len;
            return ret;
        }
        if (!aggregateEnabled_) {
            nonAggRegLocalMem((uint64_t)dev_addr, g_transfer_dev_len, true);
        }
    } else {
        if (aggregateEnabled_) {
            aggRegLocalMem(buffer_desc.addr, buffer_desc.length, false);
        } else {
            nonAggRegLocalMem(buffer_desc.addr, buffer_desc.length, false);
        } 
    }

    ret = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (ret) {
        LOG(ERROR) << "HcclTransport: addLocalMemoryBuffer failed, ret: "
                   << ret;
        return ret;
    }

    return 0;
}

int HcclTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int HcclTransport::allocateLocalSegmentID() {
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "ascend";
    desc->rank_info.rankId = local_rank_info_.rankId;
    desc->rank_info.hostIp = inet_ntoa(local_rank_info_.hostIp);
    desc->rank_info.hostPort = local_rank_info_.hostPort;
    desc->rank_info.deviceLogicId = local_rank_info_.deviceLogicId;
    desc->rank_info.devicePhyId = local_rank_info_.devicePhyId;
    desc->rank_info.deviceIp = inet_ntoa(local_rank_info_.deviceIp);
    desc->rank_info.devicePort = local_rank_info_.devicePort;
    desc->rank_info.pid = local_rank_info_.pid;

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int HcclTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, -1);
    return metadata_->updateLocalSegmentDesc();
}

int HcclTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, -1);
    return metadata_->updateLocalSegmentDesc();
}

}  // namespace mooncake
