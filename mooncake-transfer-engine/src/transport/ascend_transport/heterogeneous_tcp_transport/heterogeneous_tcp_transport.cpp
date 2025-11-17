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

#include "transport/ascend_transport/heterogeneous_tcp_transport.h"

namespace mooncake {
HeterogeneousTcpTransport::HeterogeneousTcpTransport()
    : transport_(std::make_unique<TcpTransport>())  {
}

HeterogeneousTcpTransport::~HeterogeneousTcpTransport() {
    running_ = false;
    transfer_cond_.notify_one();
    transferThread_.join();
    free(hostAddr_);
    aclrtFree(devAddr_);
    hostAddr_ = nullptr;
    devAddr_ = nullptr;
}

void HeterogeneousTcpTransport::transferLoop() {
    Status s;
    aclrtStream stream;
    int ret = aclrtSetDevice(deviceLogicId_);
    if (ret) {
        LOG(ERROR) << "HeterogeneousTcpTransport: aclrtSetDevice error, ret: "
                   << ret;
    }

    ret = aclrtCreateStream(&stream);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousTcpTransport: aclrtCreateStream error, ret: "
            << ret;
    }

    while (running_) {
        std::unique_lock<std::mutex> lock(transfer_mutex_);
        if (transferQueues_.empty()) {
        transfer_cond_.wait(lock, [this] { return !transferQueues_.empty() || !running_; });
        if (transferQueues_.empty()) {
            continue;
        }

        auto pkg = std::move(transferQueues_.front());
        transferQueues_.pop();
        const auto &task_list = pkg.tasks;
        lock.unlock();
        if (task_list.empty()) {
            LOG(ERROR)
                << "HeterogeneousTcpTransport: empty transfer task batch";
            continue;
        }
        uint64_t total_length = pkg.total_length;
        if ((offset_ + total_length) > HUGE_HOST_SIZE) {
            offset_ = 0;
        }
        ret = aclrtMemcpyAsync(static_cast<char *>(hostAddr_) + offset_,
                               total_length, hugeDevAddrs[pkg.devId],
                               total_length, ACL_MEMCPY_DEVICE_TO_HOST, stream);
        if (ret) {
            LOG(ERROR) << "HeterogeneousTcpTransport: aclrtMemcpyAsync dtoh "
                          "error, ret: "
                       << ret << ", hostAddr: " << hostAddr_
                       << ", offset_: " << offset_
                       << ", deviceAddr: " << hugeDevAddrs[pkg.devId]
                       << "len: " << total_length;
            transfer_counter_.fetch_add(1);
            continue;
        }
        LOG(INFO) << "HeterogeneousTcpTransport aclrtMemcpyAsync dtoh ok";
        ret = aclrtSynchronizeStream(stream);
        if (ret) {
            LOG(ERROR) << "HeterogeneousTcpTransport: aclrtSynchronizeStream "
                          "error, ret: "
                       << ret;
            transfer_counter_.fetch_add(1);
            continue;
        }

        for (size_t index = 0; index < task_list.size(); ++index) {
            auto &task = *task_list[index];
            auto &request = *task.request;
            request.source = static_cast<char *>(hostAddr_) + offset_;
            offset_ += request.length;
        }

        std::unique_lock<std::mutex> lock_dev(dev_mtx_);
        mem_blocks[pkg.devId] = false;
        dev_cv_.notify_one();
        s = transport_->submitTransferTask(task_list);
        if (!s.ok()) {
            LOG(ERROR)
                << "HeterogeneousTcpTransport: Tcp submitTransferTask error";
        }
        transfer_counter_.fetch_add(1);
    }
}

int HeterogeneousTcpTransport::install(std::string &local_server_name,
                                        std::shared_ptr<TransferMetadata> meta,
                                        std::shared_ptr<Topology> topo) {
    local_server_name_ = local_server_name;
    running_ = true;

    int ret = aclrtGetDevice(&deviceLogicId_);
    if (ret) {
        LOG(ERROR) << "HeterogeneousTcpTransport: aclrtGetDevice failed, ret: "
                   << ret;
        return ret;
    }

    LOG(INFO) << "HeterogeneousTcpTransport: begin to install transport,  "
                 "local_server_name: "
              << local_server_name << "deviceLogicId_: " << deviceLogicId_;

    if (transport_ == nullptr) {
        LOG(ERROR) << "HeterogeneousTcpTransport:transport is null";
        return -1;
    }

    hostAddr_ = static_cast<char*>(aligned_alloc(64, HUGE_HOST_SIZE));
    if (hostAddr_ == nullptr) {
        LOG(ERROR) << "HeterogeneousTcpTransport:hostAddr_ is null, "
                      "aligned_alloc failed";
        return -1;
    }
    devAddr_ = nullptr;
    ret = aclrtMalloc(&devAddr_, HUGE_DEVICE_NUM * HUGE_DEVICE_SIZE,
                      ACL_MEM_MALLOC_NORMAL_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    for (int i = 0; i < HUGE_DEVICE_NUM; i++) {
        hugeDevAddrs.push_back(static_cast<char *>(devAddr_) +
                               i * HUGE_DEVICE_SIZE);
    }

    transferThread_ =
        std::thread(&HeterogeneousTcpTransport::transferLoop, this);

    ret = transport_->install(local_server_name_, meta, topo);
    if (ret) {
        LOG(ERROR) << "HeterogeneousTcpTransport::TcpTransport install error, ret: " << ret;
        return ret;
    }
    ret = transport_->registerLocalMemory(hostAddr_, HUGE_HOST_SIZE, "cpu",
                                          true, true);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousTcpTransport: registerLocalMemory error, ret: "
            << ret;
        return ret;
    }
    LOG(INFO) << "HeterogeneousTcpTransport install ok";
    return ret;
}

int HeterogeneousTcpTransport::registerLocalMemory(void *addr, size_t length,
                                                    const std::string &name,
                                                    bool remote_accessible,
                                                    bool update_metadata) {
    aclrtPtrAttributes attributes;
    int ret = aclrtPointerGetAttributes(addr, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return ret;
    }

    if (attributes.location.type != 1) {
        ret = transport_->registerLocalMemory(addr, length, "cpu", true, true);
        if (ret) {
            LOG(ERROR) << "rdma transport registerLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousTcpTransport::unregisterLocalMemory(void *addr,
                                                      bool update_metadata) {
    aclrtPtrAttributes attributes;
    int ret = aclrtPointerGetAttributes(addr, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return ret;
    }

    if (attributes.location.type != 1) {
        ret = transport_->unregisterLocalMemory(addr, true);
        if (ret) {
            LOG(ERROR) << "rdma transport unregisterLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousTcpTransport::registerLocalMemoryBatch(
    const std::vector<HeterogeneousTcpTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousTcpTransport registerLocalMemoryBatch "
                          "error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int HeterogeneousTcpTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousTcpTransport "
                          "unregisterLocalMemoryBatch error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int HeterogeneousTcpTransport::createStream() {
    if (firstSubmit_) {
        int ret = aclrtSetDevice(deviceLogicId_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousTcpTransport: aclrtSetDevice failed ret: "
                << ret;
            return ret;
        }
        ret = aclrtCreateStream(&stream_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousTcpTransport: aclrtCreateStream error, ret: "
                << ret;
            return ret;
        }
        firstSubmit_ = false;
    }
    return 0;
}

Status HeterogeneousTcpTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    if (entries.empty()) {
        LOG(ERROR)
            << "HeterogeneousTcpTransport: empty transfer request batch";
        return Status::OK();
    }
    std::vector<TransferRequest> new_entries;
    new_entries.resize(entries.size());
    int index = 0;
    int ret;
    ret = createStream();
    if (ret) {
        LOG(ERROR) << "HeterogeneousTcpTransport: createStream error, ret: "
                   << ret;
        return Status::InvalidArgument(
            "HeterogeneousTcpTransport: createStream error");
    }
    aclrtPtrAttributes attributes;
    ret = aclrtPointerGetAttributes(entries[0].source, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointerGetAttributes error, ret: " << ret;
        return Status::InvalidArgument(
            "HeterogeneousTcpTransport: aclrtPointerGetAttributes error");
    }

    if (attributes.location.type != 1) {
        return transport_->submitTransfer(batch_id, entries);
    }

    {
        std::lock_guard<std::mutex> lock(memcpy_mutex_);
        for (auto &request : entries) {
            if (offset_ + request.length > HUGE_HOST_SIZE) {
                offset_ = 0;
            }
            ret =
                aclrtMemcpyAsync(static_cast<char *>(hostAddr_) + offset_,
                                 request.length, request.source, request.length,
                                 ACL_MEMCPY_DEVICE_TO_HOST, stream_);
            if (ret) {
                LOG(ERROR) << "HeterogeneousTcpTransport: aclrtMemcpyAsync "
                              "error, ret: "
                           << ret << ", hostAddr: " << hostAddr_
                           << ", offset_: " << offset_
                           << ", deviceAddr: " << request.source
                           << "len: " << request.length;
                return Status::InvalidArgument(
                    "HeterogeneousTcpTransport: aclrtMemcpyAsync error");
            }
            new_entries[index] = request;
            new_entries[index].source =
                static_cast<char *>(hostAddr_) + offset_;
            offset_ += request.length;
        }

        ret = aclrtSynchronizeStream(stream_);
        if (ret) {
            LOG(ERROR) << "HeterogeneousTcpTransport: aclrtSynchronizeStream "
                          "error, ret: "
                       << ret;
            return Status::InvalidArgument(
                "HeterogeneousTcpTransport: aclrtSynchronizeStream error");
        }
    }

    return transport_->submitTransfer(batch_id, new_entries);
}

Status HeterogeneousTcpTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    int ret;
    if (task_list.empty()) {
        LOG(ERROR) << "HeterogeneousTcpTransport: empty transfer task list";
        return Status::OK();
    }

    ret = createStream();
    if (ret) {
        LOG(ERROR) << "HeterogeneousTcpTransport: createStream error, ret: "
                   << ret;
        return Status::InvalidArgument(
            "HeterogeneousTcpTransport: createStream error");
    }

    auto &task_f = *task_list[0];
    auto &request_f = *task_f.request;
    aclrtPtrAttributes attributes;
    ret = aclrtPointerGetAttributes(request_f.source, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointerGetAttributes error, ret: " << ret;
        return Status::InvalidArgument(
            "HeterogeneousTcpTransport: aclrtPointerGetAttributes error");
    }

    if (attributes.location.type != 1) {
        return transport_->submitTransferTask(task_list);
    }

    uint64_t total_length = 0;
    std::vector<TransferTask *> subTasks;
    uint64_t index = 0;
    int usedHugeDevNum = 0;
    {
        std::lock_guard<std::mutex> lock(memcpy_mutex_);
        while (index < task_list.size()) {
            std::unique_lock<std::mutex> lock_dev(dev_mtx_);
            dev_cv_.wait(lock_dev, [&] { return !mem_blocks[devId_]; });
            mem_blocks[devId_] = true;
            lock_dev.unlock();
            while (index < task_list.size()) {
                auto &task = *task_list[index];
                auto &request = *task.request;
                if (total_length + request.length > HUGE_DEVICE_SIZE) {
                    break;
                }
                ret = aclrtMemcpyAsync(
                    static_cast<char *>(hugeDevAddrs[devId_]) + total_length,
                    request.length, request.source, request.length,
                    ACL_MEMCPY_DEVICE_TO_DEVICE, stream_);
                if (ret) {
                    LOG(ERROR)
                        << "HeterogeneousTcpTransport: aclrtMemcpyAsync dtod "
                           "error, ret: "
                        << ret << ", offset_: " << offset_
                        << ", deviceAddr: " << request.source
                        << ", len: " << request.length;
                    return Status::InvalidArgument(
                        "HeterogeneousTcpTransport: aclrtMemcpyAsync dtod "
                        "error");
                }
                subTasks.push_back(task_list[index]);
                ++index;
                total_length += request.length;
            }

            std::unique_lock<std::mutex> lock(transfer_mutex_);
            transferQueues_.emplace(std::move(subTasks), total_length, devId_);
            lock.unlock();
            transfer_cond_.notify_one();
            subTasks.clear();
            total_length = 0;
            devId_ = (devId_ + 1) & (HUGE_DEVICE_NUM - 1);
            usedHugeDevNum++;
        }
    }

    while (transfer_counter_.load() < usedHugeDevNum) {
        std::this_thread::yield();
    }

    transfer_counter_.fetch_sub(usedHugeDevNum);

    return Status::OK();
}

Status HeterogeneousTcpTransport::getTransferStatus(BatchID batch_id,
                                                     size_t task_id,
                                                     TransferStatus &status) {
    return transport_->getTransferStatus(batch_id, task_id, status);
}

}  // namespace mooncake