#include "transport/ascend_transport/heterogeneous_rdma_transport.h"
#include <chrono>

namespace mooncake {

namespace {
bool isCpuMemory(void *addr) {
    aclrtPtrAttributes attributes{};
    if (int ret = aclrtPointerGetAttributes(addr, &attributes)) {
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return false;
    }
    return (attributes.location.type == 0);
}
}  // namespace

HeterogeneousRdmaTransport::~HeterogeneousRdmaTransport() {
    running_ = false;
    transfer_queue_cv_.notify_one();
    transfer_thread_.join();
    free(host_addr_);
    host_addr_ = nullptr;
    aclrtFree(dev_addr_);
    dev_addr_ = nullptr;
}

void HeterogeneousRdmaTransport::transferLoop() {
    Status s;
    aclrtStream stream_d2h{};
    int ret = aclrtSetDevice(logic_device_id_);
    if (ret) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtSetDevice error, ret: "
                   << ret;
    }

    ret = aclrtCreateStream(&stream_d2h);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: aclrtCreateStream error, ret: "
            << ret;
    }

    while (running_) {
        auto transfer_info = getTransfer();
        const auto &task_list = transfer_info.tasks;
        if (task_list.empty()) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: empty transfer task batch";
            continue;
        }

        int total_length = transfer_info.total_length;
        if ((host_offset_ + total_length) > HUGE_HOST_SIZE) {
            // LOG(INFO) << "HeterogeneousRdmaTransport: transferLoop reset host
            // offset";
            host_offset_ = 0;
        }

        ret = aclrtMemcpyAsync(host_addr_ + host_offset_, total_length,
                               transfer_info.block, total_length,
                               ACL_MEMCPY_DEVICE_TO_HOST, stream_d2h);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtMemcpyAsync dtoh "
                          "error, ret: "
                       << ret << ", hostAddr: " << host_addr_
                       << ", host_offset_: " << host_offset_
                       << ", deviceAddr: " << transfer_info.block
                       << "len: " << total_length;
            continue;
        }

        ret = aclrtSynchronizeStream(stream_d2h);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtSynchronizeStream "
                          "error, ret: "
                       << ret;
            continue;
        }

        for (size_t index = 0; index < task_list.size(); ++index) {
            auto &task = *task_list[index];
            auto &request = *task.request;
            request.source = host_addr_ + host_offset_;
            host_offset_ += request.length;
        }

        s = transport_->submitTransferTask(task_list);
        if (!s.ok()) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: Rdma submitTransferTask error";
        }
        releaseBlock(transfer_info.block);  // 释放block
    }
}

int HeterogeneousRdmaTransport::install(std::string &local_server_name,
                                        std::shared_ptr<TransferMetadata> meta,
                                        std::shared_ptr<Topology> topo) {
    local_server_name_ = local_server_name;
    running_ = true;

    int ret = aclrtGetDevice(&logic_device_id_);
    if (ret) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtGetDevice failed, ret: "
                   << ret;
        return ret;
    }

    LOG(INFO) << "HeterogeneousRdmaTransport: begin to install transport,  "
                 "local_server_name: "
              << local_server_name << "logic_device_id_: " << logic_device_id_;

    if (transport_ == nullptr) {
        LOG(ERROR) << "HeterogeneousRdmaTransport:transport is null";
        return ret;
    }

    host_addr_ = (char *)std::aligned_alloc(64, HUGE_HOST_SIZE);
    if (host_addr_ == nullptr) {
        LOG(ERROR) << "HeterogeneousRdmaTransport:host_addr_ is null, "
                      "aligned_alloc failed";
        return -1;
    }

    dev_addr_ = nullptr;
    ret = aclrtMalloc((void **)&dev_addr_, HUGE_DEVICE_NUM * HUGE_DEVICE_SIZE,
                      ACL_MEM_MALLOC_NORMAL_ONLY);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "Failed to allocate device memory, ret:" << ret;
        return ret;
    }

    for (int i = 0; i < HUGE_DEVICE_NUM; i++) {
        block_queue_.push(dev_addr_ + i * HUGE_DEVICE_SIZE);
    }

    transfer_thread_ =
        std::thread(&HeterogeneousRdmaTransport::transferLoop, this);

    ret = transport_->install(local_server_name_, meta, topo);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousRdmaTransport::RdmaTransport install error, ret: "
            << ret;
        return ret;
    }

    ret = transport_->registerLocalMemory(host_addr_, HUGE_HOST_SIZE, "cpu",
                                          true, true);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: registerLocalMemory error, ret: "
            << ret;
        return ret;
    }
    return ret;
}

int HeterogeneousRdmaTransport::registerLocalMemory(void *addr, size_t length,
                                                    const std::string &name,
                                                    bool remote_accessible,
                                                    bool update_metadata) {
    if (isCpuMemory(addr)) {
        if (int ret = transport_->registerLocalMemory(addr, length, "cpu", true,
                                                      true)) {
            LOG(ERROR) << "rdma transport registerLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousRdmaTransport::unregisterLocalMemory(void *addr,
                                                      bool update_metadata) {
    if (isCpuMemory(addr)) {
        int ret = transport_->unregisterLocalMemory(addr, true);
        if (ret) {
            LOG(ERROR) << "rdma transport unregisterLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousRdmaTransport::registerLocalMemoryBatch(
    const std::vector<HeterogeneousRdmaTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport registerLocalMemoryBatch "
                          "error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int HeterogeneousRdmaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport "
                          "unregisterLocalMemoryBatch error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int HeterogeneousRdmaTransport::checkAndCreateStreamCopy() {
    if (!stream_copy_created_) {
        int ret = aclrtSetDevice(logic_device_id_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtSetDevice failed ret: "
                << ret;
            return ret;
        }
        ret = aclrtCreateStream(&stream_copy_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtCreateStream error, ret: "
                << ret;
            return ret;
        }
        stream_copy_created_ = true;
    }
    return 0;
}

Status HeterogeneousRdmaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    if (entries.empty()) {
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: empty transfer request batch";
        return Status::OK();
    }

    if (isCpuMemory(entries[0].source)) {
        return transport_->submitTransfer(batch_id, entries);
    }

    std::vector<TransferRequest> new_entries;
    new_entries.resize(entries.size());
    int index = 0;
    int ret = checkAndCreateStreamCopy();
    if (ret) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: createStream error, ret: "
                   << ret;
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: createStream error");
    }

    {
        std::lock_guard<std::mutex> lock(memcpy_mutex_);
        for (auto &request : entries) {
            if (host_offset_ + request.length > HUGE_HOST_SIZE) {
                host_offset_ = 0;
            }
            ret = aclrtMemcpyAsync(host_addr_ + host_offset_, request.length,
                                   request.source, request.length,
                                   ACL_MEMCPY_DEVICE_TO_HOST, stream_copy_);
            if (ret) {
                LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtMemcpyAsync "
                              "error, ret: "
                           << ret << ", hostAddr: " << host_addr_
                           << ", host_offset_: " << host_offset_
                           << ", deviceAddr: " << request.source
                           << "len: " << request.length;
                return Status::InvalidArgument(
                    "HeterogeneousRdmaTransport: aclrtMemcpyAsync error");
            }
            new_entries[index] = request;
            new_entries[index].source = host_addr_ + host_offset_;
            host_offset_ += request.length;
        }

        ret = aclrtSynchronizeStream(stream_copy_);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtSynchronizeStream "
                          "error, ret: "
                       << ret;
            return Status::InvalidArgument(
                "HeterogeneousRdmaTransport: aclrtSynchronizeStream error");
        }
    }

    return transport_->submitTransfer(batch_id, new_entries);
}

Status HeterogeneousRdmaTransport::noAggTransport(
    const std::vector<TransferTask *> &task_list) {
    int ret = 0;
    for (size_t index = 0; index < task_list.size(); ++index) {
        auto &task = *task_list[index];
        auto &request = *task.request;

        if (host_offset_ + request.length > HUGE_HOST_SIZE) {
            host_offset_ = 0;
            // LOG(INFO) << "HeterogeneousRdmaTransport::noAggTransport
            // host_offset reset";
        }

        ret = aclrtMemcpyAsync(host_addr_ + host_offset_, request.length,
                               request.source, request.length,
                               ACL_MEMCPY_DEVICE_TO_HOST, stream_copy_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtMemcpyAsync error, ret: "
                << ret << ", hostAddr: " << host_addr_
                << ", host_offset_: " << host_offset_
                << ", deviceAddr: " << request.source
                << ", len: " << request.length;
            return Status::InvalidArgument(
                "HeterogeneousRdmaTransport: Exceed the limitation of "
                "capacity, batch id: ");
        }
        request.source = host_addr_ + host_offset_;
        host_offset_ += request.length;
    }

    ret = aclrtSynchronizeStream(stream_copy_);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: aclrtSynchronizeStream error, ret: "
            << ret;
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: Exceed the limitation of capacity, "
            "batch id: ");
    }

    return transport_->submitTransferTask(task_list);
}

Status HeterogeneousRdmaTransport::aggTransport(
    const std::vector<TransferTask *> &task_list) {
    uint64_t index = 0;
    while (index < task_list.size()) {
        auto block = acquireBlock();
        uint64_t block_offset = 0;
        std::vector<TransferTask *> tasks;

        while (index < task_list.size()) {
            auto &request = *(task_list[index]->request);
            if (block_offset + request.length > HUGE_DEVICE_SIZE) {
                break;
            }

            int ret = aclrtMemcpyAsync(
                block + block_offset, request.length, request.source,
                request.length, ACL_MEMCPY_DEVICE_TO_DEVICE, stream_copy_);
            if (ret) {
                LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtMemcpyAsync "
                              "dtod error, ret: "
                           << ret << ", host_offset_: " << host_offset_
                           << ", deviceAddr: " << request.source
                           << ", len: " << request.length;
                return Status::InvalidArgument(
                    "HeterogeneousRdmaTransport: aclrtMemcpyAsync dtod error");
            }

            tasks.push_back(task_list[index]);
            block_offset += request.length;
            ++index;
        }

        int ret = aclrtSynchronizeStream(stream_copy_);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtSynchronizeStream "
                          "failed, ret: "
                       << ret;
            return Status::InvalidArgument(
                "HeterogeneousRdmaTransport: aclrtSynchronizeStream error");
        }

        addTransfer(std::move(tasks), block, block_offset);
    }

    auto start = std::chrono::high_resolution_clock::now();
    while (!allBlockReleased()) {  // 等待transfer_queue_被处理完毕
        auto end = std::chrono::high_resolution_clock::now();
        auto duration_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        auto count = duration_ms.count();
        if (count > 100) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: transfer_queue_ wait "
                          "too long, count:"
                       << count << "ms";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(20));  // 休眠 20 毫秒
        } else {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(1));  // 休眠 1 毫秒
            // std::this_thread::yield();
        }
    }

    return Status::OK();
}

Status HeterogeneousRdmaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    if (task_list.empty()) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: empty transfer task list";
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: task_list is empty");
    }

    auto &task = *task_list[0];
    auto &request = *task.request;

    if (isCpuMemory(request.source)) {
        return transport_->submitTransferTask(task_list);
    }

    auto ret = checkAndCreateStreamCopy();
    if (ret) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: createStream error, ret: "
                   << ret;
        return Status::InvalidArgument(
            "HeterogeneousRdmaTransport: createStream error");
    }

    {
        std::lock_guard<std::mutex> lock(memcpy_mutex_);
        if (request.length >= AGGREGATE_SIZE_LIMIT) {
            return noAggTransport(task_list);
        } else {
            return aggTransport(task_list);
        }
    }

    return Status::OK();
}

Status HeterogeneousRdmaTransport::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus> &status) {
    return transport_->getTransferStatus(batch_id, status);
}

Status HeterogeneousRdmaTransport::getTransferStatus(BatchID batch_id,
                                                     size_t task_id,
                                                     TransferStatus &status) {
    return transport_->getTransferStatus(batch_id, task_id, status);
}

}  // namespace mooncake
