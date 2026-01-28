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

#include "transport/efa_transport/efa_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <chrono>
#include <cstddef>
#include <future>
#include <set>
#include <thread>

#include <dlfcn.h>

#include "common.h"
#include "config.h"
#include "memory_location.h"
#include "topology.h"
#include "transport/efa_transport/efa_context.h"
#include "transport/efa_transport/efa_endpoint.h"

namespace mooncake {

EfaTransport::EfaTransport() {
    LOG(INFO) << "[EFA] AWS Elastic Fabric Adapter transport initialized";
}

EfaTransport::~EfaTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    for (auto &entry : batch_desc_set_) delete entry.second;
    batch_desc_set_.clear();
#endif
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

int EfaTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    if (topo == nullptr) {
        LOG(ERROR) << "EfaTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;

    auto ret = initializeEfaResources();
    if (ret) {
        LOG(ERROR) << "EfaTransport: cannot initialize EFA resources";
        return ret;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "EfaTransport: cannot allocate local segment";
        return ret;
    }

    ret = startHandshakeDaemon(local_server_name);
    if (ret) {
        LOG(ERROR) << "EfaTransport: cannot start handshake daemon";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "EfaTransport: cannot publish segments";
        return ret;
    }

    return 0;
}

int EfaTransport::preTouchMemory(void *addr, size_t length) {
    if (context_list_.size() == 0) {
        return 0;
    }

    auto hwc = std::thread::hardware_concurrency();
    auto num_threads = hwc > 64 ? 16 : std::min(hwc, 8u);
    if (length > (size_t)globalConfig().max_mr_size) {
        length = (size_t)globalConfig().max_mr_size;
    }
    size_t block_size = length / num_threads;
    if (block_size == 0) {
        return 0;
    }

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<int> thread_results(num_threads, 0);

    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, addr, i, block_size, &thread_results]() {
            thread_results[i] = context_list_[0]->preTouchMemory(
                (void *)((uintptr_t)addr + i * block_size), block_size);
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &result : thread_results) {
        if (result != 0) {
            return result;
        }
    }

    return 0;
}

int EfaTransport::registerLocalMemory(void *addr, size_t length,
                                     const std::string &location,
                                     bool remote_accessible,
                                     bool update_metadata) {
    return registerLocalMemoryInternal(addr, length, location,
                                      remote_accessible, update_metadata,
                                      false);
}

int EfaTransport::registerLocalMemoryInternal(void *addr, size_t length,
                                             const std::string &location,
                                             bool remote_accessible,
                                             bool update_metadata,
                                             bool force_sequential) {
    if (context_list_.empty()) {
        LOG(WARNING) << "EfaTransport: No EFA contexts available";
        return ERR_INVALID_ARGUMENT;
    }

    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                 IBV_ACCESS_REMOTE_READ;

    // Register memory with all EFA contexts
    for (auto &context : context_list_) {
        int ret = context->registerMemoryRegion(addr, length, access);
        if (ret) {
            LOG(ERROR) << "Failed to register memory with EFA context";
            return ret;
        }
    }

    if (update_metadata) {
        BufferDesc buffer_desc;
        buffer_desc.addr = (uint64_t)addr;
        buffer_desc.length = length;
        buffer_desc.location = location;
        buffer_desc.remote_accessible = remote_accessible;

        for (auto &context : context_list_) {
            buffer_desc.rkey_list.push_back(context->rkey(addr));
        }

        auto ret = metadata_->registerLocalMemory(local_server_name_,
                                                  buffer_desc);
        if (ret) {
            LOG(ERROR) << "Failed to update metadata";
            return ret;
        }
    }

    return 0;
}

int EfaTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return unregisterLocalMemoryInternal(addr, update_metadata, false);
}

int EfaTransport::unregisterLocalMemoryInternal(void *addr,
                                               bool update_metadata,
                                               bool force_sequential) {
    for (auto &context : context_list_) {
        int ret = context->unregisterMemoryRegion(addr);
        if (ret) {
            LOG(ERROR) << "Failed to unregister memory with EFA context";
            return ret;
        }
    }

    if (update_metadata) {
        auto ret = metadata_->unregisterLocalMemory(local_server_name_, addr);
        if (ret) {
            LOG(ERROR) << "Failed to update metadata";
            return ret;
        }
    }

    return 0;
}

int EfaTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list,
    const std::string &location) {
    for (const auto &entry : buffer_list) {
        int ret = registerLocalMemoryInternal(
            entry.addr, entry.length, location, true, false, true);
        if (ret) {
            return ret;
        }
    }

    std::vector<BufferDesc> buffer_desc_list;
    for (const auto &entry : buffer_list) {
        BufferDesc buffer_desc;
        buffer_desc.addr = (uint64_t)entry.addr;
        buffer_desc.length = entry.length;
        buffer_desc.location = location;
        buffer_desc.remote_accessible = true;

        for (auto &context : context_list_) {
            buffer_desc.rkey_list.push_back(context->rkey(entry.addr));
        }

        buffer_desc_list.push_back(buffer_desc);
    }

    return metadata_->registerLocalMemoryBatch(local_server_name_,
                                              buffer_desc_list);
}

int EfaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (const auto &addr : addr_list) {
        int ret = unregisterLocalMemoryInternal(addr, false, true);
        if (ret) {
            return ret;
        }
    }

    return metadata_->unregisterLocalMemoryBatch(local_server_name_,
                                                addr_list);
}

Status EfaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    return Status::NotImplemented(
        "EfaTransport::submitTransfer not implemented, use "
        "submitTransferTask instead");
}

Status EfaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (auto *task : task_list) {
        // Submit task to appropriate EFA context
        if (!context_list_.empty()) {
            context_list_[0]->submitTask(task);
        }
    }
    return Status::OK();
}

Status EfaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                      TransferStatus &status) {
    auto &batch_desc = toBatchDesc(batch_id);
    if (task_id >= batch_desc.task_list.size()) {
        return Status::InvalidArgument("Task ID out of range");
    }

    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;

    if (task.is_finished) {
        status.s = (task.failed_slice_count > 0)
                      ? TransferStatusEnum::FAILED
                      : TransferStatusEnum::COMPLETED;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }

    return Status::OK();
}

Status EfaTransport::getTransferStatus(BatchID batch_id,
                                      std::vector<TransferStatus> &status) {
    auto &batch_desc = toBatchDesc(batch_id);
    status.resize(batch_desc.task_list.size());

    for (size_t i = 0; i < batch_desc.task_list.size(); ++i) {
        auto ret = getTransferStatus(batch_id, i, status[i]);
        if (!ret.ok()) {
            return ret;
        }
    }

    return Status::OK();
}

SegmentID EfaTransport::getSegmentID(const std::string &segment_name) {
    auto segment_desc = metadata_->getSegmentDescByName(segment_name);
    if (!segment_desc) {
        return INVALID_SEGMENT_ID;
    }
    return segment_desc->segment_id;
}

int EfaTransport::allocateLocalSegmentID() {
    SegmentDesc segment_desc;
    segment_desc.segment_name = local_server_name_;
    segment_desc.server_name = local_server_name_;
    segment_desc.protocol = "efa";

    for (auto &context : context_list_) {
        SegmentDesc::NicDescriptor nic_desc;
        nic_desc.device_name = context->device_name();
        nic_desc.port = context->port();
        nic_desc.gid_index = context->gid_index();
        nic_desc.gid = context->gid();
        nic_desc.mtu = context->mtu();
        segment_desc.nic_descriptor_list.push_back(nic_desc);
    }

    return metadata_->registerLocalSegment(segment_desc);
}

int EfaTransport::initializeEfaResources() {
    struct ibv_device **device_list = ibv_get_device_list(nullptr);
    if (!device_list) {
        LOG(ERROR) << "Failed to get IB device list";
        return ERR_INVALID_ARGUMENT;
    }

    std::vector<std::string> efa_devices;
    const auto &hca_list = local_topology_->getHcaList();

    // Filter for EFA devices
    for (const auto &device_name : hca_list) {
        // EFA devices typically have names like "rdmap0s2", "rdmap1s3", etc.
        if (device_name.find("rdmap") != std::string::npos) {
            efa_devices.push_back(device_name);
        }
    }

    if (efa_devices.empty()) {
        LOG(WARNING) << "No EFA devices found in topology";
        ibv_free_device_list(device_list);
        return ERR_INVALID_ARGUMENT;
    }

    // Initialize EFA contexts for each device
    for (const auto &device_name : efa_devices) {
        auto context = std::make_shared<EfaContext>(*this, device_name);
        int ret = context->construct();
        if (ret == 0) {
            context_list_.push_back(context);
            LOG(INFO) << "Initialized EFA context for device: " << device_name;
        } else {
            LOG(WARNING) << "Failed to initialize EFA context for device: "
                        << device_name;
        }
    }

    ibv_free_device_list(device_list);

    if (context_list_.empty()) {
        LOG(ERROR) << "No EFA contexts were successfully initialized";
        return ERR_INVALID_ARGUMENT;
    }

    return 0;
}

int EfaTransport::startHandshakeDaemon(std::string &local_server_name) {
    auto handshake_handler = [this](const HandShakeDesc &peer_desc,
                                   HandShakeDesc &local_desc) -> int {
        return onSetupEfaConnections(peer_desc, local_desc);
    };

    return metadata_->registerHandshakeHandler(handshake_handler);
}

int EfaTransport::onSetupEfaConnections(const HandShakeDesc &peer_desc,
                                       HandShakeDesc &local_desc) {
    // Implementation for setting up EFA connections
    // This would handle the handshake between local and remote EFA endpoints
    LOG(INFO) << "Setting up EFA connections";
    return 0;
}

int EfaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                              size_t length, int &buffer_id, int &device_id,
                              int retry_cnt) {
    return selectDevice(desc, offset, length, "", buffer_id, device_id,
                       retry_cnt);
}

int EfaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                              size_t length, std::string_view hint,
                              int &buffer_id, int &device_id, int retry_cnt) {
    if (!desc) {
        return ERR_INVALID_ARGUMENT;
    }

    // Simple device selection logic
    device_id = 0;
    buffer_id = 0;

    // Find appropriate buffer for the given offset and length
    for (size_t i = 0; i < desc->buffer_desc_list.size(); ++i) {
        const auto &buffer = desc->buffer_desc_list[i];
        if (offset >= buffer.addr &&
            offset + length <= buffer.addr + buffer.length) {
            buffer_id = i;
            return 0;
        }
    }

    return ERR_INVALID_ARGUMENT;
}

}  // namespace mooncake
