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

#include "transport/rdma_transport/mock_rdma_transport.h"

#include <glog/logging.h>
#include <stdlib.h>

#include <cstring>
#include <random>

#include "transport/transport.h"
#include "transfer_metadata.h"
#include "common.h"

namespace mooncake {

bool isMockRdmaEnabled() {
    const char* val = std::getenv("MC_RDMA_MOCK");
    return val && std::string(val) == "1";
}

MockRdmaTransport::MockRdmaTransport() {
    // Parse fault injection parameters
    const char* fail_rate = std::getenv("MC_RDMA_MOCK_FAIL_RATE");
    if (fail_rate) {
        fail_rate_ = std::stod(fail_rate);
        if (fail_rate_ < 0.0) fail_rate_ = 0.0;
        if (fail_rate_ > 1.0) fail_rate_ = 1.0;
    }

    const char* fail_after = std::getenv("MC_RDMA_MOCK_FAIL_AFTER");
    if (fail_after) {
        fail_after_ = std::stoi(fail_after);
    }

    if (fail_rate_ > 0.0 || fail_after_ >= 0) {
        LOG(INFO) << "[MockRdmaTransport] Fault injection enabled: fail_rate="
                  << fail_rate_ << ", fail_after=" << fail_after_;
    }
}

MockRdmaTransport::~MockRdmaTransport() {
    if (metadata_) {
        metadata_->removeSegmentDesc(local_server_name_);
    }
}

bool MockRdmaTransport::shouldInjectFailure() {
    int count = submit_count_.fetch_add(1, std::memory_order_relaxed);

    // Deterministic count-based failure takes precedence
    if (fail_after_ >= 0 && count >= fail_after_) {
        return true;
    }

    // Rate-based probabilistic failure
    if (fail_rate_ > 0.0) {
        thread_local std::mt19937 rng(std::random_device{}());
        thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(rng) < fail_rate_;
    }

    return false;
}

int MockRdmaTransport::install(std::string& local_server_name,
                                std::shared_ptr<TransferMetadata> meta,
                                std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;

    // Register as "rdma" protocol using the metadata layer directly.
    // This mock transport uses TCP sockets internally for data transfer
    // but advertises itself as "rdma" to the rest of the system.
    auto desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name;
#ifdef ENABLE_MULTI_PROTOCOL
    desc->protocol = "rdma";
#else
    desc->protocol = "rdma";
#endif
    if (topo) {
        desc->topology = *topo;
    }
    desc->tcp_data_port = 0;  // Will be set after finding a port

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name,
                               std::move(desc));

    // Start the handshake daemon (minimal - just for segment discovery)
    int ret = metadata_->startHandshakeDaemon(
        nullptr,  // No RDMA connection setup callback
        metadata_->localRpcMeta().rpc_port,
        metadata_->localRpcMeta().sockfd);

    if (ret) {
        LOG(ERROR) << "[MockRdmaTransport] Failed to start handshake daemon";
        return ret;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "[MockRdmaTransport] Failed to publish segments";
        return ret;
    }

    LOG(INFO) << "[MockRdmaTransport] Installed successfully (mock RDMA over TCP)";
    return 0;
}

int MockRdmaTransport::registerLocalMemory(void* addr, size_t length,
                                            const std::string& location,
                                            bool remote_accessible,
                                            bool update_metadata) {
    // Store buffer metadata directly (no ibverbs registration needed)
    BufferDesc buffer_desc;
    buffer_desc.name = local_server_name_;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;
#ifdef ENABLE_MULTI_PROTOCOL
    buffer_desc.protocol = "rdma";
#endif
    return metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
}

int MockRdmaTransport::unregisterLocalMemory(void* addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int MockRdmaTransport::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list,
    const std::string& location) {
    for (auto& buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int MockRdmaTransport::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    for (auto& addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

Status MockRdmaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    // Check for fault injection
    if (shouldInjectFailure()) {
        LOG(WARNING) << "[MockRdmaTransport] Injected failure for submitTransfer";
        return Status::NotSupportedTransport(
            "[MockRdmaTransport] Injected RDMA failure for testing");
    }

    auto& batch_desc = *((BatchDesc*)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::TooManyRequests(
            "[MockRdmaTransport] Exceed batch capacity");
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    // For mock RDMA: create tasks and immediately mark them as completed
    // (in real usage, data would flow via TCP sockets or memcpy)
    for (auto& request : entries) {
        auto& task = batch_desc.task_list[task_id];
        task.batch_id = batch_id;
        task.request = &request;
        task.total_bytes = request.length;
        task.slice_count = 0;
        task.success_slice_count = 0;
        task.failed_slice_count = 0;
        task.transferred_bytes = 0;
        task.is_finished = true;
        ++task_id;
    }
    return Status::OK();
}

Status MockRdmaTransport::submitTransferTask(
    const std::vector<TransferTask*>& task_list) {
    // Check for fault injection
    if (shouldInjectFailure()) {
        LOG(WARNING) << "[MockRdmaTransport] Injected failure for submitTransferTask";
        return Status::NotSupportedTransport(
            "[MockRdmaTransport] Injected RDMA failure for testing");
    }

    // Mark all tasks as completed immediately (mock transfer)
    for (auto* task : task_list) {
        if (task && task->request) {
            __atomic_store_n(&task->transferred_bytes,
                           task->request->length, __ATOMIC_RELAXED);
            __atomic_store_n(&task->success_slice_count,
                           task->slice_count, __ATOMIC_RELAXED);
            __atomic_store_n(&task->is_finished, true, __ATOMIC_RELAXED);
        }
    }
    return Status::OK();
}

Status MockRdmaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                             TransferStatus& status) {
    auto& batch_desc = *((BatchDesc*)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "[MockRdmaTransport] Invalid task ID");
    }
    auto& task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    if (task.is_finished) {
        if (task.failed_slice_count) {
            status.s = Transport::TransferStatusEnum::FAILED;
        } else {
            status.s = Transport::TransferStatusEnum::COMPLETED;
        }
    } else {
        status.s = Transport::TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

}  // namespace mooncake
