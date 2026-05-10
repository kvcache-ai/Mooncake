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

#include "tent/transport/tcp/tcp_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <thread>

#include "tent/common/status.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {
static std::string ExtractHost(const std::string &endpoint) {
    if (!endpoint.empty() && endpoint.front() == '[') {
        auto end = endpoint.find(']');
        if (end != std::string::npos) {
            return endpoint.substr(1, end - 1);
        }
    }
    auto first_colon = endpoint.find(':');
    if (first_colon != std::string::npos) {
        if (endpoint.find(':', first_colon + 1) != std::string::npos) {
            return endpoint;  // Likely an IPv6 literal without port.
        }
        return endpoint.substr(0, first_colon);
    }
    return endpoint;
}

static bool IsLoopbackEndpoint(const std::string &endpoint) {
    const std::string host = ExtractHost(endpoint);
    static const char kLoopbackV4Prefix[] = "127.";
    return host.compare(0, sizeof(kLoopbackV4Prefix) - 1, kLoopbackV4Prefix) ==
               0 ||
           host == "localhost" || host == "::1";
}

TcpTransport::TcpTransport() : installed_(false) {}

TcpTransport::~TcpTransport() { uninstall(); }

Status TcpTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    if (installed_) {
        return Status::InvalidArgument(
            "TCP transport has been installed" LOC_MARK);
    }

    metadata_ = metadata;
    local_segment_name_ = local_segment_name;
    local_topology_ = local_topology;

    if (conf) {
        params_.max_retry_count = conf->get("transports/tcp/max_retry_count",
                                            params_.max_retry_count);
        params_.retry_base_delay_ms = conf->get(
            "transports/tcp/retry_base_delay_ms", params_.retry_base_delay_ms);
        params_.retry_max_delay_ms = conf->get(
            "transports/tcp/retry_max_delay_ms", params_.retry_max_delay_ms);
        params_.max_concurrent_tasks =
            conf->get("transports/tcp/max_concurrent_tasks",
                      params_.max_concurrent_tasks);
    }

    thread_pool_ = std::make_unique<ThreadPool>(params_.max_concurrent_tasks);

    installed_ = true;
    metadata_->setNotifyCallback([&](const Notification &message) -> int {
        RWSpinlock::WriteGuard guard(notify_lock_);
        notify_list_.push_back(message);
        return 0;
    });
    caps.dram_to_dram = true;
    if (Platform::getLoader().type() == "cuda" ||
        Platform::getLoader().type() == "cann") {
        caps.dram_to_gpu = true;
        caps.gpu_to_dram = true;
        caps.gpu_to_gpu = true;
    }

    LOG(INFO) << "TCP transport installed: max_retry_count="
              << params_.max_retry_count
              << " max_concurrent_tasks=" << params_.max_concurrent_tasks;
    return Status::OK();
}

Status TcpTransport::uninstall() {
    if (installed_) {
        if (metadata_) metadata_->setNotifyCallback(nullptr);
        shutting_down_.store(true, std::memory_order_release);
        thread_pool_.reset();
        metadata_.reset();
        installed_ = false;
    }
    return Status::OK();
}

Status TcpTransport::allocateSubBatch(SubBatchRef &batch, size_t max_size) {
    auto tcp_batch = Slab<TcpSubBatch>::Get().allocate();
    if (!tcp_batch)
        return Status::InternalError("Unable to allocate TCP sub-batch");
    batch = tcp_batch;
    tcp_batch->task_list.reserve(max_size);
    tcp_batch->max_size = max_size;
    return Status::OK();
}

Status TcpTransport::freeSubBatch(SubBatchRef &batch) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    Slab<TcpSubBatch>::Get().deallocate(tcp_batch);
    batch = nullptr;
    return Status::OK();
}

Status TcpTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request> &request_list) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (!tcp_batch)
        return Status::InvalidArgument("Invalid TCP sub-batch" LOC_MARK);
    if (request_list.size() + tcp_batch->task_list.size() > tcp_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    // Emplace all tasks first, then dispatch — pointers are stable because
    // task_list was reserved to max_size in allocateSubBatch.
    size_t start_idx = tcp_batch->task_list.size();
    for (auto &request : request_list) {
        tcp_batch->task_list.emplace_back();
        auto &task = tcp_batch->task_list.back();
        task.request = request;
        task.status_word.store(TransferStatusEnum::PENDING,
                               std::memory_order_release);
    }

    for (size_t i = start_idx; i < tcp_batch->task_list.size(); ++i) {
        TcpTask *task_ptr = &tcp_batch->task_list[i];
        thread_pool_->enqueue([this, task_ptr]() { startTransfer(task_ptr); });
    }
    return Status::OK();
}

Status TcpTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus &status) {
    auto tcp_batch = dynamic_cast<TcpSubBatch *>(batch);
    if (task_id < 0 || task_id >= (int)tcp_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid task id" LOC_MARK);
    }
    auto &task = tcp_batch->task_list[task_id];
    status.s = task.status_word.load(std::memory_order_acquire);
    status.transferred_bytes =
        task.transferred_bytes.load(std::memory_order_acquire);
    return Status::OK();
}

Status TcpTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    desc.transports.push_back(TransportType::TCP);
    return Status::OK();
}

Status TcpTransport::removeMemoryBuffer(BufferDesc &desc) {
    return Status::OK();
}

void TcpTransport::startTransfer(TcpTask *task) {
    if (task->request.target_id == LOCAL_SEGMENT_ID &&
        IsLoopbackEndpoint(local_segment_name_)) {
        LOG_FIRST_N(WARNING, 1)
            << "TCP transfer targets LOCAL_SEGMENT_ID on loopback endpoint "
            << local_segment_name_
            << ". When running multiple store instances on the same host with "
               "MC_STORE_MEMCPY=0, TCP local transfers will fail. Enable "
               "MC_STORE_MEMCPY or SHM, or use a non-loopback address.";
    }

    auto status = doTransferWithRetry(task);
    if (status.ok()) {
        // Store bytes before status: a reader who acquires COMPLETED will
        // also see the final transferred_bytes value.
        task->transferred_bytes.store(task->request.length,
                                      std::memory_order_release);
        task->status_word.store(TransferStatusEnum::COMPLETED,
                                std::memory_order_release);
    } else {
        LOG(WARNING) << "TCP transfer failed after " << params_.max_retry_count
                     << " retries: " << status.ToString();
        task->status_word.store(TransferStatusEnum::FAILED,
                                std::memory_order_release);
    }
}

Status TcpTransport::doTransferWithRetry(TcpTask *task) {
    std::string rpc_server_addr;
    auto status =
        findRemoteSegment(task->request.target_offset, task->request.length,
                          task->request.target_id, rpc_server_addr);
    if (!status.ok()) return status;

    Status last_error;
    uint64_t delay_ms = params_.retry_base_delay_ms;

    for (size_t attempt = 0; attempt <= params_.max_retry_count; ++attempt) {
        if (shutting_down_.load(std::memory_order_acquire))
            return Status::InternalError("Transport shutting down");

        if (attempt > 0) {
            LOG(INFO) << "TCP transfer retry attempt " << attempt << "/"
                      << params_.max_retry_count << ", backoff " << delay_ms
                      << "ms";
            // Sleep in small increments so shutdown is not delayed
            for (uint64_t i = 0; i < delay_ms; i += 100) {
                if (shutting_down_.load(std::memory_order_acquire))
                    return Status::InternalError("Transport shutting down");
                std::this_thread::sleep_for(std::chrono::milliseconds(
                    std::min(static_cast<uint64_t>(100), delay_ms - i)));
            }
            delay_ms = std::min(delay_ms * 2, params_.retry_max_delay_ms);
        }

        if (task->request.opcode == Request::WRITE) {
            status = ControlClient::sendData(
                rpc_server_addr, task->request.target_offset,
                task->request.source, task->request.length);
        } else {
            status = ControlClient::recvData(
                rpc_server_addr, task->request.target_offset,
                task->request.source, task->request.length);
        }

        if (status.ok()) return Status::OK();

        last_error = status;
        LOG(WARNING) << "TCP transfer attempt " << attempt
                     << " failed: " << status.ToString();

        if (!status.IsRpcServiceError() && !status.IsInternalError()) {
            return status;
        }

        // Peer may have restarted with a new address; re-resolve before retry
        if (status.IsRpcServiceError()) {
            rpc_server_addr.clear();
            auto resolve = findRemoteSegment(
                task->request.target_offset, task->request.length,
                task->request.target_id, rpc_server_addr);
            if (!resolve.ok()) return resolve;
        }
    }

    return last_error;
}

Status TcpTransport::findRemoteSegment(uint64_t dest_addr, uint64_t length,
                                       uint64_t target_id,
                                       std::string &rpc_server_addr) {
    return metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc *segment) {
            auto buffer = segment->findBuffer(dest_addr, length);
            rpc_server_addr = segment->rpc_server_addr;
            if (!buffer) {
                return Status::NeedsRefreshCache(
                    "Requested address is not in registered buffer" LOC_MARK);
            }
            if (rpc_server_addr.empty()) {
                return Status::NeedsRefreshCache(
                    "Empty RPC server addr" LOC_MARK);
            }
            return Status::OK();
        });
}

Status TcpTransport::sendNotification(SegmentID target_id,
                                      const Notification &message) {
    return metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc *segment) {
            auto rpc_server_addr = segment->rpc_server_addr;
            if (rpc_server_addr.empty()) {
                return Status::NeedsRefreshCache(
                    "Empty RPC server addr" LOC_MARK);
            }
            auto status = ControlClient::notify(rpc_server_addr, message);
            if (status.IsRpcServiceError()) {
                // Perhaps rpc_server_addr can be updated in the future
                return Status::NeedsRefreshCache(
                    "RPC service error: " + std::string{status.message()} +
                    LOC_MARK);
            }
            return status;
        });
}

Status TcpTransport::receiveNotification(
    std::vector<Notification> &notify_list) {
    RWSpinlock::WriteGuard guard(notify_lock_);
    notify_list.clear();
    notify_list.swap(notify_list_);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
