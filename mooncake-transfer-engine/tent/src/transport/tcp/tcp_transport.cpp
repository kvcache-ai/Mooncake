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
#include <optional>
#include <thread>

#include "tent/common/status.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {
namespace {

Status DecodeTcpIdentity(const std::string &process_attr,
                         const std::string &buffer_attr,
                         TcpDataPlaneIdentity *identity) {
    if (!decodeTcpProcessAttr(process_attr, &identity->process) ||
        !decodeTcpRegistrationAttr(buffer_attr, &identity->registration_id)) {
        return Status::NeedsRefreshCache(
            "Malformed TCP data-plane metadata" LOC_MARK);
    }
    return Status::OK();
}

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

}  // namespace

TcpTransport::TcpTransport() : installed_(false) {}

TcpTransport::~TcpTransport() {
    auto status = uninstallImpl(true);
    if (!status.ok()) {
        LOG(ERROR) << "Forced TCP transport teardown failed: "
                   << status.ToString();
    }
}

Status TcpTransport::install(std::string &local_segment_name,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology> local_topology,
                             std::shared_ptr<Config> conf) {
    std::lock_guard<std::mutex> registration_lock(registration_mutex_);
    if (installed_) {
        return Status::InvalidArgument(
            "TCP transport has been installed" LOC_MARK);
    }
    if (!metadata) {
        return Status::InvalidArgument("TCP metadata is null" LOC_MARK);
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
        params_.allow_unsafe_legacy_data_rpc =
            conf->get("transports/tcp/allow_unsafe_legacy_data_rpc",
                      params_.allow_unsafe_legacy_data_rpc);
    }

    if (params_.max_concurrent_tasks == 0) {
        metadata_.reset();
        return Status::InvalidArgument(
            "transports/tcp/max_concurrent_tasks must be greater than "
            "0" LOC_MARK);
    }
    Status reopened = metadata_->tcpBufferRegistry().reopen();
    if (!reopened.ok()) {
        metadata_.reset();
        return reopened;
    }

    std::optional<std::string> previous_attr;
    auto local = metadata_->segmentManager().getLocal();
    if (!local || local->type != SegmentType::Memory) {
        metadata_->tcpBufferRegistry().close();
        metadata_.reset();
        return Status::InvalidMetadataType(
            "TCP requires a local memory segment" LOC_MARK);
    }
    const auto &attrs = local->getMemory().transport_attrs;
    auto previous = attrs.find(static_cast<int>(TransportType::TCP));
    if (previous != attrs.end()) previous_attr = previous->second;

    const std::string process_attr =
        encodeTcpProcessAttr(metadata_->tcpProcessIdentity());
    Status published = metadata_->segmentManager().updateLocal(
        [&](SegmentDesc &desc) -> Status {
            if (desc.type != SegmentType::Memory) {
                return Status::InvalidMetadataType(
                    "TCP local segment changed type" LOC_MARK);
            }
            std::get<MemorySegmentDesc>(desc.detail)
                .transport_attrs[static_cast<int>(TransportType::TCP)] =
                process_attr;
            return Status::OK();
        });
    if (published.ok()) {
        published = metadata_->segmentManager().synchronizeLocal();
    }
    if (!published.ok()) {
        Status rollback = metadata_->segmentManager().updateLocal(
            [&](SegmentDesc &desc) -> Status {
                auto &current =
                    std::get<MemorySegmentDesc>(desc.detail).transport_attrs;
                if (previous_attr) {
                    current[static_cast<int>(TransportType::TCP)] =
                        *previous_attr;
                } else {
                    current.erase(static_cast<int>(TransportType::TCP));
                }
                return Status::OK();
            });
        if (rollback.ok()) {
            rollback = metadata_->segmentManager().synchronizeLocal();
        }
        if (!rollback.ok()) {
            LOG(ERROR) << "TCP metadata rollback failed: "
                       << rollback.ToString();
        }
        metadata_->tcpBufferRegistry().close();
        metadata_.reset();
        return published;
    }

    metadata_->setUnsafeLegacyTcpDataRpcEnabled(
        params_.allow_unsafe_legacy_data_rpc);

    shutting_down_.store(false, std::memory_order_release);
    thread_pool_ = std::make_unique<ThreadPool>(params_.max_concurrent_tasks);

    installed_ = true;
    metadata_->setNotifyCallback([&](const Notification &message) -> int {
        RWSpinlock::WriteGuard guard(notify_lock_);
        notify_list_.push_back(message);
        return 0;
    });
    caps.dram_to_dram = true;
    // The peer's RPC handler stages its GPU memory for both READ and WRITE.
    // Only access to a local GPU buffer requires a local device platform.
    caps.dram_to_gpu = true;
    if (Platform::getLoader().type() != "cpu") {
        caps.gpu_to_dram = true;
        caps.gpu_to_gpu = true;
    }

    LOG(INFO) << "TCP transport installed: max_retry_count="
              << params_.max_retry_count
              << " max_concurrent_tasks=" << params_.max_concurrent_tasks
              << " allow_unsafe_legacy_data_rpc="
              << params_.allow_unsafe_legacy_data_rpc;
    return Status::OK();
}

Status TcpTransport::uninstall() { return uninstallImpl(false); }

Status TcpTransport::uninstallImpl(bool force) {
    std::lock_guard<std::mutex> registration_lock(registration_mutex_);
    Status result = Status::OK();
    if (installed_) {
        if (metadata_) {
            auto &registry = metadata_->tcpBufferRegistry();
            if (!force && !registry.empty()) {
                return Status::InvalidArgument(
                    "Cannot uninstall TCP transport with registered "
                    "buffers" LOC_MARK);
            }
            metadata_->setUnsafeLegacyTcpDataRpcEnabled(false);
            registry.close(force);
            metadata_->setNotifyCallback(nullptr);
        }
        quiesce();
        if (metadata_) {
            Status removed = metadata_->segmentManager().updateLocal(
                [](SegmentDesc &desc) -> Status {
                    if (desc.type != SegmentType::Memory) {
                        return Status::InvalidMetadataType(
                            "TCP local segment changed type" LOC_MARK);
                    }
                    std::get<MemorySegmentDesc>(desc.detail)
                        .transport_attrs.erase(
                            static_cast<int>(TransportType::TCP));
                    return Status::OK();
                });
            if (removed.ok()) {
                removed = metadata_->segmentManager().synchronizeLocal();
            }
            if (!removed.ok()) result = removed;
        }
        metadata_.reset();
        installed_ = false;
    }
    return result;
}

Status TcpTransport::quiesce() {
    std::unique_ptr<ThreadPool> thread_pool;
    {
        std::lock_guard<std::mutex> guard(lifecycle_mutex_);
        shutting_down_.store(true, std::memory_order_release);
        thread_pool = std::move(thread_pool_);
    }
    // Join outside the admission lock so completion callbacks can return.
    thread_pool.reset();
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
    std::lock_guard<std::mutex> guard(lifecycle_mutex_);
    if (shutting_down_.load(std::memory_order_acquire)) {
        return Status::InternalError("TCP transport is shutting down" LOC_MARK);
    }
    if (request_list.size() + tcp_batch->task_list.size() > tcp_batch->max_size)
        return Status::TooManyRequests("Exceed batch capacity" LOC_MARK);

    // Emplace all tasks first, then dispatch — pointers are stable because
    // task_list was reserved to max_size in allocateSubBatch.
    size_t start_idx = tcp_batch->task_list.size();
    for (auto &request : request_list) {
        tcp_batch->task_list.emplace_back();
        auto &task = tcp_batch->task_list.back();
        task.request = request;
        task.progress_batch_id = batch->progress_batch_id;
        task.notify_progress = batch->notify_progress;
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
    if (status.s == TransferStatusEnum::FAILED && task.non_replayable_failure)
        return Status::RpcServiceError("TCP WRITE outcome is unknown" LOC_MARK);
    return Status::OK();
}

Status TcpTransport::addMemoryBuffer(BufferDesc &desc,
                                     const MemoryOptions &options) {
    std::lock_guard<std::mutex> registration_lock(registration_mutex_);
    if (!metadata_) {
        return Status::InternalError("TCP transport is not installed" LOC_MARK);
    }
    uint64_t registration_id = 0;
    CHECK_STATUS(metadata_->tcpBufferRegistry().add(
        desc.addr, desc.length, options.perm, &registration_id));
    try {
        desc.transport_attrs[TransportType::TCP] =
            encodeTcpRegistrationAttr(registration_id);
        if (std::find(desc.transports.begin(), desc.transports.end(),
                      TransportType::TCP) == desc.transports.end()) {
            desc.transports.push_back(TransportType::TCP);
        }
    } catch (...) {
        (void)metadata_->tcpBufferRegistry().remove(registration_id);
        desc.transport_attrs.erase(TransportType::TCP);
        desc.transports.erase(
            std::remove(desc.transports.begin(), desc.transports.end(),
                        TransportType::TCP),
            desc.transports.end());
        return Status::InternalError(
            "Unable to publish TCP buffer registration" LOC_MARK);
    }
    return Status::OK();
}

Status TcpTransport::removeMemoryBuffer(BufferDesc &desc) {
    std::lock_guard<std::mutex> registration_lock(registration_mutex_);
    auto attr = desc.transport_attrs.find(TransportType::TCP);
    if (attr == desc.transport_attrs.end()) return Status::OK();
    if (!metadata_) {
        return Status::InternalError("TCP transport is not installed" LOC_MARK);
    }
    uint64_t registration_id = 0;
    if (!decodeTcpRegistrationAttr(attr->second, &registration_id)) {
        return Status::InvalidArgument(
            "Malformed TCP buffer metadata" LOC_MARK);
    }
    CHECK_STATUS(metadata_->tcpBufferRegistry().remove(registration_id));
    desc.transport_attrs.erase(attr);
    desc.transports.erase(
        std::remove(desc.transports.begin(), desc.transports.end(),
                    TransportType::TCP),
        desc.transports.end());
    return Status::OK();
}

void TcpTransport::startTransfer(TcpTask *task) {
    // Terminal publication allows freeBatch() to destroy task immediately.
    // The worker must own the callback and keep no task accesses after it.
    auto notify_progress = std::move(task->notify_progress);
    const auto progress_batch_id = task->progress_batch_id;
    bool completed = false;
    try {
        if (task->request.target_id == LOCAL_SEGMENT_ID &&
            IsLoopbackEndpoint(local_segment_name_)) {
            LOG_FIRST_N(WARNING, 1)
                << "TCP transfer targets LOCAL_SEGMENT_ID on loopback endpoint "
                << local_segment_name_
                << ". When running multiple store instances on the same host "
                   "with "
                   "MC_STORE_MEMCPY=0, TCP local transfers will fail. Enable "
                   "MC_STORE_MEMCPY or SHM, or use a non-loopback address.";
        }
        auto status = doTransferWithRetry(task);
        completed = status.ok();
        if (!completed) {
            VLOG(1) << "TCP transfer failed: " << status.ToString();
            LOG_EVERY_N(WARNING, 100)
                << "TCP transfer failed: " << status.ToString();
        }
    } catch (const std::exception &error) {
        // enqueue() stores exceptions in a future which the submit path does
        // not consume. Convert failures here while task is still owned.
        LOG(WARNING) << "TCP worker exception: " << error.what();
    } catch (...) {
        LOG(WARNING) << "TCP worker exception: unknown exception";
    }
    if (completed) {
        // Store bytes before status: a reader who acquires COMPLETED will
        // also see the final transferred_bytes value.
        task->transferred_bytes.store(task->request.length,
                                      std::memory_order_release);
        task->status_word.store(TransferStatusEnum::COMPLETED,
                                std::memory_order_release);
    } else {
        task->status_word.store(TransferStatusEnum::FAILED,
                                std::memory_order_release);
    }
    if (notify_progress) notify_progress(progress_batch_id);
}

Status TcpTransport::doTransferWithRetry(TcpTask *task) {
    if (shutting_down_.load(std::memory_order_acquire))
        return Status::InternalError("Transport shutting down");

    std::string rpc_server_addr;
    std::optional<TcpDataPlaneIdentity> identity;
    auto status =
        findRemoteSegment(task->request.target_offset, task->request.length,
                          task->request.target_id, rpc_server_addr, identity);
    if (status.IsAddressNotRegistered()) {
        CHECK_STATUS(metadata_->segmentManager().invalidateRemote(
            task->request.target_id));
        status = findRemoteSegment(
            task->request.target_offset, task->request.length,
            task->request.target_id, rpc_server_addr, identity);
    }
    if (!status.ok()) return status;

    Status last_error;
    uint64_t delay_ms = params_.retry_base_delay_ms;

    for (size_t attempt = 0; attempt <= params_.max_retry_count; ++attempt) {
        if (shutting_down_.load(std::memory_order_acquire))
            return Status::InternalError("Transport shutting down");

        if (attempt > 0) {
            VLOG(1) << "TCP transfer retry attempt " << attempt << "/"
                    << params_.max_retry_count << ", backoff " << delay_ms
                    << "ms";
            LOG_EVERY_N(INFO, 100)
                << "TCP transfer retry attempt " << attempt << "/"
                << params_.max_retry_count << ", backoff " << delay_ms << "ms";
            // Sleep in small increments so shutdown is not delayed
            for (uint64_t i = 0; i < delay_ms; i += 100) {
                if (shutting_down_.load(std::memory_order_acquire))
                    return Status::InternalError("Transport shutting down");
                std::this_thread::sleep_for(std::chrono::milliseconds(
                    std::min(static_cast<uint64_t>(100), delay_ms - i)));
            }
            delay_ms = nextTcpRetryDelay(delay_ms, params_.retry_max_delay_ms);
        }

        if (task->request.opcode == Request::WRITE) {
            status =
                identity
                    ? ControlClient::sendDataV2(
                          rpc_server_addr, task->request.target_offset,
                          task->request.source, task->request.length, *identity)
                    : ControlClient::sendData(
                          rpc_server_addr, task->request.target_offset,
                          task->request.source, task->request.length);
        } else {
            status =
                identity
                    ? ControlClient::recvDataV2(
                          rpc_server_addr, task->request.target_offset,
                          task->request.source, task->request.length, *identity)
                    : ControlClient::recvData(
                          rpc_server_addr, task->request.target_offset,
                          task->request.source, task->request.length);
        }

        if (status.ok()) return Status::OK();

        last_error = status;
        VLOG(1) << "TCP transfer attempt " << attempt
                << " failed: " << status.ToString();
        LOG_EVERY_N(WARNING, 1000) << "TCP transfer attempt " << attempt
                                   << " failed: " << status.ToString();

        if (status.IsNeedsRefreshCache()) {
            CHECK_STATUS(metadata_->segmentManager().invalidateRemote(
                task->request.target_id));
            if (attempt == params_.max_retry_count) break;
            auto resolve = findRemoteSegment(
                task->request.target_offset, task->request.length,
                task->request.target_id, rpc_server_addr, identity);
            if (!resolve.ok()) return resolve;
            continue;
        }

        if (task->request.opcode == Request::WRITE &&
            status.IsRpcServiceError() && !status.IsRpcConnectionError()) {
            task->non_replayable_failure = true;
            return status;
        }

        if (!status.IsRpcServiceError() && !status.IsInternalError()) {
            return status;
        }

        // Only a pre-send connection failure permits an endpoint refresh
        // followed by automatic WRITE replay.
        if (status.IsRpcConnectionError()) {
            CHECK_STATUS(metadata_->segmentManager().invalidateRemote(
                task->request.target_id));
            if (attempt == params_.max_retry_count) break;
            auto resolve = findRemoteSegment(
                task->request.target_offset, task->request.length,
                task->request.target_id, rpc_server_addr, identity);
            if (!resolve.ok()) return resolve;
        }
    }

    return last_error;
}

Status TcpTransport::findRemoteSegment(
    uint64_t dest_addr, uint64_t length, uint64_t target_id,
    std::string &rpc_server_addr,
    std::optional<TcpDataPlaneIdentity> &identity) {
    return metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc *segment) {
            if (segment == nullptr || segment->type != SegmentType::Memory) {
                return Status::NeedsRefreshCache(
                    "TCP target is not a memory segment" LOC_MARK);
            }
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
            const auto process = segment->getMemory().transport_attrs.find(
                static_cast<int>(TransportType::TCP));
            const auto registration =
                buffer->transport_attrs.find(TransportType::TCP);
            const bool has_process =
                process != segment->getMemory().transport_attrs.end();
            const bool has_registration =
                registration != buffer->transport_attrs.end();
            if (!has_registration) {
                identity.reset();
                if (has_process) {
                    return Status::AddressNotRegistered(
                        "TCP target buffer is not registered for TCP" LOC_MARK);
                }
                return Status::OK();
            }
            if (!has_process) {
                return Status::NeedsRefreshCache(
                    "Incomplete TCP data-plane metadata" LOC_MARK);
            }
            TcpDataPlaneIdentity decoded;
            CHECK_STATUS(DecodeTcpIdentity(process->second,
                                           registration->second, &decoded));
            identity = decoded;
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
