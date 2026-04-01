#include <cuda_runtime.h>
#include <thread>
#include <mooncake_worker.cuh>
#include <glog/logging.h>
#include <transfer_engine.h>

namespace mooncake {

enum WorkerTaskStatus {
    IDLE = 0,
    TRANSFERRED_1 = 1,
    SIGNALED_1 = 2,
    DONE = 3,
};

static constexpr size_t kInvalidTaskId = static_cast<size_t>(-1);

void MooncakeWorker::Start() {
    bool expected = false;
    if (started_.compare_exchange_strong(expected, true)) {
        startWorker();
    }
}

void MooncakeWorker::startWorker() {
    running_ = true;
    std::thread([this] {
        if (cuda_device_index_ >= 0) {
            cudaSetDevice(cuda_device_index_);
        }
        std::atomic<WorkerTaskStatus> task_status[kNumTasks_];
        using clock = std::chrono::high_resolution_clock;
        clock::time_point activeTime[kNumTasks_];
        size_t rankToTaskId[kNumTasks_][kMaxNumRanks];
        TransferMetadata::NotifyDesc msg{"ping", "ping"};
        while (running_) {
            PAUSE();
            for (size_t i = 0; i < kNumTasks_; ++i) {
                auto& task = tasks_[i];
                if (!task.active) {
                    task_status[i].store(IDLE, std::memory_order_release);
                    continue;
                }

                auto group = (TransferGroupMeta*)task.transferGroupMeta;
                bool skipTransfer = (task.opType == c10d::OpType::BROADCAST &&
                                     group->rank != task.broadcastRoot) ||
                                    (task.opType == c10d::OpType::SCATTER &&
                                     group->rank != task.broadcastRoot) ||
                                    task.opType == c10d::OpType::BARRIER;
                if (task_status[i].load(std::memory_order_acquire) == IDLE) {
                    if (skipTransfer) {
                        task_status[i].store(TRANSFERRED_1,
                                             std::memory_order_release);
                        continue;
                    }
                    for (size_t j = 0; j < kMaxNumRanks; ++j) {
                        rankToTaskId[i][j] = kInvalidTaskId;
                    }
                    std::vector<TransferRequest> entries;
                    for (int j = 0; j < group->size; ++j) {
                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        if ((task.opType == c10d::OpType::GATHER ||
                             task.opType == c10d::OpType::REDUCE) &&
                            j != task.broadcastRoot) {
                            continue;
                        }
                        uint64_t source = group->segmentInfos[group->rank]
                                              .send_buffer[task.bufferOffset];

                        switch (task.opType) {
                            case c10d::OpType::BROADCAST:
                            case c10d::OpType::ALLREDUCE:
                            case c10d::OpType::ALLGATHER:
                            case c10d::OpType::_ALLGATHER_BASE:
                            case c10d::OpType::REDUCE:
                            case c10d::OpType::GATHER:
                                break;
                            case c10d::OpType::ALLTOALL_BASE:
                            case c10d::OpType::ALLTOALL:
                            case c10d::OpType::_REDUCE_SCATTER_BASE:
                            case c10d::OpType::SCATTER:
                                source += j * task.tensorSize;
                                break;
                            default:
                                break;
                        }
                        uint64_t target_offset =
                            group->segmentInfos[j]
                                .recv_buffer[task.bufferOffset];

                        switch (task.opType) {
                            case c10d::OpType::BROADCAST:
                            case c10d::OpType::SCATTER:
                                break;
                            case c10d::OpType::ALLREDUCE:
                            case c10d::OpType::ALLGATHER:
                            case c10d::OpType::_ALLGATHER_BASE:
                            case c10d::OpType::ALLTOALL_BASE:
                            case c10d::OpType::ALLTOALL:
                            case c10d::OpType::_REDUCE_SCATTER_BASE:
                            case c10d::OpType::REDUCE:
                            case c10d::OpType::GATHER:
                                target_offset += group->rank * task.tensorSize;
                                break;

                            default:
                                break;
                        }
                        rankToTaskId[i][j] = entries.size();
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void*)source,
                            .target_id = group->segmentIDs[j],
                            .target_offset = target_offset,
                            .length = task.tensorSize,
                        });
                    }
                    task.batchID =
                        group->engine->allocateBatchID(entries.size());
                    group->engine->submitTransfer(task.batchID, entries);
                    activeTime[i] = clock::now();
                    task_status[i].store(TRANSFERRED_1,
                                         std::memory_order_release);
                } else if (task_status[i].load(std::memory_order_acquire) ==
                           TRANSFERRED_1) {
                    bool batch_done = true;
                    TransferStatus status;

                    if (!skipTransfer) {
                        auto now = clock::now();
                        auto diff = std::chrono::duration_cast<
                            std::chrono::microseconds>(now - activeTime[i]);
                        for (int j = 0; j < group->size; ++j) {
                            if (!group->activeRanks[j]) {
                                continue;
                            }
                            if (rankToTaskId[i][j] == kInvalidTaskId) {
                                continue;
                            }
                            group->engine->getTransferStatus(
                                task.batchID, rankToTaskId[i][j], status);
                            if (status.s != TransferStatusEnum::COMPLETED) {
                                if (status.s == TransferStatusEnum::FAILED ||
                                    (j != group->rank &&
                                     diff.count() > kPingTimeoutMicroseconds_ &&
                                     group->engine->sendNotifyByID(
                                         group->segmentIDs[j], msg))) {
                                    LOG(ERROR)
                                        << "Rank " << group->rank
                                        << " marking peer " << j
                                        << " as broken during transferring op "
                                        << (int)task.opType;

                                    // Set peerConnected to notify the
                                    // connection poller to reconnect it.
                                    group->peerConnected[j] = false;
                                    group->activeRanks[j] = false;
                                    group->activeRanksTensor[j] = 0;
                                } else {
                                    batch_done = false;
                                    break;
                                }
                            }
                        }
                    }

                    if (!batch_done) {
                        continue;
                    }

                    if (!skipTransfer) {
                        auto s = group->engine->freeBatchID(task.batchID);
                        if (!s.ok()) {
                            LOG(WARNING)
                                << "BatchID leaked due to freeBatchID "
                                   "failure (likely caused by a timeout): "
                                << s.message();
                        }
                    }

                    auto source_ptr = (int32_t*)group->segmentInfos[group->rank]
                                          .send_sync[task.bufferOffset];

                    for (size_t j = 0; j < kMaxNumRanks; ++j) {
                        rankToTaskId[i][j] = kInvalidTaskId;
                    }
                    std::vector<TransferRequest> entries;
                    for (int j = 0; j < group->size; ++j) {
                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        *source_ptr = 1;
                        rankToTaskId[i][j] = entries.size();
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void*)source_ptr,
                            .target_id = group->segmentIDs[j],
                            .target_offset = group->segmentInfos[j]
                                                 .recv_sync[task.bufferOffset] +
                                             group->rank * sizeof(int32_t),
                            .length = sizeof(int32_t),
                        });
                    }
                    task.batchID =
                        group->engine->allocateBatchID(entries.size());
                    group->engine->submitTransfer(task.batchID, entries);
                    activeTime[i] = clock::now();
                    task_status[i].store(SIGNALED_1, std::memory_order_release);
                } else if (task_status[i].load(std::memory_order_acquire) ==
                           SIGNALED_1) {
                    bool task_done = true;
                    auto signal_ptr = (int32_t*)group->segmentInfos[group->rank]
                                          .recv_sync[task.bufferOffset];

                    auto now = clock::now();
                    auto diff =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            now - activeTime[i]);

                    TransferStatus status;
                    for (int j = 0; j < group->size; ++j) {
                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        if (rankToTaskId[i][j] == kInvalidTaskId) {
                            continue;
                        }
                        group->engine->getTransferStatus(
                            task.batchID, rankToTaskId[i][j], status);
                        if (signal_ptr[j] != 1 ||
                            status.s != TransferStatusEnum::COMPLETED) {
                            if (status.s == TransferStatusEnum::FAILED ||
                                (j != group->rank &&
                                 diff.count() > kPingTimeoutMicroseconds_ &&
                                 group->engine->sendNotifyByID(
                                     group->segmentIDs[j], msg))) {
                                LOG(ERROR) << "Rank " << group->rank
                                           << " marking peer " << j
                                           << " as broken during syncing op "
                                           << (int)task.opType;

                                // Set peerConnected to notify the
                                // connection poller to reconnect it.
                                group->peerConnected[j] = false;
                                group->activeRanks[j] = false;
                                group->activeRanksTensor[j] = 0;
                            } else {
                                task_done = false;
                                break;
                            }
                        }
                    }
                    if (diff.count() > kPingTimeoutMicroseconds_) {
                        // reset timer
                        activeTime[i] = clock::now();
                    }
                    if (task_done) {
                        for (int j = 0; j < group->size; ++j) {
                            signal_ptr[j] = 0;
                        }
                        task_status[i].store(DONE, std::memory_order_release);
                        task.active = false;
                        if (hasCallback_[i]) {
                            // Move the callback to release the captured values
                            // immediately after execution.
                            auto callback = std::move(callbacks_[i]);
                            hasCallback_[i] = false;
                            callback();
                        }
                        auto s = group->engine->freeBatchID(task.batchID);
                        if (!s.ok()) {
                            LOG(WARNING)
                                << "BatchID leaked due to freeBatchID "
                                   "failure (likely caused by a timeout): "
                                << s.message();
                        }
                    }
                }
            }
        }
    }).detach();
}

std::shared_ptr<MooncakeWorker> MooncakeWorkerManager::GetWorker(
    int worker_id) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
        return it->second;
    }
    auto worker = std::make_shared<MooncakeWorker>(worker_id);
    workers_[worker_id] = worker;
    return worker;
}

std::shared_ptr<MooncakeWorker> MooncakeWorkerManager::GetCPUWorker() {
    return GetWorker(CPUWorkerID);
}

std::shared_ptr<MooncakeWorker> MooncakeWorkerManager::GetCUDAWorker(
    int cuda_device_index) {
    return GetWorker(cuda_device_index);
}

}  // namespace mooncake
