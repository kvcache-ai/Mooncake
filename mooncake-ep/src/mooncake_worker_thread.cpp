#include <thread>
#include <mooncake_worker.cuh>
#include <transfer_engine.h>

namespace mooncake {

enum WorkerTaskStatus {
    IDLE = 0,
    TRANSFERRED_1 = 1,
    SIGNALED_1 = 2,
    DONE = 3,
};

void MooncakeWorker::startWorker() {
    running_ = true;
    std::thread([this] {
        std::atomic<WorkerTaskStatus> task_status[kNumTasks_];
        using clock = std::chrono::high_resolution_clock;
        clock::time_point activeTime[kNumTasks_];
        while (running_) {
            _mm_pause();
            for (size_t i = 0; i < kNumTasks_; ++i) {
                auto &task = tasks_[i];
                if (!task.active) {
                    task_status[i].store(IDLE, std::memory_order_release);
                    continue;
                }

                auto group = (TransferGroupMeta *)task.transferGroupMeta;
                bool skipTransfer = (task.opType == c10d::OpType::BROADCAST &&
                                     group->rank != task.broadcastRoot) ||
                                    task.opType == c10d::OpType::BARRIER;
                if (task_status[i].load(std::memory_order_acquire) == IDLE) {
                    if (skipTransfer) {
                        task_status[i].store(TRANSFERRED_1,
                                             std::memory_order_release);
                        continue;
                    }
                    std::vector<TransferRequest> entries;
                    for (int j = 0; j < group->size; ++j) {
                        uint64_t source = group->segmentDescs[group->rank]
                                              ->buffers[task.bufferOffset]
                                              .addr;
                        switch (task.opType) {
                            case c10d::OpType::BROADCAST:
                            case c10d::OpType::ALLREDUCE:
                            case c10d::OpType::ALLGATHER:
                            case c10d::OpType::_ALLGATHER_BASE:
                                break;
                            case c10d::OpType::REDUCE_SCATTER:
                            case c10d::OpType::ALLTOALL_BASE:
                            case c10d::OpType::ALLTOALL:
                                source += j * task.tensorSize;
                                break;
                            default:
                                break;
                        }
                        uint64_t target_offset =
                            group->segmentDescs[j]
                                ->buffers[task.bufferOffset + 2]
                                .addr;
                        switch (task.opType) {
                            case c10d::OpType::BROADCAST:
                                break;
                            case c10d::OpType::ALLREDUCE:
                            case c10d::OpType::ALLGATHER:
                            case c10d::OpType::_ALLGATHER_BASE:
                            case c10d::OpType::REDUCE_SCATTER:
                            case c10d::OpType::ALLTOALL_BASE:
                            case c10d::OpType::ALLTOALL:
                                target_offset += group->rank * task.tensorSize;
                                break;
                            default:
                                break;
                        }
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void *)source,
                            .target_id = group->segmentIDs[j],
                            .target_offset = target_offset,
                            .length = task.tensorSize,
                        });
                    }
                    task.batchID =
                        group->engine->allocateBatchID(entries.size());
                    group->engine->submitTransfer(task.batchID, entries);
                    task_status[i].store(TRANSFERRED_1,
                                         std::memory_order_release);
                } else if (task_status[i].load(std::memory_order_acquire) ==
                           TRANSFERRED_1) {
                    bool batch_done = true;
                    TransferStatus status;

                    if (!skipTransfer) {
                        for (int j = 0; j < group->size; ++j) {
                            group->engine->getTransferStatus(task.batchID, j,
                                                             status);
                            if (group->activeRanks[j] &&
                                status.s != TransferStatusEnum::COMPLETED) {
                                if (status.s == TransferStatusEnum::FAILED) {
                                    LOG(ERROR)
                                        << "Rank " << group->rank
                                        << " marking peer " << j
                                        << " as broken during transferring op "
                                        << (int)task.opType;
                                    group->activeRanks[j] = false;
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
                    auto source_ptr =
                        (int32_t *)group->segmentDescs[group->rank]
                            ->buffers[task.bufferOffset + 4]
                            .addr;
                    std::vector<TransferRequest> entries;
                    for (int j = 0; j < group->size; ++j) {
                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        *source_ptr = 1;
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void *)source_ptr,
                            .target_id = group->segmentIDs[j],
                            .target_offset =
                                group->segmentDescs[j]
                                    ->buffers[task.bufferOffset + 6]
                                    .addr +
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
                    bool all_received = true;
                    auto signal_ptr =
                        (int32_t *)group->segmentDescs[group->rank]
                            ->buffers[task.bufferOffset + 6]
                            .addr;
                    auto now = clock::now();
                    auto diff =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            now - activeTime[i]);
                    for (int j = 0; j < group->size; ++j) {
                        if (group->activeRanks[j] && signal_ptr[j] != 1) {
                            TransferMetadata::NotifyDesc msg{"ping", "ping"};
                            if (diff.count() > 1 &&
                                group->engine->sendNotifyByName(
                                    group->segmentDescs[j]->name, msg)) {
                                LOG(ERROR) << "Rank " << group->rank
                                           << " marking peer " << j
                                           << " as broken during syncing op "
                                           << (int)task.opType;
                                group->activeRanks[j] = false;
                            } else {
                                all_received = false;
                                break;
                            }
                        }
                    }
                    if (diff.count() > 1) {
                        // reset timer
                        activeTime[i] = clock::now();
                    }
                    if (all_received) {
                        for (int j = 0; j < group->size; ++j) {
                            signal_ptr[j] = 0;
                        }
                        task_status[i].store(DONE, std::memory_order_release);
                        task.active = false;
                        if (hasCallback_[i]) {
                            callbacks_[i]();
                        }
                    }
                }
            }
        }
    }).detach();
}

}  // namespace mooncake