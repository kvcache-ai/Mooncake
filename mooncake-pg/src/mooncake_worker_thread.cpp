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
                        size_t task_id = 0;
                        for (int j = 0; j < group->size; ++j) {
                            if (!group->activeRanks[j]) {
                                continue;
                            }
                            group->engine->getTransferStatus(task.batchID,
                                                             task_id, status);
                            ++task_id;
                            if (group->activeRanks[j] &&
                                status.s != TransferStatusEnum::COMPLETED) {
                                if (status.s == TransferStatusEnum::FAILED ||
                                    (diff.count() > kPingTimeoutMicroseconds_ &&
                                     group->engine->sendNotifyByID(
                                         group->segmentIDs[j], msg))) {
                                    LOG(ERROR)
                                        << "Rank " << group->rank
                                        << " marking peer " << j
                                        << " as broken during transferring op "
                                        << (int)task.opType;
                                    group->store->deleteKey(
                                        "buffer_" +
                                        std::to_string(group->backendIndex) +
                                        "_" + std::to_string(j));
                                    group->store->deleteKey(
                                        "server_name_" +
                                        std::to_string(group->backendIndex) +
                                        "_" + std::to_string(j));
                                    group->store->deleteKey(
                                        "extension_task_count_" +
                                        std::to_string(group->backendIndex) +
                                        "_" + std::to_string(j));
                                    group->activeRanks[j] = false;
                                    group->peerConnected[j] = false;
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
                    auto source_ptr = (int32_t*)group->segmentInfos[group->rank]
                                          .send_sync[task.bufferOffset];

                    std::vector<TransferRequest> entries;
                    for (int j = 0; j < group->size; ++j) {
                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        *source_ptr = 1;
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
                    bool all_received = true;
                    auto signal_ptr = (int32_t*)group->segmentInfos[group->rank]
                                          .recv_sync[task.bufferOffset];

                    auto now = clock::now();
                    auto diff =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            now - activeTime[i]);
                    for (int j = 0; j < group->size; ++j) {
                        if (group->activeRanks[j] && signal_ptr[j] != 1) {
                            if (diff.count() > kPingTimeoutMicroseconds_ &&
                                group->engine->sendNotifyByID(
                                    group->segmentIDs[j], msg)) {
                                LOG(ERROR) << "Rank " << group->rank
                                           << " marking peer " << j
                                           << " as broken during syncing op "
                                           << (int)task.opType;
                                group->store->deleteKey(
                                    "buffer_" +
                                    std::to_string(group->backendIndex) + "_" +
                                    std::to_string(j));
                                group->store->deleteKey(
                                    "server_name_" +
                                    std::to_string(group->backendIndex) + "_" +
                                    std::to_string(j));
                                group->store->deleteKey(
                                    "extension_task_count_" +
                                    std::to_string(group->backendIndex) + "_" +
                                    std::to_string(j));
                                group->activeRanks[j] = false;
                                group->peerConnected[j] = false;
                            } else {
                                all_received = false;
                                break;
                            }
                        }
                    }
                    if (diff.count() > kPingTimeoutMicroseconds_) {
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
