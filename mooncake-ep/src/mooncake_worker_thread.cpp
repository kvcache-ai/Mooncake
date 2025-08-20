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

void MooncakeWorker::initWorker(const std::vector<std::string> &server_names) {
    // Sync metadata
    for (int i = 0; i < size_; ++i) {
        auto segment_id = engine_->openSegment(server_names[i]);
        segment_ids_.emplace_back(segment_id);
        auto segment_desc =
            engine_->getMetadata()->getSegmentDescByID(segment_id);
        segment_descs_.emplace_back(segment_desc);
    }

    // Start worker thread
    std::thread([this] {
        std::atomic<WorkerTaskStatus> task_status[kNumTasks_];
        int taskCount = 0;
        while (true) {
            _mm_pause();
            for (size_t i = 0; i < kNumTasks_; ++i) {
                auto &task = tasks_[i];
                if (!task.active) {
                    task_status[i].store(IDLE, std::memory_order_release);
                    continue;
                }

                bool skipTransfer = (task.opType == c10d::OpType::BROADCAST &&
                                     rank_ != task.broadcastRoot) ||
                                    task.opType == c10d::OpType::BARRIER;
                if (task_status[i].load(std::memory_order_acquire) == IDLE) {
                    if (skipTransfer) {
                        task_status[i].store(TRANSFERRED_1,
                                             std::memory_order_release);
                        continue;
                    }
                    std::vector<TransferRequest> entries;
                    for (int j = 0; j < size_; ++j) {
                        uint64_t source =
                            segment_descs_[rank_]->buffers[taskCount % 2].addr;
                        switch (task.opType) {
                            case c10d::OpType::BROADCAST:
                            case c10d::OpType::ALLREDUCE:
                            case c10d::OpType::ALLGATHER:
                            case c10d::OpType::_ALLGATHER_BASE:
                                break;
                            case c10d::OpType::ALLTOALL_BASE:
                            case c10d::OpType::ALLTOALL:
                                source += j * task.tensorSize;
                                break;
                            default:
                                break;
                        }
                        uint64_t target_offset =
                            segment_descs_[j]->buffers[2 + taskCount % 2].addr;
                        switch (task.opType) {
                            case c10d::OpType::BROADCAST:
                                break;
                            case c10d::OpType::ALLREDUCE:
                            case c10d::OpType::ALLGATHER:
                            case c10d::OpType::_ALLGATHER_BASE:
                            case c10d::OpType::ALLTOALL_BASE:
                            case c10d::OpType::ALLTOALL:
                                target_offset += rank_ * task.tensorSize;
                                break;
                            default:
                                break;
                        }
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void *)source,
                            .target_id = segment_ids_[j],
                            .target_offset = target_offset,
                            .length = task.tensorSize,
                        });
                    }
                    task.batchID = engine_->allocateBatchID(entries.size());
                    engine_->submitTransfer(task.batchID, entries);
                    task_status[i].store(TRANSFERRED_1,
                                         std::memory_order_release);
                } else if (task_status[i].load(std::memory_order_acquire) ==
                           TRANSFERRED_1) {
                    bool batch_done = true;
                    TransferStatus status;

                    if (!skipTransfer) {
                        for (int j = 0; j < size_; ++j) {
                            engine_->getTransferStatus(task.batchID, j, status);
                            if (status.s != TransferStatusEnum::COMPLETED) {
                                batch_done = false;
                                break;
                            }
                        }
                    }
                    if (!batch_done) {
                        continue;
                    }
                    auto source_ptr = (int32_t *)segment_descs_[rank_]
                                          ->buffers[4 + taskCount % 2]
                                          .addr;
                    std::vector<TransferRequest> entries;
                    for (int j = 0; j < size_; ++j) {
                        *source_ptr = 1;
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void *)source_ptr,
                            .target_id = segment_ids_[j],
                            .target_offset = segment_descs_[j]
                                                 ->buffers[6 + taskCount % 2]
                                                 .addr +
                                             rank_ * sizeof(int32_t),
                            .length = sizeof(int32_t),
                        });
                    }
                    task.batchID = engine_->allocateBatchID(entries.size());
                    engine_->submitTransfer(task.batchID, entries);
                    task_status[i].store(SIGNALED_1, std::memory_order_release);
                } else if (task_status[i].load(std::memory_order_acquire) ==
                           SIGNALED_1) {
                    bool all_received = true;
                    auto signal_ptr = (int32_t *)segment_descs_[rank_]
                                          ->buffers[6 + taskCount % 2]
                                          .addr;
                    for (int j = 0; j < size_; ++j) {
                        if (signal_ptr[j] != 1) {
                            all_received = false;
                            break;
                        }
                    }
                    if (all_received) {
                        for (int j = 0; j < size_; ++j) {
                            signal_ptr[j] = 0;
                        }
                        task_status[i].store(DONE, std::memory_order_release);
                        task.active = false;
                        ++taskCount;
                    }
                }
            }
        }
    }).detach();
}

}  // namespace mooncake