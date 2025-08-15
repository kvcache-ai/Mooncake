#include <thread>
#include <mooncake_worker.cuh>
#include <transfer_engine.h>

namespace mooncake {

enum WorkerTaskStatus {
    TASK_IDLE = 0,
    TASK_TRANSFERRED_1 = 1,
    TASK_SIGNALED_1 = 2,
    TASK_DONE = 3,
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

    uint64_t gpu_source = segment_descs_[rank_]->buffers[0].addr;
    uint64_t cpu_source = segment_descs_[rank_]->buffers[2].addr;

    // Start worker thread
    std::thread([this, gpu_source, cpu_source] {
        std::atomic<WorkerTaskStatus> task_status[kNumTasks_];
        while (true) {
            _mm_pause();
            for (size_t i = 0; i < kNumTasks_; ++i) {
                auto &task = tasks_[i];
                if (task.status == IDLE) {
                    task_status[i].store(TASK_IDLE, std::memory_order_release);
                } else if (task.status == READY) {
                    switch (task.opType) {
                        case c10d::OpType::ALLGATHER: {
                            if (task_status[i].load(
                                    std::memory_order_acquire) == TASK_IDLE) {
                                std::vector<TransferRequest> entries;
                                for (int j = 0; j < size_; ++j) {
                                    entries.push_back(TransferRequest{
                                        .opcode = TransferRequest::WRITE,
                                        .source = (void *)gpu_source,
                                        .target_id = segment_ids_[j],
                                        .target_offset =
                                            segment_descs_[j]->buffers[1].addr +
                                            rank_ * task.tensorSize,
                                        .length = task.tensorSize,
                                    });
                                }
                                task.batchID =
                                    engine_->allocateBatchID(entries.size());
                                engine_->submitTransfer(task.batchID, entries);
                                task_status[i].store(TASK_TRANSFERRED_1,
                                                     std::memory_order_release);
                            } else if (task_status[i].load(
                                           std::memory_order_acquire) ==
                                       TASK_TRANSFERRED_1) {
                                bool batch_done = true;
                                TransferStatus status;
                                for (int j = 0; j < size_; ++j) {
                                    engine_->getTransferStatus(task.batchID, j,
                                                               status);
                                    if (status.s !=
                                        TransferStatusEnum::COMPLETED) {
                                        batch_done = false;
                                        break;
                                    }
                                }
                                if (!batch_done) {
                                    break;
                                }
                                auto source_ptr =
                                    (int32_t *)cpu_source + i * size_;
                                std::vector<TransferRequest> entries;
                                for (int j = 0; j < size_; ++j) {
                                    *source_ptr = 1;
                                    entries.push_back(TransferRequest{
                                        .opcode = TransferRequest::WRITE,
                                        .source = (void *)source_ptr,
                                        .target_id = segment_ids_[j],
                                        .target_offset =
                                            segment_descs_[j]->buffers[3].addr +
                                            (i * size_ + rank_) *
                                                sizeof(int32_t),
                                        .length = sizeof(int32_t),
                                    });
                                }
                                task.batchID =
                                    engine_->allocateBatchID(entries.size());
                                engine_->submitTransfer(task.batchID, entries);
                                task_status[i].store(TASK_SIGNALED_1,
                                                     std::memory_order_release);
                            } else if (task_status[i].load(
                                           std::memory_order_acquire) ==
                                       TASK_SIGNALED_1) {
                                bool all_received = true;
                                auto signal_ptr =
                                    (int32_t *)segment_descs_[rank_]
                                        ->buffers[3]
                                        .addr +
                                    i * size_;
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
                                    task_status[i].store(
                                        TASK_DONE, std::memory_order_release);
                                    task.status = DONE;
                                }
                            }
                            break;
                        }
                        default: {
                            task.status = DONE;
                            break;
                        }
                    }
                }
            }
        }
    }).detach();
}

}  // namespace mooncake