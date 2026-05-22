#include <cuda_runtime.h>
#include <thread>
#include <mooncake_worker.cuh>
#include <glog/logging.h>
#include <transfer_engine.h>
#include "pg_utils.h"

namespace mooncake {

enum WorkerTaskStatus {
    IDLE = 0,
    TRANSFERRED_1 = 1,
    SIGNALED_1 = 2,
    DONE = 3,
};

static constexpr size_t kInvalidTaskId = static_cast<size_t>(-1);

namespace {

bool isPeerUnreachable(TransferGroupMeta* group, int peerRank) {
    if (peerRank == group->rank) {
        return false;
    }
    if (!group->peerConnected[peerRank]) {
        return true;
    }
    return group->engine->probePeerAliveByID(group->segmentIDs[peerRank]) !=
           PeerLiveness::Alive;
}

void markPeerBroken(TransferGroupMeta* group, int peerRank, const char* phase,
                    c10d::OpType opType) {
    LOG(WARNING) << "Rank " << group->rank << " marking peer " << peerRank
                 << " as broken during " << phase << " op " << (int)opType
                 << " segmentID=" << group->segmentIDs[peerRank];

    group->peerConnected[peerRank] = false;
    group->activeRanks[peerRank] = false;
    group->activeRanksTensor[peerRank] = 0;

    // Invalidate the cached segment ID so that any subsequent submitTransfer
    // (including P2P proxy or sync-phase writes) will fail at selectTransport
    // instead of submitting RDMA operations to a dead peer.  closeSegment only
    // clears the metadata cache and is safe to call even when there are still
    // in-flight transfers.
    if (group->segmentIDs[peerRank] != 0) {
        group->engine->closeSegment(group->segmentIDs[peerRank]);
    }
    group->segmentIDs[peerRank] = static_cast<TransferMetadata::SegmentID>(-1);
}

void freeBatchID(TransferGroupMeta* group, BatchID batchID) {
    auto s = group->engine->freeBatchID(batchID);
    if (!s.ok()) {
        LOG(WARNING) << "BatchID leaked due to freeBatchID failure: "
                     << s.message();
    }
}

}  // namespace

void MooncakeWorker::Start() {
    bool expected = false;
    if (started_.compare_exchange_strong(expected, true)) {
        startWorker();
    }
}

bool MooncakeWorker::hasActiveTasks(const TransferGroupMeta* meta) const {
    for (size_t i = 0; i < kNumTasks_; ++i) {
        if (tasks_[i].active && tasks_[i].transferGroupMeta == meta) {
            return true;
        }
    }
    return false;
}

bool MooncakeWorker::drainTasks(const TransferGroupMeta* meta) const {
    BackoffWaiter waiter;
    return waiter.wait_for(std::chrono::milliseconds(kDrainTasksTimeoutMs),
                           [this, meta] { return !hasActiveTasks(meta); });
}

bool MooncakeWorker::waitUntilTasksSubmitted(
    const std::vector<CudaTaskSubmissionToken>& tasks,
    std::chrono::milliseconds timeout) const {
    if (tasks.empty()) {
        return true;
    }

    auto submitted = [this, &tasks] {
        for (const auto& task : tasks) {
            if (task.task_id >= kNumTasks_) {
                LOG(ERROR) << "Invalid task id.";
                return true;
            }
            if (submitted_task_sequence_[task.task_id].load(
                    std::memory_order_acquire) < task.sequence) {
                return false;
            }
        }
        return true;
    };

    BackoffWaiter waiter(
        BackoffWaiterConfig::constantSleep(std::chrono::microseconds(10)));
    if (timeout == kNoTimeout) {
        waiter.wait(submitted);
        return true;
    }
    return waiter.wait_for(timeout, submitted);
}

void MooncakeWorker::startWorker() {
    running_ = true;
    worker_thread_ = std::thread([this] {
        if (cuda_device_index_ >= 0) {
            cudaSetDevice(cuda_device_index_);
        }
        std::atomic<WorkerTaskStatus> task_status[kNumTasks_];
        using clock = std::chrono::high_resolution_clock;
        clock::time_point activeTime[kNumTasks_];
        size_t rankToTaskId[kNumTasks_][kMaxNumRanks];
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
                    const auto submit_sequence = task.submitSequence;
                    if (skipTransfer) {
                        submitted_task_sequence_[i].store(
                            submit_sequence, std::memory_order_release);
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
                        if (!group->peerConnected[j]) {
                            markPeerBroken(group, j, "preparing", task.opType);
                            continue;
                        }
                        if (j != group->rank &&
                            group->engine->probePeerAliveByID(
                                group->segmentIDs[j]) !=
                                PeerLiveness::Alive) {
                            markPeerBroken(group, j, "preparing",
                                           task.opType);
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
                    auto submit_status =
                        group->engine->submitTransfer(task.batchID, entries);
                    if (!submit_status.ok()) {
                        LOG(ERROR) << "Worker submitTransfer failed: "
                                   << submit_status.message();
                        for (int j = 0; j < group->size; ++j) {
                            if (rankToTaskId[i][j] != kInvalidTaskId) {
                                markPeerBroken(group, j, "submitting",
                                               task.opType);
                            }
                        }
                        freeBatchID(group, task.batchID);
                        task.batchID = 0;
                        continue;
                    }
                    submitted_task_sequence_[i].store(
                        submit_sequence, std::memory_order_release);
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
                                    (diff.count() > kPingTimeoutMicroseconds_ &&
                                     isPeerUnreachable(group, j))) {
                                    markPeerBroken(group, j, "transferring",
                                                   task.opType);
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
                        freeBatchID(group, task.batchID);
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
                        if (!group->peerConnected[j]) {
                            markPeerBroken(group, j, "signaling", task.opType);
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
                                (diff.count() > kPingTimeoutMicroseconds_ &&
                                 isPeerUnreachable(group, j))) {
                                markPeerBroken(group, j, "syncing",
                                               task.opType);
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
                        freeBatchID(group, task.batchID);
                    }
                }
            }
        }
    });
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
