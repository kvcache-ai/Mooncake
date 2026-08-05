#include <cuda_alike.h>
#include <algorithm>
#include <thread>
#include <mooncake_worker.cuh>
#include <mooncake_communicator.h>
#include <glog/logging.h>
#include <transfer_engine.h>
#include "control_plane/rpc.h"
#include "pg_utils.h"
#include "control_plane/agent_host.h"

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

bool MooncakeWorker::drainTasks(const TransferGroupMeta* meta) const {
    BackoffWaiter waiter;
    return waiter.wait_for(
        std::chrono::milliseconds(kDrainTasksTimeoutMs), [this, meta] {
            for (size_t i = 0; i < kNumTasks_; ++i) {
                if (tasks_[i].active && tasks_[i].transferGroupMeta == meta)
                    return false;
            }
            return true;
        });
}

void MooncakeWorker::waitUntilTasksSubmitted(
    const std::vector<CudaTaskSubmissionToken>& tasks) const {
    if (tasks.empty()) {
        return;
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
    waiter.wait(submitted);
}

void MooncakeWorker::startWorker() {
    running_ = true;
    worker_thread_ = std::thread([this] {
        if (cuda_device_index_ >= 0) {
            cudaSetDevice(cuda_device_index_);
        }
        std::atomic<WorkerTaskStatus> task_status[kNumTasks_];
        using clock = std::chrono::high_resolution_clock;

        // Per-slot state owned exclusively by this worker thread.
        clock::time_point activeTime[kNumTasks_];
        size_t rankToTaskId[kNumTasks_][kMaxNumRanks];
        int32_t failedRanks[kNumTasks_][kMaxNumRanks]{};
        int32_t attemptedRanks[kNumTasks_][kMaxNumRanks]{};
        int32_t* active_failed_ranks_hint[kNumTasks_]{};
        bool task_detected_failure[kNumTasks_]{};

        auto hasFailed = [&failedRanks](size_t task_id, int rank) {
            return failedRanks[task_id][rank] != 0;
        };
        auto markFailed = [&failedRanks](size_t task_id, int rank) {
            failedRanks[task_id][rank] = 1;
        };
        auto hasAttempted = [&attemptedRanks](size_t task_id, int rank) {
            return attemptedRanks[task_id][rank] != 0;
        };
        auto markAttempted = [&attemptedRanks](size_t task_id, int rank) {
            attemptedRanks[task_id][rank] = 1;
        };

        while (running_) {
            PAUSE();
            for (size_t i = 0; i < kNumTasks_; ++i) {
                auto& task = tasks_[i];
                if (!task.active) {
                    task_status[i].store(IDLE, std::memory_order_release);
                    continue;
                }

                auto group = (TransferGroupMeta*)task.transferGroupMeta;
                const auto op_type = static_cast<OpType>(task.opType);
                bool skipTransfer = (op_type == OpType::Broadcast &&
                                     group->rank != task.broadcastRoot) ||
                                    (op_type == OpType::Scatter &&
                                     group->rank != task.broadcastRoot) ||
                                    op_type == OpType::Barrier;

                if (task_status[i].load(std::memory_order_acquire) == IDLE) {
                    const auto submit_sequence = task.submitSequence;
                    // A slot is reused by unrelated tasks. Start with fresh
                    // task-local observations so stale failures/attempts from
                    // the previous occupant cannot affect this task.
                    for (size_t j = 0; j < kMaxNumRanks; ++j) {
                        failedRanks[i][j] = 0;
                        attemptedRanks[i][j] = 0;
                    }
                    task_detected_failure[i] = false;
                    active_failed_ranks_hint[i] = task.failedRanksHint;

                    if (task.resetFailedRanksHint &&
                        active_failed_ranks_hint[i]) {
                        // A graph replay carries the captured first-chunk flag,
                        // so it starts a fresh hint in the same tensor storage.
                        std::fill_n(active_failed_ranks_hint[i],
                                    group->maxGroupSize, int32_t{0});
                    }
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
                    entries.reserve(group->maxGroupSize);
                    for (int j = 0; j < group->maxGroupSize; ++j) {
                        if (!group->activeRanks[j] || hasFailed(i, j)) {
                            continue;
                        }
                        if ((op_type == OpType::Gather ||
                             op_type == OpType::Reduce) &&
                            j != task.broadcastRoot) {
                            continue;
                        }

                        uint64_t source = group->segmentInfos[group->rank]
                                              .send_buffer[task.bufferOffset];

                        switch (op_type) {
                            case OpType::Broadcast:
                            case OpType::AllReduce:
                            case OpType::AllGather:
                            case OpType::Reduce:
                            case OpType::Gather:
                                break;
                            case OpType::AllToAll:
                            case OpType::ReduceScatter:
                            case OpType::Scatter:
                                source += j * task.dataSize;
                                break;
                            default:
                                break;
                        }
                        uint64_t target_offset =
                            group->segmentInfos[j]
                                .recv_buffer[task.bufferOffset];

                        switch (op_type) {
                            case OpType::Broadcast:
                            case OpType::Scatter:
                                break;
                            case OpType::AllReduce:
                            case OpType::AllGather:
                            case OpType::AllToAll:
                            case OpType::ReduceScatter:
                            case OpType::Reduce:
                            case OpType::Gather:
                                target_offset += group->rank * task.dataSize;
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
                            .length = task.dataSize,
                        });

                        // Attempted to transfer to this peer
                        markAttempted(i, j);
                    }
                    task.batchID =
                        group->engine->allocateBatchID(entries.size());
                    group->engine->submitTransfer(task.batchID, entries);
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
                        for (int j = 0; j < group->maxGroupSize; ++j) {
                            if (!group->activeRanks[j] || hasFailed(i, j)) {
                                continue;
                            }
                            if (rankToTaskId[i][j] == kInvalidTaskId) {
                                continue;
                            }
                            group->engine->getTransferStatus(
                                task.batchID, rankToTaskId[i][j], status);
                            if (status.s != TransferStatusEnum::COMPLETED) {
                                bool peer_dead = false;
                                if (status.s == TransferStatusEnum::FAILED) {
                                    peer_dead = true;
                                } else if (j != group->rank &&
                                           diff.count() >
                                               *group->collectiveTimeoutUs) {
                                    peer_dead =
                                        group->engine->probePeerAliveByID(
                                            group->segmentIDs[j]) !=
                                        PeerLiveness::Alive;
                                }
                                if (peer_dead) {
                                    markFailed(i, j);
                                    task_detected_failure[i] = true;
                                    LOG(ERROR)
                                        << "Rank " << group->globalRank
                                        << " [DATA] transfer to peer " << j
                                        << " (global=" << group->rank_order[j]
                                        << ") failed for op "
                                        << (int)task.opType
                                        << " status=" << (int)status.s
                                        << " diff_us=" << diff.count();
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
                    entries.reserve(group->maxGroupSize);
                    for (int j = 0; j < group->maxGroupSize; ++j) {
                        if (!group->activeRanks[j] || hasFailed(i, j)) {
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
                        markAttempted(i, j);
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
                    for (int j = 0; j < group->maxGroupSize; ++j) {
                        if (!group->activeRanks[j] || hasFailed(i, j)) {
                            continue;
                        }
                        if (rankToTaskId[i][j] == kInvalidTaskId) {
                            continue;
                        }
                        group->engine->getTransferStatus(
                            task.batchID, rankToTaskId[i][j], status);
                        if (signal_ptr[j] != 1 ||
                            status.s != TransferStatusEnum::COMPLETED) {
                            bool peer_dead = false;
                            if (status.s == TransferStatusEnum::FAILED) {
                                peer_dead = true;
                            } else if (j != group->rank &&
                                       diff.count() >
                                           *group->collectiveTimeoutUs) {
                                peer_dead = group->engine->probePeerAliveByID(
                                                group->segmentIDs[j]) !=
                                            PeerLiveness::Alive;
                            }
                            if (peer_dead) {
                                markFailed(i, j);
                                task_detected_failure[i] = true;
                                LOG(ERROR)
                                    << "Rank " << group->globalRank
                                    << " [SYNC] sync to peer " << j
                                    << " (global=" << group->rank_order[j]
                                    << ") failed for op " << (int)task.opType
                                    << " status=" << (int)status.s
                                    << " signal_ptr=" << (int)signal_ptr[j]
                                    << " diff_us=" << diff.count();
                            } else {
                                task_done = false;
                                break;
                            }
                        }
                    }
                    if (diff.count() > *group->collectiveTimeoutUs) {
                        // reset timer
                        activeTime[i] = clock::now();
                    }
                    if (task_done) {
                        for (int j = 0; j < group->maxGroupSize; ++j) {
                            signal_ptr[j] = 0;
                        }

                        if (active_failed_ranks_hint[i]) {
                            for (int rank = 0; rank < group->maxGroupSize;
                                 ++rank) {
                                active_failed_ranks_hint[i][rank] |=
                                    failedRanks[i][rank];
                            }
                            active_failed_ranks_hint[i] = nullptr;
                        }

                        // Push link event via communicator's Agent.
                        if (group->communicator) {
                            bool has_any_attempted = false;
                            for (int j = 0; j < group->maxGroupSize; ++j) {
                                if (hasAttempted(i, j)) {
                                    has_any_attempted = true;
                                    break;
                                }
                            }
                            if (has_any_attempted) {
                                LinkEvent event;
                                event.events.assign(kMaxNumRanks,
                                                    LinkEvent::EventType::None);
                                event.target_rank_epochs.assign(kMaxNumRanks,
                                                                0);
                                for (int j = 0; j < group->maxGroupSize; ++j) {
                                    const auto peer_global =
                                        group->rank_order[j];
                                    if (!hasAttempted(i, j)) continue;
                                    event.events[peer_global] =
                                        hasFailed(i, j)
                                            ? LinkEvent::EventType::Failure
                                            : LinkEvent::EventType::Success;
                                    event.target_rank_epochs[peer_global] =
                                        group->rankEpochs[peer_global];
                                }
                                group->communicator->getAgent().pushLinkEvent(
                                    event);
                            }
                        }

                        auto s = group->engine->freeBatchID(task.batchID);
                        if (!s.ok()) {
                            LOG(WARNING)
                                << "BatchID leaked due to freeBatchID "
                                   "failure (likely caused by a timeout): "
                                << s.message();
                        }

                        if (task_detected_failure[i] &&
                            group->autoSyncOnFailure) {
                            auto result =
                                group->communicator->syncAfterFailure();
                            PG_ASSERT(result.has_value() &&
                                          result.value().status !=
                                              SyncAfterFailureStatus::Rejected,
                                      "syncAfterFailure failed for rank ",
                                      group->globalRank);
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
