#include <cuda_alike.h>
#include <thread>
#include <mooncake_worker.cuh>
#include <mooncake_backend.h>
#include <glog/logging.h>
#include <transfer_engine.h>
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
                bool skipTransfer =
                    ((c10d::OpType)task.opType == c10d::OpType::BROADCAST &&
                     group->rank != task.broadcastRoot) ||
                    ((c10d::OpType)task.opType == c10d::OpType::SCATTER &&
                     group->rank != task.broadcastRoot) ||
                    (c10d::OpType)task.opType == c10d::OpType::BARRIER;

                if (task_status[i].load(std::memory_order_acquire) == IDLE) {
                    const auto submit_sequence = task.submitSequence;
                    if (skipTransfer) {
                        // Even though we skip the data-transfer phase, we must
                        // still mark active peers as "attempted" so the signal
                        // phase will create per-peer sync entries and wait for
                        // the root's signal before consuming the recv buffer.
                        // Without this, non-root broadcast/scatter ranks race
                        // past the root's RDMA writes and copy stale data.
                        for (int j = 0; j < group->size; ++j) {
                            if (!group->activeRanks[j]) continue;
                            if (task.attemptedRanksHintHost) {
                                task.attemptedRanksHintHost[j] = 1;
                            }
                        }
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
                        // j is InGroupRank; segment arrays are also
                        // InGroupRank-indexed.

                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        if (((c10d::OpType)task.opType ==
                                 c10d::OpType::GATHER ||
                             (c10d::OpType)task.opType ==
                                 c10d::OpType::REDUCE) &&
                            j != task.broadcastRoot) {
                            continue;
                        }

                        if (task.attemptedRanksHintHost) {
                            task.attemptedRanksHintHost[j] = 1;
                        }

                        // Use cached segment ID from TransferGroupMeta.
                        // Control plane syncs this via applyViewUpdate.
                        TransferMetadata::SegmentID target_id =
                            group->segmentIDs[j];

                        uint64_t source = group->segmentInfos[group->rank]
                                              .send_buffer[task.bufferOffset];

                        switch ((c10d::OpType)task.opType) {
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

                        switch ((c10d::OpType)task.opType) {
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
                            .target_id = target_id,
                            .target_offset = target_offset,
                            .length = task.tensorSize,
                        });
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
                        for (int j = 0; j < group->size; ++j) {
                            if (task.failedRanksHintHost[j]) {
                                continue;
                            }
                            // Use the per-op attempted bitmap as the source of
                            // truth. A peer that was attempted in the data
                            // phase must be accounted for even if the
                            // coordinator later deactivates it.
                            if (!task.attemptedRanksHintHost[j]) {
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
                                    task.failedRanksHintHost[j] = 1;
                                    LOG(ERROR) << "Rank " << group->globalRank
                                               << " transfer to peer " << j
                                               << " failed for op "
                                               << (int)task.opType;
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
                        // Send a completion signal to every active peer.
                        // The signal phase acts as a bidirectional barrier:
                        // each rank writes to every peer's recv_sync at its
                        // own rank-slot and waits for every peer's signal in
                        // its own recv_sync.  This is independent of the
                        // data-transfer direction — even ranks that only
                        // sent data (e.g. REDUCE contributors) or only
                        // received (e.g. BROADCAST non-roots) must
                        // participate so every rank sees the global barrier.
                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        if (task.failedRanksHintHost[j]) {
                            continue;
                        }

                        // Use cached segment ID from TransferGroupMeta.
                        TransferMetadata::SegmentID target_id =
                            group->segmentIDs[j];

                        *source_ptr = 1;
                        rankToTaskId[i][j] = entries.size();
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void*)source_ptr,
                            .target_id = target_id,
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
                        if (task.failedRanksHintHost[j]) {
                            continue;
                        }
                        // Check every active peer, not just data-attempted
                        // ones. The signal phase is a bidirectional barrier
                        // independent of data-transfer direction.
                        if (!group->activeRanks[j]) {
                            continue;
                        }
                        if (rankToTaskId[i][j] == kInvalidTaskId) {
                            // The peer was attempted in the data-transfer
                            // phase, but got deactivated before we could send
                            // the sync signal. Treat it as a failure so the
                            // control plane can reconcile the group view.
                            task.failedRanksHintHost[j] = 1;
                            LOG(ERROR) << "Rank " << group->globalRank
                                       << " sync to peer " << j
                                       << " skipped (peer deactivated before "
                                          "signal send)"
                                       << " for op " << (int)task.opType;
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
                                task.failedRanksHintHost[j] = 1;
                                LOG(ERROR)
                                    << "Rank " << group->globalRank
                                    << " sync to peer " << j
                                    << " failed for op " << (int)task.opType;
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
                        for (int j = 0; j < group->size; ++j) {
                            if (!group->activeRanks[j]) continue;
                            signal_ptr[j] = 0;
                        }

                        // Push transfer observation via backend's Agent.
                        if (task.attemptedRanksHintHost && group->backend) {
                            bool has_any_attempted = false;
                            for (int j = 0; j < group->size; ++j) {
                                if (task.attemptedRanksHintHost[j]) {
                                    has_any_attempted = true;
                                    break;
                                }
                            }
                            if (has_any_attempted) {
                                std::vector<uint8_t> attempted(kMaxNumRanks, 0);
                                std::vector<uint8_t> failed(kMaxNumRanks, 0);
                                std::vector<uint8_t> succeeded(kMaxNumRanks, 0);
                                for (int j = 0; j < group->size; ++j) {
                                    int32_t peer_global = group->rank_order[j];
                                    attempted[peer_global] =
                                        task.attemptedRanksHintHost[j] ? 1 : 0;
                                    failed[peer_global] =
                                        task.failedRanksHintHost[j] ? 1 : 0;
                                    succeeded[peer_global] =
                                        (attempted[peer_global] &&
                                         !failed[peer_global])
                                            ? 1
                                            : 0;
                                }
                                group->backend->getAgent()
                                    .pushTransferObservation(
                                        std::move(attempted), std::move(failed),
                                        std::move(succeeded));
                            }
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
