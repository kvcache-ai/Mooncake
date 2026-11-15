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
    AWAITING_READERS = 1,
    TRANSFERRED_1 = 2,
    SIGNALED_1 = 3,
};

static constexpr size_t kInvalidTaskId = static_cast<size_t>(-1);

// How often a task that waits for readers asks a target again for its read
// counts: at once for the first kReadPollGraceUs of the wait, then at an
// interval that doubles from the minimum to the maximum.
static constexpr int64_t kReadPollGraceUs = 100;
static constexpr int64_t kReadPollMinDelayUs = 10;
static constexpr int64_t kReadPollMaxDelayUs = 1000;

static HalfRecvSync* recvSync(const TransferGroupMeta& group, int half) {
    return (HalfRecvSync*)group.segmentInfos[group.rank].recv_sync[half];
}

static HalfSendSync* sendSync(const TransferGroupMeta& group, int half) {
    return (HalfSendSync*)group.segmentInfos[group.rank].send_sync[half];
}

// What this rank knows about the reads of the process now in peer j's slot. It
// forgets what it knew about an earlier process there.
static TransferGroupMeta::PeerReads& peerReadsOf(TransferGroupMeta& group,
                                                 int j) {
    auto& reads = group.peerReads[j];
    const GlobalRank global = group.rank_order[j];
    const uint64_t epoch =
        global >= 0 && static_cast<uint32_t>(global) < kMaxNumRanks
            ? group.rankEpochs[global]
            : 0;
    if (reads.rankEpoch != epoch) {
        reads = {};
        reads.rankEpoch = epoch;
    }
    return reads;
}

// Takes note of a peer's read count for a half. Counts only grow, so a count
// that arrives late changes nothing.
static void notePeerRead(TransferGroupMeta::PeerReads& reads, int half,
                         uint32_t count) {
    if (!reads.knownValid[half] ||
        static_cast<int32_t>(count - reads.known[half]) > 0) {
        reads.known[half] = count;
    }
    reads.knownValid[half] = true;
}

// True when a peer is known to have read the last run of its half, so that the
// half may take new data.
static bool peerHasRead(const TransferGroupMeta::PeerReads& reads, int half) {
    return !reads.awaited[half] ||
           (reads.knownValid[half] &&
            static_cast<int32_t>(reads.known[half] - reads.needed[half]) >= 0);
}

static uint64_t loadSyncFlagWord(uint64_t& word) {
    return std::atomic_ref<uint64_t>(word).load(std::memory_order_acquire);
}

void MooncakeWorker::Start() {
    bool expected = false;
    if (started_.compare_exchange_strong(expected, true)) {
        startWorker();
    }
}

bool MooncakeWorker::hasPendingActiveRanksMirrorUpdate(
    const TransferGroupMeta* meta) const {
    for (size_t i = kCudaTaskOffset_; i < kNumTasks_; ++i) {
        auto& task = tasks_[i];
        // The request generation is the publication token for this pending
        // update. Keep this acquire before reading transferGroupMeta so the
        // task-to-group association is observed after the request is
        // published.
        const uint64_t requested =
            std::atomic_ref(task.activeRanksMirrorRequestGeneration)
                .load(std::memory_order_acquire);
        if (task.transferGroupMeta != meta) {
            continue;
        }
        const uint64_t applied =
            std::atomic_ref(task.activeRanksMirrorAppliedGeneration)
                .load(std::memory_order_acquire);
        if (requested != applied) {
            return true;
        }
    }
    return false;
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
        std::atomic<WorkerTaskStatus> task_status[kNumTasks_]{};
        using clock = std::chrono::high_resolution_clock;

        // Per-slot state owned exclusively by this worker thread.
        clock::time_point activeTime[kNumTasks_];
        BatchID batchIDs[kNumTasks_]{};
        // The task's run's number on its half, and the set of sync flags that
        // the run uses.
        uint32_t syncFlagRun[kNumTasks_]{};
        size_t syncFlagOffset[kNumTasks_]{};
        // A task that waits for its targets to read asks them for their read
        // counts with a batch of its own, and asks again later while a target
        // is still short. readEpochs holds the rank epoch each target had when
        // it was asked.
        BatchID readBatchIDs[kNumTasks_]{};
        bool readPending[kNumTasks_]{};
        uint64_t readEpochs[kNumTasks_][kMaxNumRanks]{};
        // The pacing of those reads runs on a steady clock, so that a step of
        // the wall clock does not hold them back. readRejected records that the
        // engine refused a batch of the task, which is logged once.
        using pace_clock = std::chrono::steady_clock;
        pace_clock::time_point nextReadAt[kNumTasks_];
        int64_t readDelayUs[kNumTasks_]{};
        bool readRejected[kNumTasks_]{};
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
                    continue;
                }

                auto group = (TransferGroupMeta*)task.transferGroupMeta;
                const auto op_type = static_cast<OpType>(task.opType);
                bool skipTransfer = (op_type == OpType::Broadcast &&
                                     group->rank != task.broadcastRoot) ||
                                    (op_type == OpType::Scatter &&
                                     group->rank != task.broadcastRoot) ||
                                    op_type == OpType::Barrier;
                auto storesInto = [&](int j) {
                    if (!group->activeRanks[j] || hasFailed(i, j)) {
                        return false;
                    }
                    return (op_type != OpType::Gather &&
                            op_type != OpType::Reduce) ||
                           j == task.broadcastRoot;
                };

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
                    // Consecutive runs of a half alternate between its two
                    // sets of sync flags.
                    syncFlagRun[i] =
                        group->bufferHalfRuns[task.bufferOffset].fetch_add(
                            1, std::memory_order_relaxed);
                    syncFlagOffset[i] = syncFlagRun[i] % 2;

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
                    readPending[i] = false;
                    activeTime[i] = clock::now();
                    nextReadAt[i] = pace_clock::now();
                    readDelayUs[i] = 0;
                    readRejected[i] = false;
                    task_status[i].store(AWAITING_READERS,
                                         std::memory_order_release);
                }

                if (task_status[i].load(std::memory_order_acquire) ==
                    AWAITING_READERS) {
                    // A target may still be reading the last run of this half,
                    // and this chunk's data must not replace what it reads.
                    // Its read count tells when it is done. Usually a flag
                    // that the target sent since has carried the count. If
                    // not, this rank reads the count from the target, again
                    // for as long as it is too low. A rank that is no longer
                    // active itself waits too: peers that have not applied
                    // that view yet still take its data.
                    const int half = task.bufferOffset;
                    const auto waited =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            clock::now() - activeTime[i]);
                    const bool timed_out =
                        waited.count() > *group->collectiveTimeoutUs;
                    const auto pace_now = pace_clock::now();
                    // Asks again later, at an interval that doubles.
                    auto backOff = [&] {
                        readDelayUs[i] =
                            std::clamp(2 * readDelayUs[i], kReadPollMinDelayUs,
                                       kReadPollMaxDelayUs);
                        nextReadAt[i] = pace_now + std::chrono::microseconds(
                                                       readDelayUs[i]);
                    };
                    auto isDead = [&](int j) {
                        return timed_out &&
                               group->engine->probePeerAliveByID(
                                   group->segmentIDs[j]) != PeerLiveness::Alive;
                    };
                    auto giveUpOn = [&](int j) {
                        markFailed(i, j);
                        markAttempted(i, j);
                        task_detected_failure[i] = true;
                        LOG(ERROR) << "Rank " << group->globalRank
                                   << " [READS] no read count from peer " << j
                                   << " (global=" << group->rank_order[j]
                                   << ") for op " << (int)task.opType
                                   << " waited_us=" << waited.count();
                    };
                    if (readPending[i]) {
                        bool settled = true;
                        for (int j = 0; j < group->maxGroupSize; ++j) {
                            if (rankToTaskId[i][j] == kInvalidTaskId) {
                                continue;
                            }
                            // A rank that left the group is no longer a
                            // target, as in TRANSFERRED_1 and SIGNALED_1.
                            if (!group->activeRanks[j]) {
                                rankToTaskId[i][j] = kInvalidTaskId;
                                continue;
                            }
                            TransferStatus status{};
                            if (!group->engine
                                     ->getTransferStatus(readBatchIDs[i],
                                                         rankToTaskId[i][j],
                                                         status)
                                     .ok()) {
                                // Asked again below.
                                rankToTaskId[i][j] = kInvalidTaskId;
                                continue;
                            }
                            if (status.s == TransferStatusEnum::COMPLETED) {
                                // Counts read from an earlier process in the
                                // peer's slot say nothing about this one.
                                auto& reads = peerReadsOf(*group, j);
                                if (reads.rankEpoch == readEpochs[i][j]) {
                                    const volatile uint32_t* counts =
                                        recvSync(*group, half)->readCountsIn[j];
                                    for (int h = 0; h < 2; ++h) {
                                        notePeerRead(reads, h, counts[h]);
                                    }
                                }
                            } else if (status.s == TransferStatusEnum::FAILED ||
                                       isDead(j)) {
                                giveUpOn(j);
                            } else {
                                settled = false;
                                continue;
                            }
                            rankToTaskId[i][j] = kInvalidTaskId;
                        }
                        if (settled) {
                            auto s =
                                group->engine->freeBatchID(readBatchIDs[i]);
                            if (!s.ok()) {
                                LOG(WARNING)
                                    << "BatchID leaked due to freeBatchID "
                                       "failure (likely caused by a timeout): "
                                    << s.message();
                            }
                            readPending[i] = false;
                            // A target that is still short is asked again at
                            // once while the wait is short, and then less
                            // often, so a reader that is late by milliseconds
                            // is not polled at the rate of round trips.
                            if (waited.count() >= kReadPollGraceUs) {
                                backOff();
                            }
                        }
                    }
                    // A target whose link is being set up again, or that was
                    // asked a moment ago, is asked later.
                    bool unasked = false;
                    if (!readPending[i]) {
                        std::vector<TransferRequest> reads;
                        for (int j = 0; j < group->maxGroupSize; ++j) {
                            if (j == group->rank || !storesInto(j)) {
                                continue;
                            }
                            const auto& peer = peerReadsOf(*group, j);
                            if (peerHasRead(peer, half)) {
                                continue;
                            }
                            if (isDead(j)) {
                                giveUpOn(j);
                                continue;
                            }
                            const auto target = group->segmentIDs[j];
                            if (target ==
                                    static_cast<TransferMetadata::SegmentID>(
                                        -1) ||
                                pace_now < nextReadAt[i]) {
                                unasked = true;
                                continue;
                            }
                            rankToTaskId[i][j] = reads.size();
                            readEpochs[i][j] = peer.rankEpoch;
                            reads.push_back(TransferRequest{
                                .opcode = TransferRequest::READ,
                                .source = (void*)recvSync(*group, half)
                                              ->readCountsIn[j],
                                .target_id = target,
                                .target_offset =
                                    group->segmentInfos[j].send_sync[0] +
                                    offsetof(HalfSendSync, halfReads),
                                .length = sizeof(HalfSendSync::halfReads),
                            });
                        }
                        if (!reads.empty()) {
                            readBatchIDs[i] =
                                group->engine->allocateBatchID(reads.size());
                            auto s = group->engine->submitTransfer(
                                readBatchIDs[i], reads);
                            if (s.ok()) {
                                readPending[i] = true;
                            } else {
                                // The engine took none of the reads, for
                                // example when it could not resolve one
                                // target. Ask again later.
                                if (!readRejected[i]) {
                                    readRejected[i] = true;
                                    LOG(WARNING)
                                        << "Rank " << group->globalRank
                                        << " [READS] submitting reads of read "
                                           "counts failed: "
                                        << s.message();
                                }
                                group->engine->freeBatchID(readBatchIDs[i]);
                                for (size_t j = 0; j < kMaxNumRanks; ++j) {
                                    rankToTaskId[i][j] = kInvalidTaskId;
                                }
                                backOff();
                                unasked = true;
                            }
                        }
                    }
                    if (timed_out) {
                        for (int j = 0; j < group->maxGroupSize; ++j) {
                            if (j != group->rank && storesInto(j) &&
                                !peerHasRead(peerReadsOf(*group, j), half)) {
                                LOG(WARNING)
                                    << "Rank " << group->globalRank
                                    << " [READS] peer " << j
                                    << " (global=" << group->rank_order[j]
                                    << ") has not read half " << half
                                    << " yet, waited_us=" << waited.count();
                            }
                        }
                        // reset timer
                        activeTime[i] = clock::now();
                    }
                    if (readPending[i] || unasked) {
                        continue;
                    }

                    for (size_t j = 0; j < kMaxNumRanks; ++j) {
                        rankToTaskId[i][j] = kInvalidTaskId;
                    }
                    std::vector<TransferRequest> entries;
                    entries.reserve(group->maxGroupSize);
                    for (int j = 0; j < group->maxGroupSize; ++j) {
                        if (!storesInto(j)) {
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
                    batchIDs[i] =
                        group->engine->allocateBatchID(entries.size());
                    group->engine->submitTransfer(batchIDs[i], entries);
                    submitted_task_sequence_[i].store(
                        task.submitSequence, std::memory_order_release);
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
                                batchIDs[i], rankToTaskId[i][j], status);
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
                        auto s = group->engine->freeBatchID(batchIDs[i]);
                        if (!s.ok()) {
                            LOG(WARNING)
                                << "BatchID leaked due to freeBatchID "
                                   "failure (likely caused by a timeout): "
                                << s.message();
                        }
                    }

                    // The flag carries this rank's read counts as they are
                    // now. Every earlier run of this half is read by now,
                    // because this rank's runs of one half are ordered: by
                    // the slot's enqueue stream or the eager submission wait
                    // in putTaskCuda on a GPU, and on a CPU by this thread,
                    // which counts a chunk's read in the task's callback
                    // before it takes up another task. This run is read only
                    // after its flags are delivered. So a peer knows that this
                    // rank has read this run once the half's count has passed
                    // the one here.
                    auto source_ptr = &sendSync(*group, task.bufferOffset)
                                           ->flags[syncFlagOffset[i]];
                    const uint32_t tag = syncFlagTag(
                        group->rankEpochs[group->globalRank], syncFlagRun[i]);
                    for (int half = 0; half < 2; ++half) {
                        source_ptr->words[half] = syncFlagWord(
                            tag,
                            std::atomic_ref<uint32_t>(group->halfReads[half])
                                .load(std::memory_order_acquire));
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
                        rankToTaskId[i][j] = entries.size();
                        entries.push_back(TransferRequest{
                            .opcode = TransferRequest::WRITE,
                            .source = (void*)source_ptr,
                            .target_id = group->segmentIDs[j],
                            .target_offset = group->segmentInfos[j]
                                                 .recv_sync[task.bufferOffset] +
                                             offsetof(HalfRecvSync, flags) +
                                             (syncFlagOffset[i] * kMaxNumRanks +
                                              group->rank) *
                                                 sizeof(SyncFlag),
                            .length = kSyncFlagBytes,
                        });
                        markAttempted(i, j);
                    }
                    batchIDs[i] =
                        group->engine->allocateBatchID(entries.size());
                    group->engine->submitTransfer(batchIDs[i], entries);
                    activeTime[i] = clock::now();
                    task_status[i].store(SIGNALED_1, std::memory_order_release);
                } else if (task_status[i].load(std::memory_order_acquire) ==
                           SIGNALED_1) {
                    bool task_done = true;
                    const int half = task.bufferOffset;
                    SyncFlag* signal_ptr =
                        recvSync(*group, half)->flags[syncFlagOffset[i]];

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
                            batchIDs[i], rankToTaskId[i][j], status);
                        // A flag of an earlier run on this flag set, which
                        // landed after that run had given up on its sender,
                        // does not count.
                        const uint64_t flag_word =
                            loadSyncFlagWord(signal_ptr[j].words[half]);
                        if (!syncFlagTagIsRun(syncFlagWordTag(flag_word),
                                              syncFlagRun[i]) ||
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
                                    << " flag_tag="
                                    << syncFlagWordTag(flag_word)
                                    << " diff_us=" << diff.count();
                            } else {
                                // A live peer whose flag here is of another
                                // run counts that half's runs differently, so
                                // its flag of this run may never come. Say so
                                // once per timeout.
                                const uint32_t tag = syncFlagWordTag(flag_word);
                                if (j != group->rank && tag != 0 &&
                                    !syncFlagTagIsRun(tag, syncFlagRun[i]) &&
                                    diff.count() >
                                        *group->collectiveTimeoutUs) {
                                    LOG(WARNING)
                                        << "Rank " << group->globalRank
                                        << " [SYNC] peer " << j
                                        << " (global=" << group->rank_order[j]
                                        << ") left a flag of run "
                                        << ((tag >> 1) & kSyncFlagRunMask)
                                        << " in half " << half
                                        << ", waiting for run "
                                        << (syncFlagRun[i] & kSyncFlagRunMask);
                                }
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
                        // A peer's flag tells what the peer had read when it
                        // sent it, and the peer reads this run next. So it has
                        // read this run once its count for the half has passed
                        // the flag's, and until then its half takes no new
                        // data from this rank. Only words with the tag of this
                        // run and of the process now in the peer's slot count:
                        // a flag left by an earlier run, or by an earlier
                        // process there, says nothing about this one.
                        // A peer marked failed in this task is held to its
                        // flag too, if the flag landed: the peer runs this
                        // collective. A peer that stays without a flag keeps
                        // its earlier requirement.
                        for (int j = 0; j < group->maxGroupSize; ++j) {
                            if (j == group->rank || !group->activeRanks[j]) {
                                group->peerReads[j].awaited[half] = false;
                            } else {
                                auto& reads = peerReadsOf(*group, j);
                                const uint32_t tag = syncFlagTag(
                                    reads.rankEpoch, syncFlagRun[i]);
                                uint64_t words[2] = {};
                                words[half] =
                                    loadSyncFlagWord(signal_ptr[j].words[half]);
                                if (syncFlagWordTag(words[half]) == tag) {
                                    words[1 - half] = loadSyncFlagWord(
                                        signal_ptr[j].words[1 - half]);
                                    for (int h = 0; h < 2; ++h) {
                                        if (syncFlagWordTag(words[h]) == tag) {
                                            notePeerRead(
                                                reads, h,
                                                syncFlagWordCount(words[h]));
                                        }
                                    }
                                    reads.needed[half] =
                                        syncFlagWordCount(words[half]) + 1;
                                    reads.awaited[half] = true;
                                }
                            }
                            std::atomic_ref<uint64_t>(signal_ptr[j].words[half])
                                .store(0, std::memory_order_relaxed);
                        }

                        if (active_failed_ranks_hint[i]) {
                            for (int rank = 0; rank < group->maxGroupSize;
                                 ++rank) {
                                active_failed_ranks_hint[i][rank] |=
                                    failedRanks[i][rank];
                            }
                            active_failed_ranks_hint[i] = nullptr;
                        }

                        // Mark the resident-kernel mirror update as pending
                        // before publishing the link event. The resulting
                        // ViewUpdate must observe request != applied and skip
                        // the host H2D path. The kernel consumes this request
                        // and copies the updated active ranks.
                        if (task_detected_failure[i] &&
                            group->autoSyncOnFailure &&
                            group->activeRanksMirrorDevice) {
                            const uint64_t generation =
                                next_active_ranks_mirror_generation_.fetch_add(
                                    1, std::memory_order_relaxed);
                            std::atomic_ref(
                                task.activeRanksMirrorRequestGeneration)
                                .store(generation, std::memory_order_release);
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
                                const auto max_world_size =
                                    group->communicator->getMaxWorldSize();
                                event.events.assign(max_world_size,
                                                    LinkEvent::EventType::None);
                                event.target_rank_epochs.assign(max_world_size,
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

                        auto s = group->engine->freeBatchID(batchIDs[i]);
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

                        // The slot is idle again once it is released: the GPU
                        // may publish the slot's next task before this thread
                        // looks at the slot again, and that task starts from
                        // IDLE.
                        task_status[i].store(IDLE, std::memory_order_release);
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
