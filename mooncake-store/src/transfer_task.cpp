#include "transfer_task.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include "config/transfer_submitter_config.h"
#include "device/accelerator_registry.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif

static int GetPositiveEnvOrDefault(const char* name, int default_value) {
    const char* raw_value = std::getenv(name);
    if (!raw_value || raw_value[0] == '\0') {
        return default_value;
    }

    errno = 0;
    char* end_ptr = nullptr;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed <= 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid value for " << name << ": " << raw_value
                     << ", using default " << default_value;
        return default_value;
    }

    return static_cast<int>(parsed);
}

#ifdef USE_NOF
static bool IsTruthyEnv(const char* value) {
    if (!value) {
        return false;
    }
    std::string normalized(value);
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return normalized == "1" || normalized == "true" || normalized == "yes" ||
           normalized == "on";
}

static bool IsSpdkNofDebugEnabled() {
    static const bool enabled = IsTruthyEnv(std::getenv("MC_NOF_DEBUG"));
    return enabled;
}

static int GetSpdkNofDebugIntervalMs() {
    static const int interval_ms = []() {
        const char* raw_value = std::getenv("MC_NOF_DEBUG_INTERVAL_MS");
        if (!raw_value) {
            return 1000;
        }
        char* end_ptr = nullptr;
        long parsed = std::strtol(raw_value, &end_ptr, 10);
        if (end_ptr == raw_value || (end_ptr != nullptr && *end_ptr != '\0') ||
            parsed <= 0) {
            return 1000;
        }
        return static_cast<int>(parsed);
    }();
    return interval_ms;
}

static int GetSpdkNofSubmitChunkBytes() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_SUBMIT_CHUNK_BYTES", mooncake::kDefaultSpdkNofSubmitChunkBytes);
    return value;
}

static int GetSpdkNofInflightBytesLimit() {
    static const int value =
        GetPositiveEnvOrDefault("MC_NOF_INFLIGHT_BYTES_LIMIT",
                                mooncake::kDefaultSpdkNofInflightBytesLimit);
    return value;
}

static int GetSpdkNofWorkerCount() {
    static const int value = GetPositiveEnvOrDefault(
        "MC_NOF_WORKERS", mooncake::kDefaultSpdkNofWorkers);
    return value;
}

// Read MC_NOF_MAX_QUEUE_DEPTH to cap the per-worker task queue depth.
// Default: 256.  Set to 0 to disable the limit (no backpressure).
// Uses a dedicated parser that allows zero (GetPositiveEnvOrDefault rejects
// zero, which would silently fall back to the default).
static int GetSpdkNofMaxQueueDepth() {
    static const int value = []() -> int {
        const char* raw = std::getenv("MC_NOF_MAX_QUEUE_DEPTH");
        if (!raw || raw[0] == '\0') return 256;
        errno = 0;
        char* end = nullptr;
        long parsed = std::strtol(raw, &end, 10);
        if (errno != 0 || end == raw || (end && *end != '\0') || parsed < 0 ||
            parsed > 4096)
            return 256;
        return static_cast<int>(parsed);
    }();
    return value;
}

static int CountSpdkNofQueuedTasks(const mooncake::SpdkNofTask* head) {
    int count = 0;
    const mooncake::SpdkNofTask* cursor = head;
    while (cursor != nullptr) {
        ++count;
        cursor = cursor->nxt;
    }
    return count;
}

// Forward declaration: FinalizeSubmittedTask below calls
// SpdkNofTaskCompletion, which is defined further down in the same
// translation unit.  The original ordering (SpdkNofTaskCompletion
// before FinalizeSubmittedTask) used the fact that the helper was a
// tail call, but the new helper can be entered from the sync-failure
// path which the compiler lays out before SpdkNofTaskCompletion.  A
// forward declaration keeps the file readable without reordering.
static inline void SpdkNofTaskCompletion(mooncake::SpdkNofTask* task);

// Single termination gate for worker-driven task completion.  Called
// from every path that observes a task reaching its terminal
// pre-conditions:
//
//   - sync-failure path in workerThread (after SubmitRequest returned
//     non-zero and we have rolled back the IncrementInflight, recycled
//     the sub_task, and zeroed remaining_lba / failed)
//   - submit-loop epilogue in workerThread (after all blocks have
//     been successfully submitted and remaining_lba reached 0)
//   - FinalizeAfterDrain (after the drain fence proves the qpair pool
//     is quiescent)
//
// Two-step decision:
//
//   Step 1 (chain advance): ALWAYS pop + on_chain=false when
//   remaining_lba == 0.  This is independent of outstanding_sub_io
//   because a head-stuck task with CQEs in flight prevents the worker
//   from progressing — PollAll only runs after the submit loop, so a
//   stuck head means the CQE never gets harvested and wait() hangs.
//
//   Step 2 (completion): only call SpdkNofTaskCompletion when
//   outstanding_sub_io == 0.  When outstanding > 0, the CQE trampoline
//   (nvmf_io_complete) will call SpdkNofTaskCompletion itself when the
//   CQE lands.  try_complete() arbitrates set_completed + delete so
//   both paths are safe — exactly one wins.
//
// Guarantees:
//   - The task is popped from its qos chain AT MOST ONCE per call:
//     the if (task->on_chain) guard makes the PopTask + on_chain=false
//     pair idempotent across re-entries (e.g. when the trampoline
//     triggers SpdkNofTaskCompletion concurrently with the worker
//     epilogue).
//   - SpdkNofTaskCompletion's try_complete() CAS guarantees
//     set_completed + delete happen at most once across all callers.
//
// This eliminates the historical "double-pop" pattern that produced
// "sibling task silently skipped" failures in the DRAINING stress
// test (SiblingQpairFailure::Stress_DrainRecoveryCycles_NoLeakNoHang):
// the sync-failure path used to call PopTask + on_chain=false
// inline AND the submit-loop epilogue did the same, leaving every
// alternate task without a termination path.
static inline void FinalizeSubmittedTask(mooncake::SpdkNofTask* task,
                                         mooncake::SpdkNofQos* qos, int op) {
    int rem = task->remaining_lba.load(std::memory_order_acquire);
    // remaining_lba > 0 means the worker has not submitted all blocks
    // yet — bail without touching chain state.
    if (rem != 0) return;

    // ALWAYS advance the head chain when remaining_lba == 0.  This is
    // the critical step that the previous (over-strict) guard got
    // wrong: a task with remaining_lba == 0 but outstanding_sub_io > 0
    // is mid-flight (CQEs pending) and MUST be popped so the worker
    // loop advances to the next head task and PollAll gets a chance to
    // fire the CQE.  Otherwise the worker busy-loops on the same head
    // and wait() hangs forever — the bug we just diagnosed at
    // primer_future->wait().
    if (task->on_chain) {
        qos->PopTask(op);
        task->on_chain = false;
    }

    // Completion-side decision: only safe to run SpdkNofTaskCompletion
    // when no CQE is still in flight.  If outstanding_sub_io > 0, the
    // CQE trampoline (nvmf_io_complete) will call SpdkNofTaskCompletion
    // itself when the CQE lands.  try_complete() arbitrates set_completed
    // + delete so both paths are safe — exactly one wins.
    int outstanding = task->outstanding_sub_io.load(std::memory_order_acquire);
    if (outstanding == 0) {
        SpdkNofTaskCompletion(task);
    }
}

static inline void SpdkNofTaskCompletion(mooncake::SpdkNofTask* task) {
    // Terminal pre-conditions: remaining_lba == 0 (worker has submitted
    // all blocks, or the trampoline wrote 0 on error CQE) and
    // outstanding_sub_io == 0 (every sub-IO CQE has been processed).
    // A DRAINING-short-circuited CQE leaves outstanding_sub_io > 0,
    // so FinalizeAfterDrain is responsible for that case instead.
    int rem_lba = task->remaining_lba.load(std::memory_order_acquire);
    int outstanding = task->outstanding_sub_io.load(std::memory_order_acquire);
    if (rem_lba != 0 || outstanding != 0) return;

    // Single-completion CAS: set_completed + delete happen exactly
    // once across all callers.
    if (!task->try_complete()) return;

    if (task->nof_qos) {
        task->nof_qos->active_tasks.erase(task);
    }

    // Acquire fence so the previous loads (remaining_lba,
    // outstanding_sub_io, failed) are visible to set_completed and
    // subsequent observers.
    std::atomic_thread_fence(std::memory_order_acquire);

    bool is_failed = task->failed.load(std::memory_order_acquire);
    task->state->set_completed(is_failed ? mooncake::ErrorCode::TRANSFER_FAIL
                                         : mooncake::ErrorCode::OK);
    if (!task->on_chain) {
        delete task;
    }
}

// ----------------------------------------------------------------------------
// SubTaskFreeList — see header for rationale.  Defined here (rather than
// in transfer_task.h's anonymous-ish inline space) because nof_connection.cpp
// also calls Acquire/Release and shares the same SubTaskFreeList instance.
// ----------------------------------------------------------------------------
std::shared_ptr<mooncake::SpdkNofSubTask> mooncake::SubTaskFreeList::Acquire() {
    std::lock_guard<std::mutex> lock(mu);
    if (free_list.empty()) {
        EnsurePopulatedUnlocked(64);
    }
    auto sp = std::move(free_list.back());
    free_list.pop_back();
    // Re-anchor free_list pointer in case the chunk was constructed
    // without back-reference (initial pre-populate path).
    sp->free_list = this;
    return sp;
}

void mooncake::SubTaskFreeList::Release(
    std::shared_ptr<mooncake::SpdkNofSubTask> sp) {
    if (!sp) return;
    std::lock_guard<std::mutex> lock(mu);
    free_list.push_back(std::move(sp));
}

void mooncake::SubTaskFreeList::EnsurePopulated(size_t chunk_size) {
    std::lock_guard<std::mutex> lock(mu);
    EnsurePopulatedUnlocked(chunk_size);
}

void mooncake::SubTaskFreeList::EnsurePopulatedUnlocked(size_t chunk_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        auto sp = std::make_shared<mooncake::SpdkNofSubTask>(
            mooncake::SpdkNofSubTask{});
        sp->free_list = this;
        free_list.push_back(std::move(sp));
    }
}

static void nvmf_io_complete(void* ctx, const struct spdk_nvme_cpl* cpl) {
    if (!ctx) {
        LOG(ERROR) << "nvmf_io_complete ctx is null";
        return;
    }

    // ctx is a heap-allocated std::shared_ptr<SpdkNofSubTask>* created
    // by the worker.  Acquiring the shared_ptr keeps the sub_task
    // alive for the entire trampoline body — even if the worker has
    // already exited and freed its local chunk array (the historical
    // UAF scenario at ~NofQpairPool CQ-drain).  After the local copy
    // is taken, the ctx allocation is freed.
    auto* ctx_sp_ptr =
        reinterpret_cast<std::shared_ptr<mooncake::SpdkNofSubTask>*>(ctx);
    if (!ctx_sp_ptr) {
        LOG(ERROR) << "nvmf_io_complete ctx_sp_ptr is null";
        return;
    }
    std::shared_ptr<mooncake::SpdkNofSubTask> sub_task_sp =
        std::move(*ctx_sp_ptr);
    delete ctx_sp_ptr;
    if (!sub_task_sp) {
        // Defensive: ctx_sp_ptr was a valid heap pointer but held a
        // null shared_ptr (shouldn't happen in practice — Acquire
        // returns a non-null shared_ptr or a freshly made one).
        return;
    }
    mooncake::SpdkNofSubTask* sub_task = sub_task_sp.get();

    // Read the pool pointer BEFORE dereferencing any task field.  The
    // pool was captured at submit time (see workerThread) and outlives
    // any in-flight CQE; the qpair pool is owned by NofConnection ->
    // NofSegment, both of which outlive the SpdkNofWorkerPool thread.
    // This ordering ensures no UAF on the task: a late CQE arriving
    // after SpdkNofTaskCompletion has deleted the task (via
    // FinalizeAfterDrain or destructor teardown) takes the DRAINING
    // short-circuit without ever touching task fields.
    mooncake::NofQpairPool* pool = sub_task->pool;

    // DRAINING short-circuit: skip task-level counters and route
    // finalisation through FinalizeAfterDrain (single source of
    // truth).  DecrementInflight is still required to balance the
    // submit-side Increment and to release the WaitForInflightCompletion
    // synchronises-with edge.  IsDraining() returns true for both
    // kDraining and kClosed states, so this also protects against a
    // late CQE that arrives after ~NofQpairPool has run.
    if (pool && pool->IsDraining()) {
        pool->DecrementInflight();
        if (sub_task->free_list) {
            sub_task->free_list->Release(std::move(sub_task_sp));
        }
        return;
    }

    mooncake::SpdkNofTask* task = sub_task->task;
    if (!task) {
        // Defensive: sub_task->task is always initialised by the worker
        // before SubmitRequest, but a callback racing task deletion
        // (FinalizeAfterDrain or destructor teardown) must not UAF.
        // In normal operation the DRAINING short-circuit above catches
        // this case; this branch only fires if pool was null (the
        // submit-side capture was skipped) or the pool never entered
        // DRAINING before task deletion.
        if (sub_task->free_list) {
            sub_task->free_list->Release(std::move(sub_task_sp));
        }
        return;
    }

    mooncake::SpdkNofQos* nof_qos = task->nof_qos;
    int op = task->op;

    // Normal path: monotonic decrements via fetch_sub (exactly
    // submit_lba_count per CQE; no clamp-to-zero needed because
    // FinalizeAfterDrain does not touch these counters).
    if (task->io_count) {
        task->io_count->fetch_sub(1, std::memory_order_acq_rel);
    }

    task->outstanding_sub_io.fetch_sub(1, std::memory_order_acq_rel);

    if (nof_qos) {
        nof_qos->inflight_blocks[op].fetch_sub(sub_task->submit_lba_count,
                                               std::memory_order_acq_rel);
    }

    task->inflight_block_count.fetch_sub(sub_task->submit_lba_count,
                                         std::memory_order_acq_rel);

    if (spdk_nvme_cpl_is_error(cpl)) {
        LOG(ERROR) << "task_complete: I/O failed"
                   << spdk_nvme_cpl_get_status_string(&cpl->status);
        task->remaining_lba.store(0, std::memory_order_release);
        task->failed.store(true, std::memory_order_release);
    }

    SpdkNofTaskCompletion(task);

    // Refcount fence: paired with the IncrementInflight the worker
    // performed right before spdk_nvme_ns_cmd_*.  Safe to run after
    // SpdkNofTaskCompletion (which may delete the task) because
    // inflight_count_ lives on the pool, not on the task.
    if (pool) {
        pool->DecrementInflight();
    }

    // Return the sub_task to the qpair pool's free list.  The free
    // list is heap-allocated and owned by the qpair pool (NOT the
    // worker thread), so this Release() is safe even after the worker
    // thread has exited and its stack frame is gone — the historical
    // UAF scenario at ~NofQpairPool CQ-drain.
    if (sub_task->free_list) {
        sub_task->free_list->Release(std::move(sub_task_sp));
    }
}
#endif
namespace mooncake {

#ifdef USE_NOF
SpdkNofQos::SpdkNofQos(uint32_t block_size) {
    int block_size_int = static_cast<int>(block_size);
    if (block_size_int <= 0) {
        block_size_int = 1;
    }

    blocks_per_chunk =
        std::max(1, GetSpdkNofSubmitChunkBytes() / block_size_int);
    // Save the absolute cap from MC_NOF_INFLIGHT_BYTES_LIMIT so that
    // UpdateInflightLimit can shrink the inflight limit (degradation) but
    // never RAISE it past the originally-configured ceiling.  Without this,
    // a partially-degraded pool could compute a pool-derived limit larger
    // than the absolute one — see UpdateInflightLimit for the formula.
    absolute_inflight_limit =
        std::max(1, GetSpdkNofInflightBytesLimit() / block_size_int);
    inflight_blocks_limit = absolute_inflight_limit;
    for (int i = 0; i < kSpdkNofOpNum; ++i) {
        inflight_blocks[i].store(0, std::memory_order_relaxed);
        head[i] = nullptr;
        tail[i] = nullptr;
    }
}

// Destructor-time invariant: active_tasks must be empty.  The qos is
// stack-owned by workerThread's seg_to_qos map and lives for the
// entire worker lifetime, so by destruction time every PushTask'd
// task must have run through SpdkNofTaskCompletion (which erases
// itself from active_tasks).
SpdkNofQos::~SpdkNofQos() {
#ifndef NDEBUG
    if (!active_tasks.empty()) {
        // One-line dump of every leaked pointer, capped at 16.
        std::ostringstream leaked;
        leaked << "~SpdkNofQos: " << active_tasks.size() << " task(s) leaked:";
        size_t n = 0;
        for (SpdkNofTask* t : active_tasks) {
            leaked << " " << t;
            if (++n >= 16) {
                leaked << " ...";
                break;
            }
        }
        DCHECK(false) << leaked.str();
    }
#endif
}

// Finalise head/tail tasks whose outstanding_sub_io == 0.  These tasks
// have already received all their CQE callbacks (nvmf_io_complete
// decremented outstanding_sub_io to 0) but the worker has not yet
// called SpdkNofTaskCompletion because the chain was non-empty when
// the pool entered DRAINING.  Off-chain tasks in active_tasks still
// have outstanding_sub_io > 0 and are finalised by
// FinalizeAfterDrain once the pool reaches inflight == 0.
//
// Safe to call from the worker thread.
void SpdkNofQos::FailQueuedTasks() {
    for (int op = 0; op < kSpdkNofOpNum; ++op) {
        while (head[op]) {
            SpdkNofTask* task = head[op];
            // A chain-head task with outstanding_sub_io > 0 means its
            // CQEs are still in flight on a live qpair — leave it for
            // the drain protocol to finish.
            if (task->outstanding_sub_io.load(std::memory_order_acquire) > 0) {
                break;
            }
            PopTask(op);
            task->on_chain = false;
            SpdkNofTaskCompletion(task);
        }
        tail[op] = nullptr;
    }
}

// Finalise the off-chain tasks in active_tasks once the qpair pool's
// inflight counter has reached 0 (verified by
// WaitForInflightCompletion).
//
// Counter pay-back contract:
//   The trampoline's DRAINING short-circuit (nvmf_io_complete) skips
//   THREE decrements per short-circuited CQE:
//     - task->io_count->fetch_sub(1)
//     - task->outstanding_sub_io.fetch_sub(1)
//     - nof_qos->inflight_blocks[op].fetch_sub(submit_lba_count)
//
//   (task->inflight_block_count.fetch_sub(submit_lba_count) is also
//   skipped, but that counter is task-local and only read by the
//   worker's submit-side budget check; it has no observable
//   consequence after FinalizeAfterDrain since the task is deleted
//   by SpdkNofTaskCompletion below.)
//
//   FinalizeAfterDrain is the SOLE writer that pays them back:
//     - For each task: outstanding_sub_io is exchange(0) (snapshot+zero
//       in one atomic op) and io_count is fetch_sub(skipped).  The
//       exchange(0) is paired with the fetch_sub(skipped) so the
//       per-task outstanding counter is consistent with the shared
//       io_count.
//     - For each op: inflight_blocks[op] is exchange(0).  The
//       trampoline's normal path (non-DRAINING) has already
//       decremented these via fetch_sub, and WaitForInflightCompletion
//       guarantees every such decrement has run; the exchange(0) is a
//       defensive guard against any future short-circuit path that
//       may touch them.
//
//   try_complete() arbitrates set_completed + delete so it happens
//   exactly once across all callers (trampoline normal-path vs
//   FinalizeAfterDrain).
//
// Safe to call from the worker thread.
void SpdkNofQos::FinalizeAfterDrain() {
    // Snapshot active_tasks first because SpdkNofTaskCompletion
    // erases each task from the set as it runs.
    std::vector<SpdkNofTask*> to_finalize(active_tasks.begin(),
                                          active_tasks.end());
    for (SpdkNofTask* task : to_finalize) {
        if (!task) continue;

        // Pay back io_count for CQEs the trampoline short-circuited.
        // outstanding_sub_io counts un-decremented sub-IOs.  Use
        // exchange(0) so the snapshot+zero is a single atomic op.
        //
        // WaitForInflightCompletion's release/acquire fence guarantees
        // that no trampoline normal-path fetch_sub can be running on
        // this task when we reach this code: by the time the fence
        // observed InflightCount==0, every prior DecrementInflight has
        // finished, and after the pool entered kDraining
        // (GetNextQpair returns nullptr) no new submissions happen.
        // Late CQEs that arrive after the fence take the DRAINING
        // short-circuit and never touch outstanding_sub_io or
        // io_count.  Therefore exchange(0) snapshots exactly the count
        // of CQEs that took the short-circuit, and fetch_sub(skipped)
        // is the matching pay-back.
        int skipped =
            task->outstanding_sub_io.exchange(0, std::memory_order_acq_rel);
        if (skipped > 0 && task->io_count) {
            task->io_count->fetch_sub(skipped, std::memory_order_acq_rel);
        }

        task->on_chain = false;
        task->failed.store(true, std::memory_order_release);
        task->remaining_lba.store(0, std::memory_order_release);

        SpdkNofTaskCompletion(task);
    }

    for (int op = 0; op < kSpdkNofOpNum; ++op) {
        // The trampoline's normal path has already decremented
        // inflight_blocks[op] via fetch_sub (WaitForInflightCompletion
        // fence guarantees all such decrements have run), and the
        // trampoline's DRAINING short-circuit does NOT touch
        // inflight_blocks[op].  The exchange(0) here is a defensive
        // zero-out: it guarantees the counter reads 0 after
        // FinalizeAfterDrain regardless of which path the trampoline
        // took.
        inflight_blocks[op].exchange(0, std::memory_order_release);
        head[op] = nullptr;
        tail[op] = nullptr;
    }
}
#endif

// ============================================================================
// FilereadWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 10 worker
// threads.
constexpr int kDefaultFilereadWorkers = 10;

// The number of fileread workers can be tuned via the MC_FILEREAD_WORKERS
// environment variable. Falls back to kDefaultFilereadWorkers when unset,
// empty, or invalid.
static int GetFilereadWorkerCount() {
    static const int value =
        GetPositiveEnvOrDefault("MC_FILEREAD_WORKERS", kDefaultFilereadWorkers);
    return value;
}

FilereadWorkerPool::FilereadWorkerPool(std::shared_ptr<StorageBackend>& backend)
    : shutdown_(false) {
    const int num_workers = GetFilereadWorkerCount();
    VLOG(1) << "Creating FilereadWorkerPool with " << num_workers << " workers";

    // Start worker threads
    workers_.reserve(num_workers);
    for (int i = 0; i < num_workers; ++i) {
        workers_.emplace_back(&FilereadWorkerPool::workerThread, this);
    }
    backend_ = backend;
}

FilereadWorkerPool::~FilereadWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "FilereadWorkerPool destroyed";
}

void FilereadWorkerPool::submitTask(FilereadTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown FilereadWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void FilereadWorkerPool::workerThread() {
    VLOG(2) << "FilereadWorkerPool worker thread started";

    while (true) {
        FilereadTask task("", 0, {}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                if (!backend_) {
                    LOG(ERROR)
                        << "Backend is not initialized, cannot load object";
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                    continue;
                }

                auto load_result = backend_->LoadObject(
                    task.file_path, task.slices, task.object_size);
                if (load_result) {
                    VLOG(2) << "Fileread task completed successfully with "
                            << task.file_path;
                    task.state->set_completed(ErrorCode::OK);
                } else {
                    LOG(ERROR)
                        << "Fileread task failed for file: " << task.file_path
                        << " with error: " << toString(load_result.error());
                    task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async fileread: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "FilereadWorkerPool worker thread exiting";
}

// ============================================================================
// SpdkNofWorkerPool Implementation
// ============================================================================
// to fully utilize the available ssd bandwidth, we use a default of 4 worker
// threads.

#ifdef USE_NOF
SpdkNofWorkerPool::SpdkNofWorkerPool(int numa_socket_id)
    : worker_count_(GetSpdkNofWorkerCount()),
      numa_socket_id_(numa_socket_id),
      task_queue_(std::make_unique<std::queue<SpdkNofTask>[]>(worker_count_)),
      queue_mutex_(std::make_unique<std::mutex[]>(worker_count_)),
      queue_cv_(std::make_unique<std::condition_variable[]>(worker_count_)),
      // Backpressure condition variable for flow control.
      queue_not_full_cv_(
          std::make_unique<std::condition_variable[]>(worker_count_)),
      max_queue_depth_(GetSpdkNofMaxQueueDepth()),
      shutdown_(false) {
    VLOG(1) << "Creating SpdkNofWorkerPool with " << worker_count_
            << " workers, max_queue_depth=" << max_queue_depth_;

    // Diagnostic: register with SpdkWrapper so Cleanup() can detect premature
    // destruction (static destruction order fiasco).
    SpdkNoF_RegisterWorkerPool();

    // Start worker threads
    workers_.reserve(worker_count_);
    for (int i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&SpdkNofWorkerPool::workerThread, this, i);
    }
}

SpdkNofWorkerPool::~SpdkNofWorkerPool() {
    if (shutdown_.exchange(true)) {
        return;
    }

    // Wake workers AND any submitters blocked on queue_not_full_cv_.
    // If a submitter is waiting because a worker queue is full, it checks
    // shutdown_ in its wait predicate but will never observe it unless we
    // explicitly notify queue_not_full_cv_.  Without this notification
    // the destructor would join workers while the submitter remains
    // blocked, causing a deadlock.
    for (int i = 0; i < worker_count_; ++i) {
        queue_cv_[i].notify_all();
        queue_not_full_cv_[i].notify_all();
    }

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    // Diagnostic: all workers joined — safe for SpdkWrapper::Cleanup() to free
    // qpairs.
    SpdkNoF_UnregisterWorkerPool();

    VLOG(1) << "SpdkNofWorkerPool destroyed";
}

void SpdkNofWorkerPool::submitTask(SpdkNofTask task) {
    if (!task.state) {
        LOG(ERROR) << "Attempting to submit spdk nof task without state";
        return;
    }

    if (shutdown_.load()) {
        LOG(WARNING)
            << "Attempting to submit task to shutdown SpdkNofWorkerPool";
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    int worker_idx = -1;
    {
        std::lock_guard<std::mutex> lock(seg_mutex_);
        nof_seg_handle* seg = task.seg_handle;
        bool new_binding = false;
        if (seg_to_worker_.find(seg) != seg_to_worker_.end()) {
            worker_idx = seg_to_worker_[seg];
        } else {
            worker_idx = (seg_num++ % worker_count_);
            seg_to_worker_[seg] = worker_idx;
            new_binding = true;
        }
        if (new_binding && IsSpdkNofDebugEnabled()) {
            LOG(INFO) << "nof_worker_bind seg_handle=" << seg
                      << " worker_idx=" << worker_idx;
        }
    }
    if (worker_idx < 0 || worker_idx >= worker_count_) {
        LOG(ERROR) << "seg is not bind to invalid worker " << worker_idx;
        task.state->set_completed(ErrorCode::TRANSFER_FAIL);
        return;
    }

    {
        // Flow control: block the caller when the worker's task queue is
        // full, creating backpressure that prevents unbounded growth
        // in task_queue_ under degraded conditions.  Per-segment
        // seg_to_qos chains remain bounded only by the inflight cap
        // (see SpdkNofQos::inflight_blocks_limit); they are NOT
        // controlled by this backpressure check.
        // When max_queue_depth_ == 0 the check is skipped (no limit).
        std::unique_lock<std::mutex> lock(queue_mutex_[worker_idx]);
        if (max_queue_depth_ > 0 &&
            static_cast<int>(task_queue_[worker_idx].size()) >=
                max_queue_depth_) {
            auto wait_start = std::chrono::steady_clock::now();
            queue_not_full_cv_[worker_idx].wait(lock, [&] {
                return shutdown_.load() ||
                       static_cast<int>(task_queue_[worker_idx].size()) <
                           max_queue_depth_;
            });
            if (shutdown_.load()) {
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
                return;
            }
            auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - wait_start)
                              .count();
            if (waited > 1000) {
                LOG(WARNING)
                    << "[SpdkNofWorkerPool] submitTask blocked for " << waited
                    << "ms (queue full, depth=" << max_queue_depth_
                    << ", worker=" << worker_idx
                    << ") — target may be overloaded or qpair pool degraded";
            }
        }
        task_queue_[worker_idx].push(std::move(task));
    }
    queue_cv_[worker_idx].notify_one();
}

static bool HasBufferedTask(
    const std::map<nof_seg_handle*, std::unique_ptr<SpdkNofQos>>& seg_to_qos) {
    for (const auto& [_, nof_qos] : seg_to_qos) {
        if (!nof_qos->Empty()) {
            return true;
        }
    }
    return false;
}

// Drain protocol for every DRAINING pool.  Three phases run in
// strict order: WAIT, FINALIZE, RETIRE.
//
//   Phase 1 (WAIT):     every DRAINING pool →
//                       WaitForInflightCompletion.  Collect timed-out
//                       pools in timed_out_set.
//   Phase 2 (FINALIZE): EVERY DRAINING pool → FinalizeAfterDrain,
//                       including those in timed_out_set.
//                       FinalizeAfterDrain only touches qos-local
//                       state (active_tasks, head/tail,
//                       inflight_blocks); it does NOT dereference the
//                       connection, so it's safe on a pool whose
//                       connection is about to be retired.
// Phase 1's timeout path force-zeroes inflight and forces DRAINING
// (WaitForInflightCompletion's terminal contract); the trampoline's
// CAS-saturating clamp keeps late CQEs safe during Phase 2.
//
// Note: Phase 3 (RETIRE) was intentionally removed.  Retiring the
// connection from the worker thread destroys the NofConnection while
// the worker still references it via seg_handle->segment->GetConnection()
// on subsequent iterations, producing a use-after-free: the trampoline
// reads sub_task->pool (which captured the destroyed pool pointer) and
// the worker dereferences the dangling conn_ pointer when processing
// future task_queue_ entries.  Connection retirement is owned by
// ~TransferSubmitter::CloseNofSegment, which runs AFTER
// spdk_nvmf_pool_.reset() joins all worker threads — i.e. no worker
// can observe the connection after that point.
static void DrainDrainingPoolsUntilQuiescent(
    const std::map<nof_seg_handle*, std::unique_ptr<SpdkNofQos>>& seg_to_qos) {
    // Phase 1: WAIT — bounded fence, force-zero + force-DRAINING on timeout.
    for (const auto& [seg_handle, nof_qos] : seg_to_qos) {
        auto* conn = seg_handle->segment->GetConnection();
        if (conn == nullptr) continue;
        auto& pool = conn->GetQpairPool();
        if (!pool.IsDraining()) continue;
        bool quiescent = pool.WaitForInflightCompletion(kWorkerDrainTimeoutMs);
        if (quiescent) continue;

        LOG(ERROR) << "[DrainDrainingPoolsUntilQuiescent] pool did not reach "
                   << "quiescence within " << kWorkerDrainTimeoutMs
                   << "ms — seg " << seg_handle
                   << " finalising in Phase 2 without retiring the connection "
                      "(see Phase 3 removal rationale).";
    }

    // Phase 2: FINALIZE — runs on every DRAINING pool.
    for (auto& [seg_handle, nof_qos] : seg_to_qos) {
        auto* conn = seg_handle->segment->GetConnection();
        if (conn == nullptr) continue;
        if (!conn->GetQpairPool().IsDraining()) continue;
        nof_qos->FinalizeAfterDrain();
    }
}

constexpr int kSpdkNofSubTaskChunkSize =
    4096;  // (legacy — kept for source-compat; sub_tasks are no longer
           // pre-allocated in chunks; SubTaskFreeList manages them.

// CheckSubTaskPool removed: sub_tasks are now acquired from
// NofQpairPool::GetSubTaskFreeList() (heap-allocated, lifetime tied
// to the qpair pool — outlives the worker thread).  See
// SubTaskFreeList comment in transfer_task.h for the full rationale.

void SpdkNofWorkerPool::workerThread(int work_idx) {
    bindToSocket(numa_socket_id_);
    VLOG(2) << "SpdkNofWorkerPool worker thread started";

    // Shared with every SpdkNofTask via task->io_count so the counter
    // outlives workerThread's stack frame (shared_ptr keeps it alive
    // until the last referencing task is destroyed).
    auto total_outstanding_io = std::make_shared<std::atomic<int64_t>>(0);
    // std::set<nof_seg_handle *> seg_set;
    std::map<nof_seg_handle*, std::unique_ptr<SpdkNofQos>> seg_to_qos;
    // Declared after seg_to_qos so its destructor runs first during
    // stack unwinding; the destructor calls FinalizeAfterDrain on
    // every still-DRAINING pool.
    DrainProtocolGuard drain_guard(&seg_to_qos);
    auto& task_queue = task_queue_[work_idx];
    auto& queue_cv = queue_cv_[work_idx];
    auto& queue_mutex = queue_mutex_[work_idx];
    auto last_debug_snapshot = std::chrono::steady_clock::now();

    // Timers for periodic rebalance and queue-backlog diagnostics.
    auto last_rebalance = std::chrono::steady_clock::now();
    auto last_queue_depth_log = std::chrono::steady_clock::now();

    while (true) {
        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [this, &task_queue, &total_outstanding_io,
                                 &seg_to_qos] {
                return shutdown_.load() || !task_queue.empty() ||
                       total_outstanding_io->load(std::memory_order_acquire) ||
                       HasBufferedTask(seg_to_qos);
            });

            if (shutdown_.load() && task_queue.empty() &&
                (total_outstanding_io->load(std::memory_order_acquire) == 0) &&
                !HasBufferedTask(seg_to_qos)) {
                break;
            }

            while (!task_queue.empty()) {
                SpdkNofTask* task = new (std::nothrow)
                    SpdkNofTask(std::move(task_queue.front()));
                if (task == nullptr) {
                    LOG(ERROR)
                        << "alloc SpdkNofTask failed, worker " << work_idx;
                    // Signal failure to the submitter before dropping
                    // the task — otherwise TransferFuture::wait() hangs
                    // forever on a task that was silently discarded.
                    if (task_queue.front().state) {
                        task_queue.front().state->set_completed(
                            ErrorCode::TRANSFER_FAIL);
                    }
                    task_queue.pop();
                    if (max_queue_depth_ > 0) {
                        queue_not_full_cv_[work_idx].notify_one();
                    }
                    continue;
                }

                SpdkNofQos* nof_qos = nullptr;
                auto it = seg_to_qos.find(task->seg_handle);
                if (it == seg_to_qos.end()) {
                    auto qos = std::make_unique<SpdkNofQos>(
                        SpdkWrapper::GetInstance().GetBlockSize(
                            task->seg_handle));
                    nof_qos = qos.get();
                    seg_to_qos[task->seg_handle] = std::move(qos);

                    // Adaptive inflight: shrink the inflight cap only
                    // when the pool is degraded (Size < target) to
                    // prevent excessive in-flight I/O on a single qpair.
                    // Full-capacity pools keep the original 32 MiB limit
                    // to ensure maximum throughput.
                    auto* seg_conn = task->seg_handle->segment->GetConnection();
                    const auto& seg_cfg = seg_conn->GetConfig();
                    if (seg_cfg.adaptive_inflight) {
                        auto& pool = seg_conn->GetQpairPool();
                        if (pool.Size() < pool.GetTargetCount()) {
                            nof_qos->UpdateInflightLimit(
                                static_cast<int>(pool.Size()),
                                static_cast<int>(
                                    seg_cfg.max_inflight_per_qpair));
                        }
                    }

                    if (IsSpdkNofDebugEnabled()) {
                        LOG(INFO)
                            << "nof_qos_create worker_idx=" << work_idx
                            << " seg_handle=" << task->seg_handle
                            << " blocks_per_chunk="
                            << seg_to_qos[task->seg_handle]->blocks_per_chunk
                            << " inflight_blocks_limit="
                            << seg_to_qos[task->seg_handle]
                                   ->inflight_blocks_limit;
                    }
                } else {
                    nof_qos = it->second.get();
                }
                task->io_count = total_outstanding_io;
                task->nof_qos = nof_qos;
                task->on_chain = true;
                nof_qos->PushTask(task);
                task_queue.pop();

                // Notify producers that a queue slot is available.
                if (max_queue_depth_ > 0) {
                    queue_not_full_cv_[work_idx].notify_one();
                }
            }
        }

        for (auto& [seg_handle, nof_qos] : seg_to_qos) {
            uint32_t block_size =
                SpdkWrapper::GetInstance().GetBlockSize(seg_handle);
            for (int i = 0; i < kSpdkNofOpNum; ++i) {
                int avail_blocks =
                    nof_qos->inflight_blocks_limit -
                    nof_qos->inflight_blocks[i].load(std::memory_order_relaxed);
                while (nof_qos->head[i] && avail_blocks > 0) {
                    SpdkNofTask* task = nof_qos->head[i];
                    SpdkNofSubTask* sub_task;
                    while (task->remaining_lba.load(std::memory_order_acquire) >
                               0 &&
                           avail_blocks > 0) {
                        uint32_t submit_lba_count =
                            std::min(avail_blocks,
                                     std::min(task->remaining_lba.load(
                                                  std::memory_order_relaxed),
                                              nof_qos->blocks_per_chunk));
                        int lba_off =
                            task->lba_count -
                            task->remaining_lba.load(std::memory_order_relaxed);
                        uint64_t submit_lba = task->lba + lba_off;
                        void* submit_ptr = reinterpret_cast<void*>(
                            reinterpret_cast<char*>(task->ptr) +
                            lba_off * block_size);

                        // Acquire a sub_task from the qpair pool's
                        // heap-allocated free list.  The shared_ptr
                        // keeps the sub_task alive past worker exit
                        // (lifetime is tied to the qpair pool, not the
                        // worker thread) — the architectural fix for
                        // the nvmf_io_complete UAF.
                        auto* submit_conn =
                            task->seg_handle->segment->GetConnection();
                        auto* submit_pool = &submit_conn->GetQpairPool();
                        auto free_list = submit_pool->GetSubTaskFreeList();
                        auto sub_task_sp = free_list->Acquire();
                        if (!sub_task_sp) {
                            task->failed.store(true, std::memory_order_release);
                            task->remaining_lba.store(
                                0, std::memory_order_release);
                            break;
                        }
                        sub_task = sub_task_sp.get();
                        sub_task->task = task;
                        sub_task->submit_lba_count = submit_lba_count;

                        // Increment BEFORE spdk_nvme_ns_cmd_* so the
                        // trampoline's DecrementInflight pairs with
                        // ~NofQpairPool's WaitForInflightCompletion.
                        // Synchronous submit failure rolls it back below.
                        submit_pool->IncrementInflight();
                        // Capture the pool pointer for the trampoline.
                        // See the matching comment in nvmf_io_complete:
                        // this is what lets a late CQE short-circuit on
                        // IsDraining() without dereferencing the (possibly
                        // deleted) task.
                        sub_task->pool = submit_pool;

                        // Allocate the ctx as a heap-allocated empty
                        // shared_ptr<SpdkNofSubTask>*.  We populate it
                        // AFTER SubmitRequest succeeds so that on sync
                        // failure we can simply delete the empty ctx
                        // and return sub_task_sp to the free list.
                        auto* ctx_sp_ptr =
                            new std::shared_ptr<mooncake::SpdkNofSubTask>();
                        int ret = SpdkWrapper::GetInstance().SubmitRequest(
                            task->seg_handle, submit_ptr, submit_lba,
                            submit_lba_count, task->op, nvmf_io_complete,
                            ctx_sp_ptr);
                        if (ret != 0) {
                            // Classify the failure for the VLOG line.
                            // DRAINING is the common case in the stress
                            // regression (every submit after iter 0's
                            // inject fails because GetNextQpair returns
                            // nullptr); the VLOG level keeps the log
                            // quiet while remaining greppable at -v 1.
                            const char* reason =
                                submit_conn->GetQpairPool().IsDraining()
                                    ? "draining"
                                    : "spdk_reject";
                            VLOG(1)
                                << "work " << work_idx << ", seg "
                                << task->seg_handle
                                << " submit io fail (reason=" << reason << ")";
                            // No CQE will fire for a failed submission —
                            // roll back the increment, free the empty
                            // ctx, and return the sub_task to the free
                            // list (so it's reused for future submits).
                            submit_conn->GetQpairPool().DecrementInflight();
                            delete ctx_sp_ptr;
                            free_list->Release(std::move(sub_task_sp));
                            task->failed.store(true, std::memory_order_release);
                            task->remaining_lba.store(
                                0, std::memory_order_release);
                            // Mark the task as terminal and exit the
                            // inner submit loop.  FinalizeSubmittedTask
                            // is NOT called here — it is called exactly
                            // once by the epilogue below
                            // (`if (task->remaining_lba == 0)`) once
                            // the inner loop has exited.  Calling it
                            // here as well was a use-after-free bug:
                            // FinalizeSubmittedTask deletes the task
                            // (via SpdkNofTaskCompletion when
                            // outstanding_sub_io == 0), and the
                            // epilogue's `task->remaining_lba.load()`
                            // then read freed memory.  See line 1008 in
                            // the inner-while condition (and line 1147
                            // in the outer epilogue) for the UAF site.
                            //
                            // The terminal state (failed=true,
                            // remaining_lba=0) is sufficient for the
                            // epilogue to run the single termination
                            // gate: it pops the chain (if on_chain) and
                            // runs SpdkNofTaskCompletion, which sets
                            // the future ready and deletes the task
                            // exactly once (via try_complete() CAS).
                        } else {
                            // SubmitRequest succeeded — the trampoline
                            // WILL fire eventually.  Transfer ownership
                            // of sub_task_sp into the heap-allocated
                            // ctx so the trampoline can safely access
                            // it via shared_ptr.
                            *ctx_sp_ptr = std::move(sub_task_sp);
                            task->idx++;
                            task->remaining_lba.fetch_sub(
                                submit_lba_count, std::memory_order_acq_rel);
                            int prev_blocks = nof_qos->inflight_blocks[i].load(
                                std::memory_order_relaxed);
                            while (true) {
                                if (nof_qos->inflight_blocks[i]
                                        .compare_exchange_weak(
                                            prev_blocks,
                                            prev_blocks + submit_lba_count,
                                            std::memory_order_acq_rel)) {
                                    break;
                                }
                            }
                            // Per-task inflight count, mirrored against
                            // nof_qos->inflight_blocks[op] via
                            // nvmf_io_complete.
                            int prev_task_blocks =
                                task->inflight_block_count.load(
                                    std::memory_order_relaxed);
                            while (true) {
                                if (task->inflight_block_count
                                        .compare_exchange_weak(
                                            prev_task_blocks,
                                            prev_task_blocks + submit_lba_count,
                                            std::memory_order_acq_rel)) {
                                    break;
                                }
                            }
                            avail_blocks -= submit_lba_count;
                            task->outstanding_sub_io.fetch_add(
                                1, std::memory_order_acq_rel);
                            total_outstanding_io->fetch_add(
                                1, std::memory_order_acq_rel);
                        }
                    }
                    if (task->remaining_lba.load(std::memory_order_acquire) ==
                        0) {
                        // Single termination gate — same helper as the
                        // sync-failure path.  This call ALWAYS pops
                        // the head when remaining_lba == 0, regardless
                        // of outstanding_sub_io, so the worker loop
                        // advances past the stuck head and PollAll gets
                        // to fire the pending CQE.  If
                        // outstanding_sub_io > 0, the helper defers
                        // SpdkNofTaskCompletion to the trampoline
                        // (which will run when the CQE lands).
                        FinalizeSubmittedTask(task, nof_qos.get(), i);
                    }
                }
            }
        }

        // Poll every cycle so that any qpair transport error is observed
        // and EnterDraining fires regardless of whether CQEs are
        // currently in flight.  spdk_nvme_qpair_process_completions
        // returns 0 when a qpair's CQ ring is empty, so the cost when
        // no IO is in flight is one no-op call per registered qpair.
        {
            bool poll_failed = false;
            for (auto& [seg_handle, nof_qos] : seg_to_qos) {
                auto* conn = seg_handle->segment->GetConnection();
                auto& pool = conn->GetQpairPool();

                int64_t ret =
                    SpdkWrapper::GetInstance().NvmePollProcessCompletion(
                        seg_handle, 0);
                if (ret < 0) {
                    poll_failed = true;
                    LOG(ERROR)
                        << "poll completion error: ret " << ret
                        << " — entering pool DRAINING for seg " << seg_handle;

                    pool.EnterDraining("poll_err");
                    nof_qos->FailQueuedTasks();
                }
            }
            if (poll_failed) {
                DrainDrainingPoolsUntilQuiescent(seg_to_qos);
                continue;
            }
        }

        if (IsSpdkNofDebugEnabled()) {
            auto now = std::chrono::steady_clock::now();
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_debug_snapshot);
            if (elapsed.count() >= GetSpdkNofDebugIntervalMs()) {
                for (const auto& [seg_handle, nof_qos] : seg_to_qos) {
                    LOG(INFO)
                        << "nof_qos_state worker_idx=" << work_idx
                        << " seg_handle=" << seg_handle << " inflight_read="
                        << nof_qos->inflight_blocks[0].load(
                               std::memory_order_relaxed)
                        << " inflight_write="
                        << nof_qos->inflight_blocks[1].load(
                               std::memory_order_relaxed)
                        << " inflight_limit=" << nof_qos->inflight_blocks_limit
                        << " queued_read="
                        << CountSpdkNofQueuedTasks(nof_qos->head[0])
                        << " queued_write="
                        << CountSpdkNofQueuedTasks(nof_qos->head[1])
                        << " total_outstanding_io="
                        << total_outstanding_io->load(
                               std::memory_order_relaxed);
                }
                last_debug_snapshot = now;
            }
        }

        // Periodic rebalance: scan every segment's qpair pool and
        // attempt TryGrow on degraded pools.  Update the inflight
        // limit to reflect the recovered qpair capacity.
        {
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - last_rebalance);
            if (elapsed.count() >= kRebalanceIntervalSeconds) {
                last_rebalance = now;
                for (auto& [seg_handle, nof_qos] : seg_to_qos) {
                    auto* conn = seg_handle->segment->GetConnection();
                    auto& pool = conn->GetQpairPool();
                    uint32_t target = pool.GetTargetCount();
                    if (pool.Size() < target) {
                        uint32_t added = pool.TryGrow(target);
                        if (added > 0) {
                            // Update inflight cap to reflect expanded pool.
                            const auto& seg_cfg = conn->GetConfig();
                            if (seg_cfg.adaptive_inflight) {
                                nof_qos->UpdateInflightLimit(
                                    static_cast<int>(pool.Size()),
                                    static_cast<int>(
                                        seg_cfg.max_inflight_per_qpair));
                            }
                        }
                    }
                }
            }
        }

        // Queue-backlog diagnostics: every ~10 s check the number
        // of queued tasks across all segments and emit a warning if
        // backlog is significant (early warning for degradation).
        {
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - last_queue_depth_log);
            if (elapsed.count() >= 10) {
                last_queue_depth_log = now;
                int total_queued = 0;
                for (const auto& [seg_handle, nof_qos] : seg_to_qos) {
                    for (int i = 0; i < kSpdkNofOpNum; ++i) {
                        total_queued +=
                            CountSpdkNofQueuedTasks(nof_qos->head[i]);
                    }
                }
                if (total_queued > 16) {
                    // Log only on significant backlog to reduce noise.
                    LOG(WARNING)
                        << "[SpdkNofWorkerPool] task backlog: queued="
                        << total_queued << " queue_depth=" << task_queue.size()
                        << " worker=" << work_idx
                        << " — target throughput may be degraded"
                        << " (check qpair allocation count)";
                }
            }
        }
    }

    // (sub_task chunks no longer freed here — sub_tasks live in the
    // qpair pool's heap-allocated SubTaskFreeList.  See SubTaskFreeList
    // comment in transfer_task.h.)

    // The main loop's break-condition requires
    //   shutdown_ && task_queue.empty() &&
    //   total_outstanding_io == 0 && !HasBufferedTask
    // so reaching this point implies total_outstanding_io == 0.
    DCHECK_EQ(total_outstanding_io->load(std::memory_order_acquire), 0);

    // seg_to_qos is destroyed by its unique_ptr; ~SpdkNofQos fires
    // its own DCHECK on active_tasks.empty().  DrainProtocolGuard's
    // destructor (declared after it) has already run.

    // No sub_task cleanup needed: sub_tasks are now owned by the
    // qpair pool's SubTaskFreeList (heap-allocated, lifetime tied to
    // the qpair pool — outlives the worker thread).  See
    // SubTaskFreeList comment in transfer_task.h.

    VLOG(2) << "SpdkNofWorkerPool worker thread exiting";
}
#endif

// ============================================================================
// MemcpyWorkerPool Implementation
// ============================================================================
// Since memcpy is bound by memory bandwidth, we only need one worker thread.
constexpr int kDefaultMemcpyWorkers = 1;

MemcpyWorkerPool::MemcpyWorkerPool() : shutdown_(false) {
    VLOG(1) << "Creating MemcpyWorkerPool with " << kDefaultMemcpyWorkers
            << " workers";

    // Start worker threads
    workers_.reserve(kDefaultMemcpyWorkers);
    for (int i = 0; i < kDefaultMemcpyWorkers; ++i) {
        workers_.emplace_back(&MemcpyWorkerPool::workerThread, this);
    }
}

MemcpyWorkerPool::~MemcpyWorkerPool() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    VLOG(1) << "MemcpyWorkerPool destroyed";
}

void MemcpyWorkerPool::submitTask(MemcpyTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING)
                << "Attempting to submit task to shutdown MemcpyWorkerPool";
            task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            return;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

void MemcpyWorkerPool::workerThread() {
    VLOG(2) << "MemcpyWorkerPool worker thread started";

    while (true) {
        MemcpyTask task({}, nullptr);

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.state) {
            try {
                bool ok = true;
                auto runtime_accelerator =
                    device::GetAcceleratorRegistry().RuntimeAccelerators();
                for (const auto& op : task.operations) {
                    device::PointerInfo src_info;
                    device::PointerInfo dst_info;
                    auto* src_device = runtime_accelerator.FindDeviceForPointer(
                        op.src, &src_info);
                    auto* dst_device = runtime_accelerator.FindDeviceForPointer(
                        op.dest, &dst_info);

                    if (!src_device && !dst_device) {
                        std::memcpy(op.dest, op.src, op.size);
                    } else {
                        if (src_device && dst_device &&
                            src_device != dst_device) {
                            LOG(ERROR)
                                << "GPU memcpy failed: source and destination "
                                   "belong to different accelerator runtimes"
                                << " src_dev=" << src_info.device_id
                                << " dst_dev=" << dst_info.device_id
                                << " size=" << op.size;
                            ok = false;
                            break;
                        }
                        const device::AcceleratorDevice* accelerator = nullptr;
                        int32_t device_id = -1;
                        device::CopyDirection direction;
                        if (src_device) {
                            accelerator = src_device;
                            device_id = src_info.device_id;
                            direction = device::CopyDirection::kDeviceToHost;
                            if (dst_device) {
                                direction =
                                    device::CopyDirection::kDeviceToDevice;
                            }
                        } else {
                            accelerator = dst_device;
                            device_id = dst_info.device_id;
                            direction = device::CopyDirection::kHostToDevice;
                        }
                        accelerator->SetContext(device_id);
                        if (!accelerator->Copy(op.dest, op.src, op.size,
                                               direction)) {
                            LOG(ERROR) << "GPU memcpy failed: src_dev="
                                       << src_info.device_id
                                       << " dst_dev=" << dst_info.device_id
                                       << " size=" << op.size;
                            ok = false;
                            break;
                        }
                    }
                }

                VLOG(2) << "Memcpy task completed with "
                        << task.operations.size() << " operations"
                        << (ok ? "" : " (with GPU copy failure)");
                task.state->set_completed(ok ? ErrorCode::OK
                                             : ErrorCode::TRANSFER_FAIL);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async memcpy: " << e.what();
                task.state->set_completed(ErrorCode::TRANSFER_FAIL);
            }
        }
    }

    VLOG(2) << "MemcpyWorkerPool worker thread exiting";
}

// ============================================================================
// TransferEngineOperationState Implementation
// ============================================================================

bool TransferEngineOperationState::is_completed() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (result_.has_value()) {
        return true;
    }

    check_task_status();
    return result_.has_value();
}

void TransferEngineOperationState::check_task_status() {
    // Check all transfers in the batch.
    // Wait for ALL tasks to reach a terminal state before setting the result,
    // even if some have already failed. This prevents the caller from seeing
    // "completed" while background transfers are still in progress, which
    // could cause issues when freeBatchID is called in the destructor.
    bool all_terminated = true;
    std::vector<size_t> failed_task_ids;

    for (size_t i = 0; i < batch_size_; ++i) {
        TransferStatus status;
        Status s = engine_.getTransferStatus(batch_id_, i, status);
        if (!s.ok()) {
            LOG(ERROR) << "Failed to get transfer status for batch "
                       << batch_id_ << " task " << i << " with error "
                       << s.message();
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        switch (status.s) {
            case TransferStatusEnum::COMPLETED:
                // This transfer is done successfully
                break;
            case TransferStatusEnum::FAILED:
            case TransferStatusEnum::CANCELED:
            case TransferStatusEnum::INVALID:
#ifndef USE_ASCEND_DIRECT
                VLOG(1) << "Transfer failed for batch " << batch_id_ << " task "
                        << i << " with status " << static_cast<int>(status.s);
#endif
                failed_task_ids.push_back(i);
                break;
            default:
                // Transfer is still in progress (WAITING, PENDING, etc.)
                all_terminated = false;
                break;
        }
    }

    if (!all_terminated) {
        // Some tasks are still in progress; wait for next poll iteration.
        // Do NOT set result yet, even if some tasks have already failed.
        return;
    }

    // All tasks have reached a terminal state.
    ErrorCode ec = ErrorCode::OK;
    if (!failed_task_ids.empty()) {
        std::ostringstream oss;
        for (size_t j = 0; j < failed_task_ids.size(); ++j) {
            if (j > 0) oss << ", ";
            oss << failed_task_ids[j];
        }
        LOG(ERROR) << "Batch " << batch_id_
                   << " completed with task failures: task_ids=[" << oss.str()
                   << "]";
        ec = ErrorCode::TRANSFER_FAIL;
    }

    set_result_internal(ec);
}

void TransferEngineOperationState::set_result_internal(ErrorCode error_code) {
    if (result_.has_value()) {
        LOG(ERROR) << "Attempting to set result multiple times for batch "
                   << batch_id_
                   << ". Previous result: " << static_cast<int>(result_.value())
                   << ", attempted new result: " << static_cast<int>(error_code)
                   << ". This indicates a race condition or logic error.";
        return;  // Don't crash, just return early
    }

    VLOG(1) << "Setting transfer result for batch " << batch_id_ << " to "
            << static_cast<int>(error_code);
    result_.emplace(error_code);
}

void TransferEngineOperationState::wait_for_completion() {
    if (is_completed()) {
        return;
    }

    // 60 seconds
    constexpr int64_t timeout_milliseconds = 60 * 1000;

#ifdef USE_EVENT_DRIVEN_COMPLETION
    VLOG(1) << "Waiting for transfer engine completion for batch " << batch_id_;

    // Wait directly on BatchDesc's condition variable.
    auto& batch_desc = Transport::toBatchDesc(batch_id_);
    bool completed;
    bool failed = false;

    // Fast path: if already finished, avoid taking the mutex and waiting.
    // Use acquire here to pair with the writer's release-store, because this
    // path may skip taking the mutex. It ensures all prior updates are visible.
    completed = batch_desc.is_finished.load(std::memory_order_acquire);
    if (!completed) {
        // Use the same mutex as the notifier when updating the predicate to
        // avoid missed notifications. The predicate is re-checked under the
        // lock. Under the mutex, relaxed is sufficient; the mutex acquire
        // orders prior writes.
        std::unique_lock<std::mutex> lock(batch_desc.completion_mutex);
        const int64_t elapsed_milliseconds =
            getCurrentTimeInMilli() - start_ts_;
        if (elapsed_milliseconds < timeout_milliseconds) {
            completed = batch_desc.completion_cv.wait_for(
                lock,
                std::chrono::milliseconds(timeout_milliseconds -
                                          elapsed_milliseconds),
                [&batch_desc] {
                    return batch_desc.is_finished.load(
                        std::memory_order_relaxed);
                });
        }
    }  // Explicitly release completion_mutex before acquiring mutex_

    // Once completion is observed, read failure flag.
    if (completed) {
        failed = batch_desc.has_failure.load(std::memory_order_relaxed);
    }

    ErrorCode error_code =
        completed ? (failed ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK)
                  : ErrorCode::TRANSFER_FAIL;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        set_result_internal(error_code);
    }

    if (completed) {
        VLOG(1) << "Transfer engine operation completed for batch " << batch_id_
                << " with result: " << static_cast<int>(error_code);
    } else {
        LOG(ERROR) << "Failed to complete transfers after "
                   << timeout_milliseconds << " milliseconds for batch "
                   << batch_id_;
    }
#else
    VLOG(1) << "Starting transfer engine polling for batch " << batch_id_;

    while (true) {
        if (getCurrentTimeInMilli() - start_ts_ > timeout_milliseconds) {
            LOG(ERROR) << "Failed to complete transfers after "
                       << timeout_milliseconds << " milliseconds for batch "
                       << batch_id_;
            set_result_internal(ErrorCode::TRANSFER_FAIL);
            return;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        check_task_status();
        if (result_.has_value()) {
            VLOG(1) << "Transfer engine operation completed for batch "
                    << batch_id_
                    << " with result: " << static_cast<int>(result_.value());
            break;
        }
        // Continue polling
        VLOG(1) << "Transfer engine operation still pending for batch "
                << batch_id_;
    }
#endif
}

// ============================================================================
// TransferFuture Implementation
// ============================================================================

TransferFuture::TransferFuture(std::shared_ptr<OperationState> state)
    : state_(std::move(state)) {
    if (!state_) {
        LOG(ERROR) << "TransferFuture requires valid state";
        throw std::invalid_argument("TransferFuture requires valid state");
    }
}

bool TransferFuture::isReady() const { return state_->is_completed(); }

ErrorCode TransferFuture::wait() {
    if (!isReady()) {
        state_->wait_for_completion();
    }
    return state_->get_result();
}

ErrorCode TransferFuture::get() { return wait(); }

TransferStrategy TransferFuture::strategy() const {
    return state_->get_strategy();
}

// ============================================================================
// TransferSubmitter Implementation
// ============================================================================

TransferSubmitter::TransferSubmitter(TransferEngine& engine,
                                     std::shared_ptr<StorageBackend>& backend,
                                     const std::string& local_hostname,
                                     TransferMetric* transfer_metric,
                                     int numa_socket_id)
    : engine_(engine),
      local_endpoint_(engine.getLocalIpAndPort()),
      memcpy_pool_(std::make_unique<MemcpyWorkerPool>()),
#ifdef USE_NOF
      spdk_nvmf_pool_(std::make_unique<SpdkNofWorkerPool>(numa_socket_id)),
#endif
      fileread_pool_(std::make_unique<FilereadWorkerPool>(backend)),
      local_hostname_(local_hostname),
      transfer_metric_(transfer_metric) {
    // Read MC_STORE_MEMCPY environment variable.
    // When not set, auto-detect based on transport type:
    //   - TCP-only environment: enable memcpy (avoids TCP loopback overhead)
    //   - RDMA/other transports: disable memcpy (RDMA is more efficient)
    const auto config = TransferSubmitterConfig::FromEnvironment();
    if (config.memcpy_enabled_override.has_value()) {
        memcpy_enabled_ = *config.memcpy_enabled_override;
    } else {
        memcpy_enabled_ = engine_.isTcpOnly();
        LOG(INFO) << "MC_STORE_MEMCPY not set, auto-detected: "
                  << (memcpy_enabled_ ? "TCP-only environment, memcpy enabled"
                                      : "non-TCP transport available, memcpy "
                                        "disabled");
    }

    VLOG(1) << "TransferSubmitter initialized with memcpy_enabled="
            << memcpy_enabled_;
}

// Release cached NoF handles so that QIDs are recycled when a client is
// closed, rather than lingering until process exit.
//
// Ordering: spdk_nvmf_pool_ must be stopped and joined BEFORE cached NoF
// handles are closed.  Worker threads may still own queued/inflight
// SpdkNofTasks that dereference these handles and qpairs.  We explicitly
// reset the pool here (destructor body runs before member destruction) so
// that workers are joined first, then handles are released.
TransferSubmitter::~TransferSubmitter() {
#ifdef USE_NOF
    spdk_nvmf_pool_.reset();
    for (auto& [endpoint, handle] : nof_handle_cache_) {
        if (handle) {
            VLOG(1) << "TransferSubmitter releasing NoF handle for "
                    << endpoint;
            SpdkWrapper::GetInstance().CloseNofSegment(handle);
        }
    }
    nof_handle_cache_.clear();
#endif
}

std::optional<TransferFuture> TransferSubmitter::submit(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code, void* ptr, size_t size) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        if (!validateTransferParams(handle, slices)) {
            return std::nullopt;
        }

        if (op_code == TransferRequest::READ) {
            future = submitMemoryReadOperation(handle, slices, 0);
        } else {
            TransferStrategy strategy = selectStrategy(handle, slices);

            switch (strategy) {
                case TransferStrategy::LOCAL_MEMCPY:
                    future = submitMemcpyOperation(handle, slices, op_code);
                    break;
                case TransferStrategy::TRANSFER_ENGINE:
                    future =
                        submitTransferEngineOperation(handle, slices, op_code);
                    break;
                default:
                    LOG(ERROR) << "Unknown transfer strategy: " << strategy;
                    return std::nullopt;
            }
        }
    } else if (replica.is_nof_replica()) {
#ifdef USE_NOF
        auto& ssd_desc = replica.get_nof_descriptor();
        auto& handle = ssd_desc.buffer_descriptor;

        if (!ptr || (size == 0)) {
            return std::nullopt;
        }

        future = submitSpdkNofOperation(handle, ptr, size, op_code);
#else
        LOG(ERROR) << "NoF transfer requested while USE_NOF is disabled";
        return std::nullopt;
#endif
    } else {
        future = submitFileReadOperation(replica, slices, op_code);
    }

    // Update metrics on successful submission
    if (future.has_value()) {
        updateTransferMetrics(slices, op_code);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submit_batch(
    const std::vector<Replica::Descriptor>& replicas,
    std::vector<std::vector<Slice>>& all_slices,
    TransferRequest::OpCode op_code) {
    if (replicas.size() != all_slices.size()) {
        LOG(ERROR) << "Mismatched replicas and slice lists";
        return std::nullopt;
    }

    bool use_local_memcpy =
        op_code == TransferRequest::WRITE && !replicas.empty();
    size_t operation_count = 0;
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (!replicas[i].is_memory_replica()) {
            LOG(ERROR) << "Batch transfer only supports memory replicas";
            return std::nullopt;
        }
        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        if (!validateTransferParams(handle, all_slices[i])) {
            return std::nullopt;
        }
        use_local_memcpy =
            use_local_memcpy && canUseLocalMemcpy(handle.transport_endpoint_);
        operation_count += all_slices[i].size();
    }

    std::vector<TransferRequest> requests;
    std::vector<MemcpyOperation> memcpy_operations;
    if (use_local_memcpy)
        memcpy_operations.reserve(operation_count);
    else
        requests.reserve(operation_count);
    for (size_t i = 0; i < replicas.size(); ++i) {
        auto& slices = all_slices[i];
        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        if (use_local_memcpy) {
            appendMemcpyOperations(handle, slices, op_code, 0,
                                   memcpy_operations);
            continue;
        }
        uint64_t offset = 0;
        SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment "
                       << handle.transport_endpoint_;
            return std::nullopt;
        }
        for (const auto& slice : slices) {
            TransferRequest request;
            request.opcode = op_code;
            request.source = static_cast<char*>(slice.ptr);
            request.target_id = seg;
            request.target_offset = handle.buffer_address_ + offset;
            request.length = slice.size;
            requests.emplace_back(request);
            offset += slice.size;
        }
    }
    auto future = use_local_memcpy
                      ? submitMemcpyOperations(std::move(memcpy_operations))
                      : submitTransfer(requests);
    // Update metrics on successful submission
    if (future.has_value()) {
        for (auto& slices : all_slices) {
            updateTransferMetrics(slices, op_code);
        }
    }
    return future;
}

TransferEngine::ScatterTransferOperation TransferSubmitter::submitScatter(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers) {
    return engine_.submitScatter(transfers);
}

std::optional<TransferFuture>
TransferSubmitter::submit_batch_get_offload_object(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys, const std::vector<uint64_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batched_slices,
    OffloadBufferAccess buffer_access) {
    if (keys.size() != pointers.size()) {
        LOG(ERROR) << "Mismatched offload transfer argument counts";
        return std::nullopt;
    }

    const bool use_local_memcpy =
        buffer_access == OffloadBufferAccess::kLocalAddress;
    if (use_local_memcpy && !canUseLocalMemcpy(transfer_engine_addr)) {
        LOG(ERROR) << "Offload source is not locally addressable: "
                   << transfer_engine_addr;
        return std::nullopt;
    }

    std::vector<TransferRequest> requests;
    std::vector<MemcpyOperation> operations;
    constexpr uint64_t kMaxAddress = std::numeric_limits<uint64_t>::max();
    SegmentHandle seg = 0;
    if (!use_local_memcpy) {
        // Open once: all keys share the same transfer endpoint.
        seg = engine_.openSegment(transfer_engine_addr);
        if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
            LOG(ERROR) << "Failed to open segment " << transfer_engine_addr;
            return std::nullopt;
        }
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        auto it = batched_slices.find(key);
        if (it == batched_slices.end()) {
            LOG(ERROR) << "Key not found in batched_slices: " << key;
            return std::nullopt;
        }
        uint64_t offset = 0;
        for (const auto& slice : it->second) {
            if (slice.size == 0) continue;
            if (!slice.ptr || pointers[i] > kMaxAddress - offset ||
                slice.size > kMaxAddress - pointers[i] - offset) {
                LOG(ERROR) << "Invalid offload transfer range for key: " << key;
                return std::nullopt;
            }
            if (use_local_memcpy) {
                operations.emplace_back(
                    slice.ptr,
                    reinterpret_cast<const void*>(pointers[i] + offset),
                    slice.size);
            } else {
                requests.emplace_back(TransferRequest{
                    .opcode = TransferRequest::READ,
                    .source = static_cast<char*>(slice.ptr),
                    .target_id = seg,
                    .target_offset = pointers[i] + offset,
                    .length = slice.size,
                });
            }
            offset += slice.size;
        }
    }
    return use_local_memcpy ? submitMemcpyOperations(std::move(operations))
                            : submitTransfer(requests);
}

void TransferSubmitter::appendMemcpyOperations(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t buffer_offset,
    std::vector<MemcpyOperation>& operations) {
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = buffer_offset;

    for (const auto& slice : slices) {
        if (slice.ptr == nullptr) continue;

        void* dest;
        const void* src;
        if (op_code == TransferRequest::READ) {
            dest = slice.ptr;
            src = reinterpret_cast<const void*>(base_address + offset);
        } else {
            dest = reinterpret_cast<void*>(base_address + offset);
            src = slice.ptr;
        }
        offset += slice.size;
        operations.emplace_back(dest, src, slice.size);
    }
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    std::vector<MemcpyOperation> operations;
    operations.reserve(slices.size());
    appendMemcpyOperations(handle, slices, op_code, src_offset, operations);
    return submitMemcpyOperations(std::move(operations));
}

std::optional<TransferFuture> TransferSubmitter::submitMemcpyOperations(
    std::vector<MemcpyOperation> operations) {
    auto state = std::make_shared<MemcpyOperationState>();
    const size_t operation_count = operations.size();
    MemcpyTask task(std::move(operations), state);
    memcpy_pool_->submitTask(std::move(task));

    VLOG(1) << "Memcpy transfer submitted to worker pool with "
            << operation_count << " operations";

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransfer(
    std::vector<TransferRequest>& requests) {
    // Allocate batch ID
    const size_t batch_size = requests.size();
    BatchID batch_id = engine_.allocateBatchID(batch_size);
    if (batch_id == INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        return std::nullopt;
    }

    // Submit transfer
    Status s = engine_.submitTransfer(batch_id, requests);
    if (!s.ok()) {
        LOG(ERROR) << "Failed to submit all transfers, error code is "
                   << s.code();
        // Note: batch_id will be freed by TransferEngineOperationState
        // destructor if we create the state object, otherwise we need to free
        // it here
        engine_.freeBatchID(batch_id);
        return std::nullopt;
    }

    if (batch_id == INVALID_BATCH_ID) {  // INVALID_BATCH_ID
        LOG(ERROR) << "Invalid batch ID for transfer engine operation";
        return std::nullopt;
    }

    // Create state with transfer engine context - no polling thread
    // needed
    auto state = std::make_shared<TransferEngineOperationState>(
        engine_, batch_id, batch_size);

    return TransferFuture(state);
}

std::optional<TransferFuture> TransferSubmitter::submitTransferEngineOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    const TransferRequest::OpCode op_code, uint64_t src_offset) {
    if (handle.transport_endpoint_.empty()) {
        LOG(ERROR) << "Transport endpoint is empty for handle with address "
                   << handle.buffer_address_;
        return std::nullopt;
    }
    SegmentHandle seg = engine_.openSegment(handle.transport_endpoint_);

    if (seg == static_cast<uint64_t>(ERR_INVALID_ARGUMENT)) {
        LOG(ERROR) << "Failed to open segment for endpoint='"
                   << handle.transport_endpoint_ << "'";
        return std::nullopt;
    }

    // Create transfer requests
    std::vector<TransferRequest> requests;
    requests.reserve(slices.size());
    uint64_t base_address = static_cast<uint64_t>(handle.buffer_address_);
    uint64_t offset = src_offset;

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) continue;

        TransferRequest request;
        request.opcode = op_code;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = base_address + offset;
        request.length = slice.size;

        offset += slice.size;
        requests.emplace_back(request);
    }
    return submitTransfer(requests);
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryReadOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t src_offset) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::READ,
                                     src_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(handle, slices,
                                             TransferRequest::READ, src_offset);
    }

    LOG(ERROR) << "Read only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitMemoryWriteOperation(
    const AllocatedBuffer::Descriptor& handle, const std::vector<Slice>& slices,
    uint64_t dst_offset) {
    TransferStrategy strategy = selectStrategy(handle, slices);

    if (strategy == TransferStrategy::LOCAL_MEMCPY) {
        return submitMemcpyOperation(handle, slices, TransferRequest::WRITE,
                                     dst_offset);
    }
    if (strategy == TransferStrategy::TRANSFER_ENGINE) {
        return submitTransferEngineOperation(
            handle, slices, TransferRequest::WRITE, dst_offset);
    }

    LOG(ERROR) << "Write only supports LOCAL_MEMCPY or TRANSFER_ENGINE, got: "
               << strategy;
    return std::nullopt;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeRead(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t src_offset) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (src_offset > std::numeric_limits<uint64_t>::max() - slices_size ||
            src_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range read overflow: src_offset=" << src_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryReadOperation(handle, slices, src_offset);
    } else if (replica.is_nof_replica()) {
        LOG(ERROR) << "Range read not supported for NoF replicas";
        return std::nullopt;
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range read not supported for disk replicas (use full read)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::READ);
    }

    return future;
}

std::optional<TransferFuture> TransferSubmitter::submitRangeWrite(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    std::optional<TransferFuture> future;

    if (replica.is_memory_replica()) {
        auto& mem_desc = replica.get_memory_descriptor();
        auto& handle = mem_desc.buffer_descriptor;

        size_t slices_size = 0;
        for (const auto& s : slices) slices_size += s.size;
        if (dst_offset > std::numeric_limits<uint64_t>::max() - slices_size ||
            dst_offset + slices_size > handle.size_) {
            LOG(ERROR) << "Range write overflow: dst_offset=" << dst_offset
                       << " + slices_size=" << slices_size
                       << " > handle.size_=" << handle.size_;
            return std::nullopt;
        }

        future = submitMemoryWriteOperation(handle, slices, dst_offset);
    } else if (replica.is_nof_replica()) {
        LOG(ERROR) << "Range write not supported for NoF replicas";
        return std::nullopt;
    } else if (replica.is_disk_replica() || replica.is_local_disk_replica()) {
        LOG(ERROR)
            << "Range write not supported for disk replicas (use full write)";
        return std::nullopt;
    }

    if (future.has_value()) {
        updateTransferMetrics(slices, TransferRequest::WRITE);
    }

    return future;
}

#ifdef USE_NOF
std::optional<TransferFuture> TransferSubmitter::submitSpdkNofOperation(
    const AllocatedBuffer::Descriptor& handle, void* ptr, size_t size,
    const TransferRequest::OpCode op_code) {
    if (handle.transport_endpoint_.empty() || handle.size_ < size) {
        LOG(ERROR) << "Invalid NoF request endpoint="
                   << handle.transport_endpoint_
                   << ", buffer_size=" << handle.size_
                   << ", request_size=" << size;
        return std::nullopt;
    }

    // Per-TransferSubmitter handle cache — reuses the connection
    // across multiple put() calls from the same client to avoid
    // exhausting QIDs and SPDK internal state with new connections.
    // First-open is single-flight per endpoint so concurrent submitters
    // for the same endpoint wait for the designated opener instead of
    // racing to OpenNofSegment (which can fail spuriously when one
    // caller loses QIDs while another succeeds and populates the cache).
    const std::string& endpoint = handle.transport_endpoint_;
    nof_seg_handle* seg_handle = nullptr;
    bool became_opener = false;
    {
        std::unique_lock<std::mutex> open_lock(nof_open_inflight_mutex_);

        // Wait while another thread is opening this endpoint.  The
        // predicate is "this endpoint is NOT in flight" — once cleared
        // by the prior opener, either the cache is populated (so we
        // become a cache-hit caller) or the slot is free for a new
        // opener.
        nof_open_done_cv_.wait(open_lock, [this, &endpoint] {
            return nof_open_inflight_.find(endpoint) ==
                   nof_open_inflight_.end();
        });

        // Re-check cache after the wait (populated by a prior opener).
        {
            std::lock_guard<std::mutex> cache_lock(nof_cache_mutex_);
            auto cache_it = nof_handle_cache_.find(endpoint);
            if (cache_it != nof_handle_cache_.end()) {
                seg_handle = cache_it->second;
            }
        }

        if (!seg_handle) {
            // Designated opener: mark in-flight and release open_lock
            // before the slow OpenNofSegment call so concurrent
            // submitters for OTHER endpoints are not blocked.
            nof_open_inflight_.insert(endpoint);
            became_opener = true;
        }
    }

    if (became_opener) {
        // Slow path: open without holding any lock so backoff/retry
        // does not block cache lookups for other endpoints or waiters
        // for this one.
        auto* new_handle = SpdkWrapper::GetInstance().OpenNofSegment(endpoint);

        // Publish: populate cache on success, drop on failure.  Then
        // clear in-flight so waiters can proceed.
        nof_seg_handle* handle_to_discard = nullptr;
        {
            std::lock_guard<std::mutex> cache_lock(nof_cache_mutex_);
            if (new_handle) {
                auto cache_it = nof_handle_cache_.find(endpoint);
                if (cache_it != nof_handle_cache_.end()) {
                    // Another opener won the race while we were
                    // opening (possible across TransferSubmitters
                    // sharing this handle map — currently impossible
                    // since each TransferSubmitter owns its own cache,
                    // but kept defensively in case that changes).
                    handle_to_discard = new_handle;
                    seg_handle = cache_it->second;
                } else {
                    nof_handle_cache_[endpoint] = new_handle;
                    seg_handle = new_handle;
                }
            }
        }
        {
            std::lock_guard<std::mutex> open_lock(nof_open_inflight_mutex_);
            nof_open_inflight_.erase(endpoint);
        }
        nof_open_done_cv_.notify_all();

        if (handle_to_discard) {
            SpdkWrapper::GetInstance().CloseNofSegment(handle_to_discard);
        }

        if (!seg_handle) {
            LOG(ERROR) << "Failed to open NoF segment endpoint=" << endpoint;
            return std::nullopt;
        }
    }

    // NOTE: an early-reject on IsDraining() was removed here.  Doing
    // so races with the worker's PollAll path: the test injects a
    // synthetic qpair transport error AFTER the primer submit but
    // BEFORE the test's subsequent submits, so by the time the test
    // thread reaches this point the pool may have flipped to
    // kDraining — returning nullopt here would make the test's
    // submitSpdkNofOperation calls fail spuriously (fut.has_value()
    // == false).  Instead we let the submits enter the worker queue;
    // the worker's SubmitRequest path goes through GetNextQpair →
    // returns nullptr → SubmitRequest returns -1 → the worker's
    // sync-failure path (FinalizeSubmittedTask) marks the task
    // failed and routes it through SpdkNofTaskCompletion, so the
    // future still reaches a terminal state with TRANSFER_FAIL.

    uint32_t block_size = SpdkWrapper::GetInstance().GetBlockSize(seg_handle);
    if (block_size == INVALID_BLOCK_SIZE ||
        handle.buffer_address_ % block_size != 0 || size % block_size != 0 ||
        reinterpret_cast<std::uintptr_t>(ptr) % block_size != 0) {
        LOG(ERROR) << "NoF request offset=" << handle.buffer_address_
                   << ", ptr=" << ptr << ", size=" << size
                   << " is not aligned to block size " << block_size;
        return std::nullopt;
    }

    auto state = std::make_shared<SpdkNofOperationState>();
    SpdkNofTask task(seg_handle, ptr, handle.buffer_address_ / block_size,
                     size / block_size, op_code, state);
    spdk_nvmf_pool_->submitTask(std::move(task));

    VLOG(1) << "SPDK NoF transfer submitted to " << handle.transport_endpoint_;
    return TransferFuture(state);
}
#endif

std::optional<TransferFuture> TransferSubmitter::submitFileReadOperation(
    const Replica::Descriptor& replica, std::vector<Slice>& slices,
    TransferRequest::OpCode op_code) {
    auto state = std::make_shared<FilereadOperationState>();
    auto disk_replica = replica.get_disk_descriptor();
    std::string file_path = disk_replica.file_path;
    size_t file_length = disk_replica.object_size;

    // Submit memcpy operations to worker pool for async execution
    FilereadTask task(file_path, file_length, slices, state);
    fileread_pool_->submitTask(std::move(task));

    VLOG(1) << "Fileread transfer submitted to worker pool with " << file_path;

    return TransferFuture(state);
}

TransferStrategy TransferSubmitter::selectStrategy(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& /* slices */) const {
    return canUseLocalMemcpy(handle.transport_endpoint_)
               ? TransferStrategy::LOCAL_MEMCPY
               : TransferStrategy::TRANSFER_ENGINE;
}

bool TransferSubmitter::canUseLocalMemcpy(const std::string& endpoint) const {
    return memcpy_enabled_ &&
           (isSameProcessEndpoint(endpoint, local_hostname_) ||
            isSameProcessEndpoint(endpoint, local_endpoint_));
}

bool TransferSubmitter::isSameProcessEndpoint(
    const std::string& handle_endpoint, const std::string& local_endpoint) {
    // Local memcpy requires that handle.buffer_address_ is a virtual address
    // valid in THIS process. Same host is not enough: two processes on the
    // same host share an IP but have distinct virtual address spaces, so a
    // memcpy on a peer process's address would segfault. Require the full
    // transport endpoint to match, which uniquely identifies the owning
    // process.
    return !handle_endpoint.empty() && handle_endpoint == local_endpoint;
}

bool TransferSubmitter::validateTransferParams(
    const AllocatedBuffer::Descriptor& handle,
    const std::vector<Slice>& slices) const {
    uint64_t all_slice_len = 0;
    for (auto slice : slices) {
        all_slice_len += slice.size;
    }
    if (handle.size_ != all_slice_len) {
        LOG(ERROR) << "handles len:" << handle.size_
                   << ", all_slice_len:" << all_slice_len;
        return false;
    }
    return true;
}

void TransferSubmitter::updateTransferMetrics(const std::vector<Slice>& slices,
                                              TransferRequest::OpCode op_code) {
    size_t total_bytes = 0;
    for (const auto& slice : slices) {
        total_bytes += slice.size;
    }

    if (transfer_metric_ == nullptr) {
        return;
    }

    if (op_code == TransferRequest::READ) {
        transfer_metric_->total_read_bytes.inc(total_bytes);

    } else if (op_code == TransferRequest::WRITE) {
        transfer_metric_->total_write_bytes.inc(total_bytes);
    }
}

}  // namespace mooncake
