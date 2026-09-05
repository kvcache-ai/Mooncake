#include <glog/logging.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <thread>
#include "spdk/spdk_wrapper.h"

namespace mooncake {

// File-scope counter tracking the number of active SpdkNofWorkerPool
// instances.  Must be file-scope (not a SpdkWrapper member) because the
// SpdkWrapper singleton may already be destroyed during static
// destruction, so WorkerPool destructors cannot access member variables.
// SpdkWrapper::Cleanup() checks this counter: > 0 means a WorkerPool has
// not been joined yet.  Freeing qpairs at that point would cause worker
// threads to poll freed memory → segfault.
static std::atomic<int> g_active_worker_count{0};

void SpdkNoF_RegisterWorkerPool() {
    g_active_worker_count.fetch_add(1, std::memory_order_relaxed);
}

void SpdkNoF_UnregisterWorkerPool() {
    g_active_worker_count.fetch_sub(1, std::memory_order_relaxed);
}

// NVMe-oF resources are managed through the NofConnection +
// NofSegment abstraction layer; nof_seg_handle is defined in the header.

SpdkWrapper::SpdkWrapper() = default;

SpdkWrapper::~SpdkWrapper() { Cleanup(); }

SpdkWrapper &SpdkWrapper::GetInstance() {
    static SpdkWrapper ins;
    return ins;
}

bool SpdkWrapper::InitializeEnv() {
    if (initialized.load(std::memory_order_acquire)) {
        return true;
    }

    std::lock_guard<std::mutex> lock(init_mutex);
    if (initialized.load(std::memory_order_acquire)) {
        return true;
    }

    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "mooncake";

    int rc = spdk_env_init(&opts);
    if (rc != 0) {
        fprintf(stderr, "SPDK init failed: %d\n", rc);
        return false;
    }

    // Read NoF config from environment (MC_NVME_* vars).
    // Must happen before any OpenNofSegment call so that num_io_queues
    // and other tuning parameters take effect.
    config_ = NofConfig::FromEnv();
    LOG(INFO) << "SpdkWrapper config: num_io_queues=" << config_.num_io_queues
              << ", io_queue_size=" << config_.io_queue_size
              << ", chunk_blocks=" << config_.chunk_blocks;

    // Use a dedicated config for heartbeat probes.
    // Probes need only 1 qpair and must not compete with the I/O path
    // for QID slots.
    config_probe_ = NofConfig::ForProbe();
    LOG(INFO) << "SpdkWrapper probe config: num_io_queues="
              << config_probe_.num_io_queues;

    // Mark SPDK as initialized.
    initialized.store(true, std::memory_order_release);
    return true;
}

void SpdkWrapper::Cleanup() {
    if (initialized.load(std::memory_order_acquire)) {
        // [Diagnostic] Detect incorrect destruction order.
        // g_active_worker_count is a file-scope variable independent of
        // the SpdkWrapper singleton lifetime.  If > 0, a WorkerPool still
        // has active workers that have not been joined.  Releasing qpairs
        // here would cause those workers to poll freed memory → segfault.
        int alive = g_active_worker_count.load(std::memory_order_relaxed);
        if (alive > 0) {
            LOG(FATAL)
                << "SpdkWrapper::Cleanup() called while " << alive
                << " SpdkNofWorkerPool instance(s) still active. "
                << "WorkerPool must be fully stopped and joined BEFORE "
                   "Cleanup. "
                << "Check static destruction order — SpdkWrapper must outlive "
                << "all SpdkNofWorkerPool instances.";
        }

        // Clean up the new open_segments_ design: NofConnection is
        // auto-destroyed by unique_ptr (including qpair pool and ctrlr
        // detach); only NofSegment and nof_seg_handle must be freed
        // manually.
        {
            std::lock_guard<std::mutex> lock(segments_mutex_);
            for (auto &[handle, conn] : open_segments_) {
                if (handle) {
                    delete handle->segment;  // Allocated by OpenNofSegment
                    delete handle;           // Allocated by OpenNofSegment
                }
                // conn (unique_ptr<NofConnection>) auto-destructs,
                // freeing the qpair pool and detaching the ctrlr.
            }
            open_segments_.clear();
        }

        {
            std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
            for (auto &[_, probe_buffer] : probe_buffers_) {
                if (probe_buffer && probe_buffer->ptr) {
                    spdk_free(probe_buffer->ptr);
                    probe_buffer->ptr = nullptr;
                    probe_buffer->size = 0;
                }
            }
            probe_buffers_.clear();
        }
        spdk_env_fini();
        initialized.store(false, std::memory_order_release);
    }
}

void *SpdkWrapper::Alloc(size_t size, size_t align, int socket_id) {
    if (!InitializeEnv()) {
        return nullptr;
    }

    return spdk_zmalloc(size, align, nullptr, socket_id, SPDK_MALLOC_DMA);
}

void SpdkWrapper::Free(void *ptr) {
    if (ptr) {
        spdk_free(ptr);
    }
}

void SpdkWrapper::ProbeReadComplete(void *ctx,
                                    const struct spdk_nvme_cpl *cpl) {
    // ctx is guaranteed alive because:
    //   (a) ProbeNofSegment pushed its shared_ptr to ProbeCtxRecycler
    //       before returning;
    //   (b) ProbeCtxRecycler drains only at the start of the next
    //       probe, after ~NofQpairPool has finished
    //       (fail + drain + free_io_qpair), so any callback firing
    //       now or in the future finds the ctx alive.
    // No self-extending shared_ptr is needed: lifecycle is managed by
    // ownership transfer to ProbeCtxRecycler, not by re-acquiring
    // shared_from_this (which would risk bad_weak_ptr if SPDK ever
    // fired the callback after the recycler drained).
    auto *probe_ctx = reinterpret_cast<ProbeRequestContext *>(ctx);
    if (spdk_nvme_cpl_is_error(cpl)) {
        {
            std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
            probe_ctx->error_reason =
                std::string("completion_error:") +
                spdk_nvme_cpl_get_status_string(&cpl->status);
        }
        probe_ctx->success.store(false, std::memory_order_release);
    } else {
        probe_ctx->success.store(true, std::memory_order_release);
    }
    probe_ctx->done.store(true, std::memory_order_release);
}

// Delegates to NofSegment::PollCompletion.
int64_t SpdkWrapper::NvmePollProcessCompletion(nof_seg_handle *seg,
                                               uint32_t complete_per_seg) {
    if (!seg || !seg->segment) return -1;
    return seg->segment->PollCompletion(complete_per_seg);
}

// ---------------------------------------------------------------------------
// ProbeCtxRecycler — defers release of probe context to the next probe.
// ---------------------------------------------------------------------------
//
// Single-instance holder for probe_ctx shared_ptrs that have outlived
// their conn. The recycler drains at the start of the next probe, after
// the previous probe's NofConnection (and ~NofQpairPool) has been fully
// torn down.  By the time Drain() releases its entries, SPDK cannot
// possibly still be holding a reference to any of them.
ProbeCtxRecycler &ProbeCtxRecycler::Instance() {
    static ProbeCtxRecycler inst;
    return inst;
}

void ProbeCtxRecycler::Push(std::shared_ptr<ProbeRequestContext> ctx) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.push_back(std::move(ctx));
}

void ProbeCtxRecycler::PushWithConn(std::shared_ptr<ProbeRequestContext> ctx,
                                    std::unique_ptr<NofConnection> conn) {
    // Convenience overload for callers that do NOT have a wrapper to
    // defer (e.g. legacy/test paths).  Forwards to the wrapper-aware
    // variant with a null wrapper shared_ptr.
    PushWithConn(std::move(ctx), std::move(conn), nullptr);
}

void ProbeCtxRecycler::PushWithConn(
    std::shared_ptr<ProbeRequestContext> ctx,
    std::unique_ptr<NofConnection> conn,
    std::shared_ptr<ProbeSubmitWrapper> wrapper) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.push_back(std::move(ctx));
    // Ownership of the conn transfers here.  When pending_conns_ is
    // drained, ~NofConnection runs, which runs ~NofQpairPool, which
    // performs the quiescent wait + free_io_qpair.  By that time
    // SPDK must have stopped delivering CQEs for this ctx.
    pending_conns_.push_back(std::move(conn));
    // Wrapper lifetime extension: when the timeout-fallback path is
    // used, the wrapper holds a raw pool pointer that would dangle
    // once the local submit_wrapper_sp in ProbeNofSegment goes out of
    // scope.  Push the wrapper here too so it survives until after
    // ~NofQpairPool has freed the qpair.  Drain() drops
    // pending_wrappers_ AFTER pending_conns_ for exactly this reason.
    if (wrapper) {
        pending_wrappers_.push_back(std::move(wrapper));
    }
}

void ProbeCtxRecycler::Drain() {
    // Swap all three pending lists out under the lock, then release
    // outside.  Releasing outside the lock keeps Drain() short and
    // avoids holding the recycler mutex across user-supplied
    // destructors.
    //
    // Destruction order:
    //   1) ctx shared_ptrs drop → ProbeRequestContext freed.
    //   2) conn unique_ptrs drop → ~NofConnection → ~NofQpairPool
    //      → drain + WaitForInflightCompletion + free_io_qpair.
    //   3) wrapper shared_ptrs drop → ProbeSubmitWrapper freed.
    // We deliberately drop ctx FIRST so that by the time ~NofQpairPool
    // frees the qpair, no user code can dereference the ctx anymore.
    // We drop conn BEFORE wrappers so that any callback firing during
    // the quiescent wait (step 2) still finds a valid wrapper
    // (ProbeIoTrampoline accesses w->pool and w->probe_ctx).
    //
    // We achieve the desired destruction order with three nested
    // scope blocks: the outermost local is destroyed LAST, so we
    // declare the wrapper list outermost.
    std::vector<std::shared_ptr<ProbeSubmitWrapper>> to_release_wrapper;
    {
        std::vector<std::unique_ptr<NofConnection>> to_release_conn;
        {
            std::vector<std::shared_ptr<ProbeRequestContext>> to_release_ctx;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                to_release_ctx.swap(pending_);
                to_release_conn.swap(pending_conns_);
                to_release_wrapper.swap(pending_wrappers_);
            }
            // to_release_ctx drops FIRST here (innermost scope ends).
        }
        // to_release_conn drops SECOND here → ~NofConnection →
        // ~NofQpairPool → drain + WaitForInflightCompletion + free_io_qpair.
        // Any callback firing during the quiescent wait still has a
        // valid wrapper (held by to_release_wrapper in the outer scope).
    }
    // to_release_wrapper drops LAST here.  By this point free_io_qpair
    // has returned, so no callback can fire against the wrapper.
}

// ---------------------------------------------------------------------------
// ProbeSubmitWrapper — bridges the user-callback API to the inflight counter.
//
// The struct itself is declared top-level in spdk_wrapper.h (so
// ProbeCtxRecycler can hold shared_ptr<ProbeSubmitWrapper> and extend
// its lifetime).  This file contributes only the trampoline.
//
// Why we need this:
//   ProbeNofSegment submits via SubmitRequest, which takes a user callback
//   (void(void*, const spdk_nvme_cpl*)).  We need every callback to:
//     (a) call ProbeReadComplete (writes done/success);
//     (b) DecrementInflight on the qpair pool (so WaitForInflightCompletion
//         can prove quiescence).
//   We can't store a pool pointer in ProbeRequestContext itself because
//   the ctx outlives the conn (it's transferred to ProbeCtxRecycler), and
//   a dangling pool pointer would be a UAF.
//
//   Lifetime:
//   - Happy path: submit_wrapper_sp is dropped at ProbeNofSegment return,
//     AFTER WaitForInflightCompletion observed InflightCount==0.  No
//     callback can fire in the future.
//   - Timeout-fallback path: submit_wrapper_sp is moved into
//     ProbeCtxRecycler alongside ctx and conn.  Drain() drops the wrapper
//     AFTER the conn destructor (which runs ~NofQpairPool's quiescent
//     wait + free_io_qpair).  This closes the window where a late
//     callback could fire against a freed wrapper.
//
//   The trampoline receives the raw pointer (void* ctx parameter).  It
//   does NOT free the wrapper — the caller (ProbeNofSegment) or the
//   recycler drops it once we are guaranteed no callback can fire.
// ---------------------------------------------------------------------------
namespace {

void ProbeIoTrampoline(void *ctx, const struct spdk_nvme_cpl *cpl) {
    auto *w = static_cast<ProbeSubmitWrapper *>(ctx);
    SpdkWrapper::ProbeReadComplete(w->probe_ctx.get(), cpl);
    w->pool->DecrementInflight();
    // The wrapper is NOT freed here.  It is held alive by either:
    //   (a) submit_wrapper_sp in ProbeNofSegment (happy path), or
    //   (b) ProbeCtxRecycler::pending_wrappers_ (timeout-fallback path).
    // Both keep the wrapper valid until SPDK cannot fire any more
    // callbacks on this qpair.  See ProbeSubmitWrapper's lifetime
    // contract in spdk_wrapper.h for details.
}

}  // anonymous namespace

// OpenNofSegment uses NofConnection::Connect() under connect_mutex_
// because spdk_nvme_probe is not thread-safe.
//
// Each connection holds an independent set of qpairs (1 connection per
// ClientService).  SPDK requires each qpair to be owned by a single
// thread, and each SpdkNofWorkerPool is dedicated to one ClientService.
// Sharing a handle across WorkerPools would let multiple worker threads
// access the same qpair pool concurrently, stealing each other's
// completions and underflowing inflight counters.
nof_seg_handle *SpdkWrapper::OpenNofSegment(const std::string &tr_str) {
    if (!InitializeEnv()) return nullptr;

    // Adaptive QID pressure handling with retry moved outside
    // the connect_mutex_ lock.
    // - Re-evaluate pressure via GetRecommended() before each retry so
    //   concurrent client failures are visible.
    // - Report failures immediately via Record() so other clients see
    //   pressure in real time.
    // - Gradual degradation: halve the target each cycle rather than
    //   retrying with the same value N times.
    std::string error;
    std::unique_ptr<NofConnection> conn;
    uint32_t max_retries =
        config_.enable_degradation ? config_.retry_max_attempts : 0;
    uint32_t current_target = config_.num_io_queues;

    for (uint32_t attempt = 0; attempt <= max_retries; attempt++) {
        // Re-evaluate QID pressure before each retry to pick up
        // Record() events from concurrent clients.
        if (config_.enable_degradation && attempt > 0) {
            uint32_t recommended =
                qid_pressure_gauge_.GetRecommended(config_.num_io_queues);
            // Gradual degradation: take the minimum of the gauge
            // recommendation and half the previous target.
            uint32_t degraded = std::max(current_target / 2, 1u);
            current_target = std::min(recommended, degraded);
        }

        NofConfig adaptive_config = config_;
        adaptive_config.num_io_queues = current_target;

        {
            std::lock_guard<std::mutex> connect_lock(connect_mutex_);
            conn = NofConnection::Connect(tr_str, adaptive_config, &error);
        }

        if (conn) break;

        // Report failure immediately so concurrent clients can
        // observe the pressure event.
        qid_pressure_gauge_.Record(current_target, 0);

        bool is_qid_exhaustion =
            (error.find("qpair_alloc_fail") != std::string::npos);
        if (!is_qid_exhaustion || attempt >= max_retries) break;

        // Exponential backoff capped at 30 s to prevent unbounded
        // blocking under extreme config values (retry_max_attempts=10,
        // retry_backoff_ms=5000 → uncapped total ~85 min).
        auto wait_ms =
            std::min(config_.retry_backoff_ms * (1u << attempt), 30000u);
        LOG(WARNING) << "SpdkWrapper::OpenNofSegment: QID exhausted"
                     << " (target=" << current_target << "), waiting "
                     << wait_ms << "ms"
                     << " (attempt " << (attempt + 1) << "/" << max_retries
                     << ") for " << tr_str;
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
    }

    if (!conn) {
        LOG(ERROR) << "SpdkWrapper::OpenNofSegment failed for " << tr_str
                   << ": " << error;
        return nullptr;
    }

    // Report successful allocation to the pressure gauge to
    // restore Green status.
    uint32_t actual_qpairs = conn->GetQpairPool().Size();
    qid_pressure_gauge_.Record(current_target, actual_qpairs);

    // Wrap allocations in unique_ptr so that a subsequent exception
    // (e.g. bad_alloc from the handle allocation, or map insertion
    // failure) does not leak the segment.
    auto segment = std::unique_ptr<NofSegment>(
        new NofSegment(conn.get(), 0, conn->GetNumBlocks()));
    auto handle =
        std::unique_ptr<nof_seg_handle>(new nof_seg_handle{segment.get()});

    {
        std::lock_guard<std::mutex> lock(segments_mutex_);
        open_segments_[handle.get()] = std::move(conn);
    }

    LOG(INFO) << "SpdkWrapper::OpenNofSegment OK: "
              << "subnqn=" << segment->GetConnection()->GetSubnqn()
              << " qpairs=" << segment->GetConnection()->GetQpairPool().Size()
              << " (requested=" << current_target << ")";

    segment.release();
    return handle.release();
}

// CloseNofSegment: release all resources allocated by OpenNofSegment.
//
// Safety contract:
//   The caller MUST have joined any SpdkNofWorkerPool that may have
//   submitted I/O through this handle.  Closing the handle while a
//   worker is still running will cause in-flight callbacks (e.g.
//   nvmf_io_complete) to access freed SpdkNofSubTask/SpdkNofTask
//   memory.
//
//   Today only ~TransferSubmitter is a legitimate caller.  Its
//   destructor body runs `spdk_nvmf_pool_.reset()` BEFORE iterating
//   nof_handle_cache_ to call CloseNofSegment — this guarantees
//   worker pool join precedes handle close.
//
//   Why we don't track this automatically: transfer tasks do NOT call
//   NofQpairPool::IncrementInflight (they track in-flight via
//   task->outstanding_sub_io and total_outstanding_io in the worker
//   loop).  So ~NofQpairPool's WaitForInflightCompletion is a
//   no-op for transfer paths — InflightCount==0 trivially — and the
//   safety guarantee comes from caller ordering, not from the pool
//   itself.  Future callers MUST follow the same protocol.
//
//   Probe paths (ProbeNofSegment) use a separate temporary conn that
//   is auto-destructed; they are unaffected by CloseNofSegment.
void SpdkWrapper::CloseNofSegment(nof_seg_handle *handle) {
    if (!handle) return;

    std::lock_guard<std::mutex> lock(segments_mutex_);
    auto it = open_segments_.find(handle);
    if (it == open_segments_.end()) {
        // Already closed (e.g. Cleanup() ran first during static
        // destruction).  Do NOT double-free — the pointers were
        // deleted when the entry was erased from open_segments_.
        return;
    }

    delete handle->segment;  // alloc'd by OpenNofSegment
    delete handle;           // alloc'd by OpenNofSegment
    // ~unique_ptr<NofConnection> runs:
    //   ~NofConnection → reset qpair_pool_ →
    //   ~NofQpairPool → drain + WaitForInflightCompletion + free_io_qpair.
    // For transfer paths the inflight counter is trivially 0 because
    // Increment/Decrement is only called from the Probe path.  Real
    // safety for transfer paths comes from ~TransferSubmitter joining
    // the worker pool before closing handles.
    open_segments_.erase(it);
}

// Obtain block size via NofSegment.
uint32_t SpdkWrapper::GetBlockSize(const nof_seg_handle *seg_handle) {
    if (!seg_handle || !seg_handle->segment) {
        return INVALID_BLOCK_SIZE;
    }
    return seg_handle->segment->GetBlockSize();
}
// SubmitRequest: delegates to NofSegment
int SpdkWrapper::SubmitRequest(const nof_seg_handle *seg_handle, void *ptr,
                               uint64_t lba, uint32_t lba_count, int op,
                               spdk_nvme_cmd_cb cb_fn, void *cb_ctx) {
    if (!seg_handle || !seg_handle->segment) return -1;

    auto *seg = seg_handle->segment;
    if (op == kSpdkNofOpRead)
        return seg->SubmitRead(ptr, lba, lba_count, cb_fn, cb_ctx);
    if (op == kSpdkNofOpWrite)
        return seg->SubmitWrite(ptr, lba, lba_count, cb_fn, cb_ctx);
    return -1;
}

SpdkWrapper::ProbeBuffer *SpdkWrapper::GetOrCreateProbeBuffer(
    const std::string &tr_str, uint32_t block_size, std::string *error_reason) {
    std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
    auto &probe_buffer = probe_buffers_[tr_str];
    if (!probe_buffer) {
        probe_buffer = std::make_unique<ProbeBuffer>();
    }

    if (probe_buffer->ptr != nullptr && probe_buffer->size == block_size) {
        return probe_buffer.get();
    }

    if (probe_buffer->ptr != nullptr) {
        spdk_free(probe_buffer->ptr);
        probe_buffer->ptr = nullptr;
        probe_buffer->size = 0;
    }

    probe_buffer->ptr =
        spdk_zmalloc(block_size, 0x1000, nullptr, -1, SPDK_MALLOC_DMA);
    if (!probe_buffer->ptr) {
        if (error_reason) {
            *error_reason = "alloc_fail";
        }
        return nullptr;
    }
    probe_buffer->size = block_size;
    return probe_buffer.get();
}

// ProbeNofSegment creates an independent temporary connection via
// NofConnection::Connect() directly.  Does NOT go through OpenNofSegment,
// does NOT insert into open_segments_, and does NOT share qpairs with
// worker threads — this avoids violating the SPDK qpair single-thread
// constraint.
//
// Synchronization point for the timeout UAF is the inflight counter on
// NofQpairPool:
//
//   IncrementInflight()       ── before spdk_nvme_ns_cmd_read
//   ProbeIoTrampoline         ── calls ProbeReadComplete, then
//                                DecrementInflight
//   WaitForInflightCompletion ── blocks until InflightCount==0
//
// After WaitForInflightCompletion returns true, no callback can fire
// in the future for this conn; freeing probe_ctx and the wrapper is
// safe.  The Recycler remains as a belt-and-suspenders fallback.
bool SpdkWrapper::ProbeNofSegment(const std::string &tr_str,
                                  uint32_t timeout_ms,
                                  std::string *error_reason) {
    if (!InitializeEnv()) {
        if (error_reason) {
            *error_reason = "spdk_env_init_fail";
        }
        return false;
    }

    // Drain the recycler before opening a new connection.
    // pending_conns_ entries run ~NofConnection → ~NofQpairPool →
    // quiescent wait + free_io_qpair.  By the time Drain() returns,
    // SPDK has stopped scheduling on those qpairs and the
    // corresponding ctx shared_ptrs are no longer reachable by
    // callbacks.
    ProbeCtxRecycler::Instance().Drain();

    // Create an independent probe connection — exclusive to the
    // heartbeat thread, never shared with worker threads.
    // Uses config_probe_ (num_io_queues=1) to avoid competing with
    // the I/O path for target QID slots.
    //
    // Serialise with OpenNofSegment: spdk_nvme_probe() is NOT
    // thread-safe; concurrent Connect calls from the heartbeat
    // thread and the I/O path cause a namespace activation race.
    std::string connect_error;
    std::unique_ptr<NofConnection> conn;
    {
        std::lock_guard<std::mutex> connect_lock(connect_mutex_);
        conn = NofConnection::Connect(tr_str, config_probe_, &connect_error);
    }
    if (!conn) {
        if (error_reason) {
            *error_reason = "open_fail: " + connect_error;
        }
        return false;
    }

    // Stack-allocated temporary segment + handle (not registered in
    // open_segments_).
    NofSegment segment(conn.get(), 0, conn->GetNumBlocks());
    nof_seg_handle seg_handle{&segment};

    uint32_t block_size = segment.GetBlockSize();
    if (block_size == INVALID_BLOCK_SIZE || block_size == 0) {
        if (error_reason) {
            *error_reason = "invalid_block_size";
        }
        return false;  // conn auto-destructs → ~NofQpairPool quiescent wait
    }

    ProbeBuffer *probe_buffer =
        GetOrCreateProbeBuffer(tr_str, block_size, error_reason);
    if (!probe_buffer || !probe_buffer->ptr) {
        return false;  // conn auto-destructs → ~NofQpairPool quiescent wait
    }

    // Each probe allocates a fresh ctx (shared_ptr) AND a fresh submit
    // wrapper.  The wrapper holds the pool pointer so the trampoline
    // can DecrementInflight; its lifetime is bound to submit_wrapper_sp
    // on the local stack.
    auto probe_ctx = std::make_shared<ProbeRequestContext>();
    auto submit_wrapper_sp = std::make_shared<ProbeSubmitWrapper>();
    submit_wrapper_sp->probe_ctx = probe_ctx;
    submit_wrapper_sp->pool = &conn->GetQpairPool();

    // Increment BEFORE submit so that any callback (including the
    // one that fires after our Phase 1 / Phase 1b window) is
    // accounted for.  Without this, late CQEs are invisible to
    // WaitForInflightCompletion.
    conn->GetQpairPool().IncrementInflight();

    int ret =
        SubmitRequest(&seg_handle, probe_buffer->ptr, 0, 1, kSpdkNofOpRead,
                      ProbeIoTrampoline, submit_wrapper_sp.get());
    if (ret != 0) {
        // Submit failed synchronously — roll back the Increment.
        // No callback will fire, so InflightCount returns to 0.
        conn->GetQpairPool().DecrementInflight();
        if (error_reason) {
            *error_reason = "submit_fail";
        }
        return false;
    }

    // ──────────────────────────────────────────────────────────────
    // Phase 1: soft timeout window.
    //
    // Wait for done==true within timeout_ms.  This is the user-
    // perceived latency budget.
    // ──────────────────────────────────────────────────────────────
    {
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
        while (!probe_ctx->done.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            segment.PollCompletion(0);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // ──────────────────────────────────────────────────────────────
    // Phase 1b: terminal-state proof.
    //
    // If Phase 1 timed out (done==false), the request may still be
    // in flight.  We prove termination by waiting for the qpair
    // pool's InflightCount to reach 0.  This is the actual fix for
    // the timeout-path UAF: by the time WaitForInflightCompletion
    // returns true, ProbeIoTrampoline has fired, DecrementInflight
    // has happened, and no callback can fire in the future.
    //
    // 30 s budget matches ~NofQpairPool's budget so the conn
    // destructor (which runs on function return) can finish
    // promptly: if Phase 1b succeeds here, ~NofQpairPool will see
    // InflightCount==0 immediately; if Phase 1b fails here, we
    // defer the conn itself to ProbeCtxRecycler.
    // ──────────────────────────────────────────────────────────────
    if (!probe_ctx->done.load(std::memory_order_acquire)) {
        // Strict spin on InflightCount == 0.  PushWithConn remains as a
        // defensive fallback if the counter never reaches 0 (target
        // crash, SPDK impl bug); the next probe's Drain() will catch
        // up.
        bool quiescent = conn->GetQpairPool().WaitForInflightCompletion();
        if (!quiescent) {
            // Forward-compatibility guard for a false return.
            LOG(ERROR) << "ProbeNofSegment: qpair pool did not quiesce "
                          "— deferring destruction to next probe";
            ProbeCtxRecycler::Instance().PushWithConn(
                std::move(probe_ctx), std::move(conn),
                std::move(submit_wrapper_sp));
            if (error_reason) {
                *error_reason = "completion_timeout";
            }
            return false;
        }
        // Quiescent reached — the callback fired but did not set
        // done.  This is a bug in ProbeReadComplete; treat as
        // completion error.
    }

    bool ok = probe_ctx->done.load(std::memory_order_acquire) &&
              probe_ctx->success.load(std::memory_order_acquire);
    if (!ok && error_reason) {
        if (!probe_ctx->done.load(std::memory_order_acquire)) {
            *error_reason = "completion_error_no_done";
        } else {
            std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
            *error_reason = probe_ctx->error_reason.empty()
                                ? "completion_error"
                                : probe_ctx->error_reason;
        }
    }

    // ──────────────────────────────────────────────────────────────
    // SAFE-TO-DESTROY: at this point, InflightCount==0 (proven by
    // Phase 1 / Phase 1b), so ProbeIoTrampoline has already run and
    // will not fire again.  All release-stores in the trampoline
    // (probe_ctx->done / success) are visible via acquire load.
    //
    // Destruction order on return (happy path):
    //   1) submit_wrapper_sp drops → wrapper freed (safe: no callback
    //      can fire, InflightCount==0 proven above).
    //   2) probe_ctx pushed to ProbeCtxRecycler (paranoid belt-and-
    //      suspenders; the next probe's Drain() will release it
    //      after the previous probe's conn is gone).
    //   3) seg_handle / segment: trivially destructible.
    //   4) conn: → ~NofConnection → ~NofQpairPool
    //      → WaitForInflightCompletion sees InflightCount==0 →
    //      free_io_qpair.  Safe because no callback can fire after.
    //
    // Note: in the timeout-fallback path (above), the wrapper was
    // moved into ProbeCtxRecycler so it survives until after
    // free_io_qpair — see the lifetime contract on ProbeSubmitWrapper
    // in spdk_wrapper.h.
    // ──────────────────────────────────────────────────────────────
    ProbeCtxRecycler::Instance().Push(std::move(probe_ctx));

    return ok;
}

// SetConfig / PipelineRead / PipelineWrite.
//
// PipelineRead/Write are lower-level APIs that submit a multi-chunk
// request across all qpairs in a single call.  They are not part of
// Mooncake's NoF transfer hot path (see TransferSubmitter →
// submitTask → SubmitRequest); they exist for callers that want to
// drive NVMe-oF directly without going through SpdkNofWorkerPool.
//
// The caller_ctx out-param gives explicit lifetime control.  Pass
// nullptr (the default) for fire-and-forget semantics; pass a non-null
// pointer for the explicit-lifetime contract.  See
// NofSegment::PipelineRead/Write for the full contract, including
// PipelineCtxRecycler fallback behaviour.
void SpdkWrapper::SetConfig(const NofConfig &config) { config_ = config; }

ssize_t SpdkWrapper::PipelineRead(nof_seg_handle *handle, void *buf,
                                  uint64_t lba, uint32_t total_blocks,
                                  std::shared_ptr<PipelineCtx> *caller_ctx) {
    if (!handle || !handle->segment) return -1;
    return handle->segment->PipelineRead(buf, lba, total_blocks, caller_ctx);
}

ssize_t SpdkWrapper::PipelineWrite(nof_seg_handle *handle, const void *buf,
                                   uint64_t lba, uint32_t total_blocks,
                                   std::shared_ptr<PipelineCtx> *caller_ctx) {
    if (!handle || !handle->segment) return -1;
    return handle->segment->PipelineWrite(buf, lba, total_blocks, caller_ctx);
}

void SpdkWrapper::PipelineDrain(nof_seg_handle *handle,
                                const std::shared_ptr<PipelineCtx> &ctx_sp,
                                uint32_t budget_us) {
    if (!handle || !handle->segment) return;
    handle->segment->DrainForInflight(ctx_sp, budget_us);
}

}  // namespace mooncake
