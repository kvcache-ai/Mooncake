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

// [Migrated] Old nof_seg_handle_ / tr_info / ctrlr_info structs removed.
// The new design manages NVMe-oF resources through the NofConnection +
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

        // [Migrated] Clean up the new open_segments_ design:
        // NofConnection is auto-destroyed by unique_ptr (including qpair
        // pool and ctrlr detach); only NofSegment and nof_seg_handle
        // must be freed manually.
        {
            std::lock_guard<std::mutex> lock(segments_mutex_);
            for (auto &[handle, conn] : open_segments_) {
                if (handle) {
                    delete handle
                        ->segment;  // Allocated by OpenNofSegment
                    delete handle;  // Allocated by OpenNofSegment
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
    // Do NOT recycle probe_ctx here.  ProbeNofSegment() still reads
    // success / error_reason after the poll loop, and a concurrent probe
    // could acquire and reset the same context before those reads complete.
    // The caller (ProbeNofSegment) is now responsible for recycling the
    // context after it has copied the results.
}

void SpdkWrapper::ReplenishProbeRequestContextPoolLocked(size_t count) {
    for (size_t i = 0; i < count; ++i) {
        auto probe_ctx = std::make_unique<ProbeRequestContext>();
        probe_request_context_pool_.push(probe_ctx.get());
        probe_request_contexts_.push_back(std::move(probe_ctx));
    }
}

SpdkWrapper::ProbeRequestContext *SpdkWrapper::AcquireProbeRequestContext() {
    std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
    if (probe_request_context_pool_.empty()) {
        ReplenishProbeRequestContextPoolLocked(8);
    }
    auto *probe_ctx = probe_request_context_pool_.top();
    probe_request_context_pool_.pop();
    probe_ctx->Reset();
    return probe_ctx;
}

void SpdkWrapper::RecycleProbeRequestContext(ProbeRequestContext *ctx) {
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
    probe_request_context_pool_.push(ctx);
}

// [Migrated] Delegates to NofSegment::PollCompletion instead of the
// old direct seg->qpair access.
int64_t SpdkWrapper::NvmePollProcessCompletion(nof_seg_handle *seg,
                                               uint32_t complete_per_seg) {
    if (!seg || !seg->segment) return -1;
    return seg->segment->PollCompletion(complete_per_seg);
}

// [Removed] ParseTransPortStr / ConnectController removed.
// Transport parsing and controller connection are now handled centrally
// by NofConnection::Connect().

// OpenNofSegment: uses the new connection layer.
// 2026-07-31: Endpoint→handle dedup added (I/O-path reuse later removed).
// 2026-07-31: spdk_nvme_probe is not thread-safe — added connect_mutex_
//             to serialise all Connect() calls.
// 2026-07-31: Removed I/O-path connection reuse.
//   Rationale: each ClientService owns an independent SpdkNofWorkerPool.
//   Sharing a handle across WorkerPools would let multiple worker threads
//   access the same qpair pool concurrently, stealing each other's
//   completions and underflowing inflight counters.  SPDK requires each
//   qpair to be owned by a single thread.  Each client now holds an
//   independent connection: 4 clients × 4 qpairs = 20 QIDs (well under 64).
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
        auto wait_ms = std::min(
            config_.retry_backoff_ms * (1u << attempt), 30000u);
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
    auto handle = std::unique_ptr<nof_seg_handle>(
        new nof_seg_handle{segment.get()});

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
    open_segments_.erase(
        it);  // ~unique_ptr<NofConnection> → qpair pool → ctrlr detach
}

// [Migrated] Obtain block size via NofSegment instead of the old
// direct seg_handle->ns access.
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

// 2026-07-31: Rewritten to use NofConnection::Connect() directly,
// creating an independent temporary connection.  Does NOT go through
// OpenNofSegment (I/O-path shared connection), does NOT insert into
// open_segments_, and does NOT share qpairs with worker threads — this
// avoids violating the SPDK qpair single-thread constraint.  The
// NofConnection auto-destructs on return → qpair pool freed → ctrlr
// detached → all QIDs reclaimed.
bool SpdkWrapper::ProbeNofSegment(const std::string &tr_str,
                                  uint32_t timeout_ms,
                                  std::string *error_reason) {
    if (!InitializeEnv()) {
        if (error_reason) {
            *error_reason = "spdk_env_init_fail";
        }
        return false;
    }

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
        return false;  // conn auto-destructs → QIDs reclaimed
    }

    ProbeBuffer *probe_buffer =
        GetOrCreateProbeBuffer(tr_str, block_size, error_reason);
    if (!probe_buffer || !probe_buffer->ptr) {
        return false;  // conn auto-destructs → QIDs reclaimed
    }

    ProbeRequestContext *probe_ctx = AcquireProbeRequestContext();
    int ret = SubmitRequest(&seg_handle, probe_buffer->ptr, 0, 1,
                            kSpdkNofOpRead, ProbeReadComplete, probe_ctx);
    if (ret != 0) {
        RecycleProbeRequestContext(probe_ctx);
        if (error_reason) {
            *error_reason = "submit_fail";
        }
        return false;  // conn auto-destructs → QIDs reclaimed
    }

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (!probe_ctx->done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        segment.PollCompletion(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    bool ok = probe_ctx->done.load(std::memory_order_acquire) &&
              probe_ctx->success.load(std::memory_order_acquire);
    if (!ok && error_reason) {
        if (!probe_ctx->done.load(std::memory_order_acquire)) {
            *error_reason = "completion_timeout";
        } else {
            std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
            *error_reason = probe_ctx->error_reason.empty()
                                ? "completion_error"
                                : probe_ctx->error_reason;
        }
    }

    // Recycle the probe context only after we have copied all results.
    // The callback (ProbeReadComplete) only publishes terminal state;
    // deferring recycle here eliminates the race where a concurrent
    // probe could acquire and Reset() the context while we still read
    // success / error_reason from it.
    RecycleProbeRequestContext(probe_ctx);

    // conn goes out of scope → ~NofConnection() → ~NofQpairPool() →
    // free io qpairs → spdk_nvme_detach().  All QIDs are reclaimed.
    return ok;
}

// [Migrated] SetConfig / PipelineRead / PipelineWrite.
//
// NOTE: PipelineRead/Write are fully implemented at the segment layer
// (NofSegment::PipelineIO) but are NOT currently wired into the Mooncake
// NoF transfer hot path.  TransferSubmitter still submits individual
// SpdkNofTasks through SubmitRequest/poll.  These pipeline APIs are
// available for future integration into the worker pool.
void SpdkWrapper::SetConfig(const NofConfig &config) { config_ = config; }

ssize_t SpdkWrapper::PipelineRead(nof_seg_handle *handle, void *buf,
                                  uint64_t lba, uint32_t total_blocks) {
    if (!handle || !handle->segment) return -1;
    return handle->segment->PipelineRead(buf, lba, total_blocks);
}

ssize_t SpdkWrapper::PipelineWrite(nof_seg_handle *handle, const void *buf,
                                   uint64_t lba, uint32_t total_blocks) {
    if (!handle || !handle->segment) return -1;
    return handle->segment->PipelineWrite(buf, lba, total_blocks);
}

}  // namespace mooncake
