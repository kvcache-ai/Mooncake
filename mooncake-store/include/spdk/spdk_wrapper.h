#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <spdk/env.h>
#include <spdk/nvme.h>

// Added includes
#include "nof_config.h"
#include "nof_connection.h"
#include "nof_segment.h"

namespace mooncake {

#define INVALID_BLOCK_SIZE 0xFFFFFFFF

constexpr int kSpdkNofOpRead = 0;
constexpr int kSpdkNofOpWrite = 1;
constexpr int kSpdkNofOpNum = 2;

// Transport parsing is handled internally by NofConnection::Connect().
struct nof_seg_handle;

/**
 * @brief QID pressure gauge.
 *
 * Uses a sliding window to record the results of the last N qpair
 * allocation attempts (allocated/requested ratio) and computes the QID
 * acquisition rate on the target side. When a new connection is
 * established, it adaptively reduces the number of requested qpairs to
 * prevent early comers from monopolizing the target QID pool.
 *
 * Thread safety: Record() and GetRecommended() are protected by an
 * internal mutex and can safely be called from multiple threads
 * concurrently.
 */
class QidPressureGauge {
   public:
    QidPressureGauge() {
        for (size_t i = 0; i < kWindowSize; i++) {
            window_[i].requested = 0;
            window_[i].allocated = 0;
        }
    }

    /// Records the result of the most recent qpair allocation.
    /// @param requested  The number of qpairs requested.
    /// @param allocated  The number of qpairs actually allocated.
    void Record(uint32_t requested, uint32_t allocated) {
        if (requested == 0) return;
        std::lock_guard<std::mutex> lock(mutex_);
        size_t idx =
            write_idx_.fetch_add(1, std::memory_order_relaxed) % kWindowSize;
        window_[idx].requested = requested;
        window_[idx].allocated = allocated;
    }

    /// Returns the current pressure level.
    /// @return 0=Green (>75% acquisition rate), 1=Yellow (50-75%),
    /// 2=Red (<50%).
    int GetPressureLevel() const {
        double ratio = GetAverageRatio();
        if (ratio > 0.75) return 0;  // Green: healthy
        if (ratio > 0.50) return 1;  // Yellow: moderate pressure
        return 2;                    // Red: severe pressure
    }

    /// Returns the recommended number of qpairs for a new connection
    /// based on the current pressure level.
    /// @param configured  The num_io_queues configured in NofConfig.
    /// @return The recommended number of qpairs to request.
    uint32_t GetRecommended(uint32_t configured) const {
        // Already small enough; no reduction needed
        if (configured <= 4) return configured;
        int level = GetPressureLevel();
        switch (level) {
            case 0:  // Green — request the full amount
                return configured;
            case 1:  // Yellow — request 3/4
                return std::max(4u, configured * 3 / 4);
            case 2:  // Red — request 1/2
                return std::max(4u, configured / 2);
            default:
                return configured;
        }
    }

   private:
    static constexpr size_t kWindowSize = 16;
    struct Sample {
        uint32_t requested;
        uint32_t allocated;
    };

    double GetAverageRatio() const {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t total_requested = 0;
        uint64_t total_allocated = 0;
        for (size_t i = 0; i < kWindowSize; i++) {
            total_requested += window_[i].requested;
            total_allocated += window_[i].allocated;
        }
        if (total_requested == 0) return 1.0;  // No history, assume healthy.
        return static_cast<double>(total_allocated) /
               static_cast<double>(total_requested);
    }

    // Guards window_ writes in Record() and reads in GetAverageRatio().
    // write_idx_ remains atomic (relaxed) to avoid contention on the
    // fast-path index increment, but all sample data access is serialised
    // through this mutex.
    mutable std::mutex mutex_;
    std::atomic<size_t> write_idx_{0};
    Sample window_[kWindowSize];
};

class SpdkWrapper {
   public:
    SpdkWrapper(const SpdkWrapper &) = delete;
    SpdkWrapper &operator=(const SpdkWrapper &) = delete;

    static SpdkWrapper &GetInstance();

    bool InitializeEnv();

    void Cleanup();

    void *Alloc(size_t size, size_t align, int socket_id = -1);

    void Free(void *ptr);

    int64_t NvmePollProcessCompletion(nof_seg_handle *seg,
                                      uint32_t complete_per_seg);

    /** @brief Open a NoF segment. */
    nof_seg_handle *OpenNofSegment(const std::string &tr_str);

    /**
     * @brief Close a NoF segment and release all associated resources.
     * Frees the NofSegment, the nof_seg_handle, and triggers
     * NofConnection destruction (qpair pool + ctrlr detach).
     * Safe to call with nullptr (no-op).
     *
     * Safety contract:
     *   The caller MUST have joined any SpdkNofWorkerPool that may have
     *   submitted I/O through this handle.  Closing while a worker is
     *   still running will cause in-flight callbacks to access freed
     *   SpdkNofSubTask/SpdkNofTask memory.  Today only ~TransferSubmitter
     *   is a legitimate caller; it joins the worker pool before closing
     *   cached handles.  Future explicit callers MUST follow the same
     *   protocol — see ~NofQpairPool's documentation on why
     *   WaitForInflightCompletion is not sufficient for transfer paths.
     */
    void CloseNofSegment(nof_seg_handle *handle);

    // Added APIs
    void SetConfig(const NofConfig &);
    const NofConfig &GetConfig() const { return config_; }
    // PipelineRead/Write accept an optional caller_ctx out-param.
    // Pass nullptr to keep the legacy fire-and-forget behaviour;
    // pass a non-null ptr for explicit lifetime control.  See
    // NofSegment::PipelineRead for the full contract.
    ssize_t PipelineRead(nof_seg_handle *, void *, uint64_t, uint32_t,
                         std::shared_ptr<PipelineCtx> *caller_ctx = nullptr);
    ssize_t PipelineWrite(nof_seg_handle *, const void *, uint64_t, uint32_t,
                          std::shared_ptr<PipelineCtx> *caller_ctx = nullptr);
    // Re-exported so callers can drain a caller-supplied ctx_sp without
    // depending on the NofSegment header directly.
    void PipelineDrain(nof_seg_handle *,
                       const std::shared_ptr<PipelineCtx> &ctx_sp,
                       uint32_t budget_us = 0);

    uint32_t GetBlockSize(const nof_seg_handle *seg_handle);

    int SubmitRequest(const nof_seg_handle *seg_handle, void *ptr, uint64_t lba,
                      uint32_t lba_count, int op, spdk_nvme_cmd_cb cb_fn,
                      void *cb_ctx);

    bool ProbeNofSegment(const std::string &tr_str, uint32_t timeout_ms,
                         std::string *error_reason = nullptr);

   private:
    // ---- Per-open-segment tracking ----
    // open_segments_ is indexed by handle; each entry owns the NofConnection
    // for that segment.
    std::map<nof_seg_handle *, std::unique_ptr<NofConnection>> open_segments_;
    std::mutex segments_mutex_;

    // ---- Configuration ----
    NofConfig config_;

    // Configuration dedicated to heartbeat probing.
    // Initialized with ForProbe(), num_io_queues=1.
    NofConfig config_probe_;

    // Global QID pressure gauge.
    QidPressureGauge qid_pressure_gauge_;

    struct ProbeBuffer {
        void *ptr{nullptr};
        uint32_t size{0};

        ProbeBuffer() = default;
        ProbeBuffer(const ProbeBuffer &) = delete;
        ProbeBuffer &operator=(const ProbeBuffer &) = delete;
        ProbeBuffer(ProbeBuffer &&) = delete;
        ProbeBuffer &operator=(ProbeBuffer &&) = delete;
    };

    explicit SpdkWrapper();
    ~SpdkWrapper();

    // Transport parsing is handled uniformly by NofConnection::Connect().
    ProbeBuffer *GetOrCreateProbeBuffer(const std::string &tr_str,
                                        uint32_t block_size,
                                        std::string *error_reason);

   public:
    // Static SPDK callback invoked via the file-scope ProbeIoTrampoline
    // (see spdk_wrapper.cpp).  Public so the trampoline — which lives in
    // an anonymous namespace and therefore cannot be friended — can
    // forward the completion here.  Takes ownership bookkeeping only;
    // it does not touch any SpdkWrapper instance state.
    static void ProbeReadComplete(void *ctx, const struct spdk_nvme_cpl *cpl);

   private:
    std::atomic<bool> initialized{false};
    std::mutex init_mutex;
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;

    // Serializes the slow path (first connection).  spdk_nvme_probe()
    // is not thread-safe — concurrent Connect calls from multiple
    // threads cause a namespace activation race where all return
    // namespace_inactive.  This mutex ensures only one thread executes
    // NofConnection::Connect() at a time.
    std::mutex connect_mutex_;
};
// ---------------------------------------------------------------------------
// ProbeRequestContext — per-probe request context.
//
// Defined at top-level (not nested in SpdkWrapper) so that ProbeCtxRecycler,
// also defined at top-level below, can hold shared_ptr<ProbeRequestContext>.
// ---------------------------------------------------------------------------
struct ProbeRequestContext {
    std::atomic<bool> done{false};
    std::atomic<bool> success{false};
    std::mutex error_mutex;
    std::string error_reason;
};

// ---------------------------------------------------------------------------
// ProbeSubmitWrapper — bridges the user-callback API to the inflight counter.
//
// Heap-allocated small struct that lives across the SPDK callback window.
// Holds:
//   - probe_ctx (shared_ptr): keeps the request context alive until SPDK
//     has delivered the CQE.  Lifetime may extend BEYOND ProbeNofSegment's
//     return in the timeout-fallback path (see ProbeCtxRecycler below).
//   - pool (raw pointer): the qpair pool whose InflightCount must be
//     decremented when the CQE arrives.  The pool is owned by the
//     NofConnection that was active during submission.
//
// Why we need it:
//   ProbeNofSegment submits via SubmitRequest with a C-style callback
//   (void(void*, const spdk_nvme_cpl*)).  We need every callback to:
//     (a) call ProbeReadComplete (writes done/success);
//     (b) DecrementInflight on the qpair pool (so WaitForInflightCompletion
//         can prove quiescence).
//   Storing the pool pointer in ProbeRequestContext itself would be
//   unsafe: probe_ctx outlives the conn (it is transferred to
//   ProbeCtxRecycler), and a dangling pool pointer would be a UAF if a
//   late callback fired after ~NofConnection.
//
// Lifetime invariants:
//   - In the happy path, the wrapper's local shared_ptr in
//     ProbeNofSegment is dropped AFTER the function returns.  By that
//     point InflightCount has been observed == 0, so no callback can
//     fire in the future and dropping the wrapper is safe.
//   - In the timeout-fallback path (WaitForInflightCompletion timed
//     out), the wrapper is moved into ProbeCtxRecycler alongside the
//     ctx and conn.  Drain() drops wrappers AFTER the conn destructor
//     runs (~NofQpairPool → free_io_qpair), guaranteeing the wrapper
//     outlives any possible callback firing during the quiescent wait.
// ---------------------------------------------------------------------------
struct ProbeSubmitWrapper {
    std::shared_ptr<ProbeRequestContext> probe_ctx;
    NofQpairPool *pool{nullptr};
};

// ---------------------------------------------------------------------------
// nof_seg_handle — opaque handle wrapping a NofSegment.
// The connection (NofConnection) is owned by SpdkWrapper.
// ---------------------------------------------------------------------------
struct nof_seg_handle {
    NofSegment *segment = nullptr;
};

// ---------------------------------------------------------------------------
// ProbeCtxRecycler — defers release of probe context to the next probe.
// ---------------------------------------------------------------------------
//
// Holds probe_ctx shared_ptrs until SPDK has guaranteed no further
// callback can fire on them.  Each probe transfers ownership of its ctx
// to the recycler at ProbeNofSegment exit; the recycler drains at the
// start of the next probe, after the previous probe's NofConnection
// (and ~NofQpairPool) has been fully torn down.
//
// Why this is needed:
//   The previous design let probe_ctx be destroyed when ProbeNofSegment
//   returned, by RAII.  If SPDK scheduled a callback that arrived after
//   free_io_qpair had been called, the callback would access a
//   destroyed ctx (UAF).  By deferring the release to the next probe,
//   we guarantee that:
//     (1) the previous NofConnection is destroyed;
//     (2) ~NofQpairPool has drained pending completions and waited
//         up to 30 s for the inflight counter to reach 0, then freed
//         the qpair.  SPDK does NOT expose a public
//         spdk_nvme_qpair_fail symbol (it is a static helper inside
//         lib/nvme), so the drain + WaitForInflightCompletion pair
//         is what bridges the gap;
//     (3) SPDK has had ample time to deliver any pending callback
//         (it cannot fire after free_io_qpair returns in normal SPDK
//         behaviour);
//     (4) no code holds a strong reference to ctx anymore.
//
// Ownership path of probe_ctx during one probe:
//   1) here in ProbeNofSegment: refcount=1
//   2) transferred to ProbeCtxRecycler before return: refcount=1
//   3) drained at the START of the next probe: refcount=0 → destroyed
//
// Thread safety: single heartbeat thread drives the probe loop today,
// but ProbeCtxRecycler is mutex-protected for forward compatibility
// (e.g. parallel probes in the future).
class ProbeCtxRecycler {
   public:
    static ProbeCtxRecycler &Instance();

    // Take ownership of a ctx.  Called from ProbeNofSegment just before
    // it returns.  The caller MUST NOT keep its own shared_ptr after
    // this call.
    void Push(std::shared_ptr<ProbeRequestContext> ctx);

    // Take ownership of BOTH a ctx and its associated NofConnection.
    // Used by ProbeNofSegment's fallback path when the qpair pool did
    // not reach quiescent within the 30 s budget.  Both objects are
    // released together at the next probe's Drain(), AFTER the previous
    // conn's ~NofQpairPool has completed its quiescent wait.
    //
    // Forward declaration of NofConnection avoids forcing this header
    // to depend on nof_connection.h transitively (it already does,
    // but the forward decl documents intent).
    void PushWithConn(std::shared_ptr<ProbeRequestContext> ctx,
                      std::unique_ptr<class NofConnection> conn);

    // Extended variant that also takes the submit wrapper.  Used in
    // the timeout-fallback path so the wrapper's lifetime matches the
    // conn's lifetime — both are released at the next probe's Drain()
    // AFTER ~NofQpairPool has freed the qpair.  This closes the
    // window where a late callback could fire against a freed wrapper.
    void PushWithConn(std::shared_ptr<ProbeRequestContext> ctx,
                      std::unique_ptr<class NofConnection> conn,
                      std::shared_ptr<ProbeSubmitWrapper> wrapper);

    // Release every pending ctx (and conn/wrapper, if any).  Called at
    // the start of each probe.  Must run after the previous probe's
    // NofConnection is destroyed (i.e. at the entry of
    // ProbeNofSegment, before any new conn is created).
    void Drain();

   private:
    ProbeCtxRecycler() = default;
    std::mutex mutex_;
    std::vector<std::shared_ptr<ProbeRequestContext>> pending_;
    // Deferred-destruction conns paired with their ctx.  Drained
    // together with pending_ at the next probe's Drain().
    std::vector<std::unique_ptr<class NofConnection>> pending_conns_;
    // Deferred-destruction submit wrappers paired with their conn.
    // Drained AFTER pending_conns_ so the wrapper outlives
    // ~NofQpairPool (and thus any callback that might fire during the
    // quiescent wait).
    std::vector<std::shared_ptr<ProbeSubmitWrapper>> pending_wrappers_;
};

// WorkerPool lifecycle tracking — free functions, NOT SpdkWrapper
// methods. Reason: during static destruction the SpdkWrapper singleton
// may already be destroyed, but the SpdkNofWorkerPool destructor still
// needs to unregister. Free functions can safely access file-level
// global counters.
void SpdkNoF_RegisterWorkerPool();
void SpdkNoF_UnregisterWorkerPool();

}  // namespace mooncake
