// NofSegment class declaration, PipelineCtx.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sys/types.h>
#include <vector>

#include <spdk/nvme.h>

#include "nof_config.h"
#include "nof_connection.h"

namespace mooncake {

// Callback signature: void(void* ctx, const spdk_nvme_cpl* cpl)
using NofIoCallback = void (*)(void *, const struct spdk_nvme_cpl *);

// ---------------------------------------------------------------------------
// PipelineCtx — shared completion context for PipelineIO.
//
// All fields MUST be std::atomic<> or trivially copyable.  PipelineCtx is
// heap-allocated and may outlive the PipelineIO call site:
//   - caller_ctx != nullptr path: caller owns the shared_ptr<PipelineCtx>
//     until release.
//   - caller_ctx == nullptr path: PipelineCtxRecycler owns the shared_ptr
//     until the next PipelineIO entry's Drain().
// Callbacks (pipeline_io_cb) write to pc->inflight and pc->error via the
// raw pointer passed to spdk_nvme_ns_cmd_*.  Atomic ordering documents
// intent and future-proofs against accidental multi-thread access
// (single-thread per pool is the current contract).
// ---------------------------------------------------------------------------
struct PipelineCtx {
    std::atomic<int32_t> inflight{0};
    std::atomic<bool> error{false};
};

// ---------------------------------------------------------------------------
// PipelineCtxRecycler — defers release of PipelineCtx shared_ptrs until the
// next PipelineIO call's Drain().  Mirrors the ProbeCtxRecycler pattern
// (spdk_wrapper.h) used for ProbeRequestContext.
//
// Why this is still needed:
//   When the caller passes caller_ctx == nullptr (the default, fire-and-
//   forget path), PipelineIO internally manages ctx_sp and transfers
//   ownership to this recycler at exit.  The recycler keeps the heap
//   object alive until the next PipelineIO entry's Drain(), so any late
//   CQE arrives at a live object.  This is the fallback mitigation for
//   callers that don't opt into the explicit lifetime API.
//
//   For callers that pass caller_ctx != nullptr, ownership stays with the
//   caller; the recycler is NOT involved.  That path provides
//   explicit-lifetime ownership and lets the caller (a) keep ctx_sp
//   alive as long as needed, and (b) use DrainForInflight() to
//   synchronously wait for in-flight callbacks before releasing ctx_sp
//   and buf.
//
// Thread safety: single-thread per pool today, but PipelineCtxRecycler
// uses an internal mutex for forward compatibility with parallel callers.
// ---------------------------------------------------------------------------
class PipelineCtxRecycler {
   public:
    static PipelineCtxRecycler &Instance();

    // Take ownership of ctx.  Called from PipelineIO just before it
    // returns.  The caller MUST NOT keep its own shared_ptr after this
    // call.
    void Push(std::shared_ptr<PipelineCtx> ctx);

    // Release every pending ctx.  Called at the start of every PipelineIO.
    // Safe because the prior call's qpair is owned by conn_ and outlives
    // the call (until SpdkWrapper::Cleanup).
    void Drain();

   private:
    PipelineCtxRecycler() = default;
    std::mutex mutex_;
    std::vector<std::shared_ptr<PipelineCtx>> pending_;
};

// ---------------------------------------------------------------------------
// NofSegment — a contiguous LBA range on one NofConnection.
//
// Provides two I/O patterns:
//   1. Async single-request (SubmitRead / SubmitWrite) — compatible with the
//      old SpdkWrapper API.
//   2. Blocking pipeline I/O (PipelineRead / PipelineWrite) — high-throughput
//      bulk transfer using all qpairs simultaneously.
//
// Thread safety: SPDK qpairs are NOT thread-safe.  All methods on a given
// NofSegment must be called from a single thread at a time (or externally
// serialised).
// ---------------------------------------------------------------------------
class NofSegment {
   public:
    /// @param conn       Connection to submit I/O on (non-owning).
    /// @param start_lba  Starting LBA (in blocks).
    /// @param num_blocks Total blocks in this segment.
    NofSegment(NofConnection *conn, uint64_t start_lba, uint64_t num_blocks);

    ~NofSegment() = default;

    // Non-copyable, movable
    NofSegment(const NofSegment &) = delete;
    NofSegment &operator=(const NofSegment &) = delete;

    // ---- Async single-request API (backwards-compatible) ----

    /// Submit a read.  The callback is invoked from PollCompletion().
    int SubmitRead(void *buf, uint64_t lba, uint32_t num_blocks,
                   NofIoCallback cb, void *cb_ctx);

    /// Submit a write.
    int SubmitWrite(void *buf, uint64_t lba, uint32_t num_blocks,
                    NofIoCallback cb, void *cb_ctx);

    /// Poll all qpairs for completions.
    /// @return total completions processed, or negative on error.
    int32_t PollCompletion(uint32_t max_completions = 0);

    // ---- Pipeline I/O (high-performance bulk transfer) ----
    //
    // PipelineRead/Write take an optional caller_ctx out-param so callers
    // can opt into explicit lifetime control.  Default
    // (caller_ctx == nullptr) preserves the fire-and-forget semantics;
    // PipelineCtxRecycler defers ctx release to the next PipelineIO entry.

    /// Pipeline read: interleaves submission and polling across ALL qpairs.
    /// Blocks until all data is transferred (or an error occurs).
    /// @param buf        Destination buffer (must be DMA-accessible).
    /// @param lba        Start LBA (in blocks).
    /// @param total_blocks  Number of blocks to read.
    /// @param caller_ctx Optional explicit-lifetime sink (see contract).
    /// @return Total bytes read on success, -1 on error.
    ///
    /// Explicit-lifetime contract (caller_ctx != nullptr):
    ///   The caller MUST initialise *caller_ctx to an empty shared_ptr
    ///   before invoking PipelineRead/Write.  On success, *caller_ctx is
    ///   filled with the pipeline's ctx_sp; caller takes ownership and is
    ///   responsible for keeping ctx_sp alive as long as the buffer might
    ///   be touched by SPDK callbacks.  The caller MUST call
    ///   DrainForInflight(*caller_ctx) before releasing buf or the
    ///   shared_ptr itself.  On failure (return < 0), *caller_ctx remains
    ///   empty — PipelineIO has already drained and recycled the ctx
    ///   internally.
    ///
    /// Fire-and-forget contract (caller_ctx == nullptr, the default):
    ///   PipelineIO retains ctx_sp ownership and transfers it to
    ///   PipelineCtxRecycler at exit.  The caller must keep buf alive
    ///   until the next PipelineIO call (which drains the recycler) OR
    ///   until NofSegment is destroyed.
    ///
    /// Thread safety: must be called from the same thread that owns this
    /// segment's qpair pool (SPDK single-thread-per-pool).  Mixing
    /// caller_ctx and nullptr paths across calls is allowed; each call
    /// is independent.
    ssize_t PipelineRead(void *buf, uint64_t lba, uint32_t total_blocks,
                         std::shared_ptr<PipelineCtx> *caller_ctx = nullptr);

    /// Pipeline write: same pattern as PipelineRead.  See PipelineRead for
    /// caller_ctx contract.
    ssize_t PipelineWrite(const void *buf, uint64_t lba, uint32_t total_blocks,
                          std::shared_ptr<PipelineCtx> *caller_ctx = nullptr);

    /// Block (poll) until ctx_sp->inflight reaches 0 or budget_us elapses,
    /// whichever comes first.
    ///
    /// Safe to call only from the same thread that issued PipelineIO.
    /// After DrainForInflight returns, the caller may safely release ctx_sp
    /// and the buf passed to PipelineIO — no in-flight callbacks remain.
    ///
    /// budget_us == 0 → use NofConfig::pipeline_drain_budget_us.
    /// budget_us >  0 → use the explicit budget (overrides the config).
    ///
    /// Does NOT clear *caller_ctx on budget timeout; the caller continues
    /// to own the shared_ptr and may retry DrainForInflight with a larger
    /// budget.
    void DrainForInflight(const std::shared_ptr<PipelineCtx> &ctx_sp,
                          uint32_t budget_us = 0);

    // ---- Accessors ----
    NofConnection *GetConnection() const { return conn_; }
    uint64_t GetStartLba() const { return start_lba_; }
    uint64_t GetNumBlocks() const { return num_blocks_; }
    uint32_t GetBlockSize() const { return conn_->GetBlockSize(); }
    const NofConfig &GetConfig() const { return config_; }

   private:
    /// Common pipeline loop (read or write).
    ssize_t PipelineIO(void *buf, uint64_t lba, uint32_t total_blocks,
                       bool is_write, std::shared_ptr<PipelineCtx> *caller_ctx);

    NofConnection *conn_;  // non-owning
    uint64_t start_lba_;
    uint64_t num_blocks_;
    NofConfig config_;
};

}  // namespace mooncake

// ---------------------------------------------------------------------------
// Test hook — exposed only when MOONCAKE_TEST_PIPELINE_IO is defined so unit
// tests can drive the file-scope pipeline_io_cb without poking private state.
// Production builds MUST NOT define this macro.
// ---------------------------------------------------------------------------
#ifdef MOONCAKE_TEST_PIPELINE_IO
namespace mooncake::detail {
// Forwards to the file-scope pipeline_io_cb.  Production callers must
// not use this; it is exposed only so unit tests can drive the callback
// with synthetic spdk_nvme_cpl values.
void InvokePipelineIoCbForTest(void *ctx, const spdk_nvme_cpl *cpl);
}  // namespace mooncake::detail
#endif
