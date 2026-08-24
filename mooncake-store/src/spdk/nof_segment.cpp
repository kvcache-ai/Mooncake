#include "spdk/nof_segment.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>

#include <spdk/nvme.h>

namespace mooncake {

// ===================================================================
// Completion callback for pipeline I/O
//
// Atomic ordering tightened to release/acquire pair.  In single-thread-
// per-pool operation this is just intent documentation, but it prevents
// a stale error flag from racing ahead of its inflight decrement on
// contract violation.
// ===================================================================
static void pipeline_io_cb(void *ctx, const struct spdk_nvme_cpl *cpl) {
    auto *pc = static_cast<PipelineCtx *>(ctx);
    if (spdk_nvme_cpl_is_error(cpl)) {
        // Release so any subsequent acquire on ctx->error implies the
        // inflight decrement below is also visible.  In single-thread-
        // per-pool operation this documents intent; on contract
        // violation (multi-thread access) it prevents a stale error
        // flag from racing ahead of its inflight decrement.
        pc->error.store(true, std::memory_order_release);
    }
    pc->inflight.fetch_sub(1, std::memory_order_release);
}

// ===================================================================
// Drain helper
//
// Bounded drain of in-flight PipelineIO chunks.  Repeatedly polls the
// qpair pool until either:
//   (a) all in-flight chunks have been accounted for via callback,
//   (b) the qpair pool returns negative (qpair dead / transport fatal),
//   (c) the wall-clock budget elapses.
//
// Used in two places:
//   - PipelineIO internal: legacy fire-and-forget path, before
//     transferring ctx_sp to PipelineCtxRecycler.
//   - DrainForInflight: public member, called by explicit-lifetime
//     callers to synchronously wait before releasing ctx_sp / buf.
//
// budget_us comes from NofConfig::pipeline_drain_budget_us (env var
// MC_NVME_PIPELINE_DRAIN_BUDGET_US, default 1000 us).
// ===================================================================
static void DrainPipelineInflight(const std::shared_ptr<PipelineCtx> &ctx_sp,
                                  NofQpairPool &pool, uint32_t budget_us) {
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::microseconds(budget_us);
    while (ctx_sp->inflight.load(std::memory_order_acquire) > 0 &&
           std::chrono::steady_clock::now() < deadline) {
        int32_t rc = pool.PollAll(0);
        if (rc < 0) {
            // qpair dead; further PollAll is futile.  Caller's
            // responsibility (recycler holds ctx_sp until next
            // PipelineIO entry; explicit caller keeps ctx_sp alive).
            break;
        }
    }
}

// ===================================================================
// PipelineCtxRecycler implementation (file-scope part; class declared in
// nof_segment.h).
// ===================================================================
PipelineCtxRecycler &PipelineCtxRecycler::Instance() {
    static PipelineCtxRecycler inst;
    return inst;
}

void PipelineCtxRecycler::Push(std::shared_ptr<PipelineCtx> ctx) {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.push_back(std::move(ctx));
}

void PipelineCtxRecycler::Drain() {
    std::vector<std::shared_ptr<PipelineCtx>> to_release;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        to_release.swap(pending_);
    }
    // to_release destructs at end of scope; each shared_ptr refcount
    // drops to 0; PipelineCtx is freed.
}

// ===================================================================
// Test hook for the file-scope pipeline_io_cb.
//
// The symbol is defined unconditionally so unit tests can link against
// it regardless of how the OBJECT library was configured.  The
// declaration in nof_segment.h is still gated by MOONCAKE_TEST_PIPELINE_IO,
// so production code has no way to call it; the linker is free to
// strip it as dead code.
// ===================================================================
namespace mooncake::detail {
void InvokePipelineIoCbForTest(void *ctx, const spdk_nvme_cpl *cpl) {
    pipeline_io_cb(ctx, cpl);
}
}  // namespace mooncake::detail

// ===================================================================
// NofSegment
// ===================================================================

NofSegment::NofSegment(NofConnection *conn, uint64_t start_lba,
                       uint64_t num_blocks)
    : conn_(conn),
      start_lba_(start_lba),
      num_blocks_(num_blocks),
      config_(conn->GetConfig()) {}

// ---- Async single-request API ----

int NofSegment::SubmitRead(void *buf, uint64_t lba, uint32_t num_blocks,
                           NofIoCallback cb, void *cb_ctx) {
    // Guard against out-of-range access: the caller-supplied `lba` is
    // relative to the segment base, so we must translate to an absolute
    // device LBA via start_lba_ and reject requests that exceed the
    // segment extent.
    //
    // Overflow-safe comparison: when lba is close to UINT64_MAX, the
    // expression `lba + num_blocks > num_blocks_` can wrap around and
    // bypass the check.  Split the comparison so the addition never
    // occurs: lba must be within the segment, AND the requested count
    // must fit in the remaining space.
    if (lba > num_blocks_ || num_blocks > num_blocks_ - lba) {
        LOG(ERROR) << "[NofSegment] LBA out of range: lba=" << lba
                   << " count=" << num_blocks << " max=" << num_blocks_;
        return -1;
    }
    // Guard the start_lba_ + lba translation against overflow.  With the
    // bound above, lba <= num_blocks_, but start_lba_ is independent;
    // an attacker-controlled or corrupt descriptor could supply a huge
    // start_lba_ that would wrap when added to a still-in-range lba.
    if (lba > UINT64_MAX - start_lba_) {
        LOG(ERROR) << "[NofSegment] LBA offset overflow: start_lba="
                   << start_lba_ << " lba=" << lba;
        return -1;
    }
    uint64_t abs_lba = start_lba_ + lba;
    auto *qp = conn_->GetQpairPool().GetNextQpair();
    return spdk_nvme_ns_cmd_read(conn_->GetNs(), qp, buf, abs_lba, num_blocks,
                                 cb, cb_ctx, 0);
}

int NofSegment::SubmitWrite(void *buf, uint64_t lba, uint32_t num_blocks,
                            NofIoCallback cb, void *cb_ctx) {
    // Same bounds check and start_lba_ translation as SubmitRead.
    // Overflow-safe form: see SubmitRead for the rationale.
    if (lba > num_blocks_ || num_blocks > num_blocks_ - lba) {
        LOG(ERROR) << "[NofSegment] LBA out of range: lba=" << lba
                   << " count=" << num_blocks << " max=" << num_blocks_;
        return -1;
    }
    if (lba > UINT64_MAX - start_lba_) {
        LOG(ERROR) << "[NofSegment] LBA offset overflow: start_lba="
                   << start_lba_ << " lba=" << lba;
        return -1;
    }
    uint64_t abs_lba = start_lba_ + lba;
    auto *qp = conn_->GetQpairPool().GetNextQpair();
    return spdk_nvme_ns_cmd_write(conn_->GetNs(), qp, buf, abs_lba, num_blocks,
                                  cb, cb_ctx, 0);
}

int32_t NofSegment::PollCompletion(uint32_t max_completions) {
    return conn_->GetQpairPool().PollAll(max_completions);
}

// ---- Pipeline I/O ----
//
// PipelineRead/Write split a request into chunks and submit them across
// all qpairs in the connection's qpair pool, polling for completions
// until all chunks have completed.  The caller_ctx out-param gives
// explicit lifetime control over PipelineCtx; DrainForInflight blocks
// until in-flight callbacks have fired.

ssize_t NofSegment::PipelineRead(void *buf, uint64_t lba, uint32_t total_blocks,
                                 std::shared_ptr<PipelineCtx> *caller_ctx) {
    return PipelineIO(buf, lba, total_blocks, false, caller_ctx);
}

ssize_t NofSegment::PipelineWrite(const void *buf, uint64_t lba,
                                  uint32_t total_blocks,
                                  std::shared_ptr<PipelineCtx> *caller_ctx) {
    return PipelineIO(const_cast<void *>(buf), lba, total_blocks, true,
                      caller_ctx);
}

void NofSegment::DrainForInflight(const std::shared_ptr<PipelineCtx> &ctx_sp,
                                  uint32_t budget_us) {
    if (!ctx_sp) return;  // empty shared_ptr — nothing to drain.
    if (budget_us == 0) budget_us = config_.pipeline_drain_budget_us;
    DrainPipelineInflight(ctx_sp, conn_->GetQpairPool(), budget_us);
}

ssize_t NofSegment::PipelineIO(void *buf, uint64_t lba, uint32_t total_blocks,
                               bool is_write,
                               std::shared_ptr<PipelineCtx> *caller_ctx) {
    // Guard against out-of-range access, matching SubmitRead/SubmitWrite.
    // Overflow-safe form: see SubmitRead for the rationale.
    if (lba > num_blocks_ || total_blocks > num_blocks_ - lba) {
        LOG(ERROR) << "[NofSegment::PipelineIO] LBA out of range: lba=" << lba
                   << " count=" << total_blocks << " max=" << num_blocks_;
        return -1;
    }
    // Guard against degenerate config that would cause an infinite loop.
    uint32_t chunk_blocks = config_.chunk_blocks;
    uint32_t max_inflight = conn_->GetQpairPool().MaxInflight();
    if (chunk_blocks == 0 || max_inflight == 0) {
        LOG(ERROR) << "[NofSegment::PipelineIO] invalid config: chunk_blocks="
                   << chunk_blocks << " max_inflight=" << max_inflight;
        return -1;
    }

    // Guard the start_lba_ + lba translation against overflow.
    if (lba > UINT64_MAX - start_lba_) {
        LOG(ERROR) << "[NofSegment::PipelineIO] LBA offset overflow: start_lba="
                   << start_lba_ << " lba=" << lba;
        return -1;
    }

    // Translate caller-relative LBA to absolute device LBA via the
    // segment base, matching the single-request API behaviour.
    uint64_t abs_lba = start_lba_ + lba;
    auto &pool = conn_->GetQpairPool();
    auto *ns = conn_->GetNs();
    uint32_t block_size = conn_->GetBlockSize();

    // Drain any ctx still held by the recycler from a previous call.
    // By this point the prior call has returned; its qpair is owned by
    // conn_ and remains valid (until SpdkWrapper::Cleanup).  Releasing
    // here is safe and reclaims memory promptly.
    PipelineCtxRecycler::Instance().Drain();

    // Avoid pointless shared_ptr allocation for the zero-block case.
    if (total_blocks == 0) {
        if (caller_ctx) *caller_ctx = nullptr;
        return 0;
    }

    // Heap-allocate the ctx so it can outlive this scope.  The caller
    // (or the recycler) decides when to release.
    auto ctx_sp = std::make_shared<PipelineCtx>();
    auto &ctx = *ctx_sp;  // local alias for readability in the hot loop
    uint32_t next_block = 0;

    while (next_block < total_blocks ||
           ctx.inflight.load(std::memory_order_relaxed) > 0) {
        // Submit while there is room in the pipeline
        while (ctx.inflight.load(std::memory_order_relaxed) <
                   static_cast<int32_t>(max_inflight) &&
               next_block < total_blocks) {
            uint32_t chunk = std::min(total_blocks - next_block, chunk_blocks);
            uint8_t *ptr = static_cast<uint8_t *>(buf) +
                           static_cast<uint64_t>(next_block) * block_size;

            ctx.inflight.fetch_add(1, std::memory_order_relaxed);

            auto *qp = pool.GetNextQpair();
            int rc;
            if (is_write) {
                rc = spdk_nvme_ns_cmd_write(ns, qp, ptr, abs_lba + next_block,
                                            chunk, pipeline_io_cb, ctx_sp.get(),
                                            0);
            } else {
                rc = spdk_nvme_ns_cmd_read(ns, qp, ptr, abs_lba + next_block,
                                           chunk, pipeline_io_cb, ctx_sp.get(),
                                           0);
            }

            if (rc != 0) {
                ctx.inflight.fetch_sub(1, std::memory_order_relaxed);
                ctx.error.store(true, std::memory_order_release);
                break;
            }

            next_block += chunk;
        }

        // Poll all qpairs.  A negative return signals a qpair transport
        // error; we then drain with a bounded budget and return -1.
        int32_t poll_rc = pool.PollAll(0);
        if (poll_rc < 0) {
            LOG(ERROR) << "[NofSegment::PipelineIO] poll error rc=" << poll_rc
                       << " — draining inflight="
                       << ctx.inflight.load(std::memory_order_relaxed);
            // Drains with bounded budget; CQE that arrive during drain
            // still see a live heap object because ctx_sp is held below.
            DrainPipelineInflight(ctx_sp, pool,
                                  config_.pipeline_drain_budget_us);
            // Error never hands ownership to the caller.  Recycler
            // holds ctx_sp until the next PipelineIO entry's Drain().
            if (caller_ctx) *caller_ctx = nullptr;
            PipelineCtxRecycler::Instance().Push(std::move(ctx_sp));
            return -1;
        }

        // Error flag set by a callback: drain remaining in-flight chunks
        // with the same bounded helper as the poll-error path above.
        if (ctx.error.load(std::memory_order_acquire)) {
            DrainPipelineInflight(ctx_sp, pool,
                                  config_.pipeline_drain_budget_us);
            LOG(ERROR) << "[NofSegment::PipelineIO] I/O error at "
                       << (is_write ? "write" : "read")
                       << " next_block=" << next_block
                       << " total=" << total_blocks;
            // On error, ownership stays internal: ctx_sp goes to the
            // recycler so any late CQE still sees a live object.
            if (caller_ctx) *caller_ctx = nullptr;
            PipelineCtxRecycler::Instance().Push(std::move(ctx_sp));
            return -1;
        }
    }

    // All-or-nothing semantics: a return value of -1 is reported even if
    // some chunks succeeded.  On the success path the caller's buffer is
    // fully populated; on the failure paths above the buffer past
    // next_block is left untouched.
    //
    // Ownership transfer on success:
    //   - caller_ctx != nullptr: caller takes ownership.  Caller MUST
    //     call DrainForInflight(*caller_ctx) before releasing buf or
    //     *caller_ctx.
    //   - caller_ctx == nullptr: fire-and-forget.  Recycler holds ctx_sp
    //     until the next PipelineIO entry's Drain().
    if (caller_ctx) {
        *caller_ctx = std::move(ctx_sp);
    } else {
        PipelineCtxRecycler::Instance().Push(std::move(ctx_sp));
    }
    return static_cast<ssize_t>(static_cast<uint64_t>(total_blocks) *
                                block_size);
}

}  // namespace mooncake
