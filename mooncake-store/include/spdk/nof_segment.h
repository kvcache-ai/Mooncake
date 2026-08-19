/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File: mooncake-store/include/spdk/nof_segment.h
 * @Description: NofSegment class declaration, PipelineCtx
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <sys/types.h>

#include <spdk/nvme.h>

#include "nof_config.h"
#include "nof_connection.h"

namespace mooncake {

// Callback signature: void(void* ctx, const spdk_nvme_cpl* cpl)
using NofIoCallback = void (*)(void *, const struct spdk_nvme_cpl *);

// ---------------------------------------------------------------------------
// PipelineCtx — shared completion context for PipelineIO.
// ---------------------------------------------------------------------------
struct PipelineCtx {
    std::atomic<int32_t> inflight{0};
    std::atomic<bool> error{false};
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

    /// Pipeline read: interleaves submission and polling across ALL qpairs.
    /// Blocks until all data is transferred (or an error occurs).
    /// @param buf        Destination buffer (must be DMA-accessible).
    /// @param lba        Start LBA (in blocks).
    /// @param total_blocks  Number of blocks to read.
    /// @return Total bytes read on success, -1 on error.
    ssize_t PipelineRead(void *buf, uint64_t lba, uint32_t total_blocks);

    /// Pipeline write: same pattern as PipelineRead.
    ssize_t PipelineWrite(const void *buf, uint64_t lba, uint32_t total_blocks);

    // ---- Accessors ----
    NofConnection *GetConnection() const { return conn_; }
    uint64_t GetStartLba() const { return start_lba_; }
    uint64_t GetNumBlocks() const { return num_blocks_; }
    uint32_t GetBlockSize() const { return conn_->GetBlockSize(); }
    const NofConfig &GetConfig() const { return config_; }

   private:
    /// Common pipeline loop (read or write).
    ssize_t PipelineIO(void *buf, uint64_t lba, uint32_t total_blocks,
                       bool is_write);

    NofConnection *conn_;  // non-owning
    uint64_t start_lba_;
    uint64_t num_blocks_;
    NofConfig config_;
};

}  // namespace mooncake
