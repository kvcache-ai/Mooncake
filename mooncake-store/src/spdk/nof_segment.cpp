/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File: mooncake-store/src/spdk/nof_segment.cpp
 * @Description: PipelineIO 核心循环实现
 */
#include "spdk/nof_segment.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>

#include <spdk/nvme.h>

namespace mooncake {

// ===================================================================
// Completion callback for pipeline I/O
// ===================================================================
static void pipeline_io_cb(void *ctx, const struct spdk_nvme_cpl *cpl) {
    auto *pc = static_cast<PipelineCtx *>(ctx);
    if (spdk_nvme_cpl_is_error(cpl)) {
        pc->error.store(true, std::memory_order_relaxed);
    }
    pc->inflight.fetch_sub(1, std::memory_order_relaxed);
}

// ===================================================================
// NofSegment
// ===================================================================

NofSegment::NofSegment(NofConnection *conn, uint64_t start_lba, uint64_t num_blocks)
    : conn_(conn), start_lba_(start_lba), num_blocks_(num_blocks),
      config_(conn->GetConfig()) {}

// ---- Async single-request API ----

int NofSegment::SubmitRead(void *buf, uint64_t lba, uint32_t num_blocks,
                            NofIoCallback cb, void *cb_ctx) {
    auto *qp = conn_->GetQpairPool().GetNextQpair();
    return spdk_nvme_ns_cmd_read(conn_->GetNs(), qp, buf, lba, num_blocks, cb, cb_ctx, 0);
}

int NofSegment::SubmitWrite(void *buf, uint64_t lba, uint32_t num_blocks,
                             NofIoCallback cb, void *cb_ctx) {
    auto *qp = conn_->GetQpairPool().GetNextQpair();
    return spdk_nvme_ns_cmd_write(conn_->GetNs(), qp, buf, lba, num_blocks, cb, cb_ctx, 0);
}

int32_t NofSegment::PollCompletion(uint32_t max_completions) {
    return conn_->GetQpairPool().PollAll(max_completions);
}

// ---- Pipeline I/O ----

ssize_t NofSegment::PipelineRead(void *buf, uint64_t lba, uint32_t total_blocks) {
    return PipelineIO(buf, lba, total_blocks, false);
}

ssize_t NofSegment::PipelineWrite(const void *buf, uint64_t lba, uint32_t total_blocks) {
    return PipelineIO(const_cast<void *>(buf), lba, total_blocks, true);
}

ssize_t NofSegment::PipelineIO(void *buf, uint64_t lba, uint32_t total_blocks, bool is_write) {
    auto &pool = conn_->GetQpairPool();
    auto *ns = conn_->GetNs();
    uint32_t block_size = conn_->GetBlockSize();
    uint32_t max_inflight = pool.MaxInflight();
    uint32_t chunk_blocks = config_.chunk_blocks;

    PipelineCtx ctx;
    uint32_t next_block = 0;

    while (next_block < total_blocks || ctx.inflight.load(std::memory_order_relaxed) > 0) {
        // Submit while there is room in the pipeline
        while (ctx.inflight.load(std::memory_order_relaxed) < static_cast<int32_t>(max_inflight)
               && next_block < total_blocks) {
            uint32_t chunk = std::min(total_blocks - next_block, chunk_blocks);
            uint8_t *ptr = static_cast<uint8_t *>(buf) + static_cast<uint64_t>(next_block) * block_size;

            ctx.inflight.fetch_add(1, std::memory_order_relaxed);

            auto *qp = pool.GetNextQpair();
            int rc;
            if (is_write) {
                rc = spdk_nvme_ns_cmd_write(ns, qp, ptr,
                                            lba + next_block, chunk,
                                            pipeline_io_cb, &ctx, 0);
            } else {
                rc = spdk_nvme_ns_cmd_read(ns, qp, ptr,
                                           lba + next_block, chunk,
                                           pipeline_io_cb, &ctx, 0);
            }

            if (rc != 0) {
                ctx.inflight.fetch_sub(1, std::memory_order_relaxed);
                ctx.error.store(true, std::memory_order_relaxed);
                break;
            }

            next_block += chunk;
        }

        // Poll all qpairs
        pool.PollAll(0);

        // Check for errors
        if (ctx.error.load(std::memory_order_relaxed)) {
            // Drain remaining inflight I/Os — poll until everything settles
            while (ctx.inflight.load(std::memory_order_relaxed) > 0) {
                pool.PollAll(0);
            }
            LOG(ERROR) << "[NofSegment::PipelineIO] I/O error at "
                       << (is_write ? "write" : "read")
                       << " next_block=" << next_block
                       << " total=" << total_blocks;
            return -1;
        }
    }

    return static_cast<ssize_t>(static_cast<uint64_t>(total_blocks) * block_size);
}

}  // namespace mooncake
