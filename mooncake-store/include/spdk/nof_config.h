/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File: mooncake-store/include/spdk/nof_config.h
 * @Description: NofConfig 配置结构, FromEnv()/ForRead()/ForWrite()
 */
#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

namespace mooncake {

/**
 * @brief NVMe-oF I/O configuration.
 *
 * All fields are populated from MC_NVME_* environment variables via
 * FromEnv(), with sensible defaults derived from benchmark data on the
 * FORINN HWE62P447T6L00LN NVMe-oF target.
 */
struct NofConfig {
    // ---- Controller options ----
    uint32_t num_io_queues = 16;
    uint32_t io_queue_size = 256;
    uint32_t io_queue_requests = 512;

    // Keep-alive timeout in milliseconds.  MUST be > 0 so the target can
    // detect dead connections.  10 s is a safe default.
    uint32_t keep_alive_timeout_ms = 10000;

    uint32_t transport_ack_timeout = 0;  // 0 = SPDK default
    uint16_t admin_queue_size = 64;
    uint64_t fabrics_connect_timeout_us = 0;
    bool header_digest = false;
    bool data_digest = false;

    // ---- Pipeline parameters ----
    // Maximum outstanding I/Os per qpair (depth).
    uint32_t max_inflight_per_qpair = 64;

    // I/O chunk size in blocks.  512 blocks = 2 MiB with 4 KiB blocks.
    // Benchmark showed 512 gives best read throughput (3,345 MB/s)
    // and peak write (3,087 MB/s) on FORINN HWE62P447T6L00LN.
    uint32_t chunk_blocks = 512;

    // ---- RAID 0 ----
    // Stripe size in KiB.  Must ≥ chunk_blocks in blocks for optimal
    // RAID 0 pipeline throughput.  2048 KiB = 512 blocks with 4 KiB blocks,
    // matching the optimal chunk_blocks.
    uint32_t raid0_stripe_size_kb = 2048;

    // ---- Degradation / fallback ----
    // 修改履历 | 2026-08-03 | 新增降级配置字段（本 PR）

    // 最小可接受的 IO qpair 数。即使 target QID 池接近耗尽，
    // 只要至少能分配此数量的 qpair，连接就不会失败。
    // 默认 1 = 极限降级模式（1 qpair 仍可工作，只是吞吐量降低）。
    // 环境变量: MC_NVME_MIN_IO_QUEUES，有效范围 [1, num_io_queues]。
    uint32_t min_io_queues = 1;

    // 分配失败时的最大重试次数。重试间隔指数增长 (retry_backoff_ms *
    // 2^attempt)。 默认 5 次 = 最多约 3100ms 总退避（100+200+400+800+1600ms）。
    // 覆盖 keep-alive 典型回收窗口 (MC_NVME_KEEP_ALIVE_TIMEOUT_MS=2000)。
    // 环境变量: MC_NVME_RETRY_MAX_ATTEMPTS，有效范围 [0, 10]。
    uint32_t retry_max_attempts = 5;

    // 重试退避基础间隔 (ms)。实际间隔 = retry_backoff_ms * (1 << attempt)。
    // 默认 100ms → 总退避 ~3.1s，覆盖 keep-alive 2s 窗口后有充足重试机会。
    // 环境变量: MC_NVME_RETRY_BACKOFF_MS，有效范围 [10, 5000]。
    uint32_t retry_backoff_ms = 100;

    // 启用自适应降级。关闭后 Connect() 回退到旧行为（0 qpair 即失败）。
    // 环境变量: MC_NVME_ENABLE_DEGRADATION (1=on, 0=off)。
    bool enable_degradation = true;

    // ---- Flow control (修改履历 | 2026-08-03 | 本 PR) ----

    // 每个 worker 线程的任务队列最大深度。0 = 不限制。
    // 超过此值时 submitTask() 阻塞调用方以形成背压，防止内存溢出。
    // 默认 256 = 基于 128KB chunk 可容纳约 32MB 的典型 payload。
    // 环境变量: MC_NOF_MAX_QUEUE_DEPTH，有效范围 [0, 4096]。
    int max_queue_depth = 256;

    // 是否根据 qpair 数自适应调整 inflight_blocks_limit。
    // 启用后，降级到 1 qpair 时自动缩小 inflight 封顶，防止
    // 在单 qpair 上积累过多在途 I/O 导致长时间排空。
    // 环境变量: MC_NOF_ADAPTIVE_INFLIGHT (1=on, 0=off)。
    bool adaptive_inflight = true;

    // -------------------------------------------------------------------
    // Factory helpers
    // -------------------------------------------------------------------

    /// Populate from environment variables (MC_NVME_*).  Unset vars keep
    /// their default values.
    static NofConfig FromEnv();

    /// Profile tuned for read-mostly workloads.
    static NofConfig ForRead();

    /// Profile tuned for write-heavy workloads.
    static NofConfig ForWrite();

    /// 心跳探测专用：最小 qpair 数 (num_io_queues=1)。
    /// 探测只需 1 次 1-block read，不应与 I/O 路径竞争 QID。
    /// 修改履历 | 2026-08-03 | 新增（本 PR）
    static NofConfig ForProbe();
};

// ===================================================================
// Inline helpers used by FromEnv()
// ===================================================================

namespace {

inline bool ParseEnvU64_(const char *name, uint64_t *out) {
    const char *val = std::getenv(name);
    if (!val || *val == '\0') return false;

    char *end = nullptr;
    unsigned long long parsed = std::strtoull(val, &end, 10);
    if (errno != 0 || end == val || (end && *end != '\0')) return false;
    *out = static_cast<uint64_t>(parsed);
    return true;
}

inline bool ParseEnvBool_(const char *name, bool *out) {
    uint64_t v = 0;
    if (!ParseEnvU64_(name, &v)) return false;
    *out = (v != 0);
    return true;
}

}  // anonymous namespace

inline NofConfig NofConfig::FromEnv() {
    NofConfig cfg;
    uint64_t v = 0;
    bool bv = false;

    if (ParseEnvU64_("MC_NVME_NUM_IO_QUEUES", &v))
        cfg.num_io_queues = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_IO_QUEUE_SIZE", &v))
        cfg.io_queue_size = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_IO_QUEUE_REQUESTS", &v))
        cfg.io_queue_requests = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_KEEP_ALIVE_TIMEOUT_MS", &v))
        cfg.keep_alive_timeout_ms = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_TRANSPORT_ACK_TIMEOUT", &v))
        cfg.transport_ack_timeout = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_ADMIN_QUEUE_SIZE", &v))
        cfg.admin_queue_size = static_cast<uint16_t>(v);
    if (ParseEnvU64_("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", &v))
        cfg.fabrics_connect_timeout_us = v;
    if (ParseEnvBool_("MC_NVME_HEADER_DIGEST", &bv)) cfg.header_digest = bv;
    if (ParseEnvBool_("MC_NVME_DATA_DIGEST", &bv)) cfg.data_digest = bv;

    // Pipeline tuning
    if (ParseEnvU64_("MC_NVME_MAX_INFLIGHT_PER_QPAIR", &v))
        cfg.max_inflight_per_qpair = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_CHUNK_BLOCKS", &v))
        cfg.chunk_blocks = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_RAID0_STRIPE_SIZE_KB", &v))
        cfg.raid0_stripe_size_kb = static_cast<uint32_t>(v);

    // Degradation / fallback (修改履历 | 2026-08-03 | 本 PR)
    if (ParseEnvU64_("MC_NVME_MIN_IO_QUEUES", &v))
        cfg.min_io_queues = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_RETRY_MAX_ATTEMPTS", &v))
        cfg.retry_max_attempts = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_RETRY_BACKOFF_MS", &v))
        cfg.retry_backoff_ms = static_cast<uint32_t>(v);
    if (ParseEnvBool_("MC_NVME_ENABLE_DEGRADATION", &bv))
        cfg.enable_degradation = bv;

    // Flow control (修改履历 | 2026-08-03 | 本 PR)
    if (ParseEnvU64_("MC_NOF_MAX_QUEUE_DEPTH", &v))
        cfg.max_queue_depth = static_cast<int>(v);
    if (ParseEnvBool_("MC_NOF_ADAPTIVE_INFLIGHT", &bv))
        cfg.adaptive_inflight = bv;

    // Sanity checks
    if (cfg.num_io_queues < 1) cfg.num_io_queues = 1;
    if (cfg.num_io_queues > 1024) cfg.num_io_queues = 1024;
    if (cfg.chunk_blocks < 32) cfg.chunk_blocks = 32;
    if (cfg.chunk_blocks > 1024) cfg.chunk_blocks = 1024;
    if (cfg.max_inflight_per_qpair < 1) cfg.max_inflight_per_qpair = 1;
    if (cfg.max_inflight_per_qpair > 256) cfg.max_inflight_per_qpair = 256;

    // Degradation sanity checks (修改履历 | 2026-08-03 | 本 PR)
    if (cfg.min_io_queues < 1) cfg.min_io_queues = 1;
    if (cfg.min_io_queues > cfg.num_io_queues)
        cfg.min_io_queues = cfg.num_io_queues;
    if (cfg.retry_max_attempts > 10) cfg.retry_max_attempts = 10;
    if (cfg.retry_backoff_ms < 10) cfg.retry_backoff_ms = 10;
    if (cfg.retry_backoff_ms > 5000) cfg.retry_backoff_ms = 5000;

    // Flow control sanity checks (修改履历 | 2026-08-03 | 本 PR)
    if (cfg.max_queue_depth < 0) cfg.max_queue_depth = 0;
    if (cfg.max_queue_depth > 4096) cfg.max_queue_depth = 4096;

    return cfg;
}

inline NofConfig NofConfig::ForRead() {
    NofConfig cfg = FromEnv();
    if (cfg.num_io_queues > 4) cfg.num_io_queues = 4;
    return cfg;
}

inline NofConfig NofConfig::ForWrite() {
    NofConfig cfg = FromEnv();
    if (cfg.num_io_queues < 16) cfg.num_io_queues = 16;
    return cfg;
}

// 修改履历 | 2026-08-03 | 新增（本 PR）
// 心跳探测专用：最小 qpair 数，探测只需 1 次 1-block read。
// 使用 num_io_queues=1 将每次探测的 QID 消耗从 17 降到 2，
// 避免在高频探测时与 I/O 路径竞争 target QID 池。
inline NofConfig NofConfig::ForProbe() {
    NofConfig cfg = FromEnv();
    cfg.num_io_queues = 1;
    cfg.min_io_queues = 1;
    cfg.retry_max_attempts = 2;  // 探测连接重试 2 次即可
    cfg.retry_backoff_ms = 50;
    cfg.enable_degradation = false;  // 探测不降级，1 条 qpair 足够
    return cfg;
}

}  // namespace mooncake
