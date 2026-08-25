#pragma once

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

namespace mooncake {

/**
 * @brief NVMe-oF I/O configuration.
 *
 * All fields are populated from MC_NVME_* / MC_NOF_* environment
 * variables via FromEnv().  Defaults are documented per-field; tune via
 * the corresponding environment variables.
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
    // Tune via MC_NVME_CHUNK_BLOCKS, valid range [32, 1024].
    uint32_t chunk_blocks = 512;

    // PipelineIO drain budget (microseconds).  When PipelineIO needs to drain
    // in-flight callbacks (caller_ctx==nullptr path's error handling, or
    // DrainForInflight with default budget), it polls until the ctx's inflight
    // counter reaches 0 or until this budget expires.  After the budget
    // elapses, any callback still pending is delivered to a live heap object
    // (ctx_sp is held by PipelineCtxRecycler or by the caller).
    //
    // Default 1000 us matches the existing 1ms budget used in
    // NofQpairPool::~NofQpairPool (1000 CQEs per qpair).  Tune via
    // MC_NVME_PIPELINE_DRAIN_BUDGET_US, valid range [100, 100000].
    uint32_t pipeline_drain_budget_us = 1000;

    // ---- Degradation / fallback ----

    // Minimum acceptable I/O qpair count.  Even when the target QID pool is
    // nearly exhausted, the connection will succeed as long as at least this
    // many qpairs can be allocated.
    // Default 1 = extreme degradation mode (1 qpair still works, lower
    // throughput). Env var: MC_NVME_MIN_IO_QUEUES, valid range [1,
    // num_io_queues].
    uint32_t min_io_queues = 1;

    // Maximum retries on allocation failure.  Retry interval grows
    // exponentially (retry_backoff_ms * 2^attempt).
    // Default 5 = max ~3100 ms total backoff (100+200+400+800+1600 ms).
    // Covers a typical keep-alive reclamation window
    // (MC_NVME_KEEP_ALIVE_TIMEOUT_MS=2000). Env var:
    // MC_NVME_RETRY_MAX_ATTEMPTS, valid range [0, 10].
    uint32_t retry_max_attempts = 5;

    // Base retry backoff interval in ms.  Actual = retry_backoff_ms * (1 <<
    // attempt). Default 100 ms → total backoff ~3.1 s, ample opportunity after
    // keep-alive 2 s window. Env var: MC_NVME_RETRY_BACKOFF_MS, valid range
    // [10, 5000].
    uint32_t retry_backoff_ms = 100;

    // Enable adaptive degradation.  When disabled, Connect() falls back to
    // old behaviour (0 qpair = immediate failure).
    // Env var: MC_NVME_ENABLE_DEGRADATION (1=on, 0=off).
    bool enable_degradation = true;

    // ---- Flow control ----

    // Maximum per-worker task queue depth.  0 = no limit.
    // When exceeded, submitTask() blocks the caller to create backpressure,
    // preventing unbounded memory growth.
    // Default 256 = based on 128 KiB chunks, holds ~32 MiB of typical payload.
    // Env var: MC_NOF_MAX_QUEUE_DEPTH, valid range [0, 4096].
    int max_queue_depth = 256;

    // Adapt inflight_blocks_limit based on the active qpair count.
    // When enabled, degrading to 1 qpair automatically shrinks the inflight
    // cap to prevent excessive in-flight I/O on a single qpair.
    // Env var: MC_NOF_ADAPTIVE_INFLIGHT (1=on, 0=off).
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

    /// Dedicated heartbeat-probe configuration: minimum qpair count
    /// (num_io_queues=1).  Probes need only a single 1-block read and must
    /// not compete with the I/O path for QIDs.
    static NofConfig ForProbe();
};

// ===================================================================
// Inline helpers used by FromEnv()
// ===================================================================

namespace {

inline bool ParseEnvU64_(const char *name, uint64_t *out) {
    const char *val = std::getenv(name);
    if (!val || *val == '\0') return false;

    errno = 0;
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
    if (ParseEnvU64_("MC_NVME_PIPELINE_DRAIN_BUDGET_US", &v))
        cfg.pipeline_drain_budget_us = static_cast<uint32_t>(v);

    // Degradation / fallback
    if (ParseEnvU64_("MC_NVME_MIN_IO_QUEUES", &v))
        cfg.min_io_queues = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_RETRY_MAX_ATTEMPTS", &v))
        cfg.retry_max_attempts = static_cast<uint32_t>(v);
    if (ParseEnvU64_("MC_NVME_RETRY_BACKOFF_MS", &v))
        cfg.retry_backoff_ms = static_cast<uint32_t>(v);
    if (ParseEnvBool_("MC_NVME_ENABLE_DEGRADATION", &bv))
        cfg.enable_degradation = bv;

    // Flow control
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
    if (cfg.pipeline_drain_budget_us < 100) cfg.pipeline_drain_budget_us = 100;
    if (cfg.pipeline_drain_budget_us > 100000)
        cfg.pipeline_drain_budget_us = 100000;

    // Degradation sanity checks
    if (cfg.min_io_queues < 1) cfg.min_io_queues = 1;
    if (cfg.min_io_queues > cfg.num_io_queues)
        cfg.min_io_queues = cfg.num_io_queues;
    if (cfg.retry_max_attempts > 10) cfg.retry_max_attempts = 10;
    if (cfg.retry_backoff_ms < 10) cfg.retry_backoff_ms = 10;
    if (cfg.retry_backoff_ms > 5000) cfg.retry_backoff_ms = 5000;

    // Flow control sanity checks
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

// Dedicated heartbeat-probe configuration: minimum qpair count for probing.
// Probes need only a single 1-block read.  Using num_io_queues=1 drops the
// per-probe QID cost from 17 to 2, avoiding contention with the I/O path
// for the target QID pool under high-frequency probing.
inline NofConfig NofConfig::ForProbe() {
    NofConfig cfg = FromEnv();
    cfg.num_io_queues = 1;
    cfg.min_io_queues = 1;
    cfg.retry_max_attempts = 2;  // Probe needs at most 2 retries
    cfg.retry_backoff_ms = 50;
    // No degradation for probes; 1 qpair is enough
    cfg.enable_degradation = false;
    return cfg;
}

}  // namespace mooncake
