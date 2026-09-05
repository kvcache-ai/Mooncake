#include "spdk/nof_connection.h"

#include <glog/logging.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <thread>
#include <vector>

#include <spdk/env.h>
#include <spdk/nvme.h>

namespace mooncake {

// ===================================================================
// NofQpairPool
// ===================================================================

NofQpairPool::NofQpairPool(std::vector<spdk_nvme_qpair *> qpairs,
                           uint32_t max_inflight_per_qpair,
                           uint32_t target_count, spdk_nvme_ctrlr *ctrlr)
    : qpairs_(std::move(qpairs)),
      max_inflight_per_qpair_(max_inflight_per_qpair),
      target_count_(target_count > 0 ? target_count
                                     : static_cast<uint32_t>(qpairs_.size())),
      ctrlr_(ctrlr) {}

// Spin until InflightCount() == 0 with no time budget.  The spin never
// times out: a hung destructor (target crash, network partition) is
// recoverable by an external watchdog, whereas force-freeing under live
// CQE callbacks is silent memory corruption.
//
// SPDK has no synchronous abort for SQ-resident requests in v23.01.1,
// so the release/acquire pair between IncrementInflight (submit side)
// and DecrementInflight (CQE side) is the only mechanism guarantee
// that every CQE has fired before this returns.
bool NofQpairPool::WaitForInflightCompletion() {
    while (inflight_count_.load(std::memory_order_acquire) > 0) {
        // PollAll drives pending CQEs synchronously; each callback's
        // DecrementInflight (release) is observed on the next acquire
        // load.
        int32_t processed = PollAll(0);
        if (processed < 0) {
            // qpair dead — stop polling but keep waiting for the
            // inflight counter to reach 0.
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    return true;
}

// Best-effort CQ drain on a single qpair.  SPDK has no public API to
// force-fail SQ-resident requests, so this only drains CQEs that are
// already ready in the CQ.
static void AbortAllInflightRequests(spdk_nvme_ctrlr * /*ctrlr*/,
                                     spdk_nvme_qpair *qp) {
    if (qp) {
        int processed = spdk_nvme_qpair_process_completions(qp, 0);
        (void)processed;
    }
}

void NofQpairPool::EnterDraining(const char *reason) {
    QpairPoolState expected = QpairPoolState::kActive;
    if (state_.compare_exchange_strong(expected, QpairPoolState::kDraining)) {
        LOG(ERROR) << "[NofQpairPool] entering DRAINING: "
                   << (reason ? reason : "(no reason)");
        for (auto *qp : qpairs_) {
            AbortAllInflightRequests(ctrlr_, qp);
        }
    }
    // Idempotent: another qpair in the same pool may have raced here
    // first.
}

NofQpairPool::~NofQpairPool() {
    // Mark closed BEFORE freeing any qpair so any late CQE observation
    // sees a consistent kClosed state.
    state_.store(QpairPoolState::kClosed, std::memory_order_release);

    for (auto *qp : qpairs_) {
        if (qp) {
            // Bounded CQ drain: harvest CQEs that have already arrived.
            for (int i = 0; i < 1000; ++i) {
                int processed = spdk_nvme_qpair_process_completions(qp, 0);
                if (processed <= 0) break;
            }

            // Best-effort CQ drain (catches any CQE that arrived since).
            AbortAllInflightRequests(ctrlr_, qp);

            // Strict fence: WaitForInflightCompletion observes the
            // release/acquire synchronises-with edge against the last
            // DecrementInflight, proving no callback body is still
            // running.
            WaitForInflightCompletion();

            if (was_ever_used_with_inflight_.load(std::memory_order_acquire)) {
                VLOG(2) << "[NofQpairPool::~NofQpairPool] "
                        << "InflightCount==0 proven via "
                        << "WaitForInflightCompletion";
            } else {
                VLOG(2) << "[NofQpairPool::~NofQpairPool] "
                        << "InflightCount trivially 0 (no Increment "
                        << "path); safety relies on caller ordering";
            }

            spdk_nvme_ctrlr_free_io_qpair(qp);
        }
    }
    qpairs_.clear();
}

spdk_nvme_qpair *NofQpairPool::GetNextQpair() {
    // Reject submissions once the pool has entered DRAINING.
    if (state_.load(std::memory_order_acquire) != QpairPoolState::kActive) {
        return nullptr;
    }
    if (qpairs_.empty()) return nullptr;
    uint32_t idx = round_robin_idx_.fetch_add(1, std::memory_order_relaxed);
    return qpairs_[idx % qpairs_.size()];
}

int32_t NofQpairPool::PollAll(uint32_t max_completions) {
    int32_t total = 0;
    int32_t first_error = 0;
    for (size_t i = 0; i < qpairs_.size(); ++i) {
        auto *qp = qpairs_[i];
#ifdef MOONCAKE_TEST_DRAIN
        // Test-only injection: synthesise a transport error on the
        // armed qpair.  Single-shot arm — cleared after consumption.
        size_t armed = pending_inject_error_idx_.exchange(
            SIZE_MAX, std::memory_order_acq_rel);
        if (armed == i) {
            if (first_error == 0) first_error = -1;
            continue;
        }
#endif
        int32_t n = spdk_nvme_qpair_process_completions(
            qp, max_completions == 0 ? 0 : (max_completions - total));
        if (n < 0) {
            // Do NOT early-return on the first error: a dead qpair must
            // not block consumption of CQEs from sibling qpairs in the
            // same pool, otherwise their SpdkNofSubTask objects would
            // never be returned to the sub_task_pool.
            if (first_error == 0) first_error = n;
            continue;
        }
        total += n;
        if (max_completions > 0 &&
            static_cast<uint32_t>(total) >= max_completions)
            break;
    }
    return first_error != 0 ? first_error : total;
}

// Use the ctrlr_ stored at construction time to allocate new qpairs.
// spdk_nvme_qpair is only forward-declared in the public header, so we
// cannot obtain the controller via qpairs_[0]->ctrlr — the controller
// pointer must be passed explicitly to the NofQpairPool constructor.
uint32_t NofQpairPool::TryGrow(uint32_t target_total) {
    if (qpairs_.empty() || qpairs_.size() >= target_total) {
        return 0;
    }

    if (!ctrlr_) {
        LOG(ERROR) << "[NofQpairPool::TryGrow] ctrlr_ is null"
                   << " — pool was not constructed with a controller pointer";
        return 0;
    }

    // Allocate into a temporary vector and only commit to qpairs_ after
    // verifying the pool is still kActive.  Otherwise a freshly
    // allocated qpair could be visible to GetNextQpair while the pool
    // is already DRAINING — its CQEs would never be aborted nor
    // drained.
    std::vector<spdk_nvme_qpair *> new_qpairs;

    for (uint32_t i = qpairs_.size(); i < target_total; i++) {
        // Re-check state on every iteration; bail out and free any
        // already-allocated qpairs if the pool is no longer kActive.
        if (state_.load(std::memory_order_acquire) != QpairPoolState::kActive) {
            for (auto *qp : new_qpairs) {
                spdk_nvme_ctrlr_free_io_qpair(qp);
            }
            return 0;
        }
        auto *qp = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr_, nullptr, 0);
        if (!qp) {
            // No free QIDs — stop and wait for the next TryGrow cycle.
            break;
        }
        new_qpairs.push_back(qp);
    }

    // Final commit-time check: the pool may have changed state between
    // the last iteration and here.
    if (state_.load(std::memory_order_acquire) != QpairPoolState::kActive) {
        for (auto *qp : new_qpairs) {
            spdk_nvme_ctrlr_free_io_qpair(qp);
        }
        return 0;
    }

    uint32_t added = static_cast<uint32_t>(new_qpairs.size());
    for (auto *qp : new_qpairs) {
        qpairs_.push_back(qp);
    }

    if (added > 0) {
        LOG(INFO) << "[NofQpairPool] Rebalanced: grew from "
                  << (qpairs_.size() - added) << " to " << qpairs_.size()
                  << " qpairs (target=" << target_total << ", recovered "
                  << added << " qpairs after peer disconnect)";
    }

    return added;
}

// ===================================================================
// NofConnection — helpers
// ===================================================================

namespace {

// Callback data passed from the probe_cb/attach_cb lambda → Connect().
struct ConnectCtx {
    spdk_nvme_ctrlr *ctrlr = nullptr;
    const NofConfig *config = nullptr;
    bool attach_called = false;
    // Unique per-connection host NQN counter.
    // When multiple Connect() calls share the same default hostnqn within
    // a single process, the target merges them onto one controller and
    // NVMe Set Features is rejected.  Assigning a unique hostnqn per
    // connection forces the target to create independent controllers.
    uint32_t hostnqn_id = 0;
};

// Global counter incremented per connection to guarantee uniqueness.
static std::atomic<uint32_t> g_hostnqn_counter{0};

/// Parse a transport string into (traddr, trsvcid, subnqn, trtype, ns).
/// Returns 0 on success, -1 on parse error.
int ParseTransportStr(const std::string &tr_str, std::string &traddr,
                      std::string &trsvcid, std::string &subnqn,
                      std::string &trtype, uint32_t &ns) {
    struct spdk_nvme_transport_id trid;
    std::memset(&trid, 0, sizeof(trid));
    if (spdk_nvme_transport_id_parse(&trid, tr_str.c_str()) != 0) return -1;

    traddr = trid.traddr;
    trsvcid = trid.trsvcid;
    subnqn = trid.subnqn;
    trtype = (trid.trtype == SPDK_NVME_TRANSPORT_TCP) ? "TCP" : "RDMA";

    // Parse ns: field
    ns = 1;
    auto ns_pos = tr_str.find("ns:");
    if (ns_pos != std::string::npos) {
        ns = static_cast<uint32_t>(
            std::strtoul(tr_str.c_str() + ns_pos + 3, nullptr, 10));
    }
    return 0;
}

}  // anonymous namespace

// ===================================================================
// NofConnection
// ===================================================================

NofConnection::NofConnection(spdk_nvme_ctrlr *ctrlr, spdk_nvme_ns *ns,
                             std::unique_ptr<NofQpairPool> pool,
                             uint32_t block_size, uint64_t num_blocks,
                             std::string subnqn, NofConfig config)
    : ctrlr_(ctrlr),
      ns_(ns),
      qpair_pool_(std::move(pool)),
      block_size_(block_size),
      num_blocks_(num_blocks),
      subnqn_(std::move(subnqn)),
      config_(std::move(config)) {}

NofConnection::~NofConnection() {
    // QpairPool destructor frees all qpairs.
    qpair_pool_.reset();
    if (ctrlr_) {
        spdk_nvme_detach(ctrlr_);
        ctrlr_ = nullptr;
    }
}

// static
std::unique_ptr<NofConnection> NofConnection::Connect(
    const std::string &traddr, const std::string &trsvcid,
    const std::string &subnqn, uint32_t ns_id, spdk_nvme_transport_type trtype,
    const NofConfig &config, std::string *error_msg) {
    // Build transport ID
    struct spdk_nvme_transport_id trid;
    std::memset(&trid, 0, sizeof(trid));
    snprintf(trid.traddr, sizeof(trid.traddr), "%s", traddr.c_str());
    snprintf(trid.trsvcid, sizeof(trid.trsvcid), "%s", trsvcid.c_str());
    snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", subnqn.c_str());
    // Use the caller-supplied transport type instead of hard-coding RDMA.
    // This allows TCP environments (MC_NOF_TRTYPE=TCP) to work correctly
    // without master/client transport type disagreement.
    trid.trtype = trtype;
    trid.adrfam = SPDK_NVMF_ADRFAM_IPV4;

    ConnectCtx ctx;
    ctx.config = &config;
    // Assign a unique per-connection host NQN.
    // Multiple spdk_nvme_probe calls within the same process default to
    // the same hostnqn; the target merges these connections onto one
    // controller and NVMe Set Features is rejected.  A global incrementing
    // counter ensures each connection gets an independent hostnqn.
    ctx.hostnqn_id = g_hostnqn_counter.fetch_add(1, std::memory_order_relaxed);

    // Probe callback: set controller options
    auto probe_cb = [](void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                       struct spdk_nvme_ctrlr_opts *opts) -> bool {
        auto *pctx = static_cast<ConnectCtx *>(cb_ctx);
        const auto &cfg = *pctx->config;

        opts->num_io_queues = cfg.num_io_queues;
        opts->io_queue_size = cfg.io_queue_size;
        opts->io_queue_requests = cfg.io_queue_requests;
        opts->keep_alive_timeout_ms = cfg.keep_alive_timeout_ms;

        // When hostnqn is empty SPDK uses a UUID-based default
        // (nqn.2014-08.org.nvmexpress:uuid:XXX); all connections in the
        // same process share that UUID, so the target merges them and IO
        // qpair allocation fails.  The nqn.2024-08.mooncake:c<N> format
        // gives each connection an independent controller.
        snprintf(opts->hostnqn, sizeof(opts->hostnqn),
                 "nqn.2024-08.mooncake:c%u", pctx->hostnqn_id);

        if (cfg.transport_ack_timeout > 0)
            opts->transport_ack_timeout =
                static_cast<uint8_t>(cfg.transport_ack_timeout);
        if (cfg.admin_queue_size > 0)
            opts->admin_queue_size = cfg.admin_queue_size;
        if (cfg.fabrics_connect_timeout_us > 0)
            opts->fabrics_connect_timeout_us = cfg.fabrics_connect_timeout_us;
        opts->header_digest = cfg.header_digest;
        opts->data_digest = cfg.data_digest;

        LOG(INFO) << "[NofConnection] Attaching to " << trid->traddr << " "
                  << trid->subnqn << " num_io_queues=" << opts->num_io_queues;
        return true;
    };

    // Attach callback: capture ctrlr
    auto attach_cb = [](void *cb_ctx, const struct spdk_nvme_transport_id *,
                        struct spdk_nvme_ctrlr *ctrlr,
                        const struct spdk_nvme_ctrlr_opts *) {
        auto *pctx = static_cast<ConnectCtx *>(cb_ctx);
        pctx->ctrlr = ctrlr;
        pctx->attach_called = true;
    };

    int rc = spdk_nvme_probe(&trid, &ctx, probe_cb, attach_cb, nullptr);
    if (rc != 0 || !ctx.ctrlr) {
        if (error_msg) {
            *error_msg = "probe_fail: rc=" + std::to_string(rc);
        }
        LOG(ERROR) << "[NofConnection] Probe failed for " << subnqn
                   << " tradr=" << traddr << " rc=" << rc;
        return nullptr;
    }

    // Verify namespace
    if (!spdk_nvme_ctrlr_is_active_ns(ctx.ctrlr, ns_id)) {
        if (error_msg) *error_msg = "namespace_inactive";
        spdk_nvme_detach(ctx.ctrlr);
        return nullptr;
    }

    spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctx.ctrlr, ns_id);
    uint32_t block_size = spdk_nvme_ns_get_sector_size(ns);
    uint64_t num_blocks = spdk_nvme_ns_get_num_sectors(ns);

    // Sequential allocation — no retry inside Connect().
    // Retry with backoff is handled by OpenNofSegment() outside the
    // connect_mutex_ lock.  Here we make a single sequential pass:
    // allocate as many I/O qpairs as possible up to `requested`.
    // If we get zero qpairs, return failure; the caller retries outside
    // the mutex after backoff.
    std::vector<spdk_nvme_qpair *> qpairs;
    uint32_t requested = config.num_io_queues;
    uint32_t min_required =
        config.enable_degradation ? config.min_io_queues : requested;

    // Allocate greedily in one pass without discrete tiers.
    // Discrete tiers would waste QIDs (e.g. taking only 8 when 13 are
    // available), whereas greedy allocation uses every available slot.
    for (uint32_t i = 0; i < requested; i++) {
        auto *qp = spdk_nvme_ctrlr_alloc_io_qpair(ctx.ctrlr, nullptr, 0);
        if (!qp) break;  // QID pool exhausted, stop allocating
        qpairs.push_back(qp);
    }

    if (qpairs.size() < min_required) {
        // Release partial allocations (< min is unusable)
        size_t partial_count = qpairs.size();
        for (auto *qp : qpairs) {
            spdk_nvme_ctrlr_free_io_qpair(qp);
        }
        qpairs.clear();

        if (error_msg) {
            if (requested == min_required) {
                *error_msg = "qpair_alloc_fail: all allocations failed";
            } else {
                *error_msg = "qpair_alloc_fail: got " +
                             std::to_string(partial_count) +
                             " (min=" + std::to_string(min_required) +
                             ", target=" + std::to_string(requested) + ")";
            }
        }
        LOG(ERROR) << "[NofConnection] QID exhaustion: " << partial_count
                   << " qpairs for " << subnqn << " (target=" << requested
                   << ", min=" << min_required
                   << ") — target QID pool likely exhausted";
        spdk_nvme_detach(ctx.ctrlr);
        return nullptr;
    }

    if (qpairs.size() < requested) {
        LOG(WARNING) << "[NofConnection] QID degraded: allocated "
                     << qpairs.size() << "/" << requested << " qpairs for "
                     << subnqn << " — performance may be reduced";
    }

    // target_count = initial requested count for TryGrow recovery;
    // ctrlr is kept so TryGrow can allocate new qpairs later.
    auto pool = std::make_unique<NofQpairPool>(
        std::move(qpairs), config.max_inflight_per_qpair, requested, ctx.ctrlr);

    LOG(INFO) << "[NofConnection] Connected to " << subnqn << " ns=" << ns_id
              << " block_size=" << block_size << " num_blocks=" << num_blocks
              << " qpairs=" << pool->Size();

    return std::unique_ptr<NofConnection>(
        new NofConnection(ctx.ctrlr, ns, std::move(pool), block_size,
                          num_blocks, subnqn, config));
}

// static
std::unique_ptr<NofConnection> NofConnection::Connect(
    const std::string &transport_str, const NofConfig &config,
    std::string *error_msg) {
    std::string traddr, trsvcid, subnqn, trtype;
    uint32_t ns = 1;
    if (ParseTransportStr(transport_str, traddr, trsvcid, subnqn, trtype, ns) !=
        0) {
        if (error_msg) *error_msg = "parse_transport_str_fail";
        return nullptr;
    }

    // Translate parsed transport type to SPDK enum and pass it through
    // to the 6-parameter Connect() so the SPDK transport ID is set
    // correctly for both RDMA and TCP environments.
    spdk_nvme_transport_type spdk_trtype;
    if (trtype == "TCP") {
        spdk_trtype = SPDK_NVME_TRANSPORT_TCP;
    } else {
        spdk_trtype = SPDK_NVME_TRANSPORT_RDMA;
    }

    return Connect(traddr, trsvcid, subnqn, ns, spdk_trtype, config, error_msg);
}

}  // namespace mooncake
