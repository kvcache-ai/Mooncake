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

bool NofQpairPool::WaitForInflightCompletion(uint32_t budget_us) {
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::microseconds(budget_us);
    while (inflight_count_.load(std::memory_order_acquire) > 0 &&
           std::chrono::steady_clock::now() < deadline) {
        // PollAll surfaces pending CQEs synchronously; the CQE callback
        // decrements inflight_count_ via release, which we observe on
        // the next acquire load.  This is the active side of the
        // synchronizes-with edge.
        int32_t processed = PollAll(0);
        if (processed < 0) {
            // qpair dead; further polling is futile.  Caller's
            // responsibility to break out.
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return inflight_count_.load(std::memory_order_acquire) == 0;
}

NofQpairPool::~NofQpairPool() {
    for (auto *qp : qpairs_) {
        if (qp) {
            // ─────────────────────────────────────────────────────────
            // Synchronous proof of "no CQE will fire
            // after this returns" BEFORE free_io_qpair.
            //
            // Three-step protocol:
            //   Step 1: drain pending CQEs (bounded, cheap).
            //   Step 2: WaitForInflightCompletion with a hard cap.
            //           InflightCount()==0 is the actual proof that
            //           all CQEs have been processed (or that no path
            //           ever called IncrementInflight).
            //   Step 3: free_io_qpair.
            //
            // Without Step 2, free_io_qpair may be called while a CQE
            // is still in SPDK's internal queue, and that CQE would
            // fire on already-freed user memory.
            //
            // Note: SPDK does NOT expose a public API to forcibly fail
            // an in-flight qpair.  The helper `nvme_qpair_fail` exists
            // in lib/nvme/nvme_qpair.c but is declared `static` and
            // not exported via spdk/nvme.h.  The closest public
            // alternative, `spdk_nvme_ctrlr_abort_queued_requests`,
            // matches by SQID/CID mask and only aborts requests still
            // in the controller's submission queue — it does NOT abort
            // already-submitted ones that have left the SQ, so it
            // does not help in our timeout-fallback case.
            //
            // We therefore rely on WaitForInflightCompletion's 30 s
            // budget.  Two preconditions make this sound:
            //   (a) callers should already have drained in-flight
            //       before reaching ~NofQpairPool (e.g.
            //       ~TransferSubmitter joins workers first;
            //       ProbeNofSegment's Phase 1b waits for quiescent);
            //   (b) for the timeout-fallback path, ProbeNofSegment
            //       defers BOTH ctx AND conn (and the submit wrapper)
            //       to ProbeCtxRecycler, which keeps the wrapper's
            //       pool pointer valid until the next probe's Drain().
            // ─────────────────────────────────────────────────────────

            // Step 1: bounded drain of any pending CQEs.
            for (int i = 0; i < 1000; ++i) {
                int processed = spdk_nvme_qpair_process_completions(qp, 0);
                if (processed <= 0) break;
            }

            // Step 2: synchronous wait for inflight to reach 0.
            // 30 s is generous: a stuck CQE either resolves in
            // milliseconds (network blip) or is genuinely broken
            // (target crash).  30 s catches the former; the latter
            // is logged as ERROR below.
            constexpr uint32_t kQuiescentBudgetUs = 30'000'000;
            bool quiescent = WaitForInflightCompletion(kQuiescentBudgetUs);
            if (quiescent) {
                if (was_ever_used_with_inflight_.load(
                        std::memory_order_acquire)) {
                    VLOG(2) << "[NofQpairPool::~NofQpairPool] "
                            << "InflightCount==0 proven via "
                            << "WaitForInflightCompletion";
                } else {
                    // No submit path used Increment on this conn.
                    // Synchronization is provided by caller ordering
                    // (see CloseNofSegment docs: ~TransferSubmitter
                    // resets worker pool first).
                    VLOG(2) << "[NofQpairPool::~NofQpairPool] "
                            << "InflightCount trivially 0 (no Increment "
                            << "path); safety relies on caller ordering";
                }
            } else {
                LOG(ERROR) << "[NofQpairPool::~NofQpairPool] InflightCount="
                           << InflightCount()
                           << " after 30 s quiescent budget — target likely "
                           << "crashed. Freeing qpair anyway; late callbacks "
                           << "may access freed memory. This is a known "
                           << "limitation when InflightCount is non-zero and "
                           << "no fail mechanism (spdk_nvme_qpair_fail) is "
                           << "invoked.";
            }

            // Step 3: free the qpair.
            spdk_nvme_ctrlr_free_io_qpair(qp);
        }
    }
    qpairs_.clear();
}

spdk_nvme_qpair *NofQpairPool::GetNextQpair() {
    if (qpairs_.empty()) return nullptr;
    uint32_t idx = round_robin_idx_.fetch_add(1, std::memory_order_relaxed);
    return qpairs_[idx % qpairs_.size()];
}

int32_t NofQpairPool::PollAll(uint32_t max_completions) {
    int32_t total = 0;
    for (auto *qp : qpairs_) {
        int32_t n = spdk_nvme_qpair_process_completions(
            qp, max_completions == 0 ? 0 : (max_completions - total));
        if (n < 0) return n;
        total += n;
        if (max_completions > 0 &&
            static_cast<uint32_t>(total) >= max_completions)
            break;
    }
    return total;
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

    uint32_t added = 0;
    for (uint32_t i = qpairs_.size(); i < target_total; i++) {
        auto *qp = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr_, nullptr, 0);
        if (!qp) {
            // No free QIDs — stop and wait for the next TryGrow cycle.
            break;
        }
        qpairs_.push_back(qp);
        added++;
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
    const std::string &subnqn, uint32_t ns_id,
    spdk_nvme_transport_type trtype, const NofConfig &config,
    std::string *error_msg) {
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
                   << " qpairs for " << subnqn
                   << " (target=" << requested << ", min=" << min_required
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

    return Connect(traddr, trsvcid, subnqn, ns, spdk_trtype, config,
                   error_msg);
}

}  // namespace mooncake
