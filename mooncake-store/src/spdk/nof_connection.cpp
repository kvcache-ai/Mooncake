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

#ifdef USE_NOF
#include "transfer_task.h"  // for SubTaskFreeList full definition
#endif

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

// Block until InflightCount() == 0 OR `timeout_ms` elapses OR
// PollAll reports a transport error (<0).
//
// Three terminal exit paths, all force-zero + force-DRAINING:
//   1. PollAll < 0 (dead qpair): PollAll iterated the full sibling
//      set before reporting, so healthy sibling CQEs have already
//      been delivered and accounted for in inflight_count_; the
//      remaining count belongs to the dead qpair(s), whose CQEs will
//      never arrive.  Force-zero without waiting for the deadline.
//   2. timeout_ms elapsed: same terminal actions as fast-path.
//      Fallback for slow-but-not-dead targets where PollAll never
//      reports -1.
//   3. InflightCount() == 0 observed: success, no terminal actions.
//
// On every terminal path, late CQEs that arrive after force-zero are
// silenced by:
//   - state_ forced to kDraining, so the trampoline reads
//     IsDraining()==true and short-circuits before touching task
//     memory (transfer_task.cpp:nvmf_io_complete).
//   - DecrementInflight's CAS-saturating clamp, so inflight_count_
//     cannot underflow below 0.
bool NofQpairPool::WaitForInflightCompletion(uint32_t timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (inflight_count_.load(std::memory_order_acquire) > 0) {
        // timeout_ms == 0 is observe-only: do not wait, do not poll.
        // Return false immediately so the caller can decide whether
        // the non-zero count is acceptable (e.g. ~NofQpairPool can
        // log and proceed).
        int32_t processed = PollAll(0);
        if (processed < 0) {
            // Fast-path: at least one qpair reported a transport
            // error.  PollAll iterated the full sibling set before
            // reporting, so healthy sibling CQEs have already been
            // delivered and accounted for in inflight_count_.  The
            // remaining count belongs to the dead qpair(s), whose
            // CQEs will never arrive — force-zero without waiting
            // for the deadline.
            inflight_count_.exchange(0, std::memory_order_acq_rel);
            QpairPoolState expected = QpairPoolState::kActive;
            state_.compare_exchange_strong(expected, QpairPoolState::kDraining);
            return false;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            // Slow-target fallback: deadline elapsed with no error
            // signal.  Same terminal actions as the fast-path.
            inflight_count_.exchange(0, std::memory_order_acq_rel);
            QpairPoolState expected = QpairPoolState::kActive;
            state_.compare_exchange_strong(expected, QpairPoolState::kDraining);
            return false;
        }
        // PollAll drove pending CQEs synchronously; each callback's
        // DecrementInflight (release) is observed on the next acquire
        // load.  Sleep briefly before the next poll.
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return true;
}

// Best-effort CQ drain on a single qpair.
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
            // running.  timeout_ms=0 makes this observe-only: by the
            // time the destructor runs, the worker drain protocol
            // (DrainDrainingPoolsUntilQuiescent) has already paid back
            // every counter via FinalizeAfterDrain, so InflightCount
            // should be 0.  If not, log an ERROR and proceed; the
            // pool is in kClosed state so any late CQE takes the
            // trampoline's DRAINING short-circuit.
            bool quiescent = WaitForInflightCompletion(/*timeout_ms=*/0);
            if (!quiescent) {
                LOG(ERROR)
                    << "[NofQpairPool::~NofQpairPool] InflightCount="
                    << InflightCount()
                    << " at destructor entry — worker drain protocol "
                    << "did not converge; relying on trampoline DRAINING "
                    << "short-circuit and CAS-saturating DecrementInflight";
            }

#ifndef NDEBUG
            // DEBUG invariant: once the worker has run
            // DrainDrainingPoolsUntilQuiescent + FinalizeAfterDrain, every
            // callback that COULD have fired must have already decremented.
            // A non-zero counter at destruction time means a task's CQE
            // handler is still outstanding — this is a worker-drain
            // regression we want to surface loudly in DEBUG builds instead
            // of silently relying on the trampoline DRAINING short-circuit.
            int32_t final_count =
                inflight_count_.load(std::memory_order_acquire);
            if (was_ever_used_with_inflight_.load(std::memory_order_acquire) &&
                final_count != 0) {
                LOG(FATAL)
                    << "[NofQpairPool::~NofQpairPool] InflightCount="
                    << final_count
                    << " at destruction in DEBUG build — worker drain "
                    << "did not converge.  This indicates a regression in "
                    << "SpdkNofTaskCompletion / FinalizeAfterDrain / "
                    << "FinalizeSubmittedTask single-pop semantics.";
            }
#endif

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

#ifdef USE_NOF
    // Release the sub_task free list AFTER all qpairs are destroyed
    // and their CQ-drain (which may fire late trampolines that call
    // free_list->Release) has completed.  Any in-flight sub_tasks
    // captured by trampolines via shared_ptr<SpdkNofSubTask>* ctx are
    // safe — they keep themselves alive via their shared_ptr until the
    // trampoline returns them to the free list (or destroys them via
    // shared_ptr refcount == 0 if the free list is already gone).  See
    // SubTaskFreeList comment in transfer_task.h for the full
    // rationale.
    sub_task_free_list_.reset();
#endif
}

#ifdef USE_NOF
// Lazy-init so transfer_task.h stays out of nof_connection.h's include
// surface (only the forward decl of SubTaskFreeList is in the header).
// Called once on the first submission to the pool, which is the
// natural lifetime boundary (no allocation cost when the pool is never
// used).
std::shared_ptr<SubTaskFreeList> NofQpairPool::GetSubTaskFreeList() {
    if (!sub_task_free_list_) {
        sub_task_free_list_ = std::make_shared<SubTaskFreeList>();
    }
    return sub_task_free_list_;
}
#endif

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
        // Phase 1: test-only injection.  Atomic check-and-clear via
        // compare_exchange: pending_inject_error_idx_ is swapped from
        // `i` to SIZE_MAX iff its current value equals `i`, all in a
        // single atomic step.
        //
        // Race properties this guarantees:
        //   - Exactly one PollAll iteration (across all threads
        //     concurrently running PollAll on this pool) fires per
        //     arm.  Any other CAS racing on the same arm loses
        //     because pending_inject_error_idx_ is no longer `i`.
        //   - Non-matching iterations (pending != i) leave the arm
        //     untouched.  The next iteration of the same PollAll
        //     call, or the next PollAll call from any thread, retries
        //     the CAS with its own i and only fires when i matches.
        //   - A concurrent TestInjectPollErrorOnce that lands between
        //     any two PollAll iterations (or after a failing CAS)
        //     simply re-arms pending_inject_error_idx_; no CAS ever
        //     overwrites a non-matching arm with SIZE_MAX.
        //
        // Memory ordering: acq_rel on the CAS provides synchronizes-
        // with against TestInjectPollErrorOnce's release store
        // (nof_connection.h:229) and against any prior successful
        // firing iteration.
        //
        // NOTE: this path runs BEFORE the hold gate (Phase 2) so
        // TestInjectPollErrorOnce still fires while TestHoldAllCompletions
        // is set.  This matches the regression-test contract:
        // `Reproducer_DrainTimeout_FutureCompletesNotHangs` arms inject
        // after hold so the pool must still observe the error and
        // EnterDraining even while CQEs are blocked.
        size_t expected = i;
        if (pending_inject_error_idx_.compare_exchange_strong(
                expected, SIZE_MAX, std::memory_order_acq_rel)) {
            if (first_error == 0) first_error = -1;
            continue;
        }
        // CAS failed: pending_inject_error_idx_ was not `i`
        // (either SIZE_MAX, or armed for some other qpair).  Fall
        // through to the hold gate and SPDK CQE harvest.
#endif
#ifdef MOONCAKE_TEST_DRAIN
        // Phase 2: test-only "hold all completions" gate.  When armed,
        // skip ONLY the SPDK CQE harvest below — the test-injection
        // path above has already run and consumed any pending arm.
        // Sibling qpairs whose CQE harvest is suppressed will see
        // their SpdkNofSubTask objects returned to the sub_task_pool
        // via the trampoline's DRAINING short-circuit once the pool
        // enters kDraining (which the worker drives from the -1 return
        // produced in Phase 1).
        if (hold_all_completions_.load(std::memory_order_acquire)) {
            continue;
        }
#endif
        // Defensive: a null slot (e.g. test pools built with nullptr
        // qpairs, or a future code path that fails to populate every
        // index) must not reach spdk_nvme_qpair_process_completions —
        // SPDK dereferences the qpair at offset 0..8 unconditionally.
        // Production pools only insert non-null qpairs (see Connect()
        // — `if (!qp) break;`), so this branch is a no-op there.
        if (!qp) continue;
        // Phase 3: real SPDK CQE processing.
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

#ifdef MOONCAKE_TEST_DRAIN
// Single source of truth for the trampoline's DRAINING branch (mirrors
// nvmf_io_complete at transfer_task.cpp:183-187).  Centralising here
// lets the Layer-1 drain-protocol test exercise the real code path
// rather than a copy that can drift from production.
void NofQpairPool::TestRunDrainingShortCircuit() {
    if (IsDraining()) {
        DecrementInflight();
        return;
    }
    LOG(ERROR) << "TestRunDrainingShortCircuit invoked with pool not in "
                  "DRAINING state — test ordering bug";
}
#endif

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
