// NofQpairPool + NofConnection class declarations.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <spdk/env.h>
#include <spdk/nvme.h>

#include "nof_config.h"

namespace mooncake {

// ---------------------------------------------------------------------------
// NofQpairPool — manages N IO qpairs on a single NVMe controller.
//
// Thread safety: round-robin uses std::atomic for the index, but SPDK
// qpairs are NOT thread-safe.  The pool is intended for use by one
// dedicated thread (the pipeline loop).  Multi-threaded access requires
// external synchronisation.
//
// Lifecycle state machine (kActive -> kDraining -> kClosed):
//   - kActive:    normal path.  Submits and CQE callbacks run their full
//                 bodies.
//   - kDraining:  any qpair has reported a transport error.  GetNextQpair
//                 returns nullptr so further submits are rejected.
//                 CQE callbacks short-circuit and only return the
//                 SpdkNofSubTask to its pool — they DO NOT touch task
//                 memory, task-level counters, or call set_completed.
//                 Final termination is owned by the worker via
//                 SpdkNofQos::FinalizeAfterDrain once the pool's
//                 inflight_blocks reach 0.
//   - kClosed:    destructor has run.  Pure defensive state.
// ---------------------------------------------------------------------------
enum class QpairPoolState : uint32_t {
    kActive = 0,
    kDraining = 1,
    kClosed = 2,
};

class NofQpairPool {
   public:
    /// Takes ownership of an already-allocated qpair vector.
    /// @param target_count  Desired total qpair count for subsequent
    ///                      TryGrow recovery decisions.
    /// @param ctrlr         NVMe controller used by TryGrow to
    ///                      allocate new qpairs.
    explicit NofQpairPool(std::vector<spdk_nvme_qpair *> qpairs,
                          uint32_t max_inflight_per_qpair,
                          uint32_t target_count = 0,
                          spdk_nvme_ctrlr *ctrlr = nullptr);

    ~NofQpairPool();

    // Non-copyable, non-movable
    NofQpairPool(const NofQpairPool &) = delete;
    NofQpairPool &operator=(const NofQpairPool &) = delete;
    NofQpairPool(NofQpairPool &&) = delete;
    NofQpairPool &operator=(NofQpairPool &&) = delete;

    /// Round-robin dispatch — returns the next qpair for I/O submission.
    /// Returns nullptr once the pool has entered DRAINING (a qpair
    /// reported a transport error) so that callers cannot submit new
    /// IO to a half-dead pool.
    spdk_nvme_qpair *GetNextQpair();

    /// Mark the pool as DRAINING.  Idempotent.  After this call:
    ///   - GetNextQpair() returns nullptr.
    ///   - The CQE callback (nvmf_io_complete) MUST short-circuit
    ///     before touching task memory or task-level counters.
    ///   - TryGrow() returns 0.
    ///
    /// The worker calls this when NvmePollProcessCompletion returns a
    /// negative value (qpair transport error).  The pool stays in
    /// DRAINING until ~NofQpairPool transitions it to kClosed.
    void EnterDraining(const char *reason);

    bool IsDraining() const {
        // True in BOTH kDraining and kClosed states.  Callbacks must
        // short-circuit once the pool has left kActive, regardless of
        // whether it's mid-drain or fully torn down.
        auto s = state_.load(std::memory_order_acquire);
        return s == QpairPoolState::kDraining || s == QpairPoolState::kClosed;
    }
    bool IsClosed() const {
        return state_.load(std::memory_order_acquire) ==
               QpairPoolState::kClosed;
    }

    /// Poll all qpairs for completions.
    /// @param max_completions 0 = process everything that is ready.
    /// @return total number of completions processed (or negative on error).
    int32_t PollAll(uint32_t max_completions = 0);

    /// Inflight tracking — synchronisation fence against late CQEs.
    ///
    /// Pairing contract:
    ///   - IncrementInflight() MUST be called BEFORE spdk_nvme_ns_cmd_*
    ///     so that any callback (including late ones delivered after the
    ///     caller returns) is accounted for.
    ///   - DecrementInflight() MUST be called in the CQE callback, AFTER
    ///     the user callback has returned.
    ///   - InflightCount() == 0 implies no callback is in flight and none
    ///     will be issued (because all submits are accounted for).
    ///
    /// Memory ordering: release/acquire pair guarantees that
    /// InflightCount() == 0 forms a synchronizes-with edge with the last
    /// DecrementInflight, so all release-stores in the callback body
    /// are visible to a thread that observes InflightCount() == 0.
    ///
    /// The counter is pool-level (not per-qpair): every callback
    /// decrements inflight_count_ on the pool it belongs to, and every
    /// submit the pool issued is tracked by the same counter.
    ///
    /// was_ever_used_with_inflight_ distinguishes a pool that actually
    /// fired an Increment from one whose submits all synchronously
    /// failed before reaching the trampoline — both end up with
    /// InflightCount == 0 but only the former has a meaningful
    /// release/acquire synchronizes-with edge.
    void IncrementInflight() {
        inflight_count_.fetch_add(1, std::memory_order_release);
        // Set metadata on first Increment.  release (not relaxed) so
        // that the subsequent acquire load in ~NofQpairPool forms a
        // synchronizes-with edge with this store: any prior Increment
        // is guaranteed to be observable when the destructor reads
        // was_ever_used_with_inflight_==true.
        was_ever_used_with_inflight_.store(true, std::memory_order_release);
    }
    void DecrementInflight() {
        inflight_count_.fetch_sub(1, std::memory_order_release);
    }
    int32_t InflightCount() const {
        return inflight_count_.load(std::memory_order_acquire);
    }
    bool WasEverUsedWithInflight() const {
        return was_ever_used_with_inflight_.load(std::memory_order_acquire);
    }

    /// Spin until InflightCount() == 0 with no time budget.  Used by
    /// ~NofQpairPool as the proof that no CQE will fire after
    /// free_io_qpair.  Combined with the abort in EnterDraining, every
    /// in-flight CQE is delivered before this returns.
    ///
    /// Does NOT modify inflight_count_; only observes it.
    bool WaitForInflightCompletion();

    size_t Size() const { return qpairs_.size(); }
    uint32_t MaxInflight() const {
        return static_cast<uint32_t>(qpairs_.size()) * max_inflight_per_qpair_;
    }

    /**
     * @brief Grow the pool back toward target_total by allocating new qpairs.
     *
     * When other connections disconnect and free QIDs, this method
     * allows a degraded connection to gradually recover back to its
     * original target_total.  Caller is responsible for ensuring this
     * runs on the same thread as I/O operations.
     *
     * @param target_total  Desired total qpair count.
     * @return Number of qpairs added; 0 means the target QID pool has
     *         no free QIDs.
     */
    uint32_t TryGrow(uint32_t target_total);

    /// Return the target qpair count requested at construction time.
    uint32_t GetTargetCount() const { return target_count_; }

#ifdef MOONCAKE_TEST_DRAIN
    // Test-only injection: arm PollAll so that the NEXT call treats
    // qpairs_[qpair_idx] as if spdk_nvme_qpair_process_completions
    // returned a negative value.  Sibling qpairs are polled normally so
    // any in-flight CQE they own fires through the real trampoline.
    // Single-shot: the arm clears after one consumption.
    void TestInjectPollErrorOnce(size_t qpair_idx) {
        pending_inject_error_idx_.store(qpair_idx, std::memory_order_release);
    }
    size_t TestPendingInjectErrorIdx() const {
        return pending_inject_error_idx_.load(std::memory_order_acquire);
    }
#endif

   private:
    std::vector<spdk_nvme_qpair *> qpairs_;
    std::atomic<uint32_t> round_robin_idx_{0};
    std::atomic<int32_t> inflight_count_{0};
    // Lifecycle state — see QpairPoolState above.  Transitions:
    //   kActive -> kDraining on EnterDraining() (qpair transport error)
    //   kDraining -> kClosed   on ~NofQpairPool
    std::atomic<QpairPoolState> state_{QpairPoolState::kActive};
    // Set on first IncrementInflight; read by ~NofQpairPool to
    // distinguish "truly quiescent" from "inflight trivially 0".
    std::atomic<bool> was_ever_used_with_inflight_{false};
    uint32_t max_inflight_per_qpair_;

    // Target qpair count requested at construction time.
    // TryGrow attempts to grow the pool back to this number.
    // Size() < target_count_ indicates the pool is currently degraded.
    uint32_t target_count_;

    // NVMe controller used by TryGrow to allocate new qpairs.
    // spdk_nvme_qpair is only forward-declared in the public header,
    // so we cannot obtain the controller via qpairs_[0]->ctrlr; the
    // controller pointer must be stored explicitly at construction.
    spdk_nvme_ctrlr *ctrlr_;

#ifdef MOONCAKE_TEST_DRAIN
    // Test-only single-shot injection arm.  SIZE_MAX means no injection
    // armed.  See TestInjectPollErrorOnce above.
    std::atomic<size_t> pending_inject_error_idx_{SIZE_MAX};
#endif
};

// ---------------------------------------------------------------------------
// NofConnection — owns one NVMe-oF controller + namespace + qpair pool.
//
// Created via the static Connect() factories.  The destructor cleans up
// all resources (qpairs, controller detach).
// ---------------------------------------------------------------------------
class NofConnection {
   public:
    /// Connect to an NVMe-oF target.
    /// @param trtype  SPDK transport type (SPDK_NVME_TRANSPORT_RDMA or
    ///                SPDK_NVME_TRANSPORT_TCP).  Callers should derive this
    ///                from the transport string or MC_NOF_TRTYPE env var.
    /// @return nullptr on failure (error_msg receives a description).
    static std::unique_ptr<NofConnection> Connect(
        const std::string &traddr, const std::string &trsvcid,
        const std::string &subnqn, uint32_t ns_id,
        spdk_nvme_transport_type trtype, const NofConfig &config,
        std::string *error_msg = nullptr);

    /// Connect from a transport string.
    /// Format: "traddr:X trsvcid:Y subnqn:Z trtype:RDMA adrfam:IPv4 ns:N"
    static std::unique_ptr<NofConnection> Connect(
        const std::string &transport_str, const NofConfig &config,
        std::string *error_msg = nullptr);

    ~NofConnection();

    // Non-copyable
    NofConnection(const NofConnection &) = delete;
    NofConnection &operator=(const NofConnection &) = delete;

    // Accessors
    spdk_nvme_ctrlr *GetCtrlr() const { return ctrlr_; }
    spdk_nvme_ns *GetNs() const { return ns_; }
    uint32_t GetBlockSize() const { return block_size_; }
    NofQpairPool &GetQpairPool() { return *qpair_pool_; }
    const NofConfig &GetConfig() const { return config_; }
    const std::string &GetSubnqn() const { return subnqn_; }
    uint64_t GetNumBlocks() const { return num_blocks_; }

   private:
    NofConnection(spdk_nvme_ctrlr *ctrlr, spdk_nvme_ns *ns,
                  std::unique_ptr<NofQpairPool> pool, uint32_t block_size,
                  uint64_t num_blocks, std::string subnqn, NofConfig config);

    spdk_nvme_ctrlr *ctrlr_;
    spdk_nvme_ns *ns_;
    std::unique_ptr<NofQpairPool> qpair_pool_;
    uint32_t block_size_;
    uint64_t num_blocks_;
    std::string subnqn_;
    NofConfig config_;
};

}  // namespace mooncake
