/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File: mooncake-store/include/spdk/nof_connection.h
 * @Description: NofQpairPool + NofConnection class declarations
 *
 * Changelog:
 *   2026-07-31  Initial multi-qpair implementation.
 *   2026-08-03  NofQpairPool: added TryGrow / GetTargetCount for
 *               runtime rebalancing.
 */
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
// ---------------------------------------------------------------------------
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
    spdk_nvme_qpair *GetNextQpair();

    /// Poll all qpairs for completions.
    /// @param max_completions 0 = process everything that is ready.
    /// @return total number of completions processed (or negative on error).
    int32_t PollAll(uint32_t max_completions = 0);

    /// Inflight tracking for pipeline flow control.
    void IncrementInflight() {
        inflight_count_.fetch_add(1, std::memory_order_relaxed);
    }
    void DecrementInflight() {
        inflight_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    int32_t InflightCount() const {
        return inflight_count_.load(std::memory_order_relaxed);
    }

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

   private:
    std::vector<spdk_nvme_qpair *> qpairs_;
    std::atomic<uint32_t> round_robin_idx_{0};
    std::atomic<int32_t> inflight_count_{0};
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
