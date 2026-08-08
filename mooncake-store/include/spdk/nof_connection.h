/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File: mooncake-store/include/spdk/nof_connection.h
 * @Description: NofQpairPool + NofConnection 类声明
 *
 * 修改履历 | 2026-07-31 | 初始多 qpair 实现
 * 修改履历 | 2026-08-03 | NofQpairPool 新增 TryGrow/GetTargetCount
 * 支持再均衡（本 PR）
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
    /// @param target_count  期望的 qpair 总数，用于后续 TryGrow 恢复判断。
    /// @param ctrlr         NVMe controller，TryGrow 时用于分配新 qpair。
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
     * @brief 尝试向池中追加 qpair 直到达到 target_total。
     *
     * 当其他连接断开释放 QID 后，本方法允许已降级的连接逐步恢复到
     * 最初的 target_total。调用者负责确保与 I/O 操作在同一线程。
     *
     * @param target_total  期望达到的 qpair 总数。
     * @return 实际新增的 qpair 数量，0 表示 target QID 池无可用 QID。
     *
     * 修改履历 | 2026-08-03 | 新增（本 PR）
     */
    uint32_t TryGrow(uint32_t target_total);

    /// 返回创建时请求的目标 qpair 数，用于判断降级程度。
    uint32_t GetTargetCount() const { return target_count_; }

   private:
    std::vector<spdk_nvme_qpair *> qpairs_;
    std::atomic<uint32_t> round_robin_idx_{0};
    std::atomic<int32_t> inflight_count_{0};
    uint32_t max_inflight_per_qpair_;

    // 创建时请求的目标 qpair 数。TryGrow 尝试向此数字补齐。
    // 当 Size() < target_count_ 时表示当前处于降级状态。
    uint32_t target_count_;

    // NVMe controller，用于 TryGrow 时分配新 qpair。
    // spdk_nvme_qpair 在公有头文件中仅前向声明，无法通过 qpairs_[0]->ctrlr
    // 获取 controller。因此需要在构造时显式传入存储。
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
    /// @return nullptr on failure (error_msg receives a description).
    static std::unique_ptr<NofConnection> Connect(
        const std::string &traddr, const std::string &trsvcid,
        const std::string &subnqn, uint32_t ns_id, const NofConfig &config,
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
