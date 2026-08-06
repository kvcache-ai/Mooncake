/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File: mooncake-store/src/spdk/nof_connection.cpp
 * @Description: NofConnection::Connect() 工厂, qpair 协商
 *
 * 修改履历 | 2026-07-31 | 初始多 qpair 实现
 * 修改履历 | 2026-08-03 | 新增连续分配 + 退避重试 + TryGrow 再均衡（本 PR）
 */
#include "spdk/nof_connection.h"

#include <glog/logging.h>

#include <cerrno>
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

NofQpairPool::~NofQpairPool() {
    for (auto *qp : qpairs_) {
        if (qp) spdk_nvme_ctrlr_free_io_qpair(qp);
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

// 修改履历 | 2026-08-03 | 新增（本 PR）
// 使用构造时存储的 ctrlr_ 分配新 qpair。
// spdk_nvme_qpair 在公有头文件中仅前向声明，无法通过 qpairs_[0]->ctrlr
// 获取 controller，因此必须在构造 NofQpairPool 时显式传入 ctrlr。
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
            // QID 池无可用 QID — 停止尝试，等待下一次 TryGrow 周期。
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
    // 修改履历 | 2026-08-03 | 唯一 host NQN 计数器（本 PR）
    // 同一进程内多次 Connect 使用相同默认 hostnqn 会导致 target
    // 将多连接合并到同一 controller，Set Features 被 NVMe 协议拒绝。
    // 每连接分配唯一 hostnqn 确保 target 创建独立 controller。
    uint32_t hostnqn_id = 0;
};

// 修改履历 | 2026-08-03 | 全局计数器，每连接递增（本 PR）
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
    const std::string &subnqn, uint32_t ns_id, const NofConfig &config,
    std::string *error_msg) {
    // Build transport ID
    struct spdk_nvme_transport_id trid;
    std::memset(&trid, 0, sizeof(trid));
    snprintf(trid.traddr, sizeof(trid.traddr), "%s", traddr.c_str());
    snprintf(trid.trsvcid, sizeof(trid.trsvcid), "%s", trsvcid.c_str());
    snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", subnqn.c_str());
    trid.trtype = SPDK_NVME_TRANSPORT_RDMA;
    trid.adrfam = SPDK_NVMF_ADRFAM_IPV4;

    ConnectCtx ctx;
    ctx.config = &config;
    // 修改履历 | 2026-08-03 | 每连接分配唯一 host NQN（本 PR）
    // 同一进程内多次 spdk_nvme_probe 默认使用相同 hostnqn，
    // target 会合并同 hostnqn 的连接导致 Set Features 被拒绝。
    // 使用全局递增计数器确保每个连接获得独立的 hostnqn。
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

        // 修改履历 | 2026-08-03 | 设置唯一 host NQN（本 PR）
        // 默认 hostnqn 为空时 SPDK 使用 nqn.2014-08.org.nvmexpress:uuid:XXX，
        // 同进程内所有连接共享同一 UUID → target 合并连接 → IO qpair 分配失败。
        // 格式: nqn.2024-08.mooncake:c<N> 确保每个连接独立 controller。
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

    // 修改履历 | 2026-08-03 | 连续分配，不重试（本 PR）。
    // 重试退避逻辑已移至 OpenNofSegment() 的 connect_mutex_ 外部。
    // 此处仅做一次连续分配：尝试拿满 requested 条，拿到多少算多少。
    // 若 0 条则返回失败，由调用方在 mutex 外退避后重试。
    std::vector<spdk_nvme_qpair *> qpairs;
    uint32_t requested = config.num_io_queues;
    uint32_t min_required =
        config.enable_degradation ? config.min_io_queues : requested;

    // 连续分配：能拿多少拿多少，不设离散分级。
    // 理由：离散分级会在 QID 池有 13 空闲时只拿 8，浪费 5 QID。
    for (uint32_t i = 0; i < requested; i++) {
        auto *qp = spdk_nvme_ctrlr_alloc_io_qpair(ctx.ctrlr, nullptr, 0);
        if (!qp) break;  // QID 池耗尽，停止尝试
        qpairs.push_back(qp);
    }

    if (qpairs.size() < min_required) {
        // 清理可能残留的部分分配（< min 不可用）
        for (auto *qp : qpairs) {
            spdk_nvme_ctrlr_free_io_qpair(qp);
        }
        qpairs.clear();

        if (error_msg) {
            if (requested == min_required) {
                *error_msg = "qpair_alloc_fail: all allocations failed";
            } else {
                *error_msg = "qpair_alloc_fail: got " +
                             std::to_string(qpairs.size()) +
                             " (min=" + std::to_string(min_required) +
                             ", target=" + std::to_string(requested) + ")";
            }
        }
        LOG(ERROR) << "[NofConnection] QID exhaustion: 0 qpairs for " << subnqn
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

    // target_count = 请求数，TryGrow 恢复目标；ctrlr 用于 TryGrow 分配新
    // qpair。
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

    // RDMA is our only transport currently; warn if TCP requested.
    if (trtype == "TCP") {
        LOG(WARNING)
            << "[NofConnection] TCP transport requested — RDMA is preferred";
    }

    return Connect(traddr, trsvcid, subnqn, ns, config, error_msg);
}

}  // namespace mooncake
