/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File:mooncake-store/include/spdk/spdk_wrapper.h
 *
 * 修改履历 | 2026-07-31 | spdk_wrapper.h — 重写
 * 修改履历 | 2026-07-31 | 新增 CloseNofSegment；ProbeNofSegment
 * 改用独立临时连接，心跳线程 不与 Worker 共享 qpair，防止 QID 泄漏 + SPDK
 * 单线程约束违反 修改履历 | 2026-07-31 | OpenNofSegment 新增 connect_mutex_
 * 序列化 spdk_nvme_probe()， 防止多线程并发 probe 导致 namespace_inactive
 * 修改履历 | 2026-07-31 | 新增 g_active_worker_count 全局计数器 +
 * SpdkNoF_Register/Unregister 自由函数；Cleanup 中 LOG(FATAL)
 * 检测静态析构顺序错误 修改履历 | 2026-08-03 | 新增 QidPressureGauge
 * 全局压力感知器 + config_probe_ 探测独立配置（本 PR）
 */
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <vector>
#include <spdk/env.h>
#include <spdk/nvme.h>

// 新增 include
#include "nof_config.h"
#include "nof_connection.h"
#include "nof_segment.h"

namespace mooncake {

#define INVALID_BLOCK_SIZE 0xFFFFFFFF

constexpr int kSpdkNofOpRead = 0;
constexpr int kSpdkNofOpWrite = 1;
constexpr int kSpdkNofOpNum = 2;

// [Migrated] tr_info / ctrlr_info 前向声明已移除，传输解析由
// NofConnection::Connect() 内部处理。
struct nof_seg_handle;

/**
 * @brief QID 压力感知器。
 *
 * 基于滑动窗口记录最近 N 次 qpair 分配结果（allocated/requested 比例），
 * 计算 target 端的 QID 获取率。用于新连接建立时自适应降低 qpair 请求数，
 * 避免先到者垄断 target QID 池。
 *
 * 线程安全：Record() 和 GetRecommended() 使用原子操作，可跨线程调用。
 *
 * 修改履历 | 2026-08-03 | 新增（本 PR）
 */
class QidPressureGauge {
   public:
    QidPressureGauge() {
        for (size_t i = 0; i < kWindowSize; i++) {
            window_[i].requested = 0;
            window_[i].allocated = 0;
        }
    }

    /// 记录最近一次 qpair 分配结果。
    /// @param requested  请求的 qpair 数。
    /// @param allocated  实际分配到的 qpair 数。
    void Record(uint32_t requested, uint32_t allocated) {
        if (requested == 0) return;
        size_t idx =
            write_idx_.fetch_add(1, std::memory_order_relaxed) % kWindowSize;
        window_[idx].requested = requested;
        window_[idx].allocated = allocated;
    }

    /// 返回当前压力等级。
    /// @return 0=绿灯(>75% 获取率), 1=黄灯(50-75%), 2=红灯(<50%)。
    int GetPressureLevel() const {
        double ratio = GetAverageRatio();
        if (ratio > 0.75) return 0;  // Green: healthy
        if (ratio > 0.50) return 1;  // Yellow: moderate pressure
        return 2;                    // Red: severe pressure
    }

    /// 根据压力等级，返回建议的新连接 qpair 请求数。
    /// @param configured  NofConfig 中配置的 num_io_queues。
    /// @return 建议的 qpair 请求数。
    uint32_t GetRecommended(uint32_t configured) const {
        if (configured <= 4) return configured;  // 已经足够小，不再缩减
        int level = GetPressureLevel();
        switch (level) {
            case 0:  // Green — 请求完整数量
                return configured;
            case 1:  // Yellow — 请求 3/4
                return std::max(4u, configured * 3 / 4);
            case 2:  // Red — 请求 1/2
                return std::max(4u, configured / 2);
            default:
                return configured;
        }
    }

   private:
    static constexpr size_t kWindowSize = 16;
    struct Sample {
        uint32_t requested;
        uint32_t allocated;
    };

    double GetAverageRatio() const {
        uint64_t total_requested = 0;
        uint64_t total_allocated = 0;
        for (size_t i = 0; i < kWindowSize; i++) {
            total_requested += window_[i].requested;
            total_allocated += window_[i].allocated;
        }
        if (total_requested == 0) return 1.0;  // 无历史数据，假设健康
        return static_cast<double>(total_allocated) /
               static_cast<double>(total_requested);
    }

    std::atomic<size_t> write_idx_{0};
    Sample window_[kWindowSize];
};

class SpdkWrapper {
   public:
    SpdkWrapper(const SpdkWrapper &) = delete;
    SpdkWrapper &operator=(const SpdkWrapper &) = delete;

    static SpdkWrapper &GetInstance();

    bool InitializeEnv();

    void Cleanup();

    void *Alloc(size_t size, size_t align, int socket_id = -1);

    void Free(void *ptr);

    int64_t NvmePollProcessCompletion(nof_seg_handle *seg,
                                      uint32_t complete_per_seg);

    /** @brief Open a NoF segment. */
    nof_seg_handle *OpenNofSegment(const std::string &tr_str);

    /**
     * @brief Close a NoF segment and release all associated resources.
     * Frees the NofSegment, the nof_seg_handle, and triggers
     * NofConnection destruction (qpair pool + ctrlr detach).
     * Safe to call with nullptr (no-op).
     * Added 2026-07-31 to fix ProbeNofSegment connection leak.
     */
    void CloseNofSegment(nof_seg_handle *handle);

    // New: per-open-segment tracking
    std::map<nof_seg_handle *, std::unique_ptr<NofConnection>> open_segments_;
    std::mutex segments_mutex_;

    NofConfig config_;

    /**
     * @brief 心跳探测专用配置。
     *
     * 使用 ForProbe() 初始化，num_io_queues=1。
     * 探测只需 1 次 read，不应消耗 16+1 QID 与 I/O 路径竞争。
     * 修改履历 | 2026-08-03 | 新增（本 PR）
     */
    NofConfig config_probe_;

    /**
     * @brief 全局 QID 压力感知器。
     *
     * 新连接建立时通过 GetRecommended() 调整请求的 qpair 数，
     * 避免在高压力下先到者垄断 target QID 池。
     * 修改履历 | 2026-08-03 | 新增（本 PR）
     */
    QidPressureGauge qid_pressure_gauge_;

    // 新增 API
    void SetConfig(const NofConfig &);
    const NofConfig &GetConfig() const { return config_; }
    ssize_t PipelineRead(nof_seg_handle *, void *, uint64_t, uint32_t);
    ssize_t PipelineWrite(nof_seg_handle *, const void *, uint64_t, uint32_t);

    uint32_t GetBlockSize(const nof_seg_handle *seg_handle);

    int SubmitRequest(const nof_seg_handle *seg_handle, void *ptr, uint64_t lba,
                      uint32_t lba_count, int op, spdk_nvme_cmd_cb cb_fn,
                      void *cb_ctx);

    bool ProbeNofSegment(const std::string &tr_str, uint32_t timeout_ms,
                         std::string *error_reason = nullptr);

   private:
    struct ProbeBuffer {
        void *ptr{nullptr};
        uint32_t size{0};

        ProbeBuffer() = default;
        ProbeBuffer(const ProbeBuffer &) = delete;
        ProbeBuffer &operator=(const ProbeBuffer &) = delete;
        ProbeBuffer(ProbeBuffer &&) = delete;
        ProbeBuffer &operator=(ProbeBuffer &&) = delete;
    };

    struct ProbeRequestContext {
        std::atomic<bool> done{false};
        std::atomic<bool> success{false};
        std::mutex error_mutex;
        std::string error_reason;
        SpdkWrapper *owner{nullptr};

        void Reset(SpdkWrapper *wrapper) {
            std::lock_guard<std::mutex> lock(error_mutex);
            owner = wrapper;
            done.store(false, std::memory_order_release);
            success.store(false, std::memory_order_release);
            error_reason.clear();
        }
    };

    explicit SpdkWrapper();
    ~SpdkWrapper();

    // [Removed] ParseTransPortStr / ConnectController — 新设计由
    // NofConnection::Connect() 统一处理。
    ProbeBuffer *GetOrCreateProbeBuffer(const std::string &tr_str,
                                        uint32_t block_size,
                                        std::string *error_reason);
    ProbeRequestContext *AcquireProbeRequestContext();
    void RecycleProbeRequestContext(ProbeRequestContext *ctx);
    void ReplenishProbeRequestContextPoolLocked(size_t count);
    static void ProbeReadComplete(void *ctx, const struct spdk_nvme_cpl *cpl);

    std::atomic<bool> initialized{false};
    std::mutex init_mutex;
    // [Removed] connected_ctrlrs / ctrlrs_mutex — 新设计使用 open_segments_
    // 管理连接。
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    std::vector<std::unique_ptr<ProbeRequestContext>> probe_request_contexts_;
    std::stack<ProbeRequestContext *> probe_request_context_pool_;
    std::mutex probe_request_context_pool_mutex_;

    // Added 2026-07-31: endpoint → handle reverse index for connection reuse.
    // open_segments_ 按 handle 索引；此 map 按 transport string 索引同一连接，
    // 避免 I/O 路径对同一 target 重复创建连接耗尽 QID。
    // 由 segments_mutex_ 保护，与 open_segments_ 同步更新。
    std::map<std::string, nof_seg_handle *> endpoint_to_handle_;

    // Added 2026-07-31: 序列化慢速路径（首次连接）。
    // spdk_nvme_probe() 不是线程安全的 — 多线程同时 Connect 会导致
    // namespace 激活竞争，全部返回 namespace_inactive。
    // 此 mutex 确保同一时刻只有一个线程执行 NofConnection::Connect()。
    std::mutex connect_mutex_;
};
// ---------------------------------------------------------------------------
// nof_seg_handle — opaque handle wrapping a NofSegment.
// The connection (NofConnection) is owned by SpdkWrapper.
// ---------------------------------------------------------------------------
struct nof_seg_handle {
    NofSegment *segment = nullptr;
};

// Added 2026-07-31: WorkerPool lifecycle tracking — free functions,
// NOT SpdkWrapper methods. 理由：静态析构期间 SpdkWrapper 单例可能已销毁，
// 但 SpdkNofWorkerPool 析构函数仍需注销。自由函数可以安全访问文件级全局计数器。
void SpdkNoF_RegisterWorkerPool();
void SpdkNoF_UnregisterWorkerPool();

}  // namespace mooncake
