/*
 * Copyright (c) 2026 绿算技术
 * All rights reserved.
 *
 * @File:mooncake-store/src/spdk/spdk_wrapper.cpp
 *
 * 修改履历 | 2026-07-31 | spdk_wrapper.h — 重写
 * 修改履历 | 2026-07-31 | 新增 CloseNofSegment；ProbeNofSegment
 * 改用独立临时连接防止 QID 泄漏 修改履历 | 2026-07-31 | OpenNofSegment 新增
 * connect_mutex_ 序列化 spdk_nvme_probe 修改履历 | 2026-07-31 | 新增
 * g_active_worker_count 全局计数器 + SpdkNoF_Register/Unregister； Cleanup 中
 * LOG(FATAL) 检测 WorkerPool 未 join 时的错误析构顺序 修改履历 | 2026-08-03 |
 * OpenNofSegment 接入 QidPressureGauge 自适应 qpair 数； ProbeNofSegment 改用
 * config_probe_ (num_io_queues=1)（本 PR）
 */
#include <glog/logging.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <thread>
#include "spdk/spdk_wrapper.h"

namespace mooncake {

// Added 2026-07-31: 文件级全局计数器，跟踪活跃的 SpdkNofWorkerPool 实例数。
// 必须在文件级（非 SpdkWrapper 成员），因为静态析构期间 SpdkWrapper 单例
// 可能已销毁，WorkerPool 析构函数无法访问成员变量。
// SpdkWrapper::Cleanup() 检查此计数器：>0 时表示 WorkerPool 尚未 join，
// 此时释放 qpair 会导致 worker 线程 poll 已释放内存 → segfault。
static std::atomic<int> g_active_worker_count{0};

void SpdkNoF_RegisterWorkerPool() {
    g_active_worker_count.fetch_add(1, std::memory_order_relaxed);
}

void SpdkNoF_UnregisterWorkerPool() {
    g_active_worker_count.fetch_sub(1, std::memory_order_relaxed);
}

namespace {

bool ParseEnvU64(const char *name, uint64_t *out) {
    const char *val = std::getenv(name);
    if (!val || *val == '\0') {
        return false;
    }

    errno = 0;
    char *end = nullptr;
    unsigned long long parsed = std::strtoull(val, &end, 10);
    if (errno != 0 || end == val || (end && *end != '\0')) {
        LOG(WARNING) << "Invalid value for " << name << ": " << val;
        return false;
    }

    *out = static_cast<uint64_t>(parsed);
    return true;
}

bool ParseEnvBool(const char *name, bool *out) {
    uint64_t v = 0;
    if (!ParseEnvU64(name, &v)) {
        return false;
    }
    *out = (v != 0);
    return true;
}

void ApplyCtrlrOptsFromEnv(struct spdk_nvme_ctrlr_opts *opts) {
    uint64_t v = 0;
    bool bv = false;
    opts->keep_alive_timeout_ms = 0;

    if (ParseEnvU64("MC_NVME_NUM_IO_QUEUES", &v)) {
        opts->num_io_queues = static_cast<uint32_t>(v);
    }
    if (ParseEnvU64("MC_NVME_IO_QUEUE_SIZE", &v)) {
        opts->io_queue_size = static_cast<uint32_t>(v);
    }
    if (ParseEnvU64("MC_NVME_IO_QUEUE_REQUESTS", &v)) {
        opts->io_queue_requests = static_cast<uint32_t>(v);
    }
    if (ParseEnvU64("MC_NVME_TRANSPORT_ACK_TIMEOUT", &v)) {
        opts->transport_ack_timeout = static_cast<uint8_t>(v);
    }
    if (ParseEnvU64("MC_NVME_ADMIN_QUEUE_SIZE", &v)) {
        opts->admin_queue_size = static_cast<uint16_t>(v);
    }
    if (ParseEnvU64("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", &v)) {
        opts->fabrics_connect_timeout_us = v;
    }
    if (ParseEnvBool("MC_NVME_HEADER_DIGEST", &bv)) {
        opts->header_digest = bv;
    }
    if (ParseEnvBool("MC_NVME_DATA_DIGEST", &bv)) {
        opts->data_digest = bv;
    }
    LOG(INFO) << "NVMe ctrlr opts: num_io_queues=" << opts->num_io_queues
              << ", io_queue_size=" << opts->io_queue_size
              << ", io_queue_requests=" << opts->io_queue_requests
              << ", keep_alive_timeout_ms=" << opts->keep_alive_timeout_ms
              << ", transport_ack_timeout="
              << static_cast<int>(opts->transport_ack_timeout)
              << ", admin_queue_size=" << opts->admin_queue_size
              << ", fabrics_connect_timeout_us="
              << opts->fabrics_connect_timeout_us
              << ", header_digest=" << opts->header_digest
              << ", data_digest=" << opts->data_digest;
}

}  // namespace

// [Migrated] 旧 struct nof_seg_handle_ / tr_info / ctrlr_info 已移除。
// 新设计通过 NofConnection + NofSegment 抽象层管理 NVMe-oF 资源，
// nof_seg_handle 统一在头文件 spdk_wrapper.h 中定义。

SpdkWrapper::SpdkWrapper() = default;

SpdkWrapper::~SpdkWrapper() { Cleanup(); }

SpdkWrapper &SpdkWrapper::GetInstance() {
    static SpdkWrapper ins;
    return ins;
}

bool SpdkWrapper::InitializeEnv() {
    if (initialized.load(std::memory_order_acquire)) {
        return true;
    }

    std::lock_guard<std::mutex> lock(init_mutex);
    if (initialized.load(std::memory_order_acquire)) {
        return true;
    }

    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "mooncake";

    int rc = spdk_env_init(&opts);
    if (rc != 0) {
        fprintf(stderr, "SPDK init failed: %d\n", rc);
        return false;
    }

    // Read NoF config from environment (MC_NVME_* vars).
    // Must happen before any OpenNofSegment call so that num_io_queues
    // and other tuning parameters take effect.
    config_ = NofConfig::FromEnv();
    LOG(INFO) << "SpdkWrapper config: num_io_queues=" << config_.num_io_queues
              << ", io_queue_size=" << config_.io_queue_size
              << ", chunk_blocks=" << config_.chunk_blocks;

    // 修改履历 | 2026-08-03 | 心跳探测独立配置（本 PR）。
    // 探测只需 1 qpair，不应与 I/O 路径竞争 QID 池。
    config_probe_ = NofConfig::ForProbe();
    LOG(INFO) << "SpdkWrapper probe config: num_io_queues="
              << config_probe_.num_io_queues;

    // Mark SPDK as initialized.
    initialized.store(true, std::memory_order_release);
    return true;
}

void SpdkWrapper::Cleanup() {
    if (initialized.load(std::memory_order_acquire)) {
        // [Diagnostic] 2026-07-31：检测错误的销毁顺序。
        // g_active_worker_count 为文件级全局变量，不依赖 SpdkWrapper
        // 单例生命周期。 若 >0，说明 WorkerPool 尚未析构（workers 未 join），
        // 此时释放 qpair 会导致 worker 线程 poll 已释放内存 → segfault。
        int alive = g_active_worker_count.load(std::memory_order_relaxed);
        if (alive > 0) {
            LOG(FATAL)
                << "SpdkWrapper::Cleanup() called while " << alive
                << " SpdkNofWorkerPool instance(s) still active. "
                << "WorkerPool must be fully stopped and joined BEFORE "
                   "Cleanup. "
                << "Check static destruction order — SpdkWrapper must outlive "
                << "all SpdkNofWorkerPool instances.";
        }

        // [Migrated] 清理新设计的 open_segments_：
        // NofConnection 由 unique_ptr 自动析构（含 qpair 池和 ctrlr detach），
        // 只需手动释放 NofSegment 和 nof_seg_handle。
        {
            std::lock_guard<std::mutex> lock(segments_mutex_);
            for (auto &[handle, conn] : open_segments_) {
                if (handle) {
                    delete handle
                        ->segment;  // NofSegment* 由 OpenNofSegment new 分配
                    delete handle;  // nof_seg_handle* 由 OpenNofSegment new
                                    // 分配
                }
                // conn (unique_ptr<NofConnection>) 自动析构，释放 qpair 池并
                // detach ctrlr
            }
            open_segments_.clear();
            endpoint_to_handle_.clear();  // 清理反向去重索引
        }

        {
            std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
            for (auto &[_, probe_buffer] : probe_buffers_) {
                if (probe_buffer && probe_buffer->ptr) {
                    spdk_free(probe_buffer->ptr);
                    probe_buffer->ptr = nullptr;
                    probe_buffer->size = 0;
                }
            }
            probe_buffers_.clear();
        }
        spdk_env_fini();
        initialized.store(false, std::memory_order_release);
    }
}

void *SpdkWrapper::Alloc(size_t size, size_t align, int socket_id) {
    if (!InitializeEnv()) {
        return nullptr;
    }

    return spdk_zmalloc(size, align, nullptr, socket_id, SPDK_MALLOC_DMA);
}

void SpdkWrapper::Free(void *ptr) {
    if (ptr) {
        spdk_free(ptr);
    }
}

void SpdkWrapper::ProbeReadComplete(void *ctx,
                                    const struct spdk_nvme_cpl *cpl) {
    auto *probe_ctx = reinterpret_cast<ProbeRequestContext *>(ctx);
    if (spdk_nvme_cpl_is_error(cpl)) {
        {
            std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
            probe_ctx->error_reason =
                std::string("completion_error:") +
                spdk_nvme_cpl_get_status_string(&cpl->status);
        }
        probe_ctx->success.store(false, std::memory_order_release);
    } else {
        probe_ctx->success.store(true, std::memory_order_release);
    }
    probe_ctx->done.store(true, std::memory_order_release);
    if (probe_ctx->owner != nullptr) {
        probe_ctx->owner->RecycleProbeRequestContext(probe_ctx);
    }
}

void SpdkWrapper::ReplenishProbeRequestContextPoolLocked(size_t count) {
    for (size_t i = 0; i < count; ++i) {
        auto probe_ctx = std::make_unique<ProbeRequestContext>();
        probe_request_context_pool_.push(probe_ctx.get());
        probe_request_contexts_.push_back(std::move(probe_ctx));
    }
}

SpdkWrapper::ProbeRequestContext *SpdkWrapper::AcquireProbeRequestContext() {
    std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
    if (probe_request_context_pool_.empty()) {
        ReplenishProbeRequestContextPoolLocked(8);
    }
    auto *probe_ctx = probe_request_context_pool_.top();
    probe_request_context_pool_.pop();
    probe_ctx->Reset(this);
    return probe_ctx;
}

void SpdkWrapper::RecycleProbeRequestContext(ProbeRequestContext *ctx) {
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
    probe_request_context_pool_.push(ctx);
}

// [Migrated] 委托给 NofSegment::PollCompletion，替代旧版直接访问 seg->qpair。
int64_t SpdkWrapper::NvmePollProcessCompletion(nof_seg_handle *seg,
                                               uint32_t complete_per_seg) {
    if (!seg || !seg->segment) return -1;
    return seg->segment->PollCompletion(complete_per_seg);
}

// [Removed] ParseTransPortStr / ConnectController 已移除。
// 新设计中，传输地址解析和控制器连接由 NofConnection::Connect() 统一处理。

// OpenNofSegment: 使用新层连接。
// 修改履历 2026-07-31：新增 endpoint→handle 去重 (已移除 I/O 路径复用，见下)。
// 修改履历 2026-07-31：spdk_nvme_probe 非线程安全 — 新增 connect_mutex_
// 序列化。 修改履历 2026-07-31：移除 I/O 路径连接复用。
//   原因：多个 ClientService 各自持有独立的 SpdkNofWorkerPool。
//   若复用同一 handle，多个 WorkerPool 的 worker 线程会并发访问同一 qpair 池，
//   互相收割对方的 completion 导致 inflight 计数器下溢。
//   SPDK qpair 单线程约束要求每 handle 仅被一个 WorkerPool 使用。
//   每个 client 持有独立连接，4 clients × 4 qpairs = 20 QID (远低于 64 上限)。
nof_seg_handle *SpdkWrapper::OpenNofSegment(const std::string &tr_str) {
    if (!InitializeEnv()) return nullptr;

    // 修改履历 | 2026-08-03 | 自适应 + 重试移出 connect_mutex_（本 PR）
    // - 每次重试前重新 GetRecommended()，感知并发 Client 的失败事件
    // - 每次失败后立即 Record()，让其他 Client 实时感知压力
    // - 渐进降级：target 逐次减半，而非全用同一个值重试 6 次
    std::string error;
    std::unique_ptr<NofConnection> conn;
    uint32_t max_retries =
        config_.enable_degradation ? config_.retry_max_attempts : 0;
    uint32_t current_target = config_.num_io_queues;

    for (uint32_t attempt = 0; attempt <= max_retries; attempt++) {
        // 每次重试前重新评估 QID 压力，感知并发 Client 的 Record 事件。
        if (config_.enable_degradation && attempt > 0) {
            uint32_t recommended =
                qid_pressure_gauge_.GetRecommended(config_.num_io_queues);
            // 渐进降级：取 gauge 建议值和上次 target 一半的最小值。
            uint32_t degraded = std::max(current_target / 2, 1u);
            current_target = std::min(recommended, degraded);
        }

        NofConfig adaptive_config = config_;
        adaptive_config.num_io_queues = current_target;

        {
            std::lock_guard<std::mutex> connect_lock(connect_mutex_);
            conn = NofConnection::Connect(tr_str, adaptive_config, &error);
        }

        if (conn) break;

        // 立即上报失败，让并发的其他 Client 感知 QID 压力。
        qid_pressure_gauge_.Record(current_target, 0);

        bool is_qid_exhaustion =
            (error.find("qpair_alloc_fail") != std::string::npos);
        if (!is_qid_exhaustion || attempt >= max_retries) break;

        auto wait_ms = config_.retry_backoff_ms * (1 << attempt);
        LOG(WARNING) << "SpdkWrapper::OpenNofSegment: QID exhausted"
                     << " (target=" << current_target << "), waiting "
                     << wait_ms << "ms"
                     << " (attempt " << (attempt + 1) << "/" << max_retries
                     << ") for " << tr_str;
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
    }

    if (!conn) {
        LOG(ERROR) << "SpdkWrapper::OpenNofSegment failed for " << tr_str
                   << ": " << error;
        return nullptr;
    }

    // 上报成功分配结果到压力感知器，恢复 Green 状态。
    uint32_t actual_qpairs = conn->GetQpairPool().Size();
    qid_pressure_gauge_.Record(current_target, actual_qpairs);

    auto *segment = new NofSegment(conn.get(), 0, conn->GetNumBlocks());
    auto *handle = new nof_seg_handle{segment};

    {
        std::lock_guard<std::mutex> lock(segments_mutex_);
        open_segments_[handle] = std::move(conn);
    }

    LOG(INFO) << "SpdkWrapper::OpenNofSegment OK: "
              << "subnqn=" << open_segments_[handle]->GetSubnqn()
              << " qpairs=" << segment->GetConnection()->GetQpairPool().Size()
              << " (requested=" << current_target << ")";

    return handle;
}

// CloseNofSegment: 释放 OpenNofSegment 分配的全部资源。
// 修改履历 2026-07-31：同步清理 endpoint_to_handle_ 反向索引，避免悬挂引用。
void SpdkWrapper::CloseNofSegment(nof_seg_handle *handle) {
    if (!handle) return;

    std::lock_guard<std::mutex> lock(segments_mutex_);
    auto it = open_segments_.find(handle);
    if (it == open_segments_.end()) {
        // 不在 map 中 — handle 可能已被 Cleanup() 释放，仅清理裸指针。
        delete handle->segment;
        delete handle;
        return;
    }

    // 清理反向索引：遍历 endpoint_to_handle_ 找到指向此 handle 的条目并移除。
    for (auto ei = endpoint_to_handle_.begin(); ei != endpoint_to_handle_.end();
         ++ei) {
        if (ei->second == handle) {
            endpoint_to_handle_.erase(ei);
            break;
        }
    }

    delete handle->segment;  // NofSegment* 由 OpenNofSegment new 分配
    delete handle;           // nof_seg_handle* 由 OpenNofSegment new 分配
    open_segments_.erase(
        it);  // unique_ptr<NofConnection> 析构 → qpair pool → ctrlr detach
}

// [Migrated] 通过 NofSegment 获取 block size，替代旧版直接访问 seg_handle->ns。
uint32_t SpdkWrapper::GetBlockSize(const nof_seg_handle *seg_handle) {
    if (!seg_handle || !seg_handle->segment) {
        return INVALID_BLOCK_SIZE;
    }
    return seg_handle->segment->GetBlockSize();
}
// SubmitRequest: 委托给 NofSegment
int SpdkWrapper::SubmitRequest(const nof_seg_handle *seg_handle, void *ptr,
                               uint64_t lba, uint32_t lba_count, int op,
                               spdk_nvme_cmd_cb cb_fn, void *cb_ctx) {
    if (!seg_handle || !seg_handle->segment) return -1;

    auto *seg = seg_handle->segment;
    if (op == kSpdkNofOpRead)
        return seg->SubmitRead(ptr, lba, lba_count, cb_fn, cb_ctx);
    if (op == kSpdkNofOpWrite)
        return seg->SubmitWrite(ptr, lba, lba_count, cb_fn, cb_ctx);
    return -1;
}

SpdkWrapper::ProbeBuffer *SpdkWrapper::GetOrCreateProbeBuffer(
    const std::string &tr_str, uint32_t block_size, std::string *error_reason) {
    std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
    auto &probe_buffer = probe_buffers_[tr_str];
    if (!probe_buffer) {
        probe_buffer = std::make_unique<ProbeBuffer>();
    }

    if (probe_buffer->ptr != nullptr && probe_buffer->size == block_size) {
        return probe_buffer.get();
    }

    if (probe_buffer->ptr != nullptr) {
        spdk_free(probe_buffer->ptr);
        probe_buffer->ptr = nullptr;
        probe_buffer->size = 0;
    }

    probe_buffer->ptr =
        spdk_zmalloc(block_size, 0x1000, nullptr, -1, SPDK_MALLOC_DMA);
    if (!probe_buffer->ptr) {
        if (error_reason) {
            *error_reason = "alloc_fail";
        }
        return nullptr;
    }
    probe_buffer->size = block_size;
    return probe_buffer.get();
}

// 修改履历 2026-07-31：重写为直接使用 NofConnection::Connect()
// 创建独立临时连接。 不经过 OpenNofSegment（I/O 路径共享连接），不加入
// open_segments_， 不与 Worker 线程共享 qpair — 避免违反 SPDK qpair
// 单线程约束。 函数返回时 NofConnection 自动析构 → qpair pool free → ctrlr
// detach → QID 回收。
bool SpdkWrapper::ProbeNofSegment(const std::string &tr_str,
                                  uint32_t timeout_ms,
                                  std::string *error_reason) {
    if (!InitializeEnv()) {
        if (error_reason) {
            *error_reason = "spdk_env_init_fail";
        }
        return false;
    }

    // 创建独立探测连接 — 不共享给 Worker 线程，心跳线程独占访问。
    // 修改履历 | 2026-08-03 | 使用 config_probe_ (num_io_queues=1)，
    // 避免心跳探测与 I/O 路径竞争 target QID 池（本 PR）。
    std::string connect_error;
    auto conn = NofConnection::Connect(tr_str, config_probe_, &connect_error);
    if (!conn) {
        if (error_reason) {
            *error_reason = "open_fail: " + connect_error;
        }
        return false;
    }

    // 栈上临时 segment + handle（不注册到 open_segments_）
    NofSegment segment(conn.get(), 0, conn->GetNumBlocks());
    nof_seg_handle seg_handle{&segment};

    uint32_t block_size = segment.GetBlockSize();
    if (block_size == INVALID_BLOCK_SIZE || block_size == 0) {
        if (error_reason) {
            *error_reason = "invalid_block_size";
        }
        return false;  // conn 自动析构 → QID 回收
    }

    ProbeBuffer *probe_buffer =
        GetOrCreateProbeBuffer(tr_str, block_size, error_reason);
    if (!probe_buffer || !probe_buffer->ptr) {
        return false;  // conn 自动析构 → QID 回收
    }

    ProbeRequestContext *probe_ctx = AcquireProbeRequestContext();
    int ret = SubmitRequest(&seg_handle, probe_buffer->ptr, 0, 1,
                            kSpdkNofOpRead, ProbeReadComplete, probe_ctx);
    if (ret != 0) {
        RecycleProbeRequestContext(probe_ctx);
        if (error_reason) {
            *error_reason = "submit_fail";
        }
        return false;  // conn 自动析构 → QID 回收
    }

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (!probe_ctx->done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        segment.PollCompletion(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    bool ok = probe_ctx->done.load(std::memory_order_acquire) &&
              probe_ctx->success.load(std::memory_order_acquire);
    if (!ok && error_reason) {
        if (!probe_ctx->done.load(std::memory_order_acquire)) {
            *error_reason = "completion_timeout";
        } else {
            std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
            *error_reason = probe_ctx->error_reason.empty()
                                ? "completion_error"
                                : probe_ctx->error_reason;
        }
    }

    // conn 出作用域自动析构：
    //   ~NofConnection() → ~NofQpairPool() → free io qpairs →
    //   spdk_nvme_detach() 16+1 QID 全部回收，无泄漏。
    return ok;
}

// [Migrated] 新增方法：SetConfig / PipelineRead / PipelineWrite 桩实现。
void SpdkWrapper::SetConfig(const NofConfig &config) { config_ = config; }

ssize_t SpdkWrapper::PipelineRead(nof_seg_handle *handle, void *buf,
                                  uint64_t lba, uint32_t total_blocks) {
    if (!handle || !handle->segment) return -1;
    return handle->segment->PipelineRead(buf, lba, total_blocks);
}

ssize_t SpdkWrapper::PipelineWrite(nof_seg_handle *handle, const void *buf,
                                   uint64_t lba, uint32_t total_blocks) {
    if (!handle || !handle->segment) return -1;
    return handle->segment->PipelineWrite(buf, lba, total_blocks);
}

}  // namespace mooncake
