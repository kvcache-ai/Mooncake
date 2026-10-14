#define _GNU_SOURCE

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <cassert>
#include <set>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif
#include <thread>
#include "config/spdk_controller_config.h"
#include "spdk/spdk_wrapper.h"

namespace mooncake {
namespace {

void ApplyCtrlrOptsFromEnv(struct spdk_nvme_ctrlr_opts *opts) {
    opts->keep_alive_timeout_ms = 0;
    const auto config = SpdkControllerConfig::FromEnvironment();
    if (config.num_io_queues.has_value()) {
        opts->num_io_queues = *config.num_io_queues;
    }
    if (config.io_queue_size.has_value()) {
        opts->io_queue_size = *config.io_queue_size;
    }
    if (config.io_queue_requests.has_value()) {
        opts->io_queue_requests = *config.io_queue_requests;
    }
    if (config.transport_ack_timeout.has_value()) {
        opts->transport_ack_timeout = *config.transport_ack_timeout;
    }
    if (config.admin_queue_size.has_value()) {
        opts->admin_queue_size = *config.admin_queue_size;
    }
    if (config.fabrics_connect_timeout_us.has_value()) {
        opts->fabrics_connect_timeout_us = *config.fabrics_connect_timeout_us;
    }
    if (config.header_digest.has_value()) {
        opts->header_digest = *config.header_digest;
    }
    if (config.data_digest.has_value()) {
        opts->data_digest = *config.data_digest;
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

struct nof_seg_handle {
    struct spdk_nvme_qpair *qpair{nullptr};
    struct spdk_nvme_ns *ns{nullptr};

    ctrlr_info *controller{nullptr};
    SpdkWrapper::WorkerId owner{nullptr, -1};

    uint32_t block_size{0};
    bool disconnecting{false};
};

struct tr_info {
    struct spdk_nvme_transport_id trid;
    std::string ctrlr_key;
    uint32_t ns;
};

struct ctrlr_info {
    enum class State { Ready, Draining, Disconnecting, Reconnecting, Backoff };

    struct spdk_nvme_ctrlr *ctrlr{nullptr};

    // Fixed at creation, probe-only controllers leave recovery disabled.
    bool recovery_enabled{false};
    State state{State::Ready};
    // One entry per owner worker, shared by its namespaces on this controller.
    std::set<SpdkWrapper::WorkerId> owners;
    // Owners still awaiting cleanup acknowledgement in the current recovery.
    std::set<SpdkWrapper::WorkerId> pending_owners;
    std::chrono::steady_clock::time_point retry_at{};

    std::map<uint32_t, std::unique_ptr<nof_seg_handle>> ns_seg;
    std::mutex ns_mutex;
};

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

    // rte_eal_init registers the calling thread as DPDK's main lcore and may
    // restrict its CPU affinity to the configured EAL cores (default
    // core_mask "0x1"). Linux threads inherit the creator's affinity mask, so
    // without restoring it, every thread the host app spawns after Mooncake's
    // setup would be pinned to a single core and KV read/write throughput
    // would drop sharply. Save the original affinity and restore it around
    // spdk_env_init to stop that propagation. DPDK's main-lcore identity is a
    // thread registration, not the kernel affinity, so later SPDK calls are
    // unaffected by the restore. pthread affinity APIs are glibc-specific, so
    // this is gated to Linux; other platforms see the previous behavior.
#if defined(__linux__)
    cpu_set_t orig_cpuset;
    CPU_ZERO(&orig_cpuset);
    bool affinity_saved =
        pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t),
                               &orig_cpuset) == 0;
#endif

    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "mooncake";

    int rc = spdk_env_init(&opts);

    // Best-effort restore of the caller's original affinity.
#if defined(__linux__)
    if (affinity_saved) {
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t),
                                   &orig_cpuset) != 0) {
            LOG(WARNING) << "Failed to restore calling thread CPU affinity "
                            "after SPDK env init";
        }
    }
#endif

    if (rc != 0) {
        fprintf(stderr, "SPDK init failed: %d\n", rc);
        return false;
    }

    // Mark SPDK as initialized.
    initialized.store(true, std::memory_order_release);
    return true;
}

void SpdkWrapper::Cleanup() {
    {
        std::lock_guard<std::mutex> lock(controller_mutex_);
        controller_stop_ = true;
    }
    controller_cv_.notify_all();
    if (controller_thread_.joinable()) controller_thread_.join();

    if (initialized.load(std::memory_order_acquire)) {
        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex);
            for (auto &[_, info] : connected_ctrlrs) {
                if (info) {
                    // Free all qpairs and segment handles
                    for (auto &[_, seg] : info->ns_seg) {
                        if (seg && seg->qpair) {
                            spdk_nvme_ctrlr_free_io_qpair(seg->qpair);
                        }
                    }
                    // Detach controller
                    if (info->ctrlr) {
                        spdk_nvme_detach(info->ctrlr);
                    }
                }
            }
            connected_ctrlrs.clear();
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

int SpdkWrapper::RegisterMemory(void *addr, size_t size) {
    if (!addr || size == 0) {
        return -1;
    }
    if (!InitializeEnv()) {
        LOG(ERROR) << "SPDK env init failed, cannot register memory";
        return -1;
    }
    int rc = spdk_mem_register(addr, size);
    if (rc != 0) {
        LOG(ERROR) << "spdk_mem_register failed (addr=" << addr
                   << ", size=" << size << "): " << strerror(-rc);
    }
    return rc;
}

bool SpdkWrapper::IsRegistrableRange(void *addr, size_t size) {
    // spdk_mem_register() rejects a range up front (memory.c:339-348) when the
    // address is outside the 256TB window it can represent (MASK_256TB) or when
    // addr/size is not 2MB-aligned. Those rejections happen before anything is
    // marked, so a caller that skips the registration for such a range can
    // still munmap it; every other failure may have left state behind.
    constexpr uintptr_t kSpdkAddressMask = (1ULL << 48) - 1;  // MASK_256TB
    constexpr uintptr_t kSpdk2MbAlignment = 2ULL * 1024 * 1024;
    const uintptr_t vaddr = reinterpret_cast<uintptr_t>(addr);
    return addr != nullptr && size != 0 && (vaddr & ~kSpdkAddressMask) == 0 &&
           (vaddr % kSpdk2MbAlignment) == 0 && (size % kSpdk2MbAlignment) == 0;
}

int SpdkWrapper::UnregisterMemory(void *addr, size_t size) {
    if (!addr || size == 0) {
        return -1;
    }
    // SPDK env may not be initialized (registration is opt-in) or may already
    // have been finalized by Cleanup() -> spdk_env_fini(). In both cases there
    // is no registered memory to release; calling spdk_mem_unregister would
    // walk a torn-down or never-created global mem_map. (With ShmHelper
    // destroyed before SpdkWrapper, the `initialized` atomic is still alive
    // here.)
    if (!initialized.load(std::memory_order_acquire)) {
        return 0;
    }
    int rc = spdk_mem_unregister(addr, size);
    if (rc != 0) {
        // Every code is reported, -EINVAL included: it does NOT mean "the range
        // was never registered" (spdk_mem_unregister returns it at memory.c:426
        // when a page of the range is not REG_MAP_REGISTERED -- exactly what
        // spdk_mem_register leaves behind when it fails after marking the range
        // but before its notify callbacks run, memory.c:370-384). In iova=va it
        // is also returned by the notify callback when the translation does not
        // cover the range, *before* the IOMMU unmap at memory.c:1224, so a live
        // DMA mapping can remain. Callers must keep the mapping unless rc == 0.
        LOG(ERROR) << "spdk_mem_unregister failed (addr=" << addr
                   << ", size=" << size << "): " << strerror(-rc);
    }
    return rc;
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

int64_t SpdkWrapper::NvmePollProcessCompletion(nof_seg_handle *seg,
                                               uint32_t complete_per_seg) {
    return seg->qpair ? spdk_nvme_qpair_process_completions(seg->qpair,
                                                            complete_per_seg)
                      : 0;
}

int SpdkWrapper::ParseTransPortStr(const std::string &tr_str, tr_info *info) {
    std::memset(&info->trid, 0, sizeof(info->trid));
    info->ns = 1;

    if (spdk_nvme_transport_id_parse(&info->trid, tr_str.c_str()) != 0) {
        LOG(ERROR) << "Error parsing transport address";
        return -1;
    }

    std::string ns_prefix = "ns:";
    size_t ns_pos = tr_str.find(ns_prefix);
    if (ns_pos != std::string::npos) {
        size_t ns_start = ns_pos + ns_prefix.length();
        size_t ns_end = tr_str.find_first_of(" \t", ns_start);

        std::string ns_str;
        if (ns_end == std::string::npos) {
            ns_str = tr_str.substr(ns_start);
        } else {
            ns_str = tr_str.substr(ns_start, ns_end - ns_start);
        }

        try {
            info->ns = std::stoul(ns_str);
        } catch (const std::exception &e) {
            LOG(ERROR) << "Failed to parse ns value: " << ns_str
                       << ", error: " << e.what();
            return -1;
        }
    } else {
        LOG(ERROR) << "No ns field found in transport string";
    }

    info->ctrlr_key = std::string(info->trid.traddr) + "|" +
                      std::string(info->trid.trsvcid) + "|" +
                      std::string(info->trid.subnqn) + "|" +
                      std::to_string(static_cast<int>(info->trid.trtype));

    LOG(INFO) << "traddr:" << info->trid.traddr
              << "trsvcid:" << info->trid.trsvcid << "ns:" << info->ns
              << "subnqn:" << info->trid.subnqn
              << "trtype:" << info->trid.trtype;

    return 0;
}

int SpdkWrapper::ConnectController(const struct spdk_nvme_transport_id *trid,
                                   ctrlr_info *info) {
    auto probe_cb = [](void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                       struct spdk_nvme_ctrlr_opts *opts) -> bool {
        ApplyCtrlrOptsFromEnv(opts);
        LOG(INFO) << "Attaching to " << trid->traddr << " " << trid->subnqn;
        return true;
    };

    auto attach_cb = [](void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                        struct spdk_nvme_ctrlr *ctrlr,
                        const struct spdk_nvme_ctrlr_opts *opts) {
        LOG(INFO) << "Attached to " << trid->traddr << " " << trid->subnqn;
        ctrlr_info *info = (ctrlr_info *)cb_ctx;
        info->ctrlr = ctrlr;
    };

    return spdk_nvme_probe(trid, (void *)info, probe_cb, attach_cb, NULL);
}

nof_seg_handle *SpdkWrapper::OpenNofSegment(const std::string &tr_str,
                                            bool recovery_enabled) {
    // ConnectController -> spdk_nvme_probe() uses env-provided sockets and
    // memory, so the SPDK env must be up. Do not rely on an earlier
    // RegisterMemory() to have initialized it: callers now skip the
    // registration for ranges SPDK would reject (IsRegistrableRange), and that
    // must not decide whether NoF can be used at all. Every other entry point
    // (Alloc, RegisterMemory, ProbeNofSegment) already initializes the env
    // itself; this makes OpenNofSegment self-sufficient too.
    if (!InitializeEnv()) {
        LOG(ERROR) << "SPDK env init failed, cannot open NoF segment";
        return nullptr;
    }

    tr_info tr;
    int ret = ParseTransPortStr(tr_str, &tr);
    if (ret != 0) {
        return nullptr;
    }

    ctrlr_info *info = nullptr;
    {
        std::lock_guard<std::mutex> lock(ctrlrs_mutex);
        auto it = connected_ctrlrs.find(tr.ctrlr_key);
        if (it == connected_ctrlrs.end()) {
            auto new_info = std::make_unique<ctrlr_info>();
            info = new_info.get();
            info->recovery_enabled = recovery_enabled;

            ret = ConnectController(&tr.trid, info);
            if (ret != 0) {
                return nullptr;
            }

            connected_ctrlrs[tr.ctrlr_key] = std::move(new_info);
        } else {
            info = it->second.get();
        }
    }

    // SPDK may reuse a controller for the same transport ID. Never enable
    // recovery around a legacy probe connection that has no worker owner.
    if (info->recovery_enabled != recovery_enabled) {
        LOG(ERROR) << "Cannot mix probe and recoverable client ownership for "
                   << tr.ctrlr_key;
        return nullptr;
    }

    nof_seg_handle *seg_handle = nullptr;
    struct spdk_nvme_qpair *qpair = nullptr;
    struct spdk_nvme_ns *ns = nullptr;
    {
        auto &ns_seg = info->ns_seg;
        std::lock_guard<std::mutex> lock(info->ns_mutex);
        if (info->state != ctrlr_info::State::Ready) return nullptr;

        auto ns_it = ns_seg.find(tr.ns);
        if (ns_it != ns_seg.end()) {
            return ns_it->second.get();
        }

        if (spdk_nvme_ctrlr_is_active_ns(info->ctrlr, tr.ns)) {
            ns = spdk_nvme_ctrlr_get_ns(info->ctrlr, tr.ns);
        } else {
            LOG(ERROR) << "spdk_nvme_ctrlr_is_active_ns failed";
            return nullptr;
        }

        auto new_seg = std::make_unique<nof_seg_handle>();
        if (!recovery_enabled) {
            qpair = spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, nullptr, 0);
            if (!qpair) {
                LOG(ERROR) << "alloc spdk_nvme_qpair failed";
                return nullptr;
            }
        }
        new_seg->qpair = qpair;
        new_seg->ns = ns;
        new_seg->controller = info;
        new_seg->block_size = spdk_nvme_ns_get_sector_size(ns);
        seg_handle = new_seg.get();
        ns_seg[tr.ns] = std::move(new_seg);
    }

    return seg_handle;
}

uint32_t SpdkWrapper::GetBlockSize(const nof_seg_handle *seg_handle) {
    if (!seg_handle) return INVALID_BLOCK_SIZE;
    if (!seg_handle->controller->recovery_enabled) {
        return seg_handle->ns ? spdk_nvme_ns_get_sector_size(seg_handle->ns)
                              : INVALID_BLOCK_SIZE;
    }
    return seg_handle->block_size;
}

int SpdkWrapper::SubmitRequest(const nof_seg_handle *seg_handle, void *ptr,
                               uint64_t lba, uint32_t lba_count, int op,
                               spdk_nvme_cmd_cb cb_fn, void *cb_ctx) {
    if (!seg_handle || !ptr || !lba_count || !seg_handle->qpair ||
        !seg_handle->ns) {
        return -1;
    }

    struct spdk_nvme_qpair *qpair = seg_handle->qpair;
    struct spdk_nvme_ns *ns = seg_handle->ns;
    if (op == kSpdkNofOpRead) {
        return spdk_nvme_ns_cmd_read(ns, qpair, ptr, lba, lba_count, cb_fn,
                                     cb_ctx, 0);
    } else if (op == kSpdkNofOpWrite) {
        return spdk_nvme_ns_cmd_write(ns, qpair, ptr, lba, lba_count, cb_fn,
                                      cb_ctx, 0);
    }
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

bool SpdkWrapper::ProbeNofSegment(const std::string &tr_str,
                                  uint32_t timeout_ms,
                                  std::string *error_reason) {
    if (!InitializeEnv()) {
        if (error_reason) {
            *error_reason = "spdk_env_init_fail";
        }
        return false;
    }

    nof_seg_handle *seg_handle = OpenNofSegment(tr_str);
    if (!seg_handle) {
        if (error_reason) {
            *error_reason = "open_fail";
        }
        return false;
    }

    uint32_t block_size = GetBlockSize(seg_handle);
    if (block_size == INVALID_BLOCK_SIZE || block_size == 0) {
        if (error_reason) {
            *error_reason = "invalid_block_size";
        }
        return false;
    }

    ProbeBuffer *probe_buffer =
        GetOrCreateProbeBuffer(tr_str, block_size, error_reason);
    if (!probe_buffer || !probe_buffer->ptr) {
        return false;
    }

    ProbeRequestContext *probe_ctx = AcquireProbeRequestContext();
    int ret = SubmitRequest(seg_handle, probe_buffer->ptr, 0, 1, kSpdkNofOpRead,
                            ProbeReadComplete, probe_ctx);
    if (ret != 0) {
        RecycleProbeRequestContext(probe_ctx);
        if (error_reason) {
            *error_reason = "submit_fail";
        }
        return false;
    }

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (!probe_ctx->done.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        NvmePollProcessCompletion(seg_handle, 0);
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

    return ok;
}

void SpdkWrapper::RegisterRecoveryTaskPoster(RecoveryTaskPoster poster) {
    // Serialize registration from concurrently constructed worker pools.
    std::lock_guard<std::mutex> lock(controller_mutex_);
    CHECK(poster != nullptr);
    if (post_recovery_task_) {
        CHECK(post_recovery_task_ == poster);
        return;
    }
    post_recovery_task_ = poster;
}

ctrlr_info *SpdkWrapper::GetController(const nof_seg_handle *seg) const {
    return seg->controller;
}

bool SpdkWrapper::BindSegmentToWorker(nof_seg_handle *seg, WorkerId owner,
                                      const std::function<void()> &enqueue) {
    auto *info = seg->controller;

    std::lock_guard<std::mutex> lock(info->ns_mutex);
    if (!info->recovery_enabled || info->state != ctrlr_info::State::Ready ||
        !seg->ns || seg->owner.first)
        return false;

    bool owner_added = info->owners.insert(owner).second;
    seg->owner = owner;
    try {
        enqueue();
    } catch (...) {
        seg->owner = {nullptr, -1};
        if (owner_added) info->owners.erase(owner);
        return false;
    }
    return true;
}

void SpdkWrapper::RequestRecovery(ctrlr_info *info) {
    std::lock_guard<std::mutex> lock(info->ns_mutex);
    if (!info->recovery_enabled || info->state != ctrlr_info::State::Ready)
        return;

    info->state = ctrlr_info::State::Draining;
    const auto *trid = spdk_nvme_ctrlr_get_transport_id(info->ctrlr);
    LOG(WARNING) << "nof_recovery begin traddr=" << trid->traddr
                 << " trsvcid=" << trid->trsvcid << " subnqn=" << trid->subnqn
                 << " owners=" << info->owners.size();
    for (const auto &owner : info->owners) {
        info->pending_owners.insert(owner);
        post_recovery_task_(owner, info, false);
    }
    controller_cv_.notify_one();
}

std::vector<nof_seg_handle *> SpdkWrapper::OwnedSegments(ctrlr_info *info,
                                                         WorkerId owner) {
    std::lock_guard<std::mutex> lock(info->ns_mutex);
    std::vector<nof_seg_handle *> result;
    for (auto &[_, seg] : info->ns_seg) {
        if (seg->owner == owner) result.push_back(seg.get());
    }
    return result;
}

void SpdkWrapper::OwnerDrained(ctrlr_info *info, WorkerId owner) {
    std::lock_guard<std::mutex> lock(info->ns_mutex);
    info->pending_owners.erase(owner);
    controller_cv_.notify_one();
}

void SpdkWrapper::UnregisterOwner(ctrlr_info *info, WorkerId owner) {
    std::lock_guard<std::mutex> lock(info->ns_mutex);
    for (auto &[_, seg] : info->ns_seg) {
        if (seg->owner == owner) {
            assert(seg->qpair == nullptr);
            seg->owner = {nullptr, -1};
        }
    }
    info->owners.erase(owner);
    // Safe even if a new recovery added this owner: its qpairs are already
    // freed.
    info->pending_owners.erase(owner);
    controller_cv_.notify_one();
}

int SpdkWrapper::SetupClientQpairIfNeeded(nof_seg_handle *seg) {
    // Called only on its owner. The common case never takes a controller lock.
    if (seg->qpair) return 0;

    auto *info = seg->controller;
    {
        std::lock_guard<std::mutex> lock(info->ns_mutex);
        if (info->state != ctrlr_info::State::Ready) return -EAGAIN;
        if (!seg->ns) return -ENODEV;
    }

    // This registered owner cannot acknowledge Cleanup while creating here.
    // The cleanup barrier protects the controller without holding admission
    // locked across a potentially slow transport connect.
    spdk_nvme_io_qpair_opts opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(info->ctrlr, &opts, sizeof(opts));
    opts.async_mode = true;
    opts.create_only = true;
    seg->qpair =
        spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, &opts, sizeof(opts));
    if (!seg->qpair) return -ENOMEM;

    LOG(INFO) << "nof_qpair_async_connect_start ctrlr=" << info
              << " nsid=" << spdk_nvme_ns_get_id(seg->ns)
              << " qpair=" << seg->qpair << " pool=" << seg->owner.first
              << " worker_idx=" << seg->owner.second;

    // Distinguish queue allocation pressure from a failed transport connect.
    int rc = spdk_nvme_ctrlr_connect_io_qpair(info->ctrlr, seg->qpair);
    return rc == 0 ? 0 : -ENXIO;
}

bool SpdkWrapper::DrainClientQpair(nof_seg_handle *seg) {
    if (!seg->qpair) return true;

    if (!seg->disconnecting) {
        seg->disconnecting = true;
        spdk_nvme_ctrlr_disconnect_io_qpair(seg->qpair);
    }

    // In SPDK 23.01, explicit disconnect puts the qpair in DISCONNECTING.
    // process_completions masks -ENXIO to 0 while still disconnecting, and
    // returns -ENXIO after transport teardown. This is not a generic "error
    // means drained" rule: it is used only after our explicit disconnect.
    if (spdk_nvme_qpair_process_completions(seg->qpair, 0) != -ENXIO)
        return false;

    spdk_nvme_ctrlr_free_io_qpair(seg->qpair);
    seg->qpair = nullptr;
    seg->disconnecting = false;
    return true;
}

void SpdkWrapper::StartControllerThread() {
    std::lock_guard<std::mutex> lock(controller_mutex_);
    assert(post_recovery_task_ != nullptr);
    if (!controller_thread_.joinable()) {
        controller_stop_ = false;
        controller_thread_ = std::thread(&SpdkWrapper::ControllerLoop, this);
    }
}

void SpdkWrapper::ControllerLoop() {
    using State = ctrlr_info::State;

    while (true) {
        bool stopping;
        {
            std::unique_lock<std::mutex> lock(controller_mutex_);
            controller_cv_.wait_for(lock, std::chrono::milliseconds(10));
            stopping = controller_stop_;
        }

        std::vector<ctrlr_info *> controllers;
        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex);
            for (auto &[_, info] : connected_ctrlrs) {
                if (info->recovery_enabled) controllers.push_back(info.get());
            }
        }

        bool reconnect_in_progress = false;
        for (auto *info : controllers) {
            std::unique_lock<std::mutex> lock(info->ns_mutex);
            auto state = info->state;

            // SPDK holds its controller lock until reconnect succeeds or fails.
            // Even during shutdown, keep polling on this thread until SPDK
            // releases the lock; skip all other controller states.
            if (stopping && state != State::Reconnecting) continue;

            if (state == State::Draining) {
                if (!info->pending_owners.empty()) continue;
            } else if (state == State::Backoff &&
                       std::chrono::steady_clock::now() < info->retry_at) {
                continue;
            }

            lock.unlock();
            if (state == State::Ready) {
                // In SPDK 23.01, admin polling fetches RDMA connection events
                // and dispatches them to the affected qpairs. Keep polling even
                // with keep-alive disabled so workers can detect remote
                // disconnects.
                if (spdk_nvme_ctrlr_process_admin_completions(info->ctrlr) < 0)
                    RequestRecovery(info);
                // RequestRecovery() may have changed info->state to Draining,
                // either here or on a worker. Skip writeback of the stale Ready
                // snapshot.
                continue;
            }

            // Only this thread advances recovery after Ready -> Draining.
            // Keep admission closed and publish the resulting state once.
            if (state == State::Draining || state == State::Backoff) {
                spdk_nvme_ctrlr_disconnect(info->ctrlr);
                state = State::Disconnecting;
            }

            if (state == State::Disconnecting &&
                spdk_nvme_ctrlr_process_admin_completions(info->ctrlr) ==
                    -ENXIO) {
                spdk_nvme_ctrlr_reconnect_async(info->ctrlr);
                state = State::Reconnecting;
            }

            int rc = 0;
            if (state == State::Reconnecting)
                rc = spdk_nvme_ctrlr_reconnect_poll_async(info->ctrlr);

            lock.lock();
            info->state = state;
            if (state != State::Reconnecting) continue;

            if (rc == -EAGAIN) {
                reconnect_in_progress = true;
                continue;
            }

            if (rc != 0) {
                info->state = State::Backoff;
                info->retry_at =
                    std::chrono::steady_clock::now() + std::chrono::seconds(1);
                const auto *trid =
                    spdk_nvme_ctrlr_get_transport_id(info->ctrlr);
                LOG(WARNING) << "nof_recovery retry traddr=" << trid->traddr
                             << " trsvcid=" << trid->trsvcid
                             << " subnqn=" << trid->subnqn << " rc=" << rc
                             << " retry_after_ms=1000";
                continue;
            }

            for (auto &[nsid, seg] : info->ns_seg) {
                seg->ns = nullptr;
                if (!spdk_nvme_ctrlr_is_active_ns(info->ctrlr, nsid)) continue;
                auto *ns = spdk_nvme_ctrlr_get_ns(info->ctrlr, nsid);
                if (spdk_nvme_ns_get_sector_size(ns) == seg->block_size)
                    seg->ns = ns;
            }

            for (const auto &owner : info->owners)
                post_recovery_task_(owner, info, true);
            info->state = State::Ready;
            const auto *trid = spdk_nvme_ctrlr_get_transport_id(info->ctrlr);
            LOG(INFO) << "nof_recovery ready traddr=" << trid->traddr
                      << " trsvcid=" << trid->trsvcid
                      << " subnqn=" << trid->subnqn;
        }

        if (stopping && !reconnect_in_progress) return;
    }
}

}  // namespace mooncake
