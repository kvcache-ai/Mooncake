#define _GNU_SOURCE

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <limits>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif
#include <thread>
#include "config/spdk_controller_config.h"
#include "spdk/spdk_wrapper.h"
#include "types.h"

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
    struct spdk_nvme_qpair *qpair;
    struct spdk_nvme_ns *ns;
};

struct tr_info {
    struct spdk_nvme_transport_id trid;
    std::string ctrlr_key;
    uint32_t ns;
};

struct ctrlr_info {
    struct spdk_nvme_ctrlr *ctrlr;
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
    return spdk_nvme_qpair_process_completions(seg->qpair, complete_per_seg);
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

nof_seg_handle *SpdkWrapper::OpenNofSegment(const std::string &tr_str) {
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

            ret = ConnectController(&tr.trid, info);
            if (ret != 0) {
                return nullptr;
            }

            connected_ctrlrs[tr.ctrlr_key] = std::move(new_info);
        } else {
            info = it->second.get();
        }
    }

    nof_seg_handle *seg_handle = nullptr;
    struct spdk_nvme_qpair *qpair = nullptr;
    struct spdk_nvme_ns *ns = nullptr;
    {
        auto &ns_seg = info->ns_seg;
        std::lock_guard<std::mutex> lock(info->ns_mutex);
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

        qpair = spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, nullptr, 0);
        if (!qpair) {
            LOG(ERROR) << "alloc spdk_nvme_qpair failed";
            return nullptr;
        }

        auto new_seg = std::make_unique<nof_seg_handle>();
        new_seg->qpair = qpair;
        new_seg->ns = ns;
        seg_handle = new_seg.get();
        ns_seg[tr.ns] = std::move(new_seg);
    }

    return seg_handle;
}

uint32_t SpdkWrapper::GetBlockSize(const nof_seg_handle *seg_handle) {
    if (!seg_handle || !seg_handle->ns) {
        return INVALID_BLOCK_SIZE;
    }

    return spdk_nvme_ns_get_sector_size(seg_handle->ns);
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

bool SpdkWrapper::QueryNamespaceInfo(const std::string &endpoint,
                                     NoFNamespaceInfo &info,
                                     std::string *error_reason) {
    auto fail = [error_reason](const std::string &reason) {
        if (error_reason) {
            *error_reason = reason;
        }
        return false;
    };
    tr_info tr;
    if (ParseTransPortStr(endpoint, &tr) != 0) {
        return fail("invalid NVMe-oF endpoint");
    }
    if (!InitializeEnv()) {
        return fail("SPDK environment initialization failed");
    }

    // Serialize probe/detach with OpenNofSegment and keep cached controllers
    // alive while their namespace information is being copied.
    std::lock_guard<std::mutex> lock(ctrlrs_mutex);
    std::unique_ptr<spdk_nvme_ctrlr, decltype(&spdk_nvme_detach)> temporary(
        nullptr, spdk_nvme_detach);
    spdk_nvme_ctrlr *ctrlr = nullptr;
    auto it = connected_ctrlrs.find(tr.ctrlr_key);
    if (it != connected_ctrlrs.end()) {
        ctrlr = it->second->ctrlr;
    } else {
        ctrlr_info connection{};
        int ret = ConnectController(&tr.trid, &connection);
        temporary.reset(connection.ctrlr);
        if (ret != 0) {
            return fail("NVMe-oF controller connection failed: " +
                        std::to_string(ret));
        }
        ctrlr = connection.ctrlr;
    }
    if (!ctrlr) {
        return fail("NVMe-oF controller was not found");
    }
    if (!spdk_nvme_ctrlr_is_active_ns(ctrlr, tr.ns)) {
        return fail("namespace is not active: " + std::to_string(tr.ns));
    }
    auto *ns = spdk_nvme_ctrlr_get_ns(ctrlr, tr.ns);
    if (!ns) {
        return fail("namespace was not found: " + std::to_string(tr.ns));
    }

    NoFNamespaceInfo result;
    result.block_size = spdk_nvme_ns_get_sector_size(ns);
    result.num_blocks = spdk_nvme_ns_get_num_sectors(ns);
    if (result.block_size == 0 || result.num_blocks == 0 ||
        result.num_blocks >
            std::numeric_limits<uint64_t>::max() / result.block_size) {
        return fail("invalid namespace size");
    }
    result.size = result.num_blocks * result.block_size;
    info = result;
    return true;
}

}  // namespace mooncake
