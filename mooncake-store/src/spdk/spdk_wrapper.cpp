#include <glog/logging.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <thread>
#include "spdk/spdk_wrapper.h"

namespace mooncake {
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

    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "mooncake";

    int rc = spdk_env_init(&opts);
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

}  // namespace mooncake
