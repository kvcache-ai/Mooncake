#include "nof/spdk_initiator.h"

#include <glog/logging.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <map>
#include <mutex>
#include <stack>
#include <thread>
#include <vector>

#include <spdk/env.h>
#include <spdk/nvme.h>

namespace mooncake {
namespace {

// ---------------------------------------------------------------------------
// Env parsing (migrated verbatim from spdk_wrapper.cpp)
// ---------------------------------------------------------------------------

bool ParseEnvU64(const char* name, uint64_t* out) {
    const char* val = std::getenv(name);
    if (!val || *val == '\0') {
        return false;
    }

    errno = 0;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(val, &end, 10);
    if (errno != 0 || end == val || (end && *end != '\0')) {
        LOG(WARNING) << "Invalid value for " << name << ": " << val;
        return false;
    }

    *out = static_cast<uint64_t>(parsed);
    return true;
}

bool ParseEnvBool(const char* name, bool* out) {
    uint64_t v = 0;
    if (!ParseEnvU64(name, &v)) {
        return false;
    }
    *out = (v != 0);
    return true;
}

void ApplyCtrlrOptsFromEnv(struct spdk_nvme_ctrlr_opts* opts) {
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

// ---------------------------------------------------------------------------
// Shared, refcounted SPDK environment guard.
//
// DPDK EAL init is process-global; one env per process, many runtimes
// allowed. The env is acquired lazily and finalized when the last guard
// reference dies.
// ---------------------------------------------------------------------------

class SpdkEnvGuard {
   public:
    static std::shared_ptr<SpdkEnvGuard> Acquire() {
        std::lock_guard<std::mutex> lock(RegistryMutex());
        if (auto existing = Registry().lock()) {
            return existing;
        }
        // Use a raw pointer for the temporary so that, on init failure, we
        // can plain-delete under the lock without re-entering the mutex (the
        // Deleter locks RegistryMutex; destroying a shared_ptr while holding
        // that mutex would deadlock).
        auto* raw = new SpdkEnvGuard();
        if (!raw->ok_) {
            delete raw;
            return nullptr;
        }
        std::shared_ptr<SpdkEnvGuard> guard(raw, Deleter());
        Registry() = guard;
        return guard;
    }

    SpdkEnvGuard(const SpdkEnvGuard&) = delete;
    SpdkEnvGuard& operator=(const SpdkEnvGuard&) = delete;

   private:
    SpdkEnvGuard() {
        struct spdk_env_opts opts;
        spdk_env_opts_init(&opts);
        opts.name = "mooncake";
        ok_ = (spdk_env_init(&opts) == 0);
        if (!ok_) {
            LOG(ERROR) << "spdk_env_init failed";
        }
    }

    // Runs when the last initiator/allocator reference dies. spdk_env_fini is
    // serialized with Acquire via RegistryMutex: a concurrent Acquire holds
    // the same mutex while running spdk_env_init, so without this lock DPDK
    // re-init could race the teardown of the dying env (undefined behavior on
    // the reconnect path, where the last ref of an old runtime dies exactly as
    // a new runtime is first used).
    struct Deleter {
        void operator()(SpdkEnvGuard* p) const {
            std::lock_guard<std::mutex> lock(RegistryMutex());
            if (p->ok_) {
                spdk_env_fini();
            }
            delete p;
        }
    };

    static std::mutex& RegistryMutex() {
        static std::mutex m;
        return m;
    }
    static std::weak_ptr<SpdkEnvGuard>& Registry() {
        static std::weak_ptr<SpdkEnvGuard> w;
        return w;
    }

    bool ok_{false};
};

// ---------------------------------------------------------------------------
// Process-global 2MB-page registration registry (#3131, 评审 R-2).
//
// SPDK's translation table is process-global, so the bookkeeping must be
// too: per-instance refcounts would let one initiator instance unmap pages
// another instance still uses ("No translation" I/O failures). All
// SpdkInitiator instances in the process share this registry.
//
// SPDK v23.01.1 semantics (lib/env_dpdk/memory.c) this mirrors:
//   - register/unregister require (vaddr, len) 2MB-aligned, else -EINVAL;
//   - registering an already-registered page fails with -EBUSY (e.g. DPDK
//     memseg memory from spdk_zmalloc, registered by the memseg walk);
//   - unmapping a sub-range of a region fails with -ERANGE — we always
//     register/unregister single pages, each its own region.
//
// The registry owns no SPDK resources and calls SPDK only inside
// Register/Unregister. From its first successful registration it holds an
// env-guard reference (env_ref_), pinning the SPDK env for the registry's own
// (process) lifetime: an UnregisterMemory call can never run against an env
// whose translation table has been finalized, even if every initiator/allocator
// instance in the process is torn down. This turns the "env outlives the
// registry" assumption into a constructive fact. Same-TU static-destruction
// order is safe (Acquire() always constructs RegistryMutex before the first
// GetInstance() constructs this registry, so the mutex outlives the registry),
// and the pinned env keeps SPDK alive until process exit — matching baseline
// SpdkWrapper's teardown-at-exit behavior.
// ---------------------------------------------------------------------------

#ifdef HAVE_SPDK_MEM_REGISTER
class NofPageRegistry {
   public:
    static NofPageRegistry& GetInstance() {
        static NofPageRegistry registry;
        return registry;
    }

    ErrorCode Register(void* ptr, size_t size) {
        constexpr uintptr_t kHugepageSize = 2ULL << 20;
        const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
        const uintptr_t first_page = begin & ~(kHugepageSize - 1);
        const uintptr_t end_page =
            (begin + size + kHugepageSize - 1) & ~(kHugepageSize - 1);

        std::lock_guard<std::mutex> lock(mutex_);
        if (registered_sizes_.count(ptr) != 0) {
            return ErrorCode::OK;  // idempotent for the same ptr
        }

        std::vector<uintptr_t> touched;  // pages whose count we bumped
        for (uintptr_t page = first_page; page < end_page;
             page += kHugepageSize) {
            auto& reg = page_regs_[page];
            if (reg.count == 0 && !reg.external) {
                int rc = spdk_mem_register(reinterpret_cast<void*>(page),
                                           kHugepageSize);
                if (rc == -EBUSY) {
                    // Already registered via DPDK's memseg walk — not
                    // ours to unregister.
                    reg.external = true;
                } else if (rc != 0) {
                    LOG(ERROR) << "spdk_mem_register failed: page="
                               << reinterpret_cast<void*>(page) << " rc=" << rc;
                    // Roll back the pages this call bumped.
                    for (uintptr_t p : touched) {
                        auto& r = page_regs_[p];
                        if (--r.count == 0) {
                            spdk_mem_unregister(reinterpret_cast<void*>(p),
                                                kHugepageSize);
                            page_regs_.erase(p);
                        }
                    }
                    return ErrorCode::INTERNAL_ERROR;
                }
            }
            if (!reg.external) {
                reg.count++;
                touched.push_back(page);
            }
        }
        registered_sizes_[ptr] = size;
        if (!env_ref_) {
            // Pin the env for the registry's (process) lifetime from the
            // first successful registration, so a later UnregisterMemory can
            // never hit an already-fini'd translation table.
            env_ref_ = SpdkEnvGuard::Acquire();
        }
        return ErrorCode::OK;
    }

    ErrorCode Unregister(void* ptr) {
        constexpr uintptr_t kHugepageSize = 2ULL << 20;
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = registered_sizes_.find(ptr);
        if (it == registered_sizes_.end()) {
            return ErrorCode::OK;  // not registered: no-op
        }
        const uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
        const uintptr_t first_page = begin & ~(kHugepageSize - 1);
        const uintptr_t end_page =
            (begin + it->second + kHugepageSize - 1) & ~(kHugepageSize - 1);
        for (uintptr_t page = first_page; page < end_page;
             page += kHugepageSize) {
            auto pit = page_regs_.find(page);
            if (pit == page_regs_.end() || pit->second.external) {
                continue;
            }
            if (--pit->second.count == 0) {
                // Single-page region == legal single-page unmap
                // (NOTIFY_START semantics).
                int rc = spdk_mem_unregister(reinterpret_cast<void*>(page),
                                             kHugepageSize);
                if (rc != 0) {
                    LOG(ERROR) << "spdk_mem_unregister failed: page="
                               << reinterpret_cast<void*>(page) << " rc=" << rc;
                }
                page_regs_.erase(pit);
            }
        }
        registered_sizes_.erase(it);
        return ErrorCode::OK;
    }

   private:
    struct PageReg {
        uint32_t count = 0;
        bool external = false;  // DPDK memseg-registered: never unregister
    };

    std::map<uint64_t, PageReg> page_regs_;     // key: 2MB-aligned page base
    std::map<void*, size_t> registered_sizes_;  // user ptr -> original size
    std::mutex mutex_;
    // Keeps the SPDK env alive for the registry's lifetime (acquired on the
    // first successful registration).
    std::shared_ptr<SpdkEnvGuard> env_ref_;
};
#endif  // HAVE_SPDK_MEM_REGISTER

// ---------------------------------------------------------------------------
// Opaque handle definition — visible only inside this file.
// ---------------------------------------------------------------------------

class NofSegmentHandle {
   public:
    struct spdk_nvme_qpair* qpair;
    struct spdk_nvme_ns* ns;
};

namespace {

struct tr_info {
    struct spdk_nvme_transport_id trid;
    std::string ctrlr_key;
    uint32_t ns;
};

struct ctrlr_info {
    struct spdk_nvme_ctrlr* ctrlr;
    std::map<uint32_t, std::unique_ptr<NofSegmentHandle>> ns_seg;
    std::mutex ns_mutex;
};

}  // namespace

// ---------------------------------------------------------------------------
// SpdkInitiator::Impl
// ---------------------------------------------------------------------------

class SpdkInitiator::Impl {
   public:
    Impl() = default;

    ~Impl() {
        if (!env_guard_) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex_);
            for (auto& [_, info] : connected_ctrlrs_) {
                if (info) {
                    for (auto& [_, seg] : info->ns_seg) {
                        if (seg && seg->qpair) {
                            spdk_nvme_ctrlr_free_io_qpair(seg->qpair);
                        }
                    }
                    if (info->ctrlr) {
                        spdk_nvme_detach(info->ctrlr);
                    }
                }
            }
            connected_ctrlrs_.clear();
        }
        {
            std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
            for (auto& [_, probe_buffer] : probe_buffers_) {
                if (probe_buffer && probe_buffer->ptr) {
                    spdk_free(probe_buffer->ptr);
                    probe_buffer->ptr = nullptr;
                    probe_buffer->size = 0;
                }
            }
            probe_buffers_.clear();
        }
        // env_guard_ released after all qpair/ctrlr teardown above;
        // spdk_env_fini runs inside ~SpdkEnvGuard.
    }

    NofSegmentHandle* OpenSegment(const std::string& tr_str) {
        if (!EnsureEnv()) {
            return nullptr;
        }
        tr_info tr;
        if (ParseTransPortStr(tr_str, &tr) != 0) {
            return nullptr;
        }

        ctrlr_info* info = nullptr;
        {
            std::lock_guard<std::mutex> lock(ctrlrs_mutex_);
            auto it = connected_ctrlrs_.find(tr.ctrlr_key);
            if (it != connected_ctrlrs_.end()) {
                info = it->second.get();
            }
        }
        if (!info) {
            // Connect OUTSIDE the mutex: spdk_nvme_probe establishes an
            // NVMe-oF connection and may block for seconds; holding
            // ctrlrs_mutex_ across it (baseline spdk_wrapper.cpp:313-329)
            // stalls first-time opens of every other endpoint (评审 #6).
            auto new_info = std::make_unique<ctrlr_info>();
            if (ConnectController(&tr.trid, new_info.get()) != 0) {
                return nullptr;
            }
            std::lock_guard<std::mutex> lock(ctrlrs_mutex_);
            auto it = connected_ctrlrs_.find(tr.ctrlr_key);
            if (it != connected_ctrlrs_.end()) {
                // Lost a concurrent first-open race: detach our duplicate
                // and use the winner.
                spdk_nvme_detach(new_info->ctrlr);
                info = it->second.get();
            } else {
                info = new_info.get();
                connected_ctrlrs_[tr.ctrlr_key] = std::move(new_info);
            }
        }

        NofSegmentHandle* seg_handle = nullptr;
        {
            auto& ns_seg = info->ns_seg;
            std::lock_guard<std::mutex> lock(info->ns_mutex);
            auto ns_it = ns_seg.find(tr.ns);
            if (ns_it != ns_seg.end()) {
                return ns_it->second.get();
            }

            struct spdk_nvme_ns* ns = nullptr;
            if (spdk_nvme_ctrlr_is_active_ns(info->ctrlr, tr.ns)) {
                ns = spdk_nvme_ctrlr_get_ns(info->ctrlr, tr.ns);
            } else {
                LOG(ERROR) << "spdk_nvme_ctrlr_is_active_ns failed";
                return nullptr;
            }

            struct spdk_nvme_qpair* qpair =
                spdk_nvme_ctrlr_alloc_io_qpair(info->ctrlr, nullptr, 0);
            if (!qpair) {
                LOG(ERROR) << "alloc spdk_nvme_qpair failed";
                return nullptr;
            }

            auto new_seg = std::make_unique<NofSegmentHandle>();
            new_seg->qpair = qpair;
            new_seg->ns = ns;
            seg_handle = new_seg.get();
            ns_seg[tr.ns] = std::move(new_seg);
        }
        return seg_handle;
    }

    uint32_t GetBlockSize(const NofSegmentHandle* handle) {
        if (!handle || !handle->ns) {
            return kInvalidBlockSize;
        }
        return spdk_nvme_ns_get_sector_size(handle->ns);
    }

    int SubmitIO(NofSegmentHandle* handle, void* buffer, uint64_t byte_offset,
                 uint64_t byte_length, NofIOOp op, NofIOAdaptor* adaptor) {
        if (!handle || !buffer || byte_length == 0 || !handle->qpair ||
            !handle->ns || !adaptor || !adaptor->cb) {
            return -EINVAL;
        }
        const uint32_t block_size = spdk_nvme_ns_get_sector_size(handle->ns);
        // Interface precondition, enforced at submit time.
        if (block_size == 0 || byte_offset % block_size != 0 ||
            byte_length % block_size != 0) {
            LOG(ERROR) << "SubmitIO alignment violation: offset=" << byte_offset
                       << " length=" << byte_length
                       << " block_size=" << block_size;
            return -EINVAL;
        }

        const uint64_t lba = byte_offset / block_size;
        const uint32_t lba_count =
            static_cast<uint32_t>(byte_length / block_size);
        if (op == NofIOOp::kRead) {
            return spdk_nvme_ns_cmd_read(handle->ns, handle->qpair, buffer, lba,
                                         lba_count, IOCompleteTrampoline,
                                         adaptor, 0);
        }
        if (op == NofIOOp::kWrite) {
            return spdk_nvme_ns_cmd_write(handle->ns, handle->qpair, buffer,
                                          lba, lba_count, IOCompleteTrampoline,
                                          adaptor, 0);
        }
        return -EINVAL;
    }

    int64_t PollCompletion(NofSegmentHandle* handle, uint32_t max_completions) {
        if (!handle || !handle->qpair) {
            return -EINVAL;
        }
        // max_completions == 0 means "all pending" (SPDK dialect, kept as an
        // explicit interface contract).
        return spdk_nvme_qpair_process_completions(handle->qpair,
                                                   max_completions);
    }

    ErrorCode RegisterMemory(void* ptr, size_t size) {
        if (!ptr || size == 0) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (!EnsureEnv()) {
            return ErrorCode::INTERNAL_ERROR;
        }
#ifdef HAVE_SPDK_MEM_REGISTER
        // R-1: a non-aligned ptr is floored to its 2MB page, which assumes
        // the whole containing page is mapped. True for hugepage-backed
        // buffers (hugetlb maps aligned 2MB extents; LMCache/vLLM pool
        // slices land here); 4KB-backed malloc memory fails later inside
        // SPDK with paddr-misalignment -EINVAL anyway. Warn loudly rather
        // than fail silently — a silent floor is the hardest failure
        // shape to debug.
        if (reinterpret_cast<uintptr_t>(ptr) & ((2ULL << 20) - 1)) {
            LOG(WARNING) << "RegisterMemory: ptr " << ptr
                         << " is not 2MB-aligned; registering the whole "
                            "containing 2MB pages, which must be mapped "
                            "and hugepage-backed";
        }
        return NofPageRegistry::GetInstance().Register(ptr, size);
#else
        LOG(WARNING) << "SPDK build lacks spdk_mem_register; NoF over RDMA "
                        "will fail for unregistered user buffers (#3131)";
        return ErrorCode::OK;
#endif
    }

    ErrorCode UnregisterMemory(void* ptr) {
#ifdef HAVE_SPDK_MEM_REGISTER
        return NofPageRegistry::GetInstance().Unregister(ptr);
#else
        (void)ptr;
        return ErrorCode::OK;
#endif
    }

    bool ProbeSegment(const std::string& tr_str, uint32_t timeout_ms,
                      std::string* error_reason) {
        if (!EnsureEnv()) {
            if (error_reason) {
                *error_reason = "spdk_env_init_fail";
            }
            return false;
        }

        NofSegmentHandle* seg_handle = OpenSegment(tr_str);
        if (!seg_handle) {
            if (error_reason) {
                *error_reason = "open_fail";
            }
            return false;
        }

        uint32_t block_size = GetBlockSize(seg_handle);
        if (block_size == kInvalidBlockSize || block_size == 0) {
            if (error_reason) {
                *error_reason = "invalid_block_size";
            }
            return false;
        }

        ProbeBuffer* probe_buffer =
            GetOrCreateProbeBuffer(tr_str, block_size, error_reason);
        if (!probe_buffer || !probe_buffer->ptr) {
            return false;
        }

        ProbeRequestContext* probe_ctx = AcquireProbeRequestContext();
        // The probe reuses the normal submit path with an embedded adaptor,
        // exactly like the worker pool's pooled sub-tasks.
        probe_ctx->adaptor = {&ProbeReadComplete, probe_ctx};
        int ret = SubmitIO(seg_handle, probe_buffer->ptr, 0, block_size,
                           NofIOOp::kRead, &probe_ctx->adaptor);
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
            PollCompletion(seg_handle, 0);
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

        // Ride-along fix (former probe-ctx recycle race): recycle only AFTER
        // the caller has read all results, never inside the callback.
        RecycleProbeRequestContext(probe_ctx);
        return ok;
    }

   private:
    struct ProbeBuffer {
        void* ptr{nullptr};
        uint32_t size{0};

        ProbeBuffer() = default;
        ProbeBuffer(const ProbeBuffer&) = delete;
        ProbeBuffer& operator=(const ProbeBuffer&) = delete;
        ProbeBuffer(ProbeBuffer&&) = delete;
        ProbeBuffer& operator=(ProbeBuffer&&) = delete;
    };

    struct ProbeRequestContext {
        std::atomic<bool> done{false};
        std::atomic<bool> success{false};
        std::mutex error_mutex;
        std::string error_reason;
        NofIOAdaptor adaptor{};

        void Reset() {
            std::lock_guard<std::mutex> lock(error_mutex);
            done.store(false, std::memory_order_release);
            success.store(false, std::memory_order_release);
            error_reason.clear();
            adaptor = {};
        }
    };

    bool EnsureEnv() {
        // Lock first, then check (评审 #4 二轮):对 shared_ptr 成员做
        // 锁外读 + 锁内写是撕裂读 UB,双检模式在这里不成立。冷路径,
        // 一次互斥锁开销可忽略。
        std::lock_guard<std::mutex> lock(env_mutex_);
        if (!env_guard_) {
            env_guard_ = SpdkEnvGuard::Acquire();
        }
        return env_guard_ != nullptr;
    }

    static void IOCompleteTrampoline(void* cb_arg,
                                     const struct spdk_nvme_cpl* cpl) {
        auto* adaptor = static_cast<NofIOAdaptor*>(cb_arg);
        NofIOCompletion completion;
        if (spdk_nvme_cpl_is_error(cpl)) {
            completion.success = false;
            completion.sc = cpl->status.sc;
            completion.sct = cpl->status.sct;
            completion.error_string =
                spdk_nvme_cpl_get_status_string(&cpl->status);
        } else {
            completion.success = true;
        }
        adaptor->cb(adaptor->ctx, completion);
    }

    static void ProbeReadComplete(void* ctx, const NofIOCompletion& c) {
        auto* probe_ctx = reinterpret_cast<ProbeRequestContext*>(ctx);
        if (!c.success) {
            {
                std::lock_guard<std::mutex> lock(probe_ctx->error_mutex);
                probe_ctx->error_reason =
                    std::string("completion_error:") + c.error_string;
            }
            probe_ctx->success.store(false, std::memory_order_release);
        } else {
            probe_ctx->success.store(true, std::memory_order_release);
        }
        probe_ctx->done.store(true, std::memory_order_release);
        // No recycle here — see ProbeSegment.
    }

    int ParseTransPortStr(const std::string& tr_str, tr_info* info) {
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
            } catch (const std::exception& e) {
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

    int ConnectController(const struct spdk_nvme_transport_id* trid,
                          ctrlr_info* info) {
        auto probe_cb = [](void* cb_ctx,
                           const struct spdk_nvme_transport_id* trid,
                           struct spdk_nvme_ctrlr_opts* opts) -> bool {
            ApplyCtrlrOptsFromEnv(opts);
            LOG(INFO) << "Attaching to " << trid->traddr << " " << trid->subnqn;
            return true;
        };

        auto attach_cb = [](void* cb_ctx,
                            const struct spdk_nvme_transport_id* trid,
                            struct spdk_nvme_ctrlr* ctrlr,
                            const struct spdk_nvme_ctrlr_opts* opts) {
            LOG(INFO) << "Attached to " << trid->traddr << " " << trid->subnqn;
            ctrlr_info* info = (ctrlr_info*)cb_ctx;
            info->ctrlr = ctrlr;
        };

        return spdk_nvme_probe(trid, (void*)info, probe_cb, attach_cb, NULL);
    }

    ProbeBuffer* GetOrCreateProbeBuffer(const std::string& tr_str,
                                        uint32_t block_size,
                                        std::string* error_reason) {
        std::lock_guard<std::mutex> lock(probe_buffers_mutex_);
        auto& probe_buffer = probe_buffers_[tr_str];
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

    void ReplenishProbeRequestContextPoolLocked(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            auto probe_ctx = std::make_unique<ProbeRequestContext>();
            probe_request_context_pool_.push(probe_ctx.get());
            probe_request_contexts_.push_back(std::move(probe_ctx));
        }
    }

    ProbeRequestContext* AcquireProbeRequestContext() {
        std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
        if (probe_request_context_pool_.empty()) {
            ReplenishProbeRequestContextPoolLocked(8);
        }
        auto* probe_ctx = probe_request_context_pool_.top();
        probe_request_context_pool_.pop();
        probe_ctx->Reset();
        return probe_ctx;
    }

    void RecycleProbeRequestContext(ProbeRequestContext* ctx) {
        if (ctx == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(probe_request_context_pool_mutex_);
        probe_request_context_pool_.push(ctx);
    }

    // Member order matters: env_guard_ is declared FIRST so it is destroyed
    // LAST — all qpairs/ctrlrs are detached before spdk_env_fini runs.
    std::shared_ptr<SpdkEnvGuard> env_guard_;
    std::mutex env_mutex_;
    std::map<std::string, std::unique_ptr<ctrlr_info>> connected_ctrlrs_;
    std::mutex ctrlrs_mutex_;
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    std::vector<std::unique_ptr<ProbeRequestContext>> probe_request_contexts_;
    std::stack<ProbeRequestContext*> probe_request_context_pool_;
    std::mutex probe_request_context_pool_mutex_;
};

// ---------------------------------------------------------------------------
// SpdkInitiator forwarding
// ---------------------------------------------------------------------------

SpdkInitiator::SpdkInitiator() : impl_(std::make_unique<Impl>()) {}
SpdkInitiator::~SpdkInitiator() = default;

NofSegmentHandle* SpdkInitiator::OpenSegment(const std::string& tr_str) {
    return impl_->OpenSegment(tr_str);
}

bool SpdkInitiator::ProbeSegment(const std::string& tr_str, uint32_t timeout_ms,
                                 std::string* error_reason) {
    return impl_->ProbeSegment(tr_str, timeout_ms, error_reason);
}

uint32_t SpdkInitiator::GetBlockSize(const NofSegmentHandle* handle) {
    return impl_->GetBlockSize(handle);
}

int SpdkInitiator::SubmitIO(NofSegmentHandle* handle, void* buffer,
                            uint64_t byte_offset, uint64_t byte_length,
                            NofIOOp op, NofIOAdaptor* adaptor) {
    return impl_->SubmitIO(handle, buffer, byte_offset, byte_length, op,
                           adaptor);
}

int64_t SpdkInitiator::PollCompletion(NofSegmentHandle* handle,
                                      uint32_t max_completions) {
    return impl_->PollCompletion(handle, max_completions);
}

ErrorCode SpdkInitiator::RegisterMemory(void* ptr, size_t size) {
    return impl_->RegisterMemory(ptr, size);
}

ErrorCode SpdkInitiator::UnregisterMemory(void* ptr) {
    return impl_->UnregisterMemory(ptr);
}

NofCapabilities SpdkInitiator::GetCapabilities() const {
    NofCapabilities caps;
    caps.supports_sgl = false;  // flips when PR #3251's SGL path lands here
    caps.dma_alignment = 4;
    // Upper bound (评审 #8): registration capability exists in this build;
    // not transport-specific. RegisterMemory on TCP NVMe-oF is harmless,
    // callers must not skip it based on this flag.
#ifdef HAVE_SPDK_MEM_REGISTER
    caps.requires_memory_registration = true;
#else
    caps.requires_memory_registration = false;
#endif
    return caps;
}

// ---------------------------------------------------------------------------
// SpdkDmaAllocator
// ---------------------------------------------------------------------------

SpdkDmaAllocator::SpdkDmaAllocator() = default;
SpdkDmaAllocator::~SpdkDmaAllocator() = default;

void* SpdkDmaAllocator::Alloc(size_t size, size_t align, int socket_id) {
    {
        // Lock first, then check (评审 #4 二轮):对 shared_ptr 成员的
        // 锁外读与锁内写构成撕裂读 UB,双检模式在这里不成立。
        // Alloc 是冷路径,一次互斥锁开销可忽略。
        std::lock_guard<std::mutex> lock(env_mutex_);
        if (!env_guard_) {
            env_guard_ = SpdkEnvGuard::Acquire();
            if (!env_guard_) {
                return nullptr;
            }
        }
    }
    return spdk_zmalloc(size, align, nullptr, socket_id, SPDK_MALLOC_DMA);
}

void SpdkDmaAllocator::Free(void* ptr) {
    if (ptr) {
        spdk_free(ptr);
    }
}

}  // namespace mooncake
