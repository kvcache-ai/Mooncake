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

namespace mooncake {

#define INVALID_BLOCK_SIZE 0xFFFFFFFF

constexpr int kSpdkNofOpRead = 0;
constexpr int kSpdkNofOpWrite = 1;
constexpr int kSpdkNofOpNum = 2;

struct nof_seg_handle;
struct tr_info;
struct ctrlr_info;

class SpdkWrapper {
   public:
    SpdkWrapper(const SpdkWrapper &) = delete;
    SpdkWrapper &operator=(const SpdkWrapper &) = delete;

    static SpdkWrapper &GetInstance();

    bool InitializeEnv();

    void Cleanup();

    void *Alloc(size_t size, size_t align, int socket_id = -1);

    void Free(void *ptr);

    /**
     * @brief Poll completions of a NoF segment's io qpair.
     *
     * Completion callbacks run on the calling thread before this returns.
     * When @p io_timed_out is not null it is set to whether SPDK reported,
     * during this poll, an I/O of this qpair that exceeded
     * MC_NOF_IO_TIMEOUT_MS since it was handed to the transport. Nothing has
     * been aborted at that point; the caller decides.
     */
    int64_t NvmePollProcessCompletion(nof_seg_handle *seg,
                                      uint32_t complete_per_seg,
                                      bool *io_timed_out = nullptr);

    /** @brief Open a NoF segment. */
    nof_seg_handle *OpenNofSegment(const std::string &tr_str);

    uint32_t GetBlockSize(const nof_seg_handle *seg_handle);

    int SubmitRequest(const nof_seg_handle *seg_handle, void *ptr, uint64_t lba,
                      uint32_t lba_count, int op, spdk_nvme_cmd_cb cb_fn,
                      void *cb_ctx);

    bool ProbeNofSegment(const std::string &tr_str, uint32_t timeout_ms,
                         std::string *error_reason = nullptr);

    /** @brief Register external memory with SPDK for NoF zero-copy transfers.
     *
     * Memory returned by Alloc() is already registered with SPDK; use this to
     * register memory allocated outside of SPDK (e.g. mmap'd shared memory) so
     * that NoF RDMA transfers can DMA to/from it directly
     * (spdk_rdma_get_translation).
     *
     * The pinned SPDK (v23.01.1) requires BOTH addr and size to be 2MB-aligned
     * (MASK_2MB), and in iova=pa mode (the typical libibverbs NoF setup) the
     * PHYSICAL pages must be 2MB-aligned too, which only HugeTLB pages satisfy.
     * 4KB page-aligned memory always fails with -EINVAL. Callers must pass
     * 2MB-aligned, hugepage-backed memory (ShmHelper forces this under
     * MC_STORE_REGISTER_SPDK=1) and check IsRegistrableRange() first: a failure
     * past that check may leave SPDK's g_mem_reg_map marked, so callers must
     * attempt UnregisterMemory() and keep the mapping unless it returns 0.
     *
     * @param addr Start of the region; must be 2MB-aligned (hugepage-backed).
     * @param size Region length; must be a multiple of 2MB.
     * @return 0 on success, non-zero on failure.
     */
    int RegisterMemory(void *addr, size_t size);

    /** @brief Unregister memory previously registered via RegisterMemory().
     *
     * Only a 0 return proves the range is clean. A non-zero return does NOT
     * prove that nothing was ever registered: -EINVAL is also what SPDK
     * returns for a half-registered range, so callers must keep the mapping
     * alive (and retry) unless this returns 0. See the definition.
     *
     * @return 0 on success, non-zero on failure.
     */
    int UnregisterMemory(void *addr, size_t size);

    /** @brief Whether RegisterMemory() can be attempted for this range at all.
     *
     * spdk_mem_register() rejects a range it cannot represent or that is not
     * 2MB-aligned before it has marked anything (SPDK v23.01.1,
     * lib/env_dpdk/memory.c:339-348), so a caller that skips the registration
     * for such a range still munmaps normally. Any failure past this check may
     * have left translation state behind and must be treated as such, with one
     * exception the caller handles separately: -EBUSY means another owner
     * already holds the range, so nothing new was marked and rolling back would
     * clear their registration instead.
     */
    static bool IsRegistrableRange(void *addr, size_t size);

    /**
     * @brief Abort every outstanding I/O of a NoF segment locally.
     *
     * Disconnects the segment's io qpair. SPDK then completes each request
     * still outstanding on that qpair through its completion callback with
     * ABORTED - SQ DELETION; with the pinned SPDK (v23.01, TCP and RDMA in
     * the default synchronous mode) this happens before the call returns.
     * Unlike the NVMe Abort admin command it needs no response from the
     * target, so it also works when the target is stalled.
     *
     * The qpair stays disconnected: SubmitRequest() fails with -ENXIO until
     * it is reconnected. Must be called from the thread that submits to and
     * polls this segment, and never from inside a completion callback.
     */
    void AbortNofSegmentIo(nof_seg_handle *seg_handle);

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

    int ParseTransPortStr(const std::string &tr_str, tr_info *info);
    int ConnectController(const struct spdk_nvme_transport_id *trid,
                          ctrlr_info *info);
    ProbeBuffer *GetOrCreateProbeBuffer(const std::string &tr_str,
                                        uint32_t block_size,
                                        std::string *error_reason);
    ProbeRequestContext *AcquireProbeRequestContext();
    void RecycleProbeRequestContext(ProbeRequestContext *ctx);
    void ReplenishProbeRequestContextPoolLocked(size_t count);
    static void ProbeReadComplete(void *ctx, const struct spdk_nvme_cpl *cpl);
    static void NofIoTimeoutCallback(void *cb_arg,
                                     struct spdk_nvme_ctrlr *ctrlr,
                                     struct spdk_nvme_qpair *qpair,
                                     uint16_t cid);

    std::atomic<bool> initialized{false};
    std::mutex init_mutex;
    std::map<std::string, std::unique_ptr<ctrlr_info>> connected_ctrlrs;
    std::mutex ctrlrs_mutex;
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    std::vector<std::unique_ptr<ProbeRequestContext>> probe_request_contexts_;
    std::stack<ProbeRequestContext *> probe_request_context_pool_;
    std::mutex probe_request_context_pool_mutex_;
};

}  // namespace mooncake
