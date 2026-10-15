#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stack>
#include <string>
#include <thread>
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

    int64_t NvmePollProcessCompletion(nof_seg_handle *seg,
                                      uint32_t complete_per_seg);

    /** @brief Open a NoF segment. */
    nof_seg_handle *OpenNofSegment(const std::string &tr_str);

    uint32_t GetBlockSize(const nof_seg_handle *seg_handle);

    int SubmitRequest(const nof_seg_handle *seg_handle, void *ptr, uint64_t lba,
                      uint32_t lba_count, int op, spdk_nvme_cmd_cb cb_fn,
                      void *cb_ctx);

    bool ProbeNofSegment(const std::string &tr_str, uint32_t timeout_ms,
                         std::string *error_reason = nullptr);

    /** @brief Whether the io qpair of a NoF segment has entered a failed
     * state, e.g. because the target went away. */
    bool IsNofSegmentFailed(const nof_seg_handle *seg_handle);

    /**
     * @brief Shared hold on the controller behind a NoF segment.
     *
     * Every thread that submits to or polls the io qpair of a segment must
     * hold this while it does so. A controller reset takes the same lock
     * exclusively, so it never runs while another thread is using any qpair
     * or cached namespace of that controller. The lock is only tried, never
     * waited on: when a reset is queued or in progress the returned lock does
     * not own the mutex and the caller should skip the segment for this
     * round.
     */
    std::shared_lock<std::shared_mutex> TryLockNofSegmentIo(
        const nof_seg_handle *seg_handle);

    /**
     * @brief Try to bring a failed NoF segment back, without blocking.
     *
     * Must be called from the thread that owns the segment's io qpair, while
     * holding TryLockNofSegmentIo(). Returns true when the qpair is healthy.
     * Otherwise the controller reset runs on a background recovery thread,
     * and once it has succeeded the next call reconnects the io qpair here,
     * on the owning thread, as SPDK requires. A thread that serves several
     * segments therefore never waits on a target that is still down.
     */
    bool TryRecoverNofSegment(nof_seg_handle *seg_handle);

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
    void ScheduleControllerReset(ctrlr_info *info);
    void RecoveryLoop();
    void ResetController(ctrlr_info *info);
    void StopRecoveryThread();

    std::atomic<bool> initialized{false};
    std::mutex init_mutex;
    std::map<std::string, std::unique_ptr<ctrlr_info>> connected_ctrlrs;
    std::mutex ctrlrs_mutex;
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    std::vector<std::unique_ptr<ProbeRequestContext>> probe_request_contexts_;
    std::stack<ProbeRequestContext *> probe_request_context_pool_;
    std::mutex probe_request_context_pool_mutex_;

    // Controller resets requested by TryRecoverNofSegment, run one at a time
    // on recovery_thread_ so no I/O worker blocks on a target that is down.
    std::thread recovery_thread_;
    std::mutex recovery_mutex_;
    std::condition_variable recovery_cv_;
    std::deque<ctrlr_info *> recovery_queue_;
    bool recovery_stop_{false};
};

}  // namespace mooncake
