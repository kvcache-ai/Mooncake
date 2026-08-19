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

// Added includes
#include "nof_config.h"
#include "nof_connection.h"
#include "nof_segment.h"

namespace mooncake {

#define INVALID_BLOCK_SIZE 0xFFFFFFFF

constexpr int kSpdkNofOpRead = 0;
constexpr int kSpdkNofOpWrite = 1;
constexpr int kSpdkNofOpNum = 2;

// [Migrated] tr_info / ctrlr_info forward declarations removed; transport
// parsing is now handled internally by NofConnection::Connect().
struct nof_seg_handle;

/**
 * @brief QID pressure gauge.
 *
 * Uses a sliding window to record the results of the last N qpair
 * allocation attempts (allocated/requested ratio) and computes the QID
 * acquisition rate on the target side. When a new connection is
 * established, it adaptively reduces the number of requested qpairs to
 * prevent early comers from monopolizing the target QID pool.
 *
 * Thread safety: Record() and GetRecommended() are protected by an
 * internal mutex and can safely be called from multiple threads
 * concurrently.
 *
 * Changelog | 2026-08-03 | Added (this PR)
 */
class QidPressureGauge {
   public:
    QidPressureGauge() {
        for (size_t i = 0; i < kWindowSize; i++) {
            window_[i].requested = 0;
            window_[i].allocated = 0;
        }
    }

    /// Records the result of the most recent qpair allocation.
    /// @param requested  The number of qpairs requested.
    /// @param allocated  The number of qpairs actually allocated.
    void Record(uint32_t requested, uint32_t allocated) {
        if (requested == 0) return;
        std::lock_guard<std::mutex> lock(mutex_);
        size_t idx =
            write_idx_.fetch_add(1, std::memory_order_relaxed) % kWindowSize;
        window_[idx].requested = requested;
        window_[idx].allocated = allocated;
    }

    /// Returns the current pressure level.
    /// @return 0=Green (>75% acquisition rate), 1=Yellow (50-75%),
    /// 2=Red (<50%).
    int GetPressureLevel() const {
        double ratio = GetAverageRatio();
        if (ratio > 0.75) return 0;  // Green: healthy
        if (ratio > 0.50) return 1;  // Yellow: moderate pressure
        return 2;                    // Red: severe pressure
    }

    /// Returns the recommended number of qpairs for a new connection
    /// based on the current pressure level.
    /// @param configured  The num_io_queues configured in NofConfig.
    /// @return The recommended number of qpairs to request.
    uint32_t GetRecommended(uint32_t configured) const {
        // Already small enough; no reduction needed
        if (configured <= 4) return configured;
        int level = GetPressureLevel();
        switch (level) {
            case 0:  // Green — request the full amount
                return configured;
            case 1:  // Yellow — request 3/4
                return std::max(4u, configured * 3 / 4);
            case 2:  // Red — request 1/2
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
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t total_requested = 0;
        uint64_t total_allocated = 0;
        for (size_t i = 0; i < kWindowSize; i++) {
            total_requested += window_[i].requested;
            total_allocated += window_[i].allocated;
        }
        if (total_requested == 0) return 1.0;  // No history, assume healthy.
        return static_cast<double>(total_allocated) /
               static_cast<double>(total_requested);
    }

    // Guards window_ writes in Record() and reads in GetAverageRatio().
    // write_idx_ remains atomic (relaxed) to avoid contention on the
    // fast-path index increment, but all sample data access is serialised
    // through this mutex.
    mutable std::mutex mutex_;
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

    // Added APIs
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
    // ---- Per-open-segment tracking ----
    // open_segments_ is indexed by handle; each entry owns the NofConnection
    // for that segment.
    std::map<nof_seg_handle *, std::unique_ptr<NofConnection>> open_segments_;
    std::mutex segments_mutex_;

    // ---- Configuration ----
    NofConfig config_;

    // Configuration dedicated to heartbeat probing.
    // Initialized with ForProbe(), num_io_queues=1.
    NofConfig config_probe_;

    // Global QID pressure gauge.
    QidPressureGauge qid_pressure_gauge_;

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

        // Reset state before reuse.  Caller must hold
        // probe_request_context_pool_mutex_ or otherwise guarantee
        // exclusive access.
        void Reset() {
            std::lock_guard<std::mutex> lock(error_mutex);
            done.store(false, std::memory_order_release);
            success.store(false, std::memory_order_release);
            error_reason.clear();
        }
    };

    explicit SpdkWrapper();
    ~SpdkWrapper();

    // [Removed] ParseTransPortStr / ConnectController — handled
    // uniformly by NofConnection::Connect() in the new design.
    ProbeBuffer *GetOrCreateProbeBuffer(const std::string &tr_str,
                                        uint32_t block_size,
                                        std::string *error_reason);
    ProbeRequestContext *AcquireProbeRequestContext();
    void RecycleProbeRequestContext(ProbeRequestContext *ctx);
    void ReplenishProbeRequestContextPoolLocked(size_t count);
    static void ProbeReadComplete(void *ctx, const struct spdk_nvme_cpl *cpl);

    std::atomic<bool> initialized{false};
    std::mutex init_mutex;
    // [Removed] connected_ctrlrs / ctrlrs_mutex — the new design
    // manages connections via open_segments_.
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    std::vector<std::unique_ptr<ProbeRequestContext>> probe_request_contexts_;
    std::stack<ProbeRequestContext *> probe_request_context_pool_;
    std::mutex probe_request_context_pool_mutex_;

    // Added 2026-07-31: serializes the slow path (first connection).
    // spdk_nvme_probe() is not thread-safe — concurrent Connect calls
    // from multiple threads cause a namespace activation race where all
    // return namespace_inactive.
    // This mutex ensures only one thread executes
    // NofConnection::Connect() at a time.
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
// NOT SpdkWrapper methods. Reason: during static destruction the
// SpdkWrapper singleton may already be destroyed, but the
// SpdkNofWorkerPool destructor still needs to unregister. Free
// functions can safely access file-level global counters.
void SpdkNoF_RegisterWorkerPool();
void SpdkNoF_UnregisterWorkerPool();

}  // namespace mooncake
