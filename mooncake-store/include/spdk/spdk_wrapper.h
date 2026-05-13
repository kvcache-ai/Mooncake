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

struct nof_seg_handle;
struct tr_info;
struct ctrlr_info;

class SpdkWrapper {
public:

    SpdkWrapper(const SpdkWrapper&) = delete;
    SpdkWrapper& operator=(const SpdkWrapper&) = delete;

    static SpdkWrapper& GetInstance();

    bool InitializeEnv();

    void Cleanup();

    void* Alloc(size_t size, size_t align, int socket_id = -1);

    void Free(void *ptr);

    int64_t NvmePollProcessCompletion(nof_seg_handle *seg, uint32_t complete_per_seg);

    /**
     * @brief 获取传输通道
     *
     * 连接控制器
     *
     * @param tr_str transport description
     * @return nof_seg handle the async operation, or nullptr
     * failure
     */
    nof_seg_handle *OpenNofSegment(const std::string &tr_str);

    uint32_t GetBlockSize(const nof_seg_handle *seg_handle);

    int SubmitRequest(const nof_seg_handle *seg_handle, void *ptr, uint64_t lba, uint32_t lba_count,
        int op, spdk_nvme_cmd_cb cb_fn, void *cb_ctx);

    bool ProbeNofSegment(const std::string &tr_str, uint32_t timeout_ms,
                         std::string *error_reason = nullptr);

private:
    struct ProbeBuffer {
        void *ptr{nullptr};
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
        SpdkWrapper *owner{nullptr};

        void Reset(SpdkWrapper *wrapper) {
            owner = wrapper;
            done.store(false, std::memory_order_release);
            success.store(false, std::memory_order_release);
            std::lock_guard<std::mutex> lock(error_mutex);
            error_reason.clear();
        }
    };

    explicit SpdkWrapper();
    ~SpdkWrapper();

    int ParseTransPortStr(const std::string &tr_str, tr_info *info);
    int ConnectController(const struct spdk_nvme_transport_id *trid, ctrlr_info *info);
    ProbeBuffer *GetOrCreateProbeBuffer(const std::string &tr_str,
                                        uint32_t block_size,
                                        std::string *error_reason);
    ProbeRequestContext *AcquireProbeRequestContext();
    void RecycleProbeRequestContext(ProbeRequestContext *ctx);
    void ReplenishProbeRequestContextPoolLocked(size_t count);
    static void ProbeReadComplete(void *ctx, const struct spdk_nvme_cpl *cpl);

    bool initialized;
    std::mutex init_mutex;
    std::map<std::string, std::unique_ptr<ctrlr_info>> connected_ctrlrs;
    std::mutex ctrlrs_mutex;
    std::map<std::string, std::unique_ptr<ProbeBuffer>> probe_buffers_;
    std::mutex probe_buffers_mutex_;
    std::vector<std::unique_ptr<ProbeRequestContext>> probe_request_contexts_;
    std::stack<ProbeRequestContext *> probe_request_context_pool_;
    std::mutex probe_request_context_pool_mutex_;
};

}
