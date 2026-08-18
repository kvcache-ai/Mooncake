#include "nvme_kv_executor.h"

#if defined(USE_NOF) && defined(MOONCAKE_HAVE_SPDK_NVME_KV)

#include "nvme_kv_executor_util.h"
#include "nvme_kv_object_layout.h"
#include "spdk/spdk_wrapper.h"

#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace mooncake {
namespace {

struct SpdkDmaBufferDeleter {
    void operator()(void *ptr) const { SpdkWrapper::GetInstance().Free(ptr); }
};

using SpdkDmaBuffer = std::unique_ptr<void, SpdkDmaBufferDeleter>;

struct CompletionContext {
    std::atomic<bool> done{false};
    bool submitted = false;
    bool reported = false;
    bool is_write = false;
    uint32_t status = 0;
};

void KvCommandComplete(void *ctx, const struct spdk_nvme_cpl *cpl) {
    auto *completion = static_cast<CompletionContext *>(ctx);
    if (spdk_nvme_cpl_is_error(cpl)) {
        completion->status = cpl->status.sc;
    } else {
        completion->status = 0;
    }
    completion->done.store(true, std::memory_order_release);
}

ErrorCode MapSpdkSubmitError(int ret, bool is_write) {
    if (ret == 0) {
        return ErrorCode::OK;
    }
    return MapNvmeKvTransportError(ret < 0 ? -ret : ret, is_write);
}

bool CanUseCallerBuffer(std::string_view value, uint32_t submission_bytes) {
    if (value.empty() || value.size() != submission_bytes) {
        return false;
    }
    const size_t alignment = std::max<size_t>(
        kDefaultNvmeKvTransferAlignmentBytes, NvmeKvTransferAlignmentBytes());
    return reinterpret_cast<uintptr_t>(value.data()) % alignment == 0;
}

}  // namespace

class NvmeKvSpdkExecutor : public NvmeKvCommandExecutor {
   public:
    NvmeKvSpdkExecutor(SpdkWrapper &wrapper, nof_seg_handle *segment,
                       Capabilities capabilities)
        : wrapper_(wrapper),
          segment_(segment),
          capabilities_(std::move(capabilities)) {}

    tl::expected<void, ErrorCode> Store(const PhysicalKey &key,
                                        std::string value) override {
        std::vector<StoreRequest> requests;
        requests.push_back(StoreRequest{key, value});
        StoreBatch(requests);
        return requests.front().result;
    }

    void StoreBatch(std::vector<StoreRequest> &requests) override {
        if (requests.empty()) {
            return;
        }

        std::vector<SpdkDmaBuffer> dma_buffers(requests.size());
        std::vector<const void *> submit_buffers(requests.size(), nullptr);
        std::vector<uint32_t> submit_sizes(requests.size(), 0);

        for (size_t index = 0; index < requests.size(); ++index) {
            auto &request = requests[index];
            if (request.value.size() > capabilities_.effective_max_value_size) {
                request.result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            const uint32_t submission_bytes = ResolveNvmeKvStoreSubmissionBytes(
                static_cast<uint32_t>(request.value.size()));
            if (submission_bytes == 0 ||
                submission_bytes > capabilities_.effective_max_value_size) {
                request.result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            submit_sizes[index] = submission_bytes;
            if (CanUseCallerBuffer(request.value, submission_bytes)) {
                submit_buffers[index] = request.value.data();
                continue;
            }

            void *buffer = wrapper_.Alloc(submission_bytes,
                                          NvmeKvTransferAlignmentBytes());
            if (buffer == nullptr) {
                request.result =
                    tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
                continue;
            }
            if (!request.value.empty()) {
                std::memcpy(buffer, request.value.data(), request.value.size());
            }
            if (submission_bytes > request.value.size()) {
                std::memset(static_cast<char *>(buffer) + request.value.size(),
                            0, submission_bytes - request.value.size());
            }
            dma_buffers[index].reset(buffer);
            submit_buffers[index] = buffer;
        }

        SubmitBatch(
            requests.size(), true,
            [&](size_t index, CompletionContext *ctx) {
                if (submit_buffers[index] == nullptr) {
                    return false;
                }
                const int ret = wrapper_.SubmitKvStore(
                    segment_, requests[index].key.data(),
                    static_cast<uint8_t>(requests[index].key.size()),
                    submit_buffers[index], submit_sizes[index],
                    SPDK_NVME_KV_STORE_OPT_DONT_STORE_IF_KEY_EXISTS,
                    KvCommandComplete, ctx);
                if (ret != 0) {
                    requests[index].result =
                        tl::make_unexpected(MapSpdkSubmitError(ret, true));
                    return false;
                }
                return true;
            },
            [&](size_t index, ErrorCode error) {
                if (error == ErrorCode::OK) {
                    requests[index].result = {};
                } else {
                    requests[index].result = tl::make_unexpected(error);
                }
            });
    }

    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey &key, uint32_t size_hint = 0) const override {
        std::vector<RetrieveBufferRequest> requests;
        RetrieveBufferRequest request;
        request.key = key;
        request.size_hint = size_hint;
        requests.push_back(std::move(request));
        RetrieveBufferBatch(requests);
        auto &result = requests.front().result;
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        return result.value().ToString();
    }

    void RetrieveBufferBatch(
        std::vector<RetrieveBufferRequest> &requests) const override {
        RetrieveBufferBatchOnce(requests, false);

        std::vector<size_t> retry_indexes;
        for (size_t index = 0; index < requests.size(); ++index) {
            const uint32_t request_bytes = ResolveNvmeKvInitialRetrieveBytes(
                requests[index].size_hint,
                capabilities_.effective_max_value_size);
            if (!requests[index].result &&
                ShouldRetryNvmeKvRetrieveWithMaxBuffer(
                    requests[index].result.error(), requests[index].size_hint,
                    request_bytes, capabilities_.effective_max_value_size)) {
                retry_indexes.push_back(index);
            }
        }
        if (retry_indexes.empty()) {
            return;
        }

        std::vector<RetrieveBufferRequest> retries;
        retries.reserve(retry_indexes.size());
        for (size_t index : retry_indexes) {
            RetrieveBufferRequest retry;
            retry.key = requests[index].key;
            retry.size_hint = capabilities_.effective_max_value_size;
            retries.push_back(std::move(retry));
        }
        RetrieveBufferBatchOnce(retries, true);
        for (size_t index = 0; index < retry_indexes.size(); ++index) {
            requests[retry_indexes[index]].result =
                std::move(retries[index].result);
        }
    }

    void RetrieveIntoBatch(
        std::vector<RetrieveIntoRequest> &requests) const override {
        std::vector<RetrieveBufferRequest> buffer_requests;
        buffer_requests.reserve(requests.size());
        for (const auto &request : requests) {
            RetrieveBufferRequest buffer_request;
            buffer_request.key = request.key;
            buffer_request.size_hint = request.size;
            buffer_requests.push_back(std::move(buffer_request));
        }

        RetrieveBufferBatch(buffer_requests);
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto &buffer_result = buffer_requests[index].result;
            if (!buffer_result) {
                requests[index].result =
                    tl::make_unexpected(buffer_result.error());
                continue;
            }
            const auto &buffer = buffer_result.value();
            if (requests[index].data == nullptr ||
                buffer.size != requests[index].size) {
                requests[index].result =
                    tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                continue;
            }
            std::memcpy(requests[index].data, buffer.data, buffer.size);
            requests[index].result = buffer.size;
        }
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey &key) override {
        tl::expected<void, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        SubmitBatch(
            1, false,
            [&](size_t, CompletionContext *ctx) {
                const int ret = wrapper_.SubmitKvDelete(
                    segment_, key.data(), static_cast<uint8_t>(key.size()),
                    KvCommandComplete, ctx);
                if (ret != 0) {
                    result =
                        tl::make_unexpected(MapSpdkSubmitError(ret, false));
                    return false;
                }
                return true;
            },
            [&](size_t, ErrorCode error) {
                if (error == ErrorCode::OK) {
                    result = {};
                } else {
                    result = tl::make_unexpected(error);
                }
            });
        return result;
    }

    const Capabilities &GetCapabilities() const override {
        return capabilities_;
    }

   private:
    template <typename SubmitFn, typename CompleteFn>
    void SubmitBatch(size_t count, bool is_write, SubmitFn submit,
                     CompleteFn complete) const {
        std::vector<CompletionContext> contexts(count);
        size_t next = 0;
        size_t completed = 0;
        size_t inflight = 0;
        const size_t queue_depth =
            std::max<uint32_t>(1, capabilities_.queue_depth);

        std::lock_guard<std::mutex> lock(qpair_mutex_);
        while (completed < count) {
            while (next < count && inflight < queue_depth) {
                auto &ctx = contexts[next];
                ctx.is_write = is_write;
                if (!submit(next, &ctx)) {
                    if (ctx.done.load(std::memory_order_acquire)) {
                        complete(next, MapNvmeKvStatus(ctx.status, is_write));
                    }
                    ++completed;
                    ++next;
                    continue;
                }
                ctx.submitted = true;
                ++inflight;
                ++next;
            }

            if (inflight == 0) {
                continue;
            }
            const int rc = static_cast<int>(
                wrapper_.NvmePollProcessCompletion(segment_, 0));
            if (rc < 0) {
                LOG(ERROR) << "SPDK NVMe KV completion polling failed: " << rc;
            }

            size_t observed = 0;
            for (size_t index = 0; index < count; ++index) {
                auto &ctx = contexts[index];
                if (!ctx.submitted || ctx.reported ||
                    !ctx.done.load(std::memory_order_acquire)) {
                    continue;
                }
                ctx.reported = true;
                --inflight;
                ++completed;
                ++observed;
                complete(index, ctx.status == 0
                                    ? ErrorCode::OK
                                    : MapNvmeKvStatus(ctx.status, is_write));
            }
            if (observed == 0) {
                std::this_thread::yield();
            }
        }
    }

    void RetrieveBufferBatchOnce(std::vector<RetrieveBufferRequest> &requests,
                                 bool force_effective_max_value_size) const {
        std::vector<SpdkDmaBuffer> dma_buffers(requests.size());
        std::vector<uint32_t> request_bytes(requests.size(), 0);

        for (size_t index = 0; index < requests.size(); ++index) {
            const uint32_t bytes =
                force_effective_max_value_size
                    ? capabilities_.effective_max_value_size
                    : ResolveNvmeKvInitialRetrieveBytes(
                          requests[index].size_hint,
                          capabilities_.effective_max_value_size);
            if (bytes == 0 || bytes > capabilities_.effective_max_value_size) {
                requests[index].result =
                    tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            void *buffer =
                wrapper_.Alloc(bytes, NvmeKvTransferAlignmentBytes());
            if (buffer == nullptr) {
                requests[index].result =
                    tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
                continue;
            }
            dma_buffers[index].reset(buffer);
            request_bytes[index] = bytes;
        }

        SubmitBatch(
            requests.size(), false,
            [&](size_t index, CompletionContext *ctx) {
                if (dma_buffers[index] == nullptr) {
                    return false;
                }
                const int ret = wrapper_.SubmitKvRetrieve(
                    segment_, requests[index].key.data(),
                    static_cast<uint8_t>(requests[index].key.size()),
                    dma_buffers[index].get(), request_bytes[index], 0,
                    KvCommandComplete, ctx);
                if (ret != 0) {
                    requests[index].result =
                        tl::make_unexpected(MapSpdkSubmitError(ret, false));
                    return false;
                }
                return true;
            },
            [&](size_t index, ErrorCode error) {
                if (error != ErrorCode::OK) {
                    requests[index].result = tl::make_unexpected(error);
                    return;
                }
                const uint32_t value_size = ResolveNvmeKvRetrievedValueSize(
                    static_cast<const char *>(dma_buffers[index].get()),
                    request_bytes[index],
                    capabilities_.effective_max_value_size,
                    requests[index].size_hint);
                if (value_size == 0 || value_size > request_bytes[index]) {
                    requests[index].result =
                        tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                    return;
                }
                RetrievedBuffer buffer;
                void *owned = dma_buffers[index].release();
                buffer.owner =
                    std::shared_ptr<void>(owned, SpdkDmaBufferDeleter());
                buffer.data = static_cast<const char *>(owned);
                buffer.size = value_size;
                requests[index].result = std::move(buffer);
            });
    }

    SpdkWrapper &wrapper_;
    nof_seg_handle *segment_ = nullptr;
    Capabilities capabilities_;
    mutable std::mutex qpair_mutex_;
};

NvmeKvExecutorResult CreateNvmeKvSpdkExecutor(std::string transport_id,
                                              uint32_t nsid,
                                              uint32_t queue_depth,
                                              uint32_t runtime_transfer_limit) {
    auto &wrapper = SpdkWrapper::GetInstance();
    if (!wrapper.InitializeEnv()) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    const std::string effective_transport_id =
        NvmeKvTransportIdWithNsid(transport_id, nsid);
    auto *segment = wrapper.OpenNofSegment(effective_transport_id);
    if (segment == nullptr) {
        LOG(ERROR) << "Failed to open SPDK NVMe-oF segment: "
                   << effective_transport_id;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    if (!wrapper.IsKvNamespace(segment)) {
        LOG(ERROR) << "SPDK NVMe-oF namespace is not an NVMe KV namespace: "
                   << effective_transport_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto capabilities = BuildNvmeKvCapabilities(
        kDefaultNvmeKvQueueDepth, queue_depth, runtime_transfer_limit);
    return std::make_unique<NvmeKvSpdkExecutor>(wrapper, segment,
                                                std::move(capabilities));
}

}  // namespace mooncake

#endif
