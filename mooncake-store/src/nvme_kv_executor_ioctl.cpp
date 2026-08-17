#include "nvme_kv_executor.h"
#include "nvme_kv_executor_util.h"
#include "nvme_kv_object_layout.h"

#include <fcntl.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

#include <glog/logging.h>

namespace mooncake {
namespace {

class NvmeKvIoctlExecutor : public NvmeKvCommandExecutor {
   public:
    NvmeKvIoctlExecutor(std::string device_path, uint32_t nsid,
                        Capabilities capabilities)
        : device_path_(std::move(device_path)),
          nsid_(nsid),
          capabilities_(capabilities) {}

    tl::expected<void, ErrorCode> Init() {
        fd_ = ::open(device_path_.c_str(), O_RDWR | O_CLOEXEC);
        if (fd_ < 0) {
            LOG(ERROR) << "[NvmeKvIoctlExecutor] open failed for "
                       << device_path_ << ": " << strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        return {};
    }

    ~NvmeKvIoctlExecutor() override {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    tl::expected<void, ErrorCode> Store(const PhysicalKey &key,
                                        std::string value) override {
        if (value.size() > capabilities_.effective_max_value_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const uint32_t value_size = static_cast<uint32_t>(value.size());
        const uint32_t submission_bytes =
            ResolveNvmeKvStoreSubmissionBytes(value_size);
        if (submission_bytes > capabilities_.effective_max_value_size) {
            LOG(WARNING) << "[NvmeKvIoctlExecutor] store submission exceeds "
                            "effective_max_value_size"
                         << " logical_bytes=" << value_size
                         << " submission_bytes=" << submission_bytes
                         << " effective_max_value_size="
                         << capabilities_.effective_max_value_size;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto dma_buffer = AllocateNvmeKvAlignedBuffer(submission_bytes);
        if (dma_buffer == nullptr) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        std::memset(dma_buffer.get(), 0, submission_bytes);
        std::memcpy(dma_buffer.get(), value.data(), value.size());

        nvme_passthru_cmd cmd{};
        BuildNvmeKvStoreCommand(cmd, nsid_, key, dma_buffer.get(),
                                submission_bytes);

        auto result = Submit(cmd, true, "store-if-not-exists");
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        return {};
    }

    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey &key, uint32_t size_hint = 0) const override {
        const auto submit_retrieve = [&](uint32_t request_bytes,
                                         const char *op_name)
            -> tl::expected<std::pair<NvmeKvAlignedBuffer, uint32_t>,
                            ErrorCode> {
            const uint32_t transfer_bytes =
                RoundUpToNvmeKvTransferBytes(request_bytes);
            auto dma_buffer = AllocateNvmeKvAlignedBuffer(transfer_bytes);
            if (dma_buffer == nullptr) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            nvme_passthru_cmd cmd{};
            BuildNvmeKvRetrieveCommand(cmd, nsid_, key, dma_buffer.get(),
                                       transfer_bytes);

            auto result = Submit(cmd, false, op_name);
            if (!result) {
                return tl::make_unexpected(result.error());
            }
            return std::make_pair(std::move(dma_buffer), result.value());
        };

        const uint32_t initial_request_bytes =
            ResolveNvmeKvInitialRetrieveBytes(
                size_hint, capabilities_.effective_max_value_size);
        auto retrieve_res = submit_retrieve(initial_request_bytes, "retrieve");
        bool used_fallback_request = false;
        if (!retrieve_res) {
            if (!ShouldRetryNvmeKvRetrieveWithMaxBuffer(
                    retrieve_res.error(), size_hint, initial_request_bytes,
                    capabilities_.effective_max_value_size)) {
                return tl::make_unexpected(retrieve_res.error());
            }
            retrieve_res = submit_retrieve(
                capabilities_.effective_max_value_size, "retrieve-fallback");
            if (!retrieve_res) {
                return tl::make_unexpected(retrieve_res.error());
            }
            used_fallback_request = true;
        }

        auto [dma_buffer, reported_size] = std::move(retrieve_res.value());
        const uint32_t prefix_limit =
            used_fallback_request ? capabilities_.effective_max_value_size
                                  : initial_request_bytes;
        const uint32_t actual_size = ResolveNvmeKvRetrievedValueSize(
            dma_buffer.get(), reported_size, prefix_limit, size_hint);
        if (actual_size != 0 && actual_size <= prefix_limit) {
            return std::string(dma_buffer.get(),
                               dma_buffer.get() + actual_size);
        }

        uint32_t required_size = reported_size;
        if (required_size <= prefix_limit) {
            required_size = ResolveNvmeKvObjectBlobSizeFromPrefix(
                dma_buffer.get(), prefix_limit);
        }
        if (required_size > prefix_limit &&
            required_size <= capabilities_.effective_max_value_size) {
            auto retry_res = submit_retrieve(required_size, "retrieve-retry");
            if (!retry_res) {
                return tl::make_unexpected(retry_res.error());
            }
            auto [retry_buffer, retry_reported_size] =
                std::move(retry_res.value());
            const uint32_t retry_actual_size = ResolveNvmeKvRetrievedValueSize(
                retry_buffer.get(), retry_reported_size, required_size,
                size_hint);
            if (retry_actual_size != 0 && retry_actual_size <= required_size) {
                return std::string(retry_buffer.get(),
                                   retry_buffer.get() + retry_actual_size);
            }
        }

        return tl::make_unexpected(
            required_size > capabilities_.effective_max_value_size
                ? ErrorCode::BUFFER_OVERFLOW
                : ErrorCode::FILE_READ_FAIL);
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey &key) override {
        nvme_passthru_cmd cmd{};
        BuildNvmeKvDeleteCommand(cmd, nsid_, key);

        auto result = Submit(cmd, true, "delete");
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        return {};
    }

    const Capabilities &GetCapabilities() const override {
        return capabilities_;
    }

   private:
    tl::expected<uint32_t, ErrorCode> Submit(nvme_passthru_cmd &cmd,
                                             bool is_write,
                                             const char *op_name) const {
        errno = 0;
        const int ret = ::ioctl(fd_, NVME_IOCTL_IO_CMD, &cmd);
        const int err = errno;
        if (ret < 0) {
            const ErrorCode mapped = MapNvmeKvTransportError(err, is_write);
            LOG(ERROR) << "[NvmeKvIoctlExecutor] ioctl failed"
                       << " op=" << op_name << " device=" << device_path_
                       << " errno=" << err << " strerror=" << strerror(err)
                       << " mapped_error=" << toString(mapped);
            return tl::make_unexpected(mapped);
        }
        if (ret > 0) {
            const ErrorCode mapped =
                MapNvmeKvStatus(static_cast<uint32_t>(ret), is_write);
            if (!IsNvmeKvControlFlowError(mapped)) {
                LOG(ERROR) << "[NvmeKvIoctlExecutor] ioctl returned NVMe status"
                           << " op=" << op_name << " device=" << device_path_
                           << " status=" << ret
                           << " mapped_error=" << toString(mapped);
            }
            return tl::make_unexpected(mapped);
        }
        return static_cast<uint32_t>(cmd.result);
    }

    std::string device_path_;
    uint32_t nsid_ = 1;
    Capabilities capabilities_;
    int fd_ = -1;
};

}  // namespace

NvmeKvExecutorResult CreateNvmeKvIoctlExecutor(
    std::string device_path, uint32_t nsid, uint32_t queue_depth,
    uint32_t runtime_transfer_limit) {
    auto caps = BuildNvmeKvCapabilities(kDefaultNvmeKvQueueDepth, queue_depth,
                                        runtime_transfer_limit);

    auto executor = std::make_unique<NvmeKvIoctlExecutor>(
        std::move(device_path), nsid, caps);
    auto init_res = executor->Init();
    if (!init_res) {
        return tl::make_unexpected(init_res.error());
    }
    return std::unique_ptr<NvmeKvCommandExecutor>(std::move(executor));
}

}  // namespace mooncake
