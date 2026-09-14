#include "nvme_kv_connector.h"

#include <cstdlib>
#include <filesystem>
#include <string_view>
#include <utility>

#include <glog/logging.h>

#include "config/nvme_kv_connector_config.h"
#include "nvme_kv_executor_util.h"
#include "storage_backend.h"

namespace mooncake {

#ifdef MOONCAKE_ENABLE_NVME_KV_TEST_STUB
std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvStubExecutor(
    std::filesystem::path storage_path);
#endif

NvmeKvConnector::NvmeKvConnector(const FileStorageConfig &config)
    : storage_path_(
          (std::filesystem::path(config.storage_filepath) / "nvme_kv_blobs")
              .string()) {}

tl::expected<void, ErrorCode> NvmeKvConnector::Init() {
    if (executor_ != nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
#ifdef MOONCAKE_ENABLE_NVME_KV_TEST_STUB
    const char *driver = std::getenv("MOONCAKE_NVME_KV_DRIVER");
    if (driver != nullptr && std::string_view(driver) == "stub") {
        std::error_code ec;
        std::filesystem::create_directories(storage_path_, ec);
        if (ec) {
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        executor_ =
            CreateNvmeKvStubExecutor(std::filesystem::path(storage_path_));
        return {};
    }
#endif

    return InitRealExecutor();
}

tl::expected<void, ErrorCode> NvmeKvConnector::InitRealExecutor() {
    auto config = NvmeKvConnectorConfig::FromEnvironment();
    if (!config.has_value()) {
        return tl::make_unexpected(config.error());
    }

    const auto create_executor =
        [&](NvmeKvTransport selected_transport) -> NvmeKvExecutorResult {
        switch (selected_transport) {
            case NvmeKvTransport::kIoUring:
                return CreateNvmeKvIoUringExecutor(
                    config->device_path, config->nsid, config->queue_depth,
                    config->runtime_transfer_limit);
            case NvmeKvTransport::kIoctl:
                return CreateNvmeKvIoctlExecutor(
                    config->device_path, config->nsid, config->queue_depth,
                    config->runtime_transfer_limit);
            case NvmeKvTransport::kAuto:
                break;
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    };

    NvmeKvExecutorResult executor_result =
        tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (config->transport == NvmeKvTransport::kAuto) {
#ifdef MOONCAKE_HAVE_NVME_URING_CMD
        executor_result = create_executor(NvmeKvTransport::kIoUring);
        if (!executor_result) {
            LOG(WARNING) << "NVMe KV io_uring init failed, falling back to "
                            "ioctl: "
                         << toString(executor_result.error());
        }
#endif
        if (!executor_result) {
            executor_result = create_executor(NvmeKvTransport::kIoctl);
        }
    } else {
        executor_result = create_executor(config->transport);
    }

    if (!executor_result) {
        LOG(ERROR) << "Failed to initialize NVMe KV "
                   << NvmeKvTransportName(config->transport) << " executor";
        return tl::make_unexpected(executor_result.error());
    }
    executor_ = std::move(executor_result.value());
    return {};
}

tl::expected<void, ErrorCode> NvmeKvConnector::Store(const PhysicalKey &key,
                                                     std::string value) {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Store(key, std::move(value));
}

void NvmeKvConnector::StoreBatch(
    std::vector<NvmeKvCommandExecutor::StoreRequest> &requests) {
    if (executor_ == nullptr) {
        for (auto &request : requests) {
            request.result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return;
    }
    executor_->StoreBatch(requests);
}

tl::expected<std::string, ErrorCode> NvmeKvConnector::Retrieve(
    const PhysicalKey &key, uint32_t size_hint) const {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Retrieve(key, size_hint);
}

void NvmeKvConnector::RetrieveBufferBatch(
    std::vector<NvmeKvCommandExecutor::RetrieveBufferRequest> &requests) const {
    if (executor_ == nullptr) {
        for (auto &request : requests) {
            request.result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return;
    }
    executor_->RetrieveBufferBatch(requests);
}

void NvmeKvConnector::RetrieveIntoBatch(
    std::vector<NvmeKvCommandExecutor::RetrieveIntoRequest> &requests) const {
    if (executor_ == nullptr) {
        for (auto &request : requests) {
            request.result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return;
    }
    executor_->RetrieveIntoBatch(requests);
}

tl::expected<void, ErrorCode> NvmeKvConnector::Delete(const PhysicalKey &key) {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Delete(key);
}

const NvmeKvConnector::Capabilities &NvmeKvConnector::GetCapabilities() const {
    static const Capabilities kDefaultCapabilities{};
    return executor_ == nullptr ? kDefaultCapabilities
                                : executor_->GetCapabilities();
}

}  // namespace mooncake
