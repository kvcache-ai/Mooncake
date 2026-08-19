#include "nvme_kv_connector.h"

#include <cstdlib>
#include <filesystem>
#include <utility>

#include <glog/logging.h>

#include "nvme_kv_executor_util.h"
#include "storage_backend.h"

namespace mooncake {

#ifdef MOONCAKE_ENABLE_NVME_KV_TEST_STUB
std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvStubExecutor(
    std::filesystem::path storage_path);
#endif

namespace {

std::string GetEnvString(const char *name) {
    const char *value = std::getenv(name);
    return value == nullptr ? std::string() : std::string(value);
}

enum class RuntimeTransport {
    kAuto,
    kIoUring,
    kIoctl,
    kSpdk,
};

const char *RuntimeTransportName(RuntimeTransport transport) {
    switch (transport) {
        case RuntimeTransport::kAuto:
            return "auto";
        case RuntimeTransport::kIoUring:
            return "io_uring";
        case RuntimeTransport::kIoctl:
            return "ioctl";
        case RuntimeTransport::kSpdk:
            return "spdk";
    }
    return "unknown";
}

tl::expected<RuntimeTransport, ErrorCode> ParseRuntimeTransport() {
    const auto transport = GetEnvString("MOONCAKE_NVME_KV_TRANSPORT");
    if (transport.empty() || transport == "auto") {
        return RuntimeTransport::kAuto;
    }
    if (transport == "io_uring") {
        return RuntimeTransport::kIoUring;
    }
    if (transport == "ioctl") {
        return RuntimeTransport::kIoctl;
    }
    if (transport == "spdk" || transport == "spdk_nof" ||
        transport == "nof_spdk") {
        return RuntimeTransport::kSpdk;
    }
    LOG(ERROR) << "Unknown MOONCAKE_NVME_KV_TRANSPORT: " << transport;
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

}  // namespace

NvmeKvConnector::NvmeKvConnector(const FileStorageConfig &config)
    : storage_path_(
          (std::filesystem::path(config.storage_filepath) / "nvme_kv_blobs")
              .string()) {}

tl::expected<void, ErrorCode> NvmeKvConnector::Init() {
    if (executor_ != nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
#ifdef MOONCAKE_ENABLE_NVME_KV_TEST_STUB
    if (GetEnvString("MOONCAKE_NVME_KV_DRIVER") == "stub") {
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
    const auto device_path = GetEnvString("MOONCAKE_NVME_KV_DEVICE_PATH");
    if (device_path.empty()) {
        LOG(ERROR) << "MOONCAKE_NVME_KV_DEVICE_PATH must not be empty";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const uint32_t nsid = ParseNvmeKvU32EnvOr("MOONCAKE_NVME_KV_NSID", 1);
    const uint32_t queue_depth = ParseNvmeKvU32EnvOr(
        "MOONCAKE_NVME_KV_QUEUE_DEPTH", kDefaultNvmeKvQueueDepth);
    const uint32_t runtime_transfer_limit =
        ParseNvmeKvU32EnvOr("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT",
                            kDefaultNvmeKvRuntimeTransferLimit);
    auto transport = ParseRuntimeTransport();
    if (!transport) {
        return tl::make_unexpected(transport.error());
    }

    const auto create_executor =
        [&](RuntimeTransport selected_transport) -> NvmeKvExecutorResult {
        switch (selected_transport) {
            case RuntimeTransport::kIoUring:
                return CreateNvmeKvIoUringExecutor(
                    device_path, nsid, queue_depth, runtime_transfer_limit);
            case RuntimeTransport::kIoctl:
                return CreateNvmeKvIoctlExecutor(device_path, nsid, queue_depth,
                                                 runtime_transfer_limit);
            case RuntimeTransport::kSpdk:
#if defined(USE_NOF) && defined(MOONCAKE_HAVE_SPDK_NVME_KV)
                return CreateNvmeKvSpdkExecutor(device_path, nsid, queue_depth,
                                                runtime_transfer_limit);
#else
                LOG(ERROR) << "SPDK NVMe KV executor requires USE_NOF and SPDK "
                           << "26.05+ typed KV API support";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
#endif
            case RuntimeTransport::kAuto:
                break;
        }
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    };

    NvmeKvExecutorResult executor_result =
        tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (transport.value() == RuntimeTransport::kAuto) {
#ifdef MOONCAKE_HAVE_NVME_URING_CMD
        executor_result = create_executor(RuntimeTransport::kIoUring);
        if (!executor_result) {
            LOG(WARNING) << "NVMe KV io_uring init failed, falling back to "
                            "ioctl: "
                         << toString(executor_result.error());
        }
#endif
        if (!executor_result) {
            executor_result = create_executor(RuntimeTransport::kIoctl);
        }
    } else {
        executor_result = create_executor(transport.value());
    }

    if (!executor_result) {
        LOG(ERROR) << "Failed to initialize NVMe KV "
                   << RuntimeTransportName(transport.value()) << " executor";
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
