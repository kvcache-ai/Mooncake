#include "nvme_kv_connector.h"

#include <utility>

#include "nvme_kv_executor.h"

namespace mooncake {

NvmeKvConnector::NvmeKvConnector(std::string device_id, std::string device_path,
                                 uint32_t nsid)
    : device_id_(std::move(device_id)),
      device_path_(std::move(device_path)),
      nsid_(nsid) {}

tl::expected<void, ErrorCode> NvmeKvConnector::Init() {
    if (executor_ != nullptr || device_id_.empty() || device_path_.empty()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return InitExecutor();
}

tl::expected<void, ErrorCode> NvmeKvConnector::InitExecutor() {
    tl::expected<Capabilities, ErrorCode> capabilities =
        tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto executor =
        CreateNvmeKvIoctlExecutor(device_path_, nsid_, capabilities);
    if (executor == nullptr || !capabilities) {
        return tl::make_unexpected(capabilities ? ErrorCode::INTERNAL_ERROR
                                                : capabilities.error());
    }

    executor_ = std::move(executor);
    return {};
}

tl::expected<void, ErrorCode> NvmeKvConnector::Store(const PhysicalKey& key,
                                                     std::string value) {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Store(key, std::move(value));
}

tl::expected<std::string, ErrorCode> NvmeKvConnector::Retrieve(
    const PhysicalKey& key) const {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Retrieve(key);
}

tl::expected<bool, ErrorCode> NvmeKvConnector::Exists(
    const PhysicalKey& key) const {
    if (executor_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return executor_->Exists(key);
}

const NvmeKvConnector::Capabilities& NvmeKvConnector::GetCapabilities() const {
    static const Capabilities kDefaultCapabilities{};
    if (executor_ == nullptr) {
        return kDefaultCapabilities;
    }
    return executor_->GetCapabilities();
}

}  // namespace mooncake
