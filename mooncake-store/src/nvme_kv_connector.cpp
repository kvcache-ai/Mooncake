#include "nvme_kv_connector.h"

#include <utility>

namespace mooncake {

NvmeKvConnector::NvmeKvConnector(
    std::string device_id, std::unique_ptr<NvmeKvCommandExecutor> executor)
    : device_id_(std::move(device_id)), executor_(std::move(executor)) {}

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
