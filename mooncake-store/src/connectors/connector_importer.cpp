#include "connectors/connector_importer.h"

#include <cstring>

#include "client_buffer.hpp"
#include <glog/logging.h>

namespace mooncake {

ConnectorImporter::ConnectorImporter(std::shared_ptr<Client> client,
                                     std::unique_ptr<DataConnector> connector)
    : client_(std::move(client)), connector_(std::move(connector)) {}

tl::expected<void, ErrorCode> ConnectorImporter::ImportObject(
    const std::string& external_key, const std::string& store_key,
    const ReplicateConfig& config) {
    if (!client_ || !connector_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<uint8_t> buffer;
    auto download_result = connector_->DownloadObject(external_key, buffer);
    if (!download_result) {
        LOG(ERROR) << "Failed to download: " << download_result.error();
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    if (buffer.empty()) {
        LOG(ERROR) << "Downloaded object is empty: " << external_key;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::string key = store_key.empty() ? external_key : store_key;
    auto allocator = ClientBufferAllocator::create(buffer.size());
    if (!allocator) {
        LOG(ERROR) << "Failed to allocate local buffer for connector import";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    auto register_result = client_->RegisterLocalMemory(
        allocator->getBase(), allocator->size(), "cpu:0", false, false);
    if (!register_result) {
        LOG(ERROR) << "Failed to register local memory: "
                   << register_result.error();
        return tl::make_unexpected(register_result.error());
    }

    std::memcpy(allocator->getBase(), buffer.data(), buffer.size());
    auto slices = split_into_slices(allocator->getBase(), buffer.size());

    auto put_result = client_->Put(key, slices, config);
    auto unregister_result =
        client_->unregisterLocalMemory(allocator->getBase(), false);
    if (!unregister_result) {
        LOG(WARNING) << "Failed to unregister local memory after import: "
                     << unregister_result.error();
    }

    if (!put_result) {
        return tl::make_unexpected(put_result.error());
    }

    return {};
}

tl::expected<size_t, ErrorCode> ConnectorImporter::ImportByPrefix(
    const std::string& prefix, const ReplicateConfig& config) {
    std::vector<ExternalObject> objects;
    auto list_result = connector_->ListObjects(prefix, objects);
    if (!list_result) {
        LOG(ERROR) << "Failed to list objects: " << list_result.error();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t success_count = 0;
    for (const auto& obj : objects) {
        auto result = ImportObject(obj.key, obj.key, config);
        if (result) {
            success_count++;
        }
    }
    return success_count;
}

}  // namespace mooncake
