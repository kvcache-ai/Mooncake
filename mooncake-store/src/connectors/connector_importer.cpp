#include "connectors/connector_importer.h"

#include <glog/logging.h>

namespace mooncake {

ConnectorImporter::ConnectorImporter(std::shared_ptr<Client> client,
                                     std::unique_ptr<DataConnector> connector)
    : client_(std::move(client)), connector_(std::move(connector)) {}

tl::expected<void, ErrorCode> ConnectorImporter::ImportObject(
    const std::string& external_key, const std::string& store_key,
    const ReplicateConfig& config) {
    std::vector<uint8_t> buffer;
    auto download_result = connector_->DownloadObject(external_key, buffer);
    if (!download_result) {
        LOG(ERROR) << "Failed to download: " << download_result.error();
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }

    std::string key = store_key.empty() ? external_key : store_key;
    std::vector<Slice> slices;
    slices.push_back({buffer.data(), buffer.size()});
    auto result = client_->Put(key, slices, config);
    if (!result) {
        return tl::make_unexpected(result.error());
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
