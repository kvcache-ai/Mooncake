#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

enum class ConnectorType { OSS = 0, HUGGINGFACE = 1, REDIS = 2 };

struct ExternalObject {
    std::string key;
    size_t size{0};
    std::string etag;
};

class DataConnector {
   public:
    virtual ~DataConnector() = default;

    virtual tl::expected<void, std::string> ListObjects(
        const std::string& prefix, std::vector<ExternalObject>& objects) = 0;

    virtual tl::expected<void, std::string> DownloadObject(
        const std::string& key, std::vector<uint8_t>& buffer) = 0;

    virtual std::string GetConnectionInfo() const = 0;

    static std::unique_ptr<DataConnector> Create(ConnectorType type);
};

}  // namespace mooncake
