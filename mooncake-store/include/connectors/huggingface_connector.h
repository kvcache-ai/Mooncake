#pragma once

#include <utility>

#include "connectors/data_connector.h"
#include <curl/curl.h>

namespace mooncake {

class HuggingFaceConnector : public DataConnector {
   public:
    HuggingFaceConnector();
    ~HuggingFaceConnector() override;

    tl::expected<void, std::string> ListObjects(
        const std::string& prefix,
        std::vector<ExternalObject>& objects) override;

    tl::expected<void, std::string> DownloadObject(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    std::string GetConnectionInfo() const override;

   private:
    std::string endpoint_;
    std::string token_;
    CURL* curl_;

    std::pair<std::string, std::string> ParseKey(const std::string& key);
    tl::expected<void, std::string> HttpGet(const std::string& url,
                                            std::vector<uint8_t>& buffer);
};

}  // namespace mooncake
