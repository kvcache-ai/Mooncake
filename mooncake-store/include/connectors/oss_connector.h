#pragma once

#ifdef HAVE_AWS_SDK

#include "connectors/data_connector.h"
#include "utils/s3_helper.h"

namespace mooncake {

class OSSConnector : public DataConnector {
   public:
    OSSConnector();
    ~OSSConnector() override = default;

    tl::expected<void, std::string> ListObjects(
        const std::string& prefix,
        std::vector<ExternalObject>& objects) override;

    tl::expected<void, std::string> DownloadObject(
        const std::string& key, std::vector<uint8_t>& buffer) override;

    std::string GetConnectionInfo() const override;

   private:
    std::unique_ptr<S3Helper> s3_helper_;
};

}  // namespace mooncake

#endif  // HAVE_AWS_SDK
