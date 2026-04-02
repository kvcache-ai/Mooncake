#ifdef HAVE_AWS_SDK

#include "connectors/oss_connector.h"

#include <cstdlib>
#include <glog/logging.h>

namespace mooncake {

OSSConnector::OSSConnector() {
    const char* endpoint = std::getenv("MOONCAKE_OSS_ENDPOINT");
    const char* bucket = std::getenv("MOONCAKE_OSS_BUCKET");
    const char* access_key = std::getenv("MOONCAKE_OSS_ACCESS_KEY_ID");
    const char* secret_key = std::getenv("MOONCAKE_OSS_SECRET_ACCESS_KEY");
    const char* region = std::getenv("MOONCAKE_OSS_REGION");

    if (!endpoint || !bucket || !access_key || !secret_key) {
        throw std::runtime_error(
            "OSS configuration incomplete. Required: MOONCAKE_OSS_ENDPOINT, "
            "MOONCAKE_OSS_BUCKET, MOONCAKE_OSS_ACCESS_KEY_ID, "
            "MOONCAKE_OSS_SECRET_ACCESS_KEY");
    }

    std::string oss_region = region ? region : "oss-cn-hangzhou";
    s3_helper_ = std::make_unique<S3Helper>(endpoint, bucket, oss_region);
    LOG(INFO) << "OSSConnector initialized: " << GetConnectionInfo();
}

tl::expected<void, std::string> OSSConnector::ListObjects(
    const std::string& prefix, std::vector<ExternalObject>& objects) {
    std::vector<std::string> keys;
    auto result = s3_helper_->ListObjectsWithPrefix(prefix, keys);
    if (!result) {
        return result;
    }

    objects.clear();
    for (const auto& key : keys) {
        objects.push_back({key, 0, ""});
    }
    return {};
}

tl::expected<void, std::string> OSSConnector::DownloadObject(
    const std::string& key, std::vector<uint8_t>& buffer) {
    return s3_helper_->DownloadBufferMultipart(key, buffer);
}

std::string OSSConnector::GetConnectionInfo() const {
    return s3_helper_->GetConnectionInfo();
}

}  // namespace mooncake

#endif  // HAVE_AWS_SDK
