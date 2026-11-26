#pragma once

#include <chrono>
#include <string>

namespace mooncake {

struct S3ClientConfig {
    std::string region;
    std::string s3_endpoint;
    std::string bucket_name;
    std::string access_key_id;
    std::string secret_access_key;
    bool use_virtual_addressing = true;
    bool use_https = true;
    std::string request_checksum_calculation;
    std::string response_checksum_validation;
    std::chrono::milliseconds connect_timeout{10000};
    std::chrono::milliseconds request_timeout{30000};

    // True when both static keys are set. Otherwise the client uses the
    // AWS SDK default credential provider chain (environment, profile,
    // web identity, container, and instance metadata providers).
    [[nodiscard]] bool UsesStaticCredentials() const {
        return !access_key_id.empty() && !secret_access_key.empty();
    }

    static S3ClientConfig FromEnvironment();
};

}  // namespace mooncake
