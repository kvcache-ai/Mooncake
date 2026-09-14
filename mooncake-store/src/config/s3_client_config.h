#pragma once

#include <cstdint>
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
    int64_t connect_timeout_ms = 10000;
    int64_t request_timeout_ms = 30000;

    static S3ClientConfig FromEnvironment();
};

}  // namespace mooncake
