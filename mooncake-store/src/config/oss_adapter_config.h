#pragma once

#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct OssAdapterConfig {
    std::string endpoint;
    std::string bucket;
    std::string region;
    std::string access_key_id;
    std::string access_key_secret;
    std::string security_token;
    bool path_style = false;
    bool anonymous = false;
    int max_connections = 64;
    int receive_buffer_size = 1024 * 1024;
    int upload_buffer_size = 1024 * 1024;

    static tl::expected<OssAdapterConfig, ErrorCode> FromEnvironment();
};

}  // namespace mooncake
