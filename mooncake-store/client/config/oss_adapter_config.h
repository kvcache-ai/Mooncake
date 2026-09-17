#pragma once

#include <string>

#include <ylt/util/tl/expected.hpp>

#include "common/types.h"

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

    static tl::expected<OssAdapterConfig, ErrorCode> FromEnvironment();
};

}  // namespace mooncake
