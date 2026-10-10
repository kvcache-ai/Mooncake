#pragma once

#include <cstdint>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct KvcsEfcTarget {
    std::string id;
    uint32_t mountpoint_index = 1;
};

// This stage accepts exactly one shared KVCacheStore mountpoint.
tl::expected<KvcsEfcTarget, ErrorCode> LoadKvcsEfcTarget(
    const std::string& config_path);

}  // namespace mooncake
