#pragma once

#include <cstdint>
#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

class KvcsDriver;

// PR1 binds one Low-Level client to one validated EFC mountpoint.
tl::expected<std::unique_ptr<KvcsDriver>, ErrorCode> CreateKvcsLowLevelDriver(
    uint32_t mountpoint_index);

}  // namespace mooncake
