#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

class KvcsDriver;

// Constructs target-bound drivers that share one Low-Level SDK client. The
// mountpoint remains a per-batch option, so one client can reuse its worker and
// ring resources across all configured EFC filesystems.
tl::expected<std::vector<std::unique_ptr<KvcsDriver>>, ErrorCode>
CreateKvcsLowLevelDrivers(std::span<const uint32_t> mountpoint_indices);

}  // namespace mooncake
