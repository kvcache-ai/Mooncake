#pragma once

#include <optional>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry);

}  // namespace mooncake
