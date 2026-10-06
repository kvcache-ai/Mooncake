#pragma once

#include <cstdint>
#include <optional>

namespace mooncake {

std::optional<uint32_t> TryParseNvmeKvU32(const char* value);

}  // namespace mooncake
