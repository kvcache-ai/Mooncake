#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace mooncake {

uint32_t Crc32c(const uint8_t* data, size_t len);
uint32_t Crc32c(const std::vector<uint8_t>& data);

}  // namespace mooncake
