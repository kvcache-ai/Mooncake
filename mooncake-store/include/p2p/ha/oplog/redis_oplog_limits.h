#pragma once

#include <cstddef>

namespace mooncake {

inline constexpr size_t kDefaultRedisOpLogWriteBatchSize = 64;
inline constexpr size_t kMaxRedisOpLogWriteBatchSize = 1024;

}  // namespace mooncake
