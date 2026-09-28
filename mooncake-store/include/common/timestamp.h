#pragma once

#include <chrono>
#include <cstdint>

namespace mooncake {

inline int64_t time_gen() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace mooncake
