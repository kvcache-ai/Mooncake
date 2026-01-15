#pragma once

#include <memory>
#include <filesystem>

namespace mooncake {

// Fork log file should be isolated and instantiated separately, otherwise
// underlying time-related functions may deadlock
#define SNAP_LOG_INFO(...) LOG(INFO) << fmt::format(__VA_ARGS__)
#define SNAP_LOG_ERROR(...) LOG(ERROR) << fmt::format(__VA_ARGS__)
#define SNAP_LOG_WARN(...) LOG(WARNING) << fmt::format(__VA_ARGS__)
#define SNAP_LOG_DEBUG(...) LOG(DEBUG) << fmt::format(__VA_ARGS__)

}  // namespace mooncake
