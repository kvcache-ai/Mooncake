#pragma once

#include <memory>
#include <filesystem>

namespace mooncake {

// fork 日志文件最好隔离单独实例化，否则低层时间相关函数有概率会存在死锁
#define SNAP_LOG_INFO(...) LOG(INFO) << fmt::format(__VA_ARGS__)
#define SNAP_LOG_ERROR(...) LOG(ERROR) << fmt::format(__VA_ARGS__)
#define SNAP_LOG_WARN(...) LOG(WARNING) << fmt::format(__VA_ARGS__)
#define SNAP_LOG_DEBUG(...) LOG(DEBUG) << fmt::format(__VA_ARGS__)

}  // namespace mooncake
