#pragma once

#include <fmt/format.h>
#include <unistd.h>

#include <cstring>

namespace mooncake {

// Use pipe proxy for child process logging to avoid glog lock deadlock after
// fork
inline thread_local int g_snapshot_log_pipe_fd = -1;

// Write log message to pipe (async-signal-safe, no locks)
inline void SnapshotLogWrite(const char* level, const std::string& msg) {
    if (g_snapshot_log_pipe_fd < 0) return;
    std::string line = std::string("[") + level + "] " + msg + "\n";
    // write() is async-signal-safe, won't deadlock after fork
    ::write(g_snapshot_log_pipe_fd, line.c_str(), line.size());
}

// Macros for child process logging - writes to pipe instead of glog
#define SNAP_LOG_INFO(fmt_str, ...) \
    ::mooncake::SnapshotLogWrite("INFO", fmt::format(fmt_str, ##__VA_ARGS__))
#define SNAP_LOG_ERROR(fmt_str, ...) \
    ::mooncake::SnapshotLogWrite("ERROR", fmt::format(fmt_str, ##__VA_ARGS__))
#define SNAP_LOG_WARN(fmt_str, ...) \
    ::mooncake::SnapshotLogWrite("WARN", fmt::format(fmt_str, ##__VA_ARGS__))
#define SNAP_LOG_DEBUG(fmt_str, ...) \
    ::mooncake::SnapshotLogWrite("DEBUG", fmt::format(fmt_str, ##__VA_ARGS__))

}  // namespace mooncake
