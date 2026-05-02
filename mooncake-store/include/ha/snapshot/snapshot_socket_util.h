#pragma once

// Shared I/O helpers for the snapshot socketpair IPC protocol.
// Used by MasterService::PersistStateViaChildSerialize() and
// MasterService::PersistStateViaParentUpload().

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <unistd.h>

namespace mooncake {
namespace snapshot_socket {

/// Read exactly @p len bytes from @p fd.  Returns false on EOF or error.
inline bool ReadExact(int fd, void* buf, size_t len) {
    size_t total = 0;
    auto* ptr = static_cast<char*>(buf);
    while (total < len) {
        ssize_t n = ::read(fd, ptr + total, len - total);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;  // EOF
        total += static_cast<size_t>(n);
    }
    return true;
}

/// Write exactly @p len bytes to @p fd.  Returns false on error.
inline bool WriteExact(int fd, const void* buf, size_t len) {
    size_t total = 0;
    const auto* ptr = static_cast<const char*>(buf);
    while (total < len) {
        ssize_t n = ::write(fd, ptr + total, len - total);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        total += static_cast<size_t>(n);
    }
    return true;
}

}  // namespace snapshot_socket
}  // namespace mooncake
