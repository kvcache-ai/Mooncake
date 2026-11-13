#pragma once

#include <sys/uio.h>

#include <algorithm>
#include <cstddef>
#include <vector>

// Helpers for continuing a short pwritev/preadv on a copy of the caller's
// iovecs. Internal to PosixFile; in a header so that they can be unit tested.
namespace mooncake::detail {

// Advances `first` past empty iovecs; false once all of them are done.
inline bool SkipEmptyIovecs(const std::vector<iovec>& iovs, size_t& first) {
    while (first < iovs.size() && iovs[first].iov_len == 0) ++first;
    return first < iovs.size();
}

// Drops the first `bytes` transferred bytes from iovs[first..].
inline void ConsumeIovecs(std::vector<iovec>& iovs, size_t& first,
                          size_t bytes) {
    while (bytes > 0 && first < iovs.size()) {
        iovec& v = iovs[first];
        const size_t n = std::min(bytes, v.iov_len);
        v.iov_base = static_cast<char*>(v.iov_base) + n;
        v.iov_len -= n;
        bytes -= n;
        if (v.iov_len == 0) ++first;
    }
}

}  // namespace mooncake::detail
