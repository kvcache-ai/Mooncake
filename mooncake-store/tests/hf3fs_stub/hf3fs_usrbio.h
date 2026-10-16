#pragma once

// Resource-manager-only USRBIO test double. No 3FS mount or client is required.
#include <atomic>
#include <cstddef>
#include <thread>

struct hf3fs_iov {};
struct hf3fs_ior {};

namespace hf3fs_test {
inline std::atomic<int> live_iovs{0};
inline std::atomic<int> live_rings{0};
inline std::atomic<int> calls{0};
inline std::atomic<int> fail_call{0};
inline std::atomic<bool> block_create{false};
inline std::atomic<bool> create_entered{false};
inline bool Fail() { return ++calls == fail_call.load(); }
}  // namespace hf3fs_test

inline int hf3fs_iovcreate(hf3fs_iov*, const char*, size_t, int, int) {
    if (hf3fs_test::block_create) {
        hf3fs_test::create_entered = true;
        while (hf3fs_test::block_create) std::this_thread::yield();
    }
    if (hf3fs_test::Fail()) return -1;
    ++hf3fs_test::live_iovs;
    return 0;
}
inline int hf3fs_iorcreate4(hf3fs_ior*, const char*, size_t, bool, size_t, int,
                            int, int) {
    if (hf3fs_test::Fail()) return -1;
    ++hf3fs_test::live_rings;
    return 0;
}
inline void hf3fs_iovdestroy(hf3fs_iov*) { --hf3fs_test::live_iovs; }
inline void hf3fs_iordestroy(hf3fs_ior*) { --hf3fs_test::live_rings; }
