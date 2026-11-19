#pragma once

#include <functional>

namespace mooncake {
namespace uring_testing {

// Test-only hooks into UringFile's thread-local rings (posix_file_test).

// Replaces the calling thread's ring the way I/O error recovery does.
void reset_thread_ring();

// Called when a thread-local ring is destroyed, while that ring can still be
// reached by a concurrent unregister. The hook must not call into UringFile.
void set_ring_exit_hook(std::function<void()> hook);

}  // namespace uring_testing
}  // namespace mooncake
