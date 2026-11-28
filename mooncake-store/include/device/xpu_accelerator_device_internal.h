#pragma once

// Internals of the Intel XPU accelerator device that unit tests reach
// directly: the owner-device lookup and a copy fault-injection hook.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace mooncake {
namespace device {
namespace xpu {

// DeviceIndexOf results that are not a queue index.
inline constexpr int kNotDeviceMemory = -1;
// USM device memory owned by a GPU the runtime did not enumerate (for example
// one without usm_device_allocations). No queue is known to be able to copy
// it, so copies touching it fail instead of running on another device.
inline constexpr int kUnsupportedDevice = -2;

// Index of `owner` in `devices`, the GPUs that have a queue, or
// kUnsupportedDevice when it is not one of them.
template <typename Device>
int OwnerQueueIndex(const Device& owner, const std::vector<Device>& devices) {
    for (size_t i = 0; i < devices.size(); ++i) {
        if (devices[i] == owner) return static_cast<int>(i);
    }
    return kUnsupportedDevice;
}

// Remembers which keys were already reported, so a warning is logged once
// per key. Only reached on an error path, so the mutex is not hot.
template <typename Key, typename Hash = std::hash<Key>>
class OncePerKey {
   public:
    // True the first time `key` is seen.
    bool First(const Key& key) {
        std::lock_guard<std::mutex> lock(mu_);
        return seen_.insert(key).second;
    }

   private:
    std::mutex mu_;
    std::unordered_set<Key, Hash> seen_;
};

namespace testing {

enum class CopyFault {
    kNone,
    kSyclError,  // the submission throws sycl::exception
    kBadAlloc,   // the submission throws std::bad_alloc
};

// Test only: make the copy submitted `skip` submissions from now, on any
// thread, throw `fault` instead of starting. kNone cancels a pending fault.
void InjectCopyFault(CopyFault fault, int64_t skip = 0);

}  // namespace testing
}  // namespace xpu
}  // namespace device
}  // namespace mooncake
