#include "device/accelerator_device.h"

namespace mooncake {
namespace device {

namespace {

constexpr uint8_t kAvailableProbed = 1 << 0;
constexpr uint8_t kAvailable = 1 << 1;

}  // namespace

bool ProbeCachedAcceleratorDevice::Available(bool ensure) const {
    uint8_t state = available_state_.load(std::memory_order_acquire);
    if (!ensure) {
        return (state & kAvailableProbed) ? (state & kAvailable) : true;
    }
    const bool available = ProbeAvailable();
    state = kAvailableProbed | (available ? kAvailable : 0);
    available_state_.store(state, std::memory_order_release);
    return available;
}

}  // namespace device
}  // namespace mooncake
