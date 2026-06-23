#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace mooncake {
namespace device {

enum class AcceleratorVendor {
    kHost,
    kNvidia,
    kMusa,
    kMaca,
    kHygon,
    kCorex,
    kHip,
    kAscend,
    kSunrise,
};

enum class MemoryKind {
    kHost,
    kDevice,
    kUnknown,
};

enum class CopyDirection {
    kHostToHost,
    kHostToDevice,
    kDeviceToHost,
    kDeviceToDevice,
    kAuto,
};

struct PointerInfo {
    MemoryKind kind = MemoryKind::kUnknown;
    int32_t device_id = -1;
};

struct PinnedHostBuffer;

class AcceleratorDevice {
   public:
    virtual ~AcceleratorDevice() = default;

    virtual AcceleratorVendor Vendor() const = 0;
    virtual bool Available(bool ensure = false) const = 0;
    virtual PointerInfo QueryPointer(const void* ptr) const = 0;
    virtual int32_t CurrentDeviceId() const = 0;
    virtual void SetContext(int32_t device_id) const = 0;
    virtual bool Copy(void* dst, const void* src, size_t size,
                      CopyDirection direction) const = 0;
    virtual PinnedHostBuffer AllocatePinnedHost(size_t size) const = 0;
};

class ProbeCachedAcceleratorDevice : public AcceleratorDevice {
   public:
    bool Available(bool ensure = false) const override;

   protected:
    virtual bool ProbeAvailable() const = 0;

   private:
    mutable std::atomic<uint8_t> available_state_{0};
};

}  // namespace device
}  // namespace mooncake
