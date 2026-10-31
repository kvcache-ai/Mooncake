#include "device/accelerator_registry.h"

#include <cstring>

#include <gtest/gtest.h>

namespace mooncake::device {
namespace {

// Claims exactly one pointer as device memory.
class FakeAcceleratorDevice final : public AcceleratorDevice {
   public:
    explicit FakeAcceleratorDevice(const void* device_ptr)
        : device_ptr_(device_ptr) {}

    AcceleratorVendor Vendor() const override {
        return AcceleratorVendor::kNvidia;
    }

    bool Available(bool ensure = false) const override {
        (void)ensure;
        return true;
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        if (ptr != device_ptr_) {
            return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
        }
        return PointerInfo{.kind = MemoryKind::kDevice, .device_id = 0};
    }

    int32_t CurrentDeviceId() const override { return 0; }

    void SetContext(int32_t device_id) const override { (void)device_id; }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection direction) const override {
        (void)direction;
        std::memcpy(dst, src, size);
        return true;
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        (void)size;
        return PinnedHostBuffer();
    }

   private:
    const void* device_ptr_;
};

// The registry keeps one device per vendor. Registering a second device for
// the same vendor after RuntimeAccelerators() has been used must take effect;
// otherwise the cached runtime list keeps answering with the replaced device
// (seen on CUDA hosts, where the built-in CUDA device fills the cache first).
TEST(AcceleratorRegistryTest, ReplacingAVendorDeviceRefreshesRuntimeList) {
    static char first_byte = 'a';
    static char second_byte = 'b';
    static FakeAcceleratorDevice first(&first_byte);
    static FakeAcceleratorDevice second(&second_byte);

    AcceleratorDeviceRegistrar first_registrar(first);
    EXPECT_EQ(
        GetAcceleratorRegistry().RuntimeAccelerators().FindDeviceForPointer(
            &first_byte),
        &first);

    AcceleratorDeviceRegistrar second_registrar(second);
    auto runtime = GetAcceleratorRegistry().RuntimeAccelerators();
    EXPECT_EQ(runtime.FindDeviceForPointer(&second_byte), &second);
    EXPECT_EQ(runtime.FindDeviceForPointer(&first_byte), nullptr);
}

}  // namespace
}  // namespace mooncake::device
