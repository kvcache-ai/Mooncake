#include "device/runtime_accelerator.h"

#include <cstring>

#include <gtest/gtest.h>

namespace mooncake::device {
namespace {

class FakeAcceleratorDevice final : public AcceleratorDevice {
   public:
    FakeAcceleratorDevice(AcceleratorVendor vendor, const void* device_ptr,
                          int32_t device_id)
        : vendor_(vendor), device_ptr_(device_ptr), device_id_(device_id) {}

    AcceleratorVendor Vendor() const override { return vendor_; }

    bool Available(bool ensure = false) const override {
        (void)ensure;
        return true;
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        if (ptr != device_ptr_) {
            return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
        }
        return PointerInfo{.kind = MemoryKind::kDevice,
                           .device_id = device_id_};
    }

    int32_t CurrentDeviceId() const override { return current_device_id_; }

    void SetContext(int32_t device_id) const override {
        current_device_id_ = device_id;
    }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection direction) const override {
        last_direction_ = direction;
        std::memcpy(dst, src, size);
        return copy_succeeds_;
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        (void)size;
        return PinnedHostBuffer();
    }

    void set_copy_succeeds(bool succeeds) { copy_succeeds_ = succeeds; }
    int32_t current_device_id() const { return current_device_id_; }
    CopyDirection last_direction() const { return last_direction_; }

   private:
    AcceleratorVendor vendor_;
    const void* device_ptr_;
    int32_t device_id_;
    mutable int32_t current_device_id_ = -1;
    mutable CopyDirection last_direction_ = CopyDirection::kAuto;
    mutable bool copy_succeeds_ = true;
};

TEST(RuntimeAcceleratorTest, FindDeviceForPointerReturnsMatchingDevice) {
    char device_byte = 'd';
    FakeAcceleratorDevice device(AcceleratorVendor::kNvidia, &device_byte, 7);
    RuntimeAccelerator runtime_accelerator({&device});

    PointerInfo info;
    auto* found =
        runtime_accelerator.FindDeviceForPointer(&device_byte, &info);

    EXPECT_EQ(found, &device);
    EXPECT_EQ(info.kind, MemoryKind::kDevice);
    EXPECT_EQ(info.device_id, 7);
}

TEST(RuntimeAcceleratorTest, IsDevicePointerReturnsFalseForHostPointer) {
    char device_byte = 'd';
    char host_byte = 'h';
    FakeAcceleratorDevice device(AcceleratorVendor::kNvidia, &device_byte, 0);
    RuntimeAccelerator runtime_accelerator({&device});

    EXPECT_FALSE(runtime_accelerator.IsDevicePointer(&host_byte));
}

TEST(RuntimeAcceleratorTest, CopyToHostUsesDeviceToHostCopy) {
    char src[] = "abc";
    char dst[sizeof(src)] = {};
    FakeAcceleratorDevice device(AcceleratorVendor::kNvidia, src, 3);
    RuntimeAccelerator runtime_accelerator({&device});

    EXPECT_TRUE(runtime_accelerator.CopyToHost(dst, src, sizeof(src)));

    EXPECT_STREQ(dst, src);
    EXPECT_EQ(device.current_device_id(), 3);
    EXPECT_EQ(device.last_direction(), CopyDirection::kDeviceToHost);
}

TEST(RuntimeAcceleratorTest, CopyFromHostUsesHostToDeviceCopy) {
    char src[] = "abc";
    char dst[sizeof(src)] = {};
    FakeAcceleratorDevice device(AcceleratorVendor::kNvidia, dst, 4);
    RuntimeAccelerator runtime_accelerator({&device});

    EXPECT_TRUE(runtime_accelerator.CopyFromHost(dst, src, sizeof(src)));

    EXPECT_STREQ(dst, src);
    EXPECT_EQ(device.current_device_id(), 4);
    EXPECT_EQ(device.last_direction(), CopyDirection::kHostToDevice);
}

TEST(RuntimeAcceleratorTest, CopyMaybeAcceleratorUsesMemcpyForHostPointers) {
    char src[] = "abc";
    char dst[sizeof(src)] = {};
    RuntimeAccelerator runtime_accelerator;

    EXPECT_TRUE(runtime_accelerator.CopyMaybeAccelerator(dst, src,
                                                        sizeof(src)));
    EXPECT_STREQ(dst, src);
}

TEST(RuntimeAcceleratorTest, CopyMaybeAcceleratorRejectsDifferentDevices) {
    char src[] = "abc";
    char dst[sizeof(src)] = {};
    FakeAcceleratorDevice src_device(AcceleratorVendor::kNvidia, src, 0);
    FakeAcceleratorDevice dst_device(AcceleratorVendor::kMusa, dst, 1);
    RuntimeAccelerator runtime_accelerator({&src_device, &dst_device});

    EXPECT_FALSE(runtime_accelerator.CopyMaybeAccelerator(dst, src,
                                                         sizeof(src)));
}

TEST(RuntimeAcceleratorTest, CopyMaybeAcceleratorUsesAutoForSingleDevice) {
    char src[] = "abc";
    char dst[sizeof(src)] = {};
    FakeAcceleratorDevice device(AcceleratorVendor::kNvidia, src, 5);
    RuntimeAccelerator runtime_accelerator({&device});

    EXPECT_TRUE(runtime_accelerator.CopyMaybeAccelerator(dst, src,
                                                        sizeof(src)));

    EXPECT_STREQ(dst, src);
    EXPECT_EQ(device.current_device_id(), 5);
    EXPECT_EQ(device.last_direction(), CopyDirection::kAuto);
}

}  // namespace
}  // namespace mooncake::device
