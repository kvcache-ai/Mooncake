#pragma once

#include <cnrt.h>
#include <gtest/gtest.h>

#include <cstdlib>

#include "device/accelerator_registry.h"

namespace mooncake::testing {

class MluTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto result = cnrtGetDeviceCount(&device_count_);
        if (result != cnrtSuccess || device_count_ == 0) {
            if (std::getenv("MC_STORE_TEST_REQUIRE_MLU")) {
                FAIL() << "MLU required but unavailable: " << result;
            }
            GTEST_SKIP() << "No MLU device available";
        }
        accelerator_ = device::GetAcceleratorRegistry().GetDevice(
            device::AcceleratorVendor::kMlu);
        ASSERT_NE(accelerator_, nullptr);
        ASSERT_TRUE(accelerator_->Available(true));
        ASSERT_EQ(cnrtSetDevice(0), cnrtSuccess);
    }

    unsigned int device_count_ = 0;
    const device::AcceleratorDevice* accelerator_ = nullptr;
};

// Keep test allocations alive until Store registrations have been removed.
class MluTestBuffer {
   public:
    ~MluTestBuffer() { reset(); }
    MluTestBuffer() = default;
    MluTestBuffer(const MluTestBuffer&) = delete;
    MluTestBuffer& operator=(const MluTestBuffer&) = delete;

    cnrtRet_t allocate(size_t size, int device = 0) {
        reset();
        device_ = device;
        auto ret = cnrtSetDevice(device);
        return ret == cnrtSuccess ? cnrtMalloc(&ptr_, size) : ret;
    }
    void reset() {
        if (!ptr_) return;
        EXPECT_EQ(cnrtSetDevice(device_), cnrtSuccess);
        EXPECT_EQ(cnrtFree(ptr_), cnrtSuccess);
        ptr_ = nullptr;
    }
    void* data(size_t offset = 0) const {
        return ptr_ ? static_cast<char*>(ptr_) + offset : nullptr;
    }

   private:
    void* ptr_ = nullptr;
    int device_ = 0;
};

}  // namespace mooncake::testing
