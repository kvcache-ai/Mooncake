#include "real_client.h"

#include <array>
#include <limits>

#include <gtest/gtest.h>

#include "device/accelerator_registry.h"

namespace mooncake {
namespace {

using device::AcceleratorVendor;

class WriteSourceDevice final : public device::AcceleratorDevice {
   public:
    explicit WriteSourceDevice(AcceleratorVendor vendor) : vendor_(vendor) {}

    AcceleratorVendor Vendor() const override { return vendor_; }
    bool Available(bool = false) const override { return true; }
    device::PointerInfo QueryPointer(const void* ptr) const override {
        const auto address = reinterpret_cast<uintptr_t>(ptr);
        const auto base = reinterpret_cast<uintptr_t>(buffer.data());
        if (address >= base && address - base < buffer.size()) {
            return {device::MemoryKind::kDevice, 0};
        }
        return {device::MemoryKind::kHost, -1};
    }
    int32_t CurrentDeviceId() const override { return 0; }
    void SetContext(int32_t) const override {}
    bool Copy(void*, const void*, size_t,
              device::CopyDirection) const override {
        return false;
    }
    PinnedHostBuffer AllocatePinnedHost(size_t) const override { return {}; }

    std::array<char, 32> buffer{};

   private:
    AcceleratorVendor vendor_;
};

WriteSourceDevice cuda_device(AcceleratorVendor::kNvidia);
WriteSourceDevice mlu_device(AcceleratorVendor::kMlu);
WriteSourceDevice other_device(AcceleratorVendor::kHip);

}  // namespace

// Register fake device ranges only in this test executable. The production
// registry, pointer lookup and RealClient admission predicate remain in use.
class DirectDeviceWriteTest
    : public ::testing::TestWithParam<AcceleratorVendor> {
   protected:
    static void SetUpTestSuite() {
        device::GetAcceleratorRegistry();
        static const device::AcceleratorDeviceRegistrar cuda(cuda_device);
        static const device::AcceleratorDeviceRegistrar mlu(mlu_device);
        static const device::AcceleratorDeviceRegistrar other(other_device);
    }

    void SetUp() override { client_.protocol = "rdma"; }

    char* Source() {
        return GetParam() == AcceleratorVendor::kNvidia
                   ? cuda_device.buffer.data()
                   : mlu_device.buffer.data();
    }

    void RegisterRange(void* ptr, size_t size, bool eligible = true) {
        client_.registered_buffer_sizes_[ptr] = {size, eligible};
    }

    bool CanRetain(void* ptr, size_t size) const {
        std::shared_lock<std::shared_mutex> guard(
            client_.registered_buffer_mutex_, std::defer_lock);
        return client_.is_registered_device_write_source({ptr, size}, guard);
    }

    void SetProtocol(const std::string& protocol) {
        client_.protocol = protocol;
    }

    RealClient client_;
};

TEST_P(DirectDeviceWriteTest, RequiresCompleteLogicalRegistration) {
    auto* source = Source();
    EXPECT_FALSE(CanRetain(source, 16));
    RegisterRange(source, 16);
    EXPECT_TRUE(CanRetain(source, 16));
    EXPECT_TRUE(CanRetain(source + 4, 12));
    EXPECT_FALSE(CanRetain(source + 4, 13));
    EXPECT_FALSE(CanRetain(source + 16, 1));
    EXPECT_FALSE(CanRetain(source, std::numeric_limits<size_t>::max()));
}

TEST_P(DirectDeviceWriteTest, CleanupPendingRegistrationKeepsStaging) {
    RegisterRange(Source(), 16, false);
    EXPECT_FALSE(CanRetain(Source(), 16));
}

TEST_P(DirectDeviceWriteTest, OtherInputsKeepStaging) {
    RegisterRange(Source(), 16);
    EXPECT_FALSE(CanRetain(Source(), 0));
    EXPECT_FALSE(CanRetain(nullptr, 1));
    std::array<char, 16> host{};
    RegisterRange(host.data(), host.size());
    EXPECT_FALSE(CanRetain(host.data(), host.size()));
    RegisterRange(other_device.buffer.data(), 16);
    EXPECT_FALSE(CanRetain(other_device.buffer.data(), 16));
    SetProtocol("tcp");
    EXPECT_FALSE(CanRetain(Source(), 16));
}

INSTANTIATE_TEST_SUITE_P(CudaAndMlu, DirectDeviceWriteTest,
                         ::testing::Values(AcceleratorVendor::kNvidia,
                                           AcceleratorVendor::kMlu));

}  // namespace mooncake
