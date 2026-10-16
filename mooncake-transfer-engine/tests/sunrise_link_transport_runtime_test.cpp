#include <gtest/gtest.h>

#include <memory>

#include "gpu_vendor/sunrise.h"
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace {

static tangError_t allocOnDevice(size_t size, void** buffer) {
    *buffer = nullptr;
    if (tangSetDevice(0) != tangSuccess) return tangErrorInvalidDevice;
    return tangMalloc(buffer, size);
}

}  // namespace

TEST(SunriseLinkTransportRuntimeTest,
     InstallDoesNotBreakFirstDeviceAllocation) {
    int gpu_count = 0;
    if (tangGetDeviceCount(&gpu_count) != tangSuccess || gpu_count <= 0) {
        GTEST_SKIP() << "Sunrise device is unavailable";
    }

    auto engine = std::make_unique<TransferEngine>(false);
    engine->init("P2PHANDSHAKE", "sunrise_runtime_test:12345");

    Transport* transport = engine->installTransport("sunrise_link", nullptr);
    ASSERT_NE(transport, nullptr);

    ASSERT_EQ(tangSetDevice(0), tangSuccess);
    void* buffer = nullptr;
    tangError_t ret = tangMalloc(&buffer, 4096);
    ASSERT_EQ(ret, tangSuccess) << tangGetErrorString(ret);
    ASSERT_NE(buffer, nullptr);

    ASSERT_EQ(tangFree(buffer), tangSuccess);
}

TEST(SunriseLinkTransportRuntimeTest,
     InstallDoesNotChangeLargeAllocationBehavior) {
    int gpu_count = 0;
    if (tangGetDeviceCount(&gpu_count) != tangSuccess || gpu_count <= 0) {
        GTEST_SKIP() << "Sunrise device is unavailable";
    }

    void* before = nullptr;
    tangError_t before_ret = allocOnDevice(1 << 20, &before);
    if (before_ret == tangSuccess && before != nullptr) {
        ASSERT_EQ(tangFree(before), tangSuccess);
    }

    auto engine = std::make_unique<TransferEngine>(false);
    engine->init("P2PHANDSHAKE", "sunrise_runtime_test:12346");
    Transport* transport = engine->installTransport("sunrise_link", nullptr);
    ASSERT_NE(transport, nullptr);

    void* after = nullptr;
    tangError_t after_ret = allocOnDevice(1 << 20, &after);
    EXPECT_EQ(after_ret, before_ret)
        << "before=" << tangGetErrorString(before_ret)
        << " after=" << tangGetErrorString(after_ret);
    if (after_ret == tangSuccess && after != nullptr) {
        ASSERT_EQ(tangFree(after), tangSuccess);
    }
}

TEST(SunriseLinkTransportRuntimeTest, EngineInitDoesNotBreakDeviceState) {
    int gpu_count = 0;
    if (tangGetDeviceCount(&gpu_count) != tangSuccess || gpu_count <= 0) {
        GTEST_SKIP() << "Sunrise device is unavailable";
    }

    auto engine = std::make_unique<TransferEngine>(false);
    engine->init("P2PHANDSHAKE", "sunrise_runtime_test:12347");

    int current_dev = -1;
    tangError_t set_ret = tangSetDevice(0);
    tangError_t get_ret = tangGetDevice(&current_dev);
    EXPECT_EQ(set_ret, tangSuccess) << tangGetErrorString(set_ret);
    EXPECT_EQ(get_ret, tangSuccess) << tangGetErrorString(get_ret);
    EXPECT_EQ(current_dev, 0);
}

TEST(SunriseLinkTransportRuntimeTest,
     MetadataConstructionDoesNotBreakDeviceState) {
    int gpu_count = 0;
    if (tangGetDeviceCount(&gpu_count) != tangSuccess || gpu_count <= 0) {
        GTEST_SKIP() << "Sunrise device is unavailable";
    }

    auto metadata = std::make_shared<TransferMetadata>("P2PHANDSHAKE");
    ASSERT_NE(metadata, nullptr);

    int current_dev = -1;
    tangError_t set_ret = tangSetDevice(0);
    tangError_t get_ret = tangGetDevice(&current_dev);
    EXPECT_EQ(set_ret, tangSuccess) << tangGetErrorString(set_ret);
    EXPECT_EQ(get_ret, tangSuccess) << tangGetErrorString(get_ret);
    EXPECT_EQ(current_dev, 0);
}

TEST(SunriseLinkTransportRuntimeTest,
     DestroyingOneTransportDoesNotBreakAnother) {
    int gpu_count = 0;
    if (tangGetDeviceCount(&gpu_count) != tangSuccess || gpu_count <= 0) {
        GTEST_SKIP() << "Sunrise device is unavailable";
    }

    auto engine1 = std::make_unique<TransferEngine>(false);
    engine1->init("P2PHANDSHAKE", "sunrise_runtime_test:12350");
    Transport* transport1 = engine1->installTransport("sunrise_link", nullptr);
    ASSERT_NE(transport1, nullptr);

    {
        auto engine2 = std::make_unique<TransferEngine>(false);
        engine2->init("P2PHANDSHAKE", "sunrise_runtime_test:12351");
        Transport* transport2 =
            engine2->installTransport("sunrise_link", nullptr);
        ASSERT_NE(transport2, nullptr);
    }

    ASSERT_EQ(tangSetDevice(0), tangSuccess);
    void* buffer = nullptr;
    tangError_t ret = tangMalloc(&buffer, 4096);
    EXPECT_EQ(ret, tangSuccess) << tangGetErrorString(ret);
    if (buffer) tangFree(buffer);
}
