// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <acl/acl.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <unordered_set>

#include "tent/platform/ascend.h"

namespace {

struct AclState {
    int device_id = 2;
    aclError get_device_result = ACL_SUCCESS;
    aclError copy_result = ACL_SUCCESS;
    size_t copy_calls = 0;
    size_t device_free_calls = 0;
    std::unordered_set<void*> device_allocations;
};

// Only this executable uses these symbols. Its tests run serially and call the
// real AscendPlatform directly, without changing the CPU platform singleton.
AclState g_acl;

}  // namespace

extern "C" {

aclError aclrtGetDevice(int32_t* deviceId) {
    if (g_acl.get_device_result != ACL_SUCCESS) return g_acl.get_device_result;
    *deviceId = g_acl.device_id;
    return ACL_SUCCESS;
}

aclError aclrtPointerGetAttributes(const void* ptr,
                                   aclrtPtrAttributes* attributes) {
    *attributes = {};
    attributes->location.type =
        g_acl.device_allocations.count(const_cast<void*>(ptr))
            ? ACL_MEM_LOCATION_TYPE_DEVICE
            : ACL_MEM_LOCATION_TYPE_HOST;
    return ACL_SUCCESS;
}

aclError aclrtMalloc(void** devPtr, size_t size, aclrtMemMallocPolicy) {
    *devPtr = std::malloc(size);
    if (*devPtr == nullptr) return 1;
    g_acl.device_allocations.insert(*devPtr);
    return ACL_SUCCESS;
}

aclError aclrtFree(void* devPtr) {
    if (g_acl.device_allocations.erase(devPtr) == 0) return 1;
    ++g_acl.device_free_calls;
    std::free(devPtr);
    return ACL_SUCCESS;
}

aclError aclrtMemcpy(void* dst, size_t destMax, const void* src, size_t count,
                     aclrtMemcpyKind kind) {
    ++g_acl.copy_calls;
    if (g_acl.copy_result != ACL_SUCCESS) return g_acl.copy_result;
    if (kind != ACL_MEMCPY_DEFAULT || count > destMax ||
        (count != 0 && (dst == nullptr || src == nullptr))) {
        return 1;
    }
    if (count != 0) std::memcpy(dst, src, count);
    return ACL_SUCCESS;
}

}  // extern "C"

namespace mooncake::tent {
namespace {

class AscendPlatformMockTest : public ::testing::Test {
   protected:
    void SetUp() override {
        g_acl = AclState{};
        platform_ =
            std::make_unique<AscendPlatform>(std::make_shared<Config>());
    }

    void TearDown() override {
        platform_.reset();
        EXPECT_TRUE(g_acl.device_allocations.empty());
        for (void* ptr : g_acl.device_allocations) std::free(ptr);
        g_acl.device_allocations.clear();
    }

    std::unique_ptr<AscendPlatform> platform_;
};

TEST_F(AscendPlatformMockTest, DeviceAllocationLocationAndFree) {
    MemoryOptions options;
    options.location = "npu:2";
    void* ptr = nullptr;
    ASSERT_TRUE(platform_->allocate(&ptr, 64, options).ok());
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(platform_->type(), "ascend");

    auto locations = platform_->getLocation(ptr, 64);
    ASSERT_EQ(locations.size(), 1);
    EXPECT_EQ(locations[0].location, "npu:2");
    EXPECT_TRUE(platform_->free(ptr, 64).ok());
    EXPECT_EQ(g_acl.device_free_calls, 1);
    EXPECT_TRUE(g_acl.device_allocations.empty());
}

TEST_F(AscendPlatformMockTest, HostLocation) {
    std::array<char, 16> data{};
    auto locations = platform_->getLocation(data.data(), data.size());
    ASSERT_EQ(locations.size(), 1);
    EXPECT_EQ(locations[0].location, "cpu:0");
}

TEST_F(AscendPlatformMockTest, CopyUsesAclAndPreservesData) {
    std::array<char, 4> source{'a', 'b', 'c', 'd'};
    std::array<char, 4> output{};
    ASSERT_TRUE(
        platform_->copy(output.data(), source.data(), source.size()).ok());
    EXPECT_EQ(output, source);
    EXPECT_EQ(g_acl.copy_calls, 1);
}

TEST_F(AscendPlatformMockTest, CopyFailurePropagates) {
    std::array<char, 4> source{'a', 'b', 'c', 'd'};
    std::array<char, 4> output{};
    g_acl.copy_result = 17;
    EXPECT_FALSE(
        platform_->copy(output.data(), source.data(), source.size()).ok());
    EXPECT_EQ(output, (std::array<char, 4>{}));
    EXPECT_EQ(g_acl.copy_calls, 1);
}

TEST_F(AscendPlatformMockTest, DeviceLookupFailureDoesNotAllocate) {
    MemoryOptions options;
    options.location = "npu:2";
    void* ptr = nullptr;
    g_acl.get_device_result = 19;
    EXPECT_FALSE(platform_->allocate(&ptr, 64, options).ok());
    EXPECT_EQ(ptr, nullptr);
    EXPECT_TRUE(g_acl.device_allocations.empty());
}

}  // namespace
}  // namespace mooncake::tent
