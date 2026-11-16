// Copyright 2026 Mooncake Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>
#include <hip/hip_runtime.h>

#include <cstdlib>
#include <cstring>
#include <vector>

#include "transport/hip_transport/stream_pool.h"

TEST(HipStreamPoolTest, PriorityFlagsRoundRobinAndCopy) {
    int devices = 0;
    ASSERT_EQ(hipGetDeviceCount(&devices), hipSuccess);
    if (devices == 0) GTEST_SKIP() << "Requires a HIP device";
    ASSERT_EQ(hipSetDevice(0), hipSuccess);
    int least = 0, greatest = 0;
    ASSERT_EQ(hipDeviceGetStreamPriorityRange(&least, &greatest), hipSuccess);
    const char* value = std::getenv("MC_HIP_STREAM_PRIORITY");
    const int expected =
        value && std::strcmp(value, "high") == 0 ? greatest : 0;

    mooncake::StreamPool pool(2);
    auto first = pool.getNextStream(0);
    auto second = pool.getNextStream(0);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(first, second);
    EXPECT_EQ(pool.getNextStream(0), first);
    for (auto stream : {first, second}) {
        int priority = 0;
        unsigned int flags = 0;
        ASSERT_EQ(hipStreamGetPriority(stream, &priority), hipSuccess);
        EXPECT_EQ(priority, expected);
        ASSERT_EQ(hipStreamGetFlags(stream, &flags), hipSuccess);
        EXPECT_EQ(flags, hipStreamNonBlocking);
    }

    std::vector<unsigned char> source(65536, 0xA5), result(source.size());
    void* device = nullptr;
    ASSERT_EQ(hipMalloc(&device, source.size()), hipSuccess);
    EXPECT_EQ(hipMemcpyAsync(device, source.data(), source.size(),
                             hipMemcpyHostToDevice, first),
              hipSuccess);
    EXPECT_EQ(hipMemcpyAsync(result.data(), device, result.size(),
                             hipMemcpyDeviceToHost, first),
              hipSuccess);
    EXPECT_EQ(hipStreamSynchronize(first), hipSuccess);
    EXPECT_EQ(result, source);
    EXPECT_EQ(hipFree(device), hipSuccess);
}

TEST(HipStreamPoolTest, ZeroCountStillCreatesOneStream) {
    int devices = 0;
    ASSERT_EQ(hipGetDeviceCount(&devices), hipSuccess);
    if (devices == 0) GTEST_SKIP() << "Requires a HIP device";
    mooncake::StreamPool pool(0);
    auto stream = pool.getNextStream(0);
    ASSERT_NE(stream, nullptr);
    EXPECT_EQ(pool.getNextStream(0), stream);
}
