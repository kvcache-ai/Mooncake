// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

#include "tent/platform/cuda.h"
#include "tent/platform/cuda_utils.h"
#include "tent/device_plugin.h"

#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <thread>

using namespace mooncake::tent;

extern "C" int tent_register_device_plugin(device_plugin_t* out);

TEST(CudaPointerGpuTest, RealAllocationsCopiesAndContextRestoration) {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0) GTEST_SKIP();
    ASSERT_EQ(cudaSuccess, cudaSetDevice(0));
    CudaPlatform platform(nullptr);
    std::array<char, 64> src{}, dst{};
    src.fill('x');
    void *device = nullptr, *managed = nullptr, *pinned = nullptr;
    ASSERT_EQ(cudaSuccess, cudaMalloc(&device, src.size()));
    ASSERT_EQ(cudaSuccess, cudaMallocManaged(&managed, src.size()));
    ASSERT_EQ(cudaSuccess, cudaMallocHost(&pinned, src.size()));
    CudaPointerInfo info;
    ASSERT_EQ(CUDA_SUCCESS, getCudaPointerInfo(src.data(), info));
    EXPECT_EQ(CudaMemoryKind::Host, info.kind);
    ASSERT_EQ(CUDA_SUCCESS, getCudaPointerInfo(pinned, info));
    EXPECT_EQ(CudaMemoryKind::Host, info.kind);
    EXPECT_TRUE(info.registered_host);
    ASSERT_EQ(CUDA_SUCCESS, getCudaPointerInfo(managed, info));
    EXPECT_EQ(CudaMemoryKind::Managed, info.kind);
    EXPECT_EQ(0, info.device);

    device_plugin_t plugin{};
    ASSERT_EQ(0, tent_register_device_plugin(&plugin));
    location_t location{};
    EXPECT_EQ(
        TENT_DEVICE_PLUGIN_ERROR,
        plugin.query_location(nullptr, managed, src.size(), &location, 1));
    ASSERT_EQ(0, plugin.memcpy_sync(nullptr, managed, src.data(), src.size()));
    ASSERT_EQ(0, plugin.memcpy_sync(nullptr, dst.data(), managed, dst.size()));
    EXPECT_EQ(src, dst);
    void* plugin_managed = nullptr;
    ASSERT_EQ(cudaSuccess, cudaMallocManaged(&plugin_managed, src.size()));
    EXPECT_EQ(0, plugin.free(nullptr, plugin_managed, src.size()));
    ASSERT_EQ(CUDA_SUCCESS, getCudaPointerInfo(device, info));
    EXPECT_EQ(CudaMemoryKind::Device, info.kind);
    EXPECT_EQ(0, info.device);

    CUcontext custom = nullptr;
#if CUDA_VERSION >= 13000
    ASSERT_EQ(CUDA_SUCCESS, cuCtxCreate(&custom, nullptr, 0, 0));
#else
    ASSERT_EQ(CUDA_SUCCESS, cuCtxCreate(&custom, 0, 0));
#endif
    for (void* ptr : {device, managed}) {
        EXPECT_EQ(ptr == managed ? MTYPE_UNKNOWN : MTYPE_CUDA,
                  platform.getMemoryType(ptr));
        ASSERT_TRUE(platform.copy(ptr, src.data(), src.size()).ok());
        ASSERT_TRUE(platform.copy(dst.data(), ptr, dst.size()).ok());
        EXPECT_EQ(src, dst);
        ASSERT_EQ(0, plugin.memcpy_sync(nullptr, dst.data(), ptr, dst.size()));
        CUcontext restored = nullptr;
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&restored));
        EXPECT_EQ(custom, restored);
        auto locations = platform.getLocation(ptr, src.size());
        if (ptr == managed) {
            EXPECT_TRUE(locations.empty());
        } else {
            ASSERT_EQ(1, locations.size());
            EXPECT_EQ("cuda:0", locations[0].location);
        }
    }
    // A fresh worker starts without a context. Queries and a successful CUDA
    // copy must leave it that way, including for managed memory.
    std::thread worker([&] {
        CUcontext before = nullptr, after = nullptr;
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&before));
        ASSERT_EQ(nullptr, before);
        EXPECT_EQ(MTYPE_CPU, platform.getMemoryType(src.data()));
        EXPECT_EQ(MTYPE_UNKNOWN, platform.getMemoryType(managed));
        ASSERT_TRUE(platform.copy(dst.data(), managed, dst.size()).ok());
        EXPECT_EQ(src, dst);
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&after));
        EXPECT_EQ(nullptr, after);
        ASSERT_EQ(0,
                  plugin.memcpy_sync(nullptr, dst.data(), managed, dst.size()));
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&after));
        EXPECT_EQ(nullptr, after);
    });
    worker.join();
    ASSERT_EQ(CUDA_SUCCESS, cuCtxSetCurrent(nullptr));
    ASSERT_EQ(CUDA_SUCCESS, cuCtxDestroy(custom));
    ASSERT_EQ(cudaSuccess, cudaSetDevice(0));
    EXPECT_FALSE(platform.free(pinned, src.size()).ok());
    EXPECT_EQ(cudaSuccess, cudaFreeHost(pinned));
    EXPECT_TRUE(platform.free(device, src.size()).ok());
    EXPECT_TRUE(platform.free(managed, src.size()).ok());
}
