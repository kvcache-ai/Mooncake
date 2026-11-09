// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/device_plugin.h"

#include <cuda.h>
#include <cuda_runtime.h>
#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <thread>

extern "C" int tent_register_device_plugin(device_plugin_t* out);

#ifdef TENT_CUDA_PLUGIN_CONTEXT_MOCK
#include "tent/runtime/memory_prober.h"

namespace {
CUcontext current_context;
int current_device, device_queries, switches, copies, context_queries;
bool fail_copy, fail_switch, fail_context_query, fail_context_restore;
bool fail_device_restore;
bool fail_device_query;
const void* device_pointer;
constexpr int kBufferDevice = 2;

class CudaPluginContextTest : public testing::Test {
   protected:
    device_plugin_t plugin{};
    void SetUp() override {
        ASSERT_EQ(0, tent_register_device_plugin(&plugin));
        current_context = reinterpret_cast<CUcontext>(0x1234);
        current_device = 0;
        device_queries = switches = copies = context_queries = 0;
        fail_copy = fail_switch = fail_context_query = false;
        fail_context_restore = fail_device_restore = false;
        fail_device_query = false;
        device_pointer = nullptr;
    }
};
}  // namespace

extern "C" {
CUresult __wrap_cuInit(unsigned int) { return CUDA_SUCCESS; }
CUresult __wrap_cuPointerGetAttribute(void* value, CUpointer_attribute attr,
                                      CUdeviceptr ptr) {
    if (!device_pointer || ptr != (CUdeviceptr)device_pointer)
        return CUDA_ERROR_INVALID_VALUE;
    if (attr == CU_POINTER_ATTRIBUTE_MEMORY_TYPE)
        *static_cast<unsigned int*>(value) = CU_MEMORYTYPE_DEVICE;
    else if (attr == CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL)
        *static_cast<int*>(value) = kBufferDevice;
    else
        return CUDA_ERROR_INVALID_VALUE;
    return CUDA_SUCCESS;
}
CUresult __wrap_cuCtxGetCurrent(CUcontext* ctx) {
    ++context_queries;
    if (fail_context_query) return CUDA_ERROR_UNKNOWN;
    *ctx = current_context;
    return CUDA_SUCCESS;
}
CUresult __wrap_cuCtxSetCurrent(CUcontext ctx) {
    if (fail_context_restore) return CUDA_ERROR_UNKNOWN;
    current_context = ctx;
    return CUDA_SUCCESS;
}
cudaError_t __wrap_cudaGetDevice(int* device) {
    ++device_queries;
    if (fail_device_query) return cudaErrorUnknown;
    *device = current_device;
    return cudaSuccess;
}
cudaError_t __wrap_cudaSetDevice(int device) {
    ++switches;
    // Model a switch even when injecting failure, to exercise cleanup after
    // partial mutation rather than relying on an error being side-effect free.
    current_device = device;
    current_context = reinterpret_cast<CUcontext>(0x2000 + device);
    if ((fail_switch && device == kBufferDevice) ||
        (fail_device_restore && device != kBufferDevice))
        return cudaErrorUnknown;
    return cudaSuccess;
}
cudaError_t __wrap_cudaMemcpy(void* dst, const void* src, size_t size,
                              cudaMemcpyKind) {
    ++copies;
    if (fail_copy) return cudaErrorUnknown;
    std::memcpy(dst, src, size);
    return cudaSuccess;
}
}

TEST_F(CudaPluginContextTest, RestoresCustomAndAbsentContexts) {
    char src[] = "data", dst[sizeof(src)] = {};
    device_pointer = src;
    for (CUcontext caller :
         {reinterpret_cast<CUcontext>(0x1234), CUcontext{}}) {
        for (bool error : {false, true}) {
            current_context = caller;
            current_device = 0;
            device_queries = 0;
            fail_copy = error;
            int rc = plugin.memcpy_sync(nullptr, dst, src, sizeof(src));
            EXPECT_EQ(!error, rc == 0);
            EXPECT_EQ(caller, current_context);
            if (caller) {
                EXPECT_EQ(0, current_device);
            } else {
                EXPECT_EQ(0, device_queries);
            }
        }
    }
}

TEST_F(CudaPluginContextTest, RestoresCustomContextOnSameDevice) {
    char src[8] = {}, dst[8] = {};
    device_pointer = dst;
    current_device = kBufferDevice;
    const CUcontext caller = current_context;
    ASSERT_EQ(0, plugin.memcpy_sync(nullptr, dst, src, sizeof(src)));
    EXPECT_EQ(caller, current_context);
    EXPECT_EQ(kBufferDevice, current_device);
}

TEST_F(CudaPluginContextTest, SwitchFailureStillRestoresCaller) {
    char src[8] = {}, dst[8] = {};
    device_pointer = src;
    const CUcontext caller = current_context;
    fail_switch = true;
    EXPECT_NE(0, plugin.memcpy_sync(nullptr, dst, src, sizeof(src)));
    EXPECT_EQ(caller, current_context);
    EXPECT_EQ(0, current_device);
    EXPECT_EQ(0, copies);
}

TEST_F(CudaPluginContextTest, SaveFailureDoesNotSwitchOrCopy) {
    char src[8] = {}, dst[8] = {};
    device_pointer = src;
    fail_context_query = true;
    EXPECT_NE(0, plugin.memcpy_sync(nullptr, dst, src, sizeof(src)));
    EXPECT_EQ(0, switches);
    EXPECT_EQ(0, copies);
}

TEST_F(CudaPluginContextTest, DeviceQueryFailureDoesNotSwitchOrCopy) {
    char src[8] = {}, dst[8] = {};
    device_pointer = src;
    const CUcontext caller = current_context;
    fail_device_query = true;
    EXPECT_NE(0, plugin.memcpy_sync(nullptr, dst, src, sizeof(src)));
    EXPECT_EQ(caller, current_context);
    EXPECT_EQ(0, switches);
    EXPECT_EQ(0, copies);
}

TEST_F(CudaPluginContextTest, RestoreFailuresAreReported) {
    char src[] = "data", dst[sizeof(src)] = {};
    device_pointer = src;
    fail_context_restore = true;
    EXPECT_NE(0, plugin.memcpy_sync(nullptr, dst, src, sizeof(src)));
    EXPECT_STREQ(src,
                 dst);  // The data copy completed before restoration failed.
    fail_context_restore = false;
    current_device = 0;
    current_context = reinterpret_cast<CUcontext>(0x1234);
    fail_device_restore = true;
    EXPECT_NE(0, plugin.memcpy_sync(nullptr, dst, src, sizeof(src)));
    EXPECT_EQ(reinterpret_cast<CUcontext>(0x1234), current_context);
}

TEST_F(CudaPluginContextTest, HostControlDoesNotTouchContext) {
    char src[] = "host", dst[sizeof(src)] = {};
    EXPECT_NE(0, plugin.memcpy_sync(nullptr, dst, src, sizeof(src)));
    EXPECT_EQ(0, switches);
    EXPECT_EQ(0, context_queries);
    EXPECT_EQ(0, copies);
}

TEST_F(CudaPluginContextTest, DispatchStopsOnCopyAndRestoreFailure) {
    auto& prober = mooncake::tent::MemoryProber::Instance();
    ASSERT_TRUE(prober.loadPlugins(CUDA_PLUGIN_CONTEXT_TEST_DIR).ok());
    char src[] = "data", dst[sizeof(src)] = {};
    device_pointer = src;
    fail_copy = true;
    const CUcontext caller = current_context;
    EXPECT_FALSE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_EQ('\0', dst[0]);
    EXPECT_EQ(caller, current_context);
    fail_copy = false;
    fail_context_restore = true;
    EXPECT_FALSE(prober.memcpy(dst, src, sizeof(src)).ok());
    fail_context_restore = false;
    device_pointer = nullptr;
    ASSERT_TRUE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_STREQ(src, dst);
    prober.unloadPlugins();
}

#else

TEST(CudaPluginContextGpuTest, DeviceCopyPreservesCallerOnSuccessAndError) {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0) GTEST_SKIP();
    ASSERT_EQ(cudaSuccess, cudaSetDevice(0));
    void* device = nullptr;
    std::array<char, 64> src{}, dst{};
    src.fill('x');
    ASSERT_EQ(cudaSuccess, cudaMalloc(&device, src.size()));
    device_plugin_t plugin{};
    ASSERT_EQ(0, tent_register_device_plugin(&plugin));
    CUcontext custom = nullptr;
#if CUDA_VERSION >= 13000
    ASSERT_EQ(CUDA_SUCCESS, cuCtxCreate(&custom, nullptr, 0, 0));
#else
    ASSERT_EQ(CUDA_SUCCESS, cuCtxCreate(&custom, 0, 0));
#endif
    for (bool error : {false, true}) {
        ASSERT_EQ(CUDA_SUCCESS, cuCtxSetCurrent(custom));
        int rc = plugin.memcpy_sync(nullptr, device,
                                    error ? nullptr : src.data(), src.size());
        EXPECT_EQ(!error, rc == 0);
        CUcontext after = nullptr;
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&after));
        EXPECT_EQ(custom, after);
        (void)cudaGetLastError();
    }
    std::thread worker([&] {
        CUcontext before = nullptr, after = nullptr;
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&before));
        ASSERT_EQ(nullptr, before);
        ASSERT_EQ(0,
                  plugin.memcpy_sync(nullptr, dst.data(), device, dst.size()));
        EXPECT_EQ(src, dst);
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&after));
        EXPECT_EQ(nullptr, after);
        EXPECT_NE(0, plugin.memcpy_sync(nullptr, device, nullptr, src.size()));
        ASSERT_EQ(CUDA_SUCCESS, cuCtxGetCurrent(&after));
        EXPECT_EQ(nullptr, after);
        (void)cudaGetLastError();
    });
    worker.join();
    ASSERT_EQ(CUDA_SUCCESS, cuCtxSetCurrent(nullptr));
    ASSERT_EQ(CUDA_SUCCESS, cuCtxDestroy(custom));
    ASSERT_EQ(cudaSuccess, cudaSetDevice(0));
    EXPECT_EQ(cudaSuccess, cudaFree(device));
}
#endif
