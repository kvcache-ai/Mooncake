// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

#include "tent/platform/cuda.h"
#include "tent/runtime/memory_prober.h"
#ifdef TENT_TEST_SEGMENT_TRACKER
#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/runtime/segment_tracker.h"
#endif

#include <cuda.h>
#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <map>

using namespace mooncake::tent;

namespace {
struct Attributes {
    unsigned int type = 0;
    unsigned int managed = 0;
    int device = -2;
    CUresult error = CUDA_SUCCESS;
};
std::map<CUdeviceptr, Attributes> pointers;
int copies, frees, host_frees, numa_probes, stream_creates;
bool fail_copy;
bool fail_set_device, fail_free;
bool fail_restore_context;
CUcontext current_context;
int current_device;
char allocation[64];

class CudaPointerTest : public testing::Test {
   protected:
    void SetUp() override {
        pointers.clear();
        copies = frees = host_frees = numa_probes = stream_creates = 0;
        fail_copy = false;
        fail_set_device = fail_free = false;
        fail_restore_context = false;
        current_context = reinterpret_cast<CUcontext>(0x1234);
        current_device = 0;
    }
};
}  // namespace

extern "C" {
CUresult __wrap_cuInit(unsigned int) {
    if (std::getenv("TENT_TEST_CUDA_NO_DEVICE")) return CUDA_ERROR_NO_DEVICE;
    return std::getenv("TENT_TEST_CUDA_INIT_ERROR") ? CUDA_ERROR_UNKNOWN
                                                    : CUDA_SUCCESS;
}
CUresult __wrap_cuDeviceGet(CUdevice* device, int ordinal) {
    if (ordinal < 0 || ordinal >= 3) return CUDA_ERROR_INVALID_DEVICE;
    *device = ordinal;
    return CUDA_SUCCESS;
}
CUresult __wrap_cuPointerGetAttributes(unsigned int count,
                                       CUpointer_attribute* attributes,
                                       void** values, CUdeviceptr ptr) {
    const auto a = pointers[ptr];
    for (unsigned int i = 0; i < count; ++i) {
        switch (attributes[i]) {
            case CU_POINTER_ATTRIBUTE_MEMORY_TYPE:
                *static_cast<unsigned int*>(values[i]) = a.type;
                break;
            case CU_POINTER_ATTRIBUTE_IS_MANAGED:
                *static_cast<unsigned int*>(values[i]) = a.managed;
                break;
            case CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL:
                *static_cast<int*>(values[i]) = a.device;
                break;
            default:
                return CUDA_ERROR_INVALID_VALUE;
        }
    }
    // Also exercise errors after partially populated output.
    return a.error;
}
// Keeps the regression harness runnable against the original helper.
CUresult __wrap_cuPointerGetAttribute(void* value, CUpointer_attribute attr,
                                      CUdeviceptr ptr) {
    auto a = pointers[ptr];
    if (a.error != CUDA_SUCCESS) return a.error;
    if (a.type == 0) return CUDA_ERROR_INVALID_VALUE;
    if (attr == CU_POINTER_ATTRIBUTE_MEMORY_TYPE)
        *static_cast<unsigned int*>(value) = a.type;
    else if (attr == CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL)
        *static_cast<int*>(value) = a.device;
    return CUDA_SUCCESS;
}
CUresult __wrap_cuCtxGetCurrent(CUcontext* ctx) {
    *ctx = current_context;
    return CUDA_SUCCESS;
}
CUresult __wrap_cuCtxSetCurrent(CUcontext ctx) {
    if (fail_restore_context) return CUDA_ERROR_UNKNOWN;
    current_context = ctx;
    return CUDA_SUCCESS;
}
cudaError_t __wrap_cudaGetDeviceCount(int* count) {
    *count = 3;
    return cudaSuccess;
}
cudaError_t __wrap_cudaRuntimeGetVersion(int* version) {
    *version = std::getenv("TENT_TEST_CUDA_ABI_MISMATCH")
                   ? CUDART_VERSION + 1000
                   : CUDART_VERSION;
    return cudaSuccess;
}
cudaError_t __wrap_cudaGetDevice(int* device) {
    *device = current_device;
    return cudaSuccess;
}
cudaError_t __wrap_cudaSetDevice(int device) {
    current_device = device;
    current_context = reinterpret_cast<CUcontext>(0x2000 + device);
    return fail_set_device ? cudaErrorUnknown : cudaSuccess;
}
cudaError_t __wrap_cudaStreamCreateWithFlags(cudaStream_t* stream,
                                             unsigned int) {
    ++stream_creates;
    *stream = reinterpret_cast<cudaStream_t>(0x3000);
    return cudaSuccess;
}
cudaError_t __wrap_cudaStreamDestroy(cudaStream_t) { return cudaSuccess; }
cudaError_t __wrap_cudaStreamSynchronize(cudaStream_t) { return cudaSuccess; }
cudaError_t __wrap_cudaMemcpyAsync(void* dst, const void* src, size_t size,
                                   cudaMemcpyKind, cudaStream_t) {
    ++copies;
    if (fail_copy) return cudaErrorUnknown;
    std::memcpy(dst, src, size);
    return cudaSuccess;
}
cudaError_t __wrap_cudaMemcpy(void* dst, const void* src, size_t size,
                              cudaMemcpyKind kind) {
    return __wrap_cudaMemcpyAsync(dst, src, size, kind, nullptr);
}
cudaError_t __wrap_cudaMalloc(void** ptr, size_t) {
    *ptr = allocation;
    return cudaSuccess;
}
cudaError_t __wrap_cudaFree(void*) {
    ++frees;
    return fail_free ? cudaErrorUnknown : cudaSuccess;
}
void __wrap_numa_free(void*, size_t) { ++host_frees; }
int __wrap_numa_move_pages(int, unsigned long, void**, const int*, int*, int) {
    ++numa_probes;
    return -1;
}
}

TEST_F(CudaPointerTest, HostControl) {
    CudaPlatform platform(nullptr);
    char src[] = "host", dst[sizeof(src)] = {};
    EXPECT_EQ(MTYPE_CPU, platform.getMemoryType(src));
    ASSERT_TRUE(platform.copy(dst, src, sizeof(src)).ok());
    EXPECT_STREQ(src, dst);
    EXPECT_EQ(0, copies);
    EXPECT_EQ(0, stream_creates);
    EXPECT_TRUE(platform.free(dst, sizeof(dst)).ok());
    EXPECT_EQ(1, host_frees);
}

#ifdef TENT_TEST_SEGMENT_TRACKER
TEST_F(CudaPointerTest, RegistrationQueryFailureDoesNotPublish) {
    SegmentManager manager(nullptr);
    ASSERT_TRUE(manager
                    .updateLocal([](SegmentDesc& desc) {
                        desc.type = SegmentType::Memory;
                        return Status::OK();
                    })
                    .ok());
    SegmentTracker tracker(manager);
    char data[8] = {};
    pointers[(CUdeviceptr)data].error = CUDA_ERROR_UNKNOWN;
    bool called = false;
    EXPECT_FALSE(tracker
                     .add((uint64_t)data, sizeof(data),
                          [&](BufferDesc&) {
                              called = true;
                              return Status::OK();
                          })
                     .ok());
    EXPECT_FALSE(called);
    EXPECT_TRUE(std::get<MemorySegmentDesc>(manager.getLocal()->detail)
                    .buffers.empty());
    EXPECT_EQ(0, numa_probes);
}

TEST_F(CudaPointerTest, ManagedRegistrationDoesNotPublish) {
    SegmentManager manager(nullptr);
    ASSERT_TRUE(manager
                    .updateLocal([](SegmentDesc& desc) {
                        desc.type = SegmentType::Memory;
                        return Status::OK();
                    })
                    .ok());
    SegmentTracker tracker(manager);
    char data[8] = {};
    pointers[(CUdeviceptr)data] = {CU_MEMORYTYPE_DEVICE, 1, 2};
    bool called = false;
    EXPECT_FALSE(tracker
                     .add((uint64_t)data, sizeof(data),
                          [&](BufferDesc&) {
                              called = true;
                              return Status::OK();
                          })
                     .ok());
    EXPECT_FALSE(called);
    EXPECT_TRUE(std::get<MemorySegmentDesc>(manager.getLocal()->detail)
                    .buffers.empty());
    EXPECT_EQ(0, numa_probes);
}
#endif

TEST_F(CudaPointerTest, QueryErrorsNeverAuthorizeHostHandling) {
    CudaPlatform platform(nullptr);
    char src[] = "data", dst[sizeof(src)] = {};
    for (CUresult error : {CUDA_ERROR_UNKNOWN, CUDA_ERROR_INVALID_VALUE,
                           CUDA_ERROR_NO_DEVICE, CUDA_ERROR_INVALID_CONTEXT}) {
        for (void* bad : {static_cast<void*>(src), static_cast<void*>(dst)}) {
            pointers.clear();
            pointers[(CUdeviceptr)bad] = {CU_MEMORYTYPE_DEVICE, 0, 2, error};
            EXPECT_EQ(MTYPE_UNKNOWN, platform.getMemoryType(bad));
            EXPECT_FALSE(platform.copy(dst, src, sizeof(src)).ok());
            EXPECT_EQ('\0', dst[0]);
            EXPECT_FALSE(platform.free(bad, sizeof(src)).ok());
            EXPECT_TRUE(platform.getLocation(bad, sizeof(src)).empty());
        }
    }
    EXPECT_EQ(0, copies);
    EXPECT_EQ(0, frees);
    EXPECT_EQ(0, host_frees);
    EXPECT_EQ(0, numa_probes);
}

TEST_F(CudaPointerTest, UnknownTypesAndInvalidOrdinalsAreErrors) {
    CudaPlatform platform(nullptr);
    char data[8] = {};
    for (Attributes a : {Attributes{CU_MEMORYTYPE_ARRAY, 0, 0},
                         Attributes{CU_MEMORYTYPE_DEVICE, 0, -1},
                         Attributes{CU_MEMORYTYPE_UNIFIED, 0, -2},
                         Attributes{CU_MEMORYTYPE_DEVICE, 0, 3}}) {
        pointers[(CUdeviceptr)data] = a;
        EXPECT_EQ(MTYPE_UNKNOWN, platform.getMemoryType(data));
        EXPECT_FALSE(platform.free(data, sizeof(data)).ok());
        EXPECT_TRUE(platform.getLocation(data, sizeof(data)).empty());
    }
    EXPECT_EQ(0, host_frees);
    EXPECT_EQ(0, frees);
    EXPECT_EQ(0, numa_probes);
}

TEST_F(CudaPointerTest, DeviceAndBothManagedRepresentationsUseCuda) {
    CudaPlatform platform(nullptr);
    char src[] = "data", dst[sizeof(src)] = {};
    for (Attributes a : {Attributes{CU_MEMORYTYPE_DEVICE, 0, 2},
                         Attributes{CU_MEMORYTYPE_UNIFIED, 0, 2},
                         Attributes{CU_MEMORYTYPE_HOST, 1, 2}}) {
        pointers[(CUdeviceptr)src] = a;
        const bool managed = a.managed || a.type == CU_MEMORYTYPE_UNIFIED;
        EXPECT_EQ(managed ? MTYPE_UNKNOWN : MTYPE_CUDA,
                  platform.getMemoryType(src));
        auto location = platform.getLocation(src, sizeof(src));
        if (managed) {
            EXPECT_TRUE(location.empty());
        } else {
            ASSERT_EQ(1, location.size());
            EXPECT_EQ("cuda:2", location[0].location);
        }
        ASSERT_TRUE(platform.copy(dst, src, sizeof(src)).ok());
        EXPECT_STREQ(src, dst);
        EXPECT_TRUE(platform.free(src, sizeof(src)).ok());
    }
    EXPECT_EQ(3, copies);
    EXPECT_EQ(3, frees);
    EXPECT_EQ(0, host_frees);
    EXPECT_EQ(0, numa_probes);
}

TEST_F(CudaPointerTest, BothEndpointsAreCheckedEvenWhenDestinationIsDevice) {
    CudaPlatform platform(nullptr);
    char src[8] = {}, dst[8] = {};
    pointers[(CUdeviceptr)dst] = {CU_MEMORYTYPE_DEVICE, 0, 2};
    pointers[(CUdeviceptr)src].error = CUDA_ERROR_UNKNOWN;
    EXPECT_FALSE(platform.copy(dst, src, sizeof(src)).ok());
    EXPECT_EQ(0, copies);
}

TEST_F(CudaPointerTest, CopyRestoresExactContextOnSuccessAndFailure) {
    CudaPlatform platform(nullptr);
    char src[8] = {}, dst[8] = {};
    pointers[(CUdeviceptr)src] = {CU_MEMORYTYPE_DEVICE, 0, 2};
    for (CUcontext ctx : {reinterpret_cast<CUcontext>(0x1234), CUcontext{}}) {
        for (bool error : {false, true}) {
            current_context = ctx;
            current_device = 0;
            fail_copy = error;
            EXPECT_EQ(!error, platform.copy(dst, src, sizeof(src)).ok());
            EXPECT_EQ(ctx, current_context);
            if (ctx) {
                EXPECT_EQ(0, current_device);
            }
        }
    }
}

TEST_F(CudaPointerTest, RegisteredHostIsCpuButNotNumaOwned) {
    CudaPlatform platform(nullptr);
    char data[8] = {};
    pointers[(CUdeviceptr)data] = {CU_MEMORYTYPE_HOST, 0, 0};
    EXPECT_EQ(MTYPE_CPU, platform.getMemoryType(data));
    EXPECT_FALSE(platform.free(data, sizeof(data)).ok());
    EXPECT_EQ(0, host_frees);
    EXPECT_EQ(0, frees);
}

TEST_F(CudaPointerTest, PluginErrorsStopCpuFallback) {
    auto& prober = MemoryProber::Instance();
    ASSERT_TRUE(prober.loadPlugins(CUDA_POINTER_TEST_PLUGIN_DIR).ok());
    char src[] = "data", dst[sizeof(src)] = {};
    pointers[(CUdeviceptr)src].error = CUDA_ERROR_UNKNOWN;
    EXPECT_FALSE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_EQ('\0', dst[0]);
    EXPECT_TRUE(prober.locate(src, sizeof(src)).empty());
    EXPECT_EQ("", prober.type(src));
    EXPECT_EQ(0, numa_probes);
    pointers[(CUdeviceptr)src] = {CU_MEMORYTYPE_HOST, 1, 2};
    EXPECT_TRUE(prober.locate(src, sizeof(src)).empty());
    EXPECT_EQ("", prober.type(src));
    pointers[(CUdeviceptr)dst].error = CUDA_ERROR_UNKNOWN;
    EXPECT_FALSE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_EQ('\0', dst[0]);
    pointers.erase((CUdeviceptr)dst);
    fail_copy = true;
    const CUcontext caller = current_context;
    EXPECT_FALSE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_EQ('\0', dst[0]);
    EXPECT_EQ(caller, current_context);
    fail_copy = false;
    pointers.clear();
    ASSERT_TRUE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_STREQ(src, dst);
    void* ptr = nullptr;
    ASSERT_TRUE(prober.alloc(&ptr, 64, "cuda:0").ok());
    pointers[(CUdeviceptr)ptr].error = CUDA_ERROR_UNKNOWN;
    EXPECT_FALSE(prober.free(ptr, 64).ok());
    EXPECT_EQ(0, frees);
    pointers[(CUdeviceptr)ptr] = {CU_MEMORYTYPE_HOST, 1, 0};
    EXPECT_TRUE(prober.free(ptr, 64).ok());
    EXPECT_EQ(1, frees);
    prober.unloadPlugins();
}

TEST_F(CudaPointerTest, PluginRuntimeFailuresDoNotFallback) {
    auto& prober = MemoryProber::Instance();
    ASSERT_TRUE(prober.loadPlugins(CUDA_POINTER_TEST_PLUGIN_DIR).ok());
    char src[] = "data", dst[sizeof(src)] = {};
    pointers[(CUdeviceptr)src] = {CU_MEMORYTYPE_HOST, 1, 2};
    const CUcontext caller = current_context;
    fail_set_device = true;
    EXPECT_FALSE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_EQ(caller, current_context);
    EXPECT_EQ('\0', dst[0]);
    EXPECT_EQ(0, copies);
    fail_set_device = false;
    void* ptr = nullptr;
    ASSERT_TRUE(prober.alloc(&ptr, 64, "cuda:0").ok());
    pointers[(CUdeviceptr)ptr] = {CU_MEMORYTYPE_DEVICE, 0, 0};
    fail_free = true;
    EXPECT_FALSE(prober.free(ptr, 64).ok());
    fail_free = false;
    // The failed free must retain the allocation's registration for retry.
    EXPECT_TRUE(prober.free(ptr, 64).ok());
    EXPECT_EQ(2, frees);
    EXPECT_EQ(0, host_frees);
    prober.unloadPlugins();
}

TEST_F(CudaPointerTest, ContextRestoreFailureIsReportedAfterCopy) {
    CudaPlatform platform(nullptr);
    char src[] = "data", dst[sizeof(src)] = {};
    pointers[(CUdeviceptr)src] = {CU_MEMORYTYPE_DEVICE, 0, 2};
    fail_restore_context = true;
    EXPECT_FALSE(platform.copy(dst, src, sizeof(src)).ok());
    EXPECT_EQ(1, copies);
    EXPECT_STREQ(src, dst);
    auto& prober = MemoryProber::Instance();
    ASSERT_TRUE(prober.loadPlugins(CUDA_POINTER_TEST_PLUGIN_DIR).ok());
    EXPECT_FALSE(prober.memcpy(dst, src, sizeof(src)).ok());
    EXPECT_EQ(2, copies);
    prober.unloadPlugins();
}

// Run separately: driver initialization is deliberately cached per process.
TEST(CudaPointerInitTest, AbiMismatchStopsPlatformProbeAndCopy) {
    if (!std::getenv("TENT_TEST_CUDA_ABI_MISMATCH")) GTEST_SKIP();
    CudaPlatform platform(nullptr);
    char src[] = "data", dst[sizeof(src)] = {};
    for (Attributes a : {Attributes{}, Attributes{CU_MEMORYTYPE_DEVICE, 0, 2},
                         Attributes{CU_MEMORYTYPE_DEVICE, 1, 2}}) {
        pointers[(CUdeviceptr)src] = a;
        EXPECT_EQ(MTYPE_UNKNOWN, platform.getMemoryType(src));
        EXPECT_TRUE(platform.getLocation(src, sizeof(src)).empty());
        EXPECT_FALSE(platform.copy(dst, src, sizeof(src)).ok());
        EXPECT_EQ('\0', dst[0]);
    }
    EXPECT_EQ(0, copies);
    EXPECT_EQ(0, numa_probes);
}

TEST(CudaPointerInitTest, FailureIsNotHostMemory) {
    if (!std::getenv("TENT_TEST_CUDA_INIT_ERROR")) GTEST_SKIP();
    CudaPlatform platform(nullptr);
    char src[8] = {}, dst[8] = {};
    EXPECT_EQ(MTYPE_UNKNOWN, platform.getMemoryType(src));
    EXPECT_FALSE(platform.copy(dst, src, sizeof(src)).ok());
    EXPECT_FALSE(platform.free(src, sizeof(src)).ok());
    EXPECT_TRUE(platform.getLocation(src, sizeof(src)).empty());
    EXPECT_EQ(0, copies);
    EXPECT_EQ(0, host_frees);
    EXPECT_EQ(0, numa_probes);
}

TEST(CudaPointerInitTest, NoVisibleDevicePreservesHostOnlyOperation) {
    if (!std::getenv("TENT_TEST_CUDA_NO_DEVICE")) GTEST_SKIP();
    CudaPlatform platform(nullptr);
    char src[] = "host", dst[sizeof(src)] = {};
    EXPECT_EQ(MTYPE_CPU, platform.getMemoryType(src));
    EXPECT_TRUE(platform.copy(dst, src, sizeof(src)).ok());
    EXPECT_STREQ(src, dst);
    EXPECT_TRUE(platform.free(src, sizeof(src)).ok());
    EXPECT_EQ(0, copies);
    EXPECT_EQ(1, host_frees);
}
