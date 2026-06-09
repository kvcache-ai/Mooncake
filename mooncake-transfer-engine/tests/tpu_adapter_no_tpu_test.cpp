// Unit tests for the TPU adapter shim (gpu_vendor/tpu.h).
//
// These tests verify correct behavior when NO adapter library is present,
// which is the common case in CI environments without TPU hardware.
// Every test here must pass without libmooncake_tpu_adapter.so installed.

#include "gpu_vendor/tpu.h"

#include <gtest/gtest.h>

#include <cstdlib>

namespace {

// ---------------------------------------------------------------------------
// defaultErrorString — pure function, no api() call
// ---------------------------------------------------------------------------

TEST(TpuDefaultErrorString, KnownCodes) {
    using namespace mooncake::tpu::detail;
    EXPECT_STREQ(defaultErrorString(cudaSuccess), "success");
    EXPECT_STREQ(defaultErrorString(cudaErrorInvalidValue), "invalid value");
    EXPECT_STREQ(defaultErrorString(cudaErrorNotSupported),
                 "TPU adapter is unavailable or incomplete");
    EXPECT_STREQ(defaultErrorString(cudaErrorSharedObjectSymbolNotFound),
                 "TPU adapter symbol not found");
    EXPECT_STREQ(defaultErrorString(cudaErrorSharedObjectInitFailed),
                 "TPU adapter initialization failed");
}

TEST(TpuDefaultErrorString, UnknownCodeFallback) {
    EXPECT_STREQ(mooncake::tpu::detail::defaultErrorString(9999),
                 "unknown TPU adapter error");
}

// ---------------------------------------------------------------------------
// copyPointerAttributes — pure helper, no api() call
// ---------------------------------------------------------------------------

TEST(TpuCopyPointerAttributes, NullDstReturnsFalse) {
    mc_tpu_pointer_attributes_t src{cudaMemoryTypeDevice, 0, 4096};
    EXPECT_FALSE(mooncake::tpu::detail::copyPointerAttributes(nullptr, src));
}

TEST(TpuCopyPointerAttributes, CopiesAllFields) {
    mc_tpu_pointer_attributes_t src{cudaMemoryTypeDevice, 2, 1024};
    cudaPointerAttributes dst{};
    EXPECT_TRUE(mooncake::tpu::detail::copyPointerAttributes(&dst, src));
    EXPECT_EQ(dst.type, cudaMemoryTypeDevice);
    EXPECT_EQ(dst.device, 2);
    EXPECT_EQ(dst.size, static_cast<size_t>(1024));
}

TEST(TpuCopyPointerAttributes, HostTypePreserved) {
    mc_tpu_pointer_attributes_t src{cudaMemoryTypeHost, -1, 0};
    cudaPointerAttributes dst{};
    EXPECT_TRUE(mooncake::tpu::detail::copyPointerAttributes(&dst, src));
    EXPECT_EQ(dst.type, cudaMemoryTypeHost);
}

// ---------------------------------------------------------------------------
// cuDeviceGet — pure ordinal passthrough, no api() call
// ---------------------------------------------------------------------------

TEST(TpuCuDeviceGet, NullDeviceReturnsInvalidValue) {
    EXPECT_EQ(cuDeviceGet(nullptr, 0), cudaErrorInvalidValue);
}

TEST(TpuCuDeviceGet, StoresOrdinalAsDevice) {
    CUdevice dev = -1;
    EXPECT_EQ(cuDeviceGet(&dev, 3), cudaSuccess);
    EXPECT_EQ(dev, 3);
}

// ---------------------------------------------------------------------------
// cuGetErrorString — delegates to defaultErrorString when no adapter
// ---------------------------------------------------------------------------

TEST(TpuCuGetErrorString, NullErrStrReturnsInvalidValue) {
    EXPECT_EQ(cuGetErrorString(cudaSuccess, nullptr), cudaErrorInvalidValue);
}

TEST(TpuCuGetErrorString, ReturnsNonNullString) {
    const char* msg = nullptr;
    EXPECT_EQ(cuGetErrorString(cudaSuccess, &msg), cudaSuccess);
    EXPECT_NE(msg, nullptr);
}

// ---------------------------------------------------------------------------
// Null pointer guards — each returns before touching api()
// ---------------------------------------------------------------------------

TEST(TpuNullGuards, GetDeviceCount_Null) {
    EXPECT_EQ(cudaGetDeviceCount(nullptr), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, GetDevice_Null) {
    EXPECT_EQ(cudaGetDevice(nullptr), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, DeviceGetPCIBusId_NullBuffer) {
    EXPECT_EQ(cudaDeviceGetPCIBusId(nullptr, 32, 0), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, DeviceGetPCIBusId_ZeroLen) {
    char buf[8];
    EXPECT_EQ(cudaDeviceGetPCIBusId(buf, 0, 0), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, PointerGetAttributes_NullAttr) {
    int x = 42;
    EXPECT_EQ(cudaPointerGetAttributes(nullptr, &x), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, PointerGetAttributes_NullPtr) {
    cudaPointerAttributes attr{};
    EXPECT_EQ(cudaPointerGetAttributes(&attr, nullptr), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, Malloc_NullDst) {
    EXPECT_EQ(cudaMalloc(nullptr, 128), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, MallocHost_NullDst) {
    EXPECT_EQ(cudaMallocHost(nullptr, 128), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, Memcpy_NullDst) {
    int src = 1;
    EXPECT_EQ(cudaMemcpy(nullptr, &src, sizeof(int), cudaMemcpyHostToDevice),
              cudaErrorInvalidValue);
}

TEST(TpuNullGuards, Memcpy_NullSrc) {
    int dst = 0;
    EXPECT_EQ(cudaMemcpy(&dst, nullptr, sizeof(int), cudaMemcpyDeviceToHost),
              cudaErrorInvalidValue);
}

TEST(TpuNullGuards, Memset_NullPtr) {
    EXPECT_EQ(cudaMemset(nullptr, 0, 64), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, StreamCreate_NullPtr) {
    EXPECT_EQ(cudaStreamCreate(nullptr), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, DeviceCanAccessPeer_NullPtr) {
    EXPECT_EQ(cudaDeviceCanAccessPeer(nullptr, 0, 1), cudaErrorInvalidValue);
}

TEST(TpuNullGuards, CuDeviceGetAttribute_NullPtr) {
    EXPECT_EQ(
        cuDeviceGetAttribute(nullptr, CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED, 0),
        cudaErrorInvalidValue);
}

TEST(TpuNullGuards, CuPointerGetAttribute_NullData) {
    EXPECT_EQ(cuPointerGetAttribute(nullptr, CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                                    0x1000),
              cudaErrorInvalidValue);
}

// ---------------------------------------------------------------------------
// No-adapter graceful fallbacks
// ---------------------------------------------------------------------------

TEST(TpuNoAdapter, GetDeviceCountReturnsErrorAndZero) {
    // With no adapter, get_device_count is null; shim sets *count = 0.
    int count = 99;
    auto ret = cudaGetDeviceCount(&count);
    EXPECT_NE(ret, cudaSuccess);
    EXPECT_EQ(count, 0);
}

TEST(TpuNoAdapter, GetLastErrorReflectsSetValue) {
    // Explicitly trigger cudaErrorInvalidValue, then read it back.
    cudaGetDeviceCount(nullptr);
    EXPECT_EQ(cudaGetLastError(), cudaErrorInvalidValue);
}

TEST(TpuNoAdapter, MallocHostFallsBackToSystemMalloc) {
    // When malloc_host is absent, shim falls back to std::malloc.
    void* ptr = nullptr;
    EXPECT_EQ(cudaMallocHost(&ptr, 64), cudaSuccess);
    ASSERT_NE(ptr, nullptr);
    cudaFreeHost(ptr);  // must not crash
}

TEST(TpuNoAdapter, FreeHostOnNullSucceeds) {
    // std::free(nullptr) is a no-op; shim must not crash.
    EXPECT_EQ(cudaFreeHost(nullptr), cudaSuccess);
}

TEST(TpuNoAdapter, StreamCreateSucceedsWithNullSentinel) {
    // When stream_create is absent, shim returns success and sets
    // stream=nullptr.
    cudaStream_t stream = reinterpret_cast<cudaStream_t>(0xdeadbeef);
    EXPECT_EQ(cudaStreamCreate(&stream), cudaSuccess);
    EXPECT_EQ(stream, nullptr);
}

TEST(TpuNoAdapter, StreamDestroyOnNullSucceeds) {
    EXPECT_EQ(cudaStreamDestroy(nullptr), cudaSuccess);
}

TEST(TpuNoAdapter, StreamSynchronizeOnNullSucceeds) {
    EXPECT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);
}

TEST(TpuNoAdapter, DeviceCanAccessPeerReturnsFalseWhenAbsent) {
    // device_can_access_peer absent → shim sets can_access=0, returns success.
    int can_access = 1;
    EXPECT_EQ(cudaDeviceCanAccessPeer(&can_access, 0, 1), cudaSuccess);
    EXPECT_EQ(can_access, 0);
}

TEST(TpuNoAdapter, MemcpyAsyncNoCrash) {
    // memcpy_async absent → falls back to cudaMemcpy; memcpy absent too
    // → returns unavailable error.  Just verify no crash or UB.
    char src[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    char dst[8] = {};
    (void)cudaMemcpyAsync(dst, src, sizeof(src), cudaMemcpyHostToHost, nullptr);
    SUCCEED();
}

// ---------------------------------------------------------------------------
// Constant value contracts
// ---------------------------------------------------------------------------

TEST(TpuConstants, ErrorCodeValues) {
    EXPECT_EQ(cudaSuccess, 0);
    EXPECT_EQ(cudaErrorInvalidValue, 1);
    EXPECT_NE(cudaErrorNotSupported, 0);
    EXPECT_NE(cudaErrorSharedObjectSymbolNotFound, 0);
    EXPECT_NE(cudaErrorSharedObjectInitFailed, 0);
    EXPECT_EQ(CUDA_SUCCESS, cudaSuccess);
}

TEST(TpuConstants, MemoryTypeValues) {
    EXPECT_NE(cudaMemoryTypeUnregistered, cudaMemoryTypeHost);
    EXPECT_NE(cudaMemoryTypeHost, cudaMemoryTypeDevice);
    EXPECT_EQ(CU_MEMORYTYPE_HOST, cudaMemoryTypeHost);
    EXPECT_EQ(CU_MEMORYTYPE_DEVICE, cudaMemoryTypeDevice);
}

TEST(TpuConstants, MemcpyKindValues) {
    EXPECT_EQ(cudaMemcpyHostToHost, 0);
    EXPECT_EQ(cudaMemcpyHostToDevice, 1);
    EXPECT_EQ(cudaMemcpyDeviceToHost, 2);
    EXPECT_EQ(cudaMemcpyDeviceToDevice, 3);
    EXPECT_EQ(cudaMemcpyDefault, 4);
}

TEST(TpuConstants, GpuPrefix) { EXPECT_EQ(GPU_PREFIX, "tpu:"); }

}  // namespace

int main(int argc, char** argv) {
    // Ensure no adapter library is loaded regardless of the host environment.
    unsetenv("MC_TPU_ADAPTER_LIB");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
