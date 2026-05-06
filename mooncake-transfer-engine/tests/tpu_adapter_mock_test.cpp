// Unit tests for the TPU adapter shim (gpu_vendor/tpu.h) with a mock adapter.
//
// These tests verify the happy-path call-through behaviour of every shim
// function without requiring real TPU hardware.  The mock adapter shared
// library (tpu_mock_adapter.cpp) is injected via MC_TPU_ADAPTER_LIB before
// the first api() call.

#include "gpu_vendor/tpu.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>

#ifndef TPU_MOCK_ADAPTER_PATH
#error "TPU_MOCK_ADAPTER_PATH must be set via target_compile_definitions"
#endif

namespace {

// ---------------------------------------------------------------------------
// Adapter loading
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, AdapterLoaded) {
    // api().handle is non-null only when the adapter loaded successfully.
    EXPECT_NE(mooncake::tpu::detail::api().handle, nullptr);
    EXPECT_EQ(mooncake::tpu::detail::api().load_error, cudaSuccess);
}

// ---------------------------------------------------------------------------
// Error string (uses adapter's get_error_string)
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, GetErrorStringUsesAdapter) {
    // The mock returns "mock success" for error code 0.
    EXPECT_STREQ(cudaGetErrorString(0), "mock success");
}

// ---------------------------------------------------------------------------
// Device enumeration
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, GetDeviceCountReturnsOne) {
    int count = 0;
    EXPECT_EQ(cudaGetDeviceCount(&count), cudaSuccess);
    EXPECT_EQ(count, 1);
}

TEST(TpuMockAdapter, SetAndGetDevice) {
    EXPECT_EQ(cudaSetDevice(0), cudaSuccess);
    int dev = -1;
    EXPECT_EQ(cudaGetDevice(&dev), cudaSuccess);
    EXPECT_EQ(dev, 0);
}

TEST(TpuMockAdapter, DeviceGetPCIBusId) {
    char buf[32] = {};
    EXPECT_EQ(cudaDeviceGetPCIBusId(buf, static_cast<int>(sizeof(buf)), 0),
              cudaSuccess);
    EXPECT_NE(buf[0], '\0');
}

// ---------------------------------------------------------------------------
// Device-memory allocation
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, MallocAndFreeDevice) {
    void *ptr = nullptr;
    EXPECT_EQ(cudaMalloc(&ptr, 256), cudaSuccess);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(cudaFree(ptr), cudaSuccess);
}

// malloc_host is intentionally absent from the mock to exercise the
// std::malloc fallback path.
TEST(TpuMockAdapter, MallocHostFallsBackToSystemMalloc) {
    void *ptr = nullptr;
    EXPECT_EQ(cudaMallocHost(&ptr, 128), cudaSuccess);
    ASSERT_NE(ptr, nullptr);
    cudaFreeHost(ptr);
}

// ---------------------------------------------------------------------------
// Memory operations
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, MemcpyDeviceToDevice) {
    void *src = nullptr, *dst = nullptr;
    ASSERT_EQ(cudaMalloc(&src, 64), cudaSuccess);
    ASSERT_EQ(cudaMalloc(&dst, 64), cudaSuccess);
    memset(src, 0xAB, 64);
    EXPECT_EQ(cudaMemcpy(dst, src, 64, cudaMemcpyDeviceToDevice), cudaSuccess);
    EXPECT_EQ(memcmp(dst, src, 64), 0);
    cudaFree(src);
    cudaFree(dst);
}

TEST(TpuMockAdapter, Memset) {
    void *ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&ptr, 32), cudaSuccess);
    EXPECT_EQ(cudaMemset(ptr, 0xCC, 32), cudaSuccess);
    char expected[32];
    memset(expected, 0xCC, 32);
    EXPECT_EQ(memcmp(ptr, expected, 32), 0);
    cudaFree(ptr);
}

// memcpy_async is absent from the mock → falls back to synchronous cudaMemcpy.
TEST(TpuMockAdapter, MemcpyAsyncFallsBackToSync) {
    void *src = nullptr, *dst = nullptr;
    ASSERT_EQ(cudaMalloc(&src, 16), cudaSuccess);
    ASSERT_EQ(cudaMalloc(&dst, 16), cudaSuccess);
    memset(src, 0x55, 16);
    EXPECT_EQ(
        cudaMemcpyAsync(dst, src, 16, cudaMemcpyDeviceToDevice, nullptr),
        cudaSuccess);
    EXPECT_EQ(memcmp(dst, src, 16), 0);
    cudaFree(src);
    cudaFree(dst);
}

// ---------------------------------------------------------------------------
// Pointer attribute queries
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, PointerGetAttributes_DeviceMemory) {
    void *ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&ptr, 64), cudaSuccess);
    cudaPointerAttributes attr{};
    EXPECT_EQ(cudaPointerGetAttributes(&attr, ptr), cudaSuccess);
    EXPECT_EQ(attr.type, cudaMemoryTypeDevice);
    EXPECT_EQ(attr.size, static_cast<size_t>(64));
    cudaFree(ptr);
}

TEST(TpuMockAdapter, PointerGetAttributes_HostMemory) {
    char host_buf[64] = {};
    cudaPointerAttributes attr{};
    EXPECT_EQ(cudaPointerGetAttributes(&attr, host_buf), cudaSuccess);
    EXPECT_EQ(attr.type, cudaMemoryTypeHost);
}

TEST(TpuMockAdapter, CuPointerGetAttribute_MemoryType_Device) {
    void *ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&ptr, 128), cudaSuccess);
    CUmemorytype mem_type = 0;
    EXPECT_EQ(cuPointerGetAttribute(&mem_type, CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                                    reinterpret_cast<CUdeviceptr>(ptr)),
              cudaSuccess);
    EXPECT_EQ(mem_type, static_cast<CUmemorytype>(CU_MEMORYTYPE_DEVICE));
    cudaFree(ptr);
}

TEST(TpuMockAdapter, CuPointerGetAttribute_MemoryType_Host) {
    char host_buf[32] = {};
    CUmemorytype mem_type = 0;
    EXPECT_EQ(cuPointerGetAttribute(&mem_type, CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                                    reinterpret_cast<CUdeviceptr>(host_buf)),
              cudaSuccess);
    EXPECT_EQ(mem_type, static_cast<CUmemorytype>(CU_MEMORYTYPE_HOST));
}

TEST(TpuMockAdapter, CuPointerGetAttribute_RangeSize) {
    void *ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&ptr, 512), cudaSuccess);
    size_t range_size = 0;
    EXPECT_EQ(cuPointerGetAttribute(&range_size,
                                    CU_POINTER_ATTRIBUTE_RANGE_SIZE,
                                    reinterpret_cast<CUdeviceptr>(ptr)),
              cudaSuccess);
    EXPECT_EQ(range_size, static_cast<size_t>(512));
    cudaFree(ptr);
}

// RangeSize query on host memory must return an error.
TEST(TpuMockAdapter, CuPointerGetAttribute_RangeSize_HostFails) {
    char host_buf[32] = {};
    size_t range_size = 0;
    EXPECT_NE(cuPointerGetAttribute(&range_size,
                                    CU_POINTER_ATTRIBUTE_RANGE_SIZE,
                                    reinterpret_cast<CUdeviceptr>(host_buf)),
              cudaSuccess);
}

// ---------------------------------------------------------------------------
// Peer access
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, DeviceCanAccessPeer_SameDevice) {
    int can_access = 0;
    EXPECT_EQ(cudaDeviceCanAccessPeer(&can_access, 0, 0), cudaSuccess);
    EXPECT_EQ(can_access, 1);
}

TEST(TpuMockAdapter, DeviceCanAccessPeer_DifferentDevices) {
    int can_access = 1;
    EXPECT_EQ(cudaDeviceCanAccessPeer(&can_access, 0, 1), cudaSuccess);
    EXPECT_EQ(can_access, 0);
}

// ---------------------------------------------------------------------------
// Device attributes
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, CuDeviceGetAttribute_DmaBufSupported) {
    int value = 0;
    EXPECT_EQ(
        cuDeviceGetAttribute(&value, CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED, 0),
        cudaSuccess);
    EXPECT_EQ(value, 1);
}

// ---------------------------------------------------------------------------
// Stream operations (stream_{create,destroy,synchronize} absent in mock →
// no-op success + nullptr sentinel)
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, StreamCreateSucceedsWithNullSentinel) {
    cudaStream_t stream = reinterpret_cast<cudaStream_t>(0xdeadbeef);
    EXPECT_EQ(cudaStreamCreate(&stream), cudaSuccess);
    EXPECT_EQ(stream, nullptr);
}

TEST(TpuMockAdapter, StreamDestroyOnNullSucceeds) {
    EXPECT_EQ(cudaStreamDestroy(nullptr), cudaSuccess);
}

TEST(TpuMockAdapter, StreamSynchronizeOnNullSucceeds) {
    EXPECT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);
}

// ---------------------------------------------------------------------------
// DMA-BUF handle (cuMemGetHandleForAddressRange)
// ---------------------------------------------------------------------------

TEST(TpuMockAdapter, MemGetHandleForAddressRange) {
    void *ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&ptr, 4096), cudaSuccess);
    int handle = 0;
    EXPECT_EQ(cuMemGetHandleForAddressRange(&handle,
                                            reinterpret_cast<CUdeviceptr>(ptr),
                                            4096, CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                                            0),
              cudaSuccess);
    // Mock returns a fake, non-zero fd sentinel.
    EXPECT_NE(handle, 0);
    cudaFree(ptr);
}

} // namespace

int main(int argc, char **argv) {
    // Point the shim at the mock adapter before the api() singleton is
    // initialized (function-local static → lazy init on first call).
    setenv("MC_TPU_ADAPTER_LIB", TPU_MOCK_ADAPTER_PATH, 1);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
