#include "gpu_read_staging_kernel.h"

#include <gtest/gtest.h>

#include <array>
#include <algorithm>
#include <vector>

namespace mooncake {

TEST(GpuReadScatter, PreservesFragmentOrderAndUnalignedBoundaryBytes) {
    int devices = 0;
    if (cudaGetDeviceCount(&devices) != cudaSuccess || devices == 0)
        GTEST_SKIP() << "CUDA device unavailable";
    constexpr size_t bytes = 16384;
    std::vector<unsigned char> input(bytes);
    for (size_t i = 0; i < bytes; ++i) input[i] = (i * 17 + i / 251) % 251;
    unsigned char* source = nullptr;
    unsigned char* destination = nullptr;
    GpuReadFragment* metadata = nullptr;
    cudaStream_t stream;
    ASSERT_EQ(cudaSuccess, cudaMalloc(&source, bytes));
    ASSERT_EQ(cudaSuccess, cudaMalloc(&destination, bytes));
    ASSERT_EQ(cudaSuccess, cudaMalloc(&metadata, 3 * sizeof(GpuReadFragment)));
    ASSERT_EQ(cudaSuccess, cudaStreamCreate(&stream));
    ASSERT_EQ(cudaSuccess,
              cudaMemcpy(source, input.data(), bytes, cudaMemcpyHostToDevice));
    ASSERT_EQ(cudaSuccess, cudaMemset(destination, 0xa5, bytes));
    const auto address = reinterpret_cast<uintptr_t>(destination);
    // One vectorized copy, one unaligned copy, and an odd-sized copy.
    const std::array<GpuReadFragment, 3> fragments{{
        {address + 8192, 0, 4096},
        {address + 3, 4097, 31},
        {address + 4096, 8192, 257},
    }};
    ASSERT_EQ(cudaSuccess,
              cudaMemcpy(metadata, fragments.data(), sizeof(fragments),
                         cudaMemcpyHostToDevice));
    ASSERT_EQ(cudaSuccess, LaunchGpuReadScatter(source, metadata, 3, stream));
    ASSERT_EQ(cudaSuccess, cudaStreamSynchronize(stream));
    std::vector<unsigned char> actual(bytes), expected(bytes, 0xa5);
    ASSERT_EQ(cudaSuccess, cudaMemcpy(actual.data(), destination, bytes,
                                      cudaMemcpyDeviceToHost));
    for (const auto& fragment : fragments) {
        const size_t start = fragment.destination - address;
        std::copy_n(input.begin() + fragment.offset, fragment.bytes,
                    expected.begin() + start);
    }
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(cudaSuccess, LaunchGpuReadScatter(source, metadata, 0, stream));
    EXPECT_EQ(cudaSuccess, cudaStreamDestroy(stream));
    EXPECT_EQ(cudaSuccess, cudaFree(metadata));
    EXPECT_EQ(cudaSuccess, cudaFree(destination));
    EXPECT_EQ(cudaSuccess, cudaFree(source));
}

}  // namespace mooncake
