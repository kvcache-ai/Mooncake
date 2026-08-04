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

#include <gtest/gtest.h>

#include <array>
#include <thread>

#include "tent/platform/cuda.h"

namespace mooncake {
namespace tent {
namespace {

TEST(CudaPlatformTest, WorkerSelectsTheDeviceOwningTheBuffer) {
    int device_count = 0;
    auto cuda_status = cudaGetDeviceCount(&device_count);
    if (cuda_status != cudaSuccess || device_count < 2) {
        cudaGetLastError();
        GTEST_SKIP() << "This test requires at least two CUDA devices";
    }

    constexpr int kTargetDevice = 1;
    constexpr size_t kBytes = 4096;
    ASSERT_EQ(cudaSetDevice(kTargetDevice), cudaSuccess);
    void* device_buffer = nullptr;
    ASSERT_EQ(cudaMalloc(&device_buffer, kBytes), cudaSuccess);

    std::array<unsigned char, kBytes> source{};
    source.fill(0x7b);
    CudaPlatform platform(nullptr);
    Status copy_status;
    int worker_device = -1;
    std::thread worker([&] {
        copy_status =
            platform.copy(device_buffer, source.data(), source.size());
        cudaGetDevice(&worker_device);
    });
    worker.join();

    EXPECT_TRUE(copy_status.ok()) << copy_status.ToString();
    EXPECT_EQ(worker_device, kTargetDevice);

    std::array<unsigned char, kBytes> destination{};
    ASSERT_EQ(cudaSetDevice(kTargetDevice), cudaSuccess);
    ASSERT_EQ(cudaMemcpy(destination.data(), device_buffer, kBytes,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(destination, source);
    EXPECT_EQ(cudaFree(device_buffer), cudaSuccess);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
