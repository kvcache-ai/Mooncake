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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/config.h"
#include "tent/common/utils/string_builder.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/transport/nvlink/nvlink_transport.h"

namespace mooncake {
namespace tent {
namespace {

// Regression test for the CUDA IPC handle-sharing bug: a caching allocator
// (e.g. PyTorch) sub-allocates multiple logical buffers inside one cudaMalloc
// segment. Registering the second buffer used to drop the segment's IPC
// handle (empty shm_path) while still tagging the buffer NVLink-capable, so
// cross-process NVLink submission for it failed. Every BufferDesc covering
// the same cudaMalloc segment must receive the SAME serialized IPC handle.
TEST(NVLinkIpcHandleSharingTest, TwoRangesInOneCudaMallocSegmentShareHandle) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        GTEST_SKIP() << "No CUDA device available";
    }

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    NVLinkTransport transport;
    std::string local_segment_name = "nvlink_ipc_test_segment";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());

    constexpr size_t kSegmentSize = 2 * 1024 * 1024;
    void* base = nullptr;
    ASSERT_EQ(cudaMalloc(&base, kSegmentSize), cudaSuccess);

    // Two logical buffers sub-allocated inside the one cudaMalloc segment,
    // as a caching allocator would produce.
    MemoryOptions options;
    BufferDesc first;
    first.addr = reinterpret_cast<uint64_t>(base) + 4096;
    first.length = 64 * 1024;
    first.location = "cuda:0";
    ASSERT_TRUE(transport.addMemoryBuffer(first, options).ok());

    // Some environments back cudaMalloc with VMM allocations, which are not
    // CUDA-IPC exportable and are skipped by design; nothing to test then.
    if (first.shm_path.empty()) {
        cudaFree(base);
        transport.uninstall();
        GTEST_SKIP() << "cudaMalloc allocation is VMM-backed; no IPC export";
    }

    BufferDesc second;
    second.addr = reinterpret_cast<uint64_t>(base) + 1024 * 1024;
    second.length = 128 * 1024;
    second.location = "cuda:0";
    ASSERT_TRUE(transport.addMemoryBuffer(second, options).ok());

    // Both descs carry the same, non-empty serialized IPC handle.
    EXPECT_FALSE(second.shm_path.empty());
    EXPECT_EQ(first.shm_path, second.shm_path);

    // Both are expanded to the full cudaMalloc segment.
    EXPECT_EQ(first.addr, reinterpret_cast<uint64_t>(base));
    EXPECT_EQ(second.addr, reinterpret_cast<uint64_t>(base));
    EXPECT_EQ(first.length, kSegmentSize);
    EXPECT_EQ(second.length, kSegmentSize);

    // Both are tagged NVLink-capable.
    EXPECT_NE(std::find(first.transports.begin(), first.transports.end(),
                        TransportType::NVLINK),
              first.transports.end());
    EXPECT_NE(std::find(second.transports.begin(), second.transports.end(),
                        TransportType::NVLINK),
              second.transports.end());

    // The shared handle equals a freshly exported one for the same segment.
    cudaIpcMemHandle_t fresh;
    ASSERT_EQ(cudaIpcGetMemHandle(&fresh, base), cudaSuccess);
    EXPECT_EQ(first.shm_path,
              serializeBinaryData(&fresh, sizeof(cudaIpcMemHandle_t)));

    cudaFree(base);
    ASSERT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
