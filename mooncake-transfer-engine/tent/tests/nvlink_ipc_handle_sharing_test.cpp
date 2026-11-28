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

    const uint64_t base_addr = reinterpret_cast<uint64_t>(base);
    const uint64_t first_addr = base_addr + 4096;
    constexpr uint64_t first_length = 64 * 1024;
    const uint64_t second_addr = base_addr + 1024 * 1024;
    constexpr uint64_t second_length = 128 * 1024;

    // Two logical buffers sub-allocated inside the one cudaMalloc segment,
    // as a caching allocator would produce.
    MemoryOptions options;
    BufferDesc first;
    first.addr = first_addr;
    first.length = first_length;
    first.location = "cuda:0";
    ASSERT_TRUE(transport.addMemoryBuffer(first, options).ok());

    // Some environments back cudaMalloc with VMM allocations, which are not
    // CUDA-IPC exportable and are skipped by design; nothing to test then.
    auto first_attr_it = first.transport_attrs.find(TransportType::NVLINK);
    if (first_attr_it == first.transport_attrs.end() ||
        first_attr_it->second.empty()) {
        cudaFree(base);
        transport.uninstall();
        GTEST_SKIP() << "cudaMalloc allocation is VMM-backed; no IPC export";
    }

    BufferDesc second;
    second.addr = second_addr;
    second.length = second_length;
    second.location = "cuda:0";
    ASSERT_TRUE(transport.addMemoryBuffer(second, options).ok());

    EXPECT_EQ(first.addr, first_addr);
    EXPECT_EQ(first.length, first_length);
    EXPECT_EQ(second.addr, second_addr);
    EXPECT_EQ(second.length, second_length);
    EXPECT_TRUE(first.shm_path.empty());
    EXPECT_TRUE(second.shm_path.empty());

    auto second_attr_it = second.transport_attrs.find(TransportType::NVLINK);
    ASSERT_NE(second_attr_it, second.transport_attrs.end());
    EXPECT_EQ(first_attr_it->second, second_attr_it->second);

    // Both are tagged NVLink-capable.
    EXPECT_NE(std::find(first.transports.begin(), first.transports.end(),
                        TransportType::NVLINK),
              first.transports.end());
    EXPECT_NE(std::find(second.transports.begin(), second.transports.end(),
                        TransportType::NVLINK),
              second.transports.end());

    cudaIpcMemHandle_t fresh;
    ASSERT_EQ(cudaIpcGetMemHandle(&fresh, base), cudaSuccess);
    json parsed = json::parse(first_attr_it->second);
    ASSERT_TRUE(parsed.is_object());
    EXPECT_EQ(parsed.at("version").get<uint64_t>(), 1u);
    EXPECT_EQ(parsed.at("kind").get<std::string>(), "ipc");
    EXPECT_EQ(parsed.at("base_ptr").get<uint64_t>(), base_addr);
    EXPECT_EQ(parsed.at("alloc_size").get<uint64_t>(), kSegmentSize);
    EXPECT_EQ(parsed.at("handle").get<std::string>(),
              serializeBinaryData(&fresh, sizeof(cudaIpcMemHandle_t)));

    cudaFree(base);
    ASSERT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
