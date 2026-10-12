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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/mnnvl/mnnvl_transport.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kBufferLength = 1 << 20;
constexpr uint32_t kPattern = 0x3c5aa5c3;

bool anyDevicePairWithoutPeerAccess(int device_count) {
    for (int i = 0; i < device_count; ++i) {
        for (int j = 0; j < device_count; ++j) {
            if (i == j) continue;
            int can_access = 0;
            if (cudaDeviceCanAccessPeer(&can_access, i, j) != cudaSuccess ||
                !can_access)
                return true;
        }
    }
    return false;
}

Status installEmptyLocalSegment(ControlService& metadata) {
    return metadata.segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            segment.name = "mnnvl_access_test_segment";
            segment.machine_id = "mnnvl_access_test_machine";
            segment.type = SegmentType::Memory;
            std::get<MemorySegmentDesc>(segment.detail).buffers.clear();
            return Status::OK();
        });
}

}  // namespace

// Regression guard: allocateLocalMemory used to grant access to every visible
// device in one cuMemSetAccess call. On a machine where some GPU has no P2P
// path to the owning one the driver rejects the whole call
// (CUDA_ERROR_INVALID_DEVICE) and no VMM buffer can be allocated unless
// CUDA_VISIBLE_DEVICES hides the other cards. Access must be granted per
// device, skipping the unreachable ones.
TEST(MnnvlAccessTest, AllocateLocalMemoryToleratesNonPeerDevices) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count < 2) {
        GTEST_SKIP() << "Needs at least two visible CUDA devices";
    }
    if (!anyDevicePairWithoutPeerAccess(device_count)) {
        GTEST_SKIP() << "All device pairs have P2P access; nothing to skip";
    }
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installEmptyLocalSegment(*metadata).ok());

    // POSIX-FD so cuMemCreate succeeds without IMEX: with the fabric handle
    // type a failing cuMemCreate silently falls back to cudaMalloc and the
    // test would not exercise the VMM path.
    auto conf = std::make_shared<Config>();
    conf->set("transports/nvlink/handle_type", std::string("posix_fd"));
    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_access_test_segment";
    auto status =
        transport.install(local_segment_name, metadata, nullptr, conf);
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    void* addr = nullptr;
    MemoryOptions options;
    options.location = "cuda:0";
    ASSERT_TRUE(
        transport.allocateLocalMemory(&addr, kBufferLength, options).ok());
    ASSERT_NE(addr, nullptr);

    // Device 0 owns the allocation and must be able to use it.
    std::vector<uint32_t> pattern(kBufferLength / sizeof(uint32_t), kPattern);
    ASSERT_EQ(
        cudaMemcpy(addr, pattern.data(), kBufferLength, cudaMemcpyHostToDevice),
        cudaSuccess);
    std::vector<uint32_t> readback(pattern.size());
    ASSERT_EQ(cudaMemcpy(readback.data(), addr, kBufferLength,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (uint32_t value : readback) {
        EXPECT_EQ(value, kPattern);
    }

    ASSERT_TRUE(transport.freeLocalMemory(addr, kBufferLength).ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

}  // namespace tent
}  // namespace mooncake
