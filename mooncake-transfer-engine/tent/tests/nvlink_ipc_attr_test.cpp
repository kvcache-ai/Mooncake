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
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/config.h"
#include "tent/common/utils/string_builder.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/transport/nvlink/nvlink_transport.h"

namespace {

struct FakeCudaSegment {
    uint64_t base = 0;
    size_t size = 0;
    cudaIpcMemHandle_t handle{};
    bool is_vmm = false;
    bool freed = false;
};

struct MockCudaState {
    int device_count = 2;
    int current_device = 0;
    std::vector<FakeCudaSegment> segments;
    size_t addr_range_calls = 0;
    size_t release_vmm_calls = 0;
    size_t get_handle_calls = 0;
    int last_get_handle_device = -1;
    void* last_get_handle_ptr = nullptr;
    size_t open_handle_calls = 0;
    cudaError_t next_open_error = cudaSuccess;
    uint64_t next_mapped_base = 0x900000000000ULL;
    std::unordered_map<int, uint64_t> last_mapped_by_dev;
    std::set<void*> opened_ptrs;
    std::set<void*> closed_ptrs;

    void reset() { *this = MockCudaState{}; }
};

MockCudaState& mockCuda() {
    static MockCudaState state;
    return state;
}

cudaIpcMemHandle_t makeHandle(uint8_t tag) {
    cudaIpcMemHandle_t handle{};
    for (size_t i = 0; i < sizeof(handle.reserved); ++i) {
        handle.reserved[i] = static_cast<char>((tag + i) & 0xFF);
    }
    return handle;
}

}  // namespace

extern "C" {

cudaError_t CUDARTAPI __wrap_cudaGetDevice(int* device) {
    *device = mockCuda().current_device;
    return cudaSuccess;
}

cudaError_t CUDARTAPI __wrap_cudaSetDevice(int device) {
    if (device < 0 || device >= mockCuda().device_count)
        return cudaErrorInvalidDevice;
    mockCuda().current_device = device;
    return cudaSuccess;
}

cudaError_t CUDARTAPI __wrap_cudaGetDeviceCount(int* count) {
    *count = mockCuda().device_count;
    return cudaSuccess;
}

cudaError_t CUDARTAPI __wrap_cudaDeviceCanAccessPeer(int* can_access, int dev,
                                                     int peer) {
    *can_access = (dev != peer) ? 1 : 0;
    return cudaSuccess;
}

cudaError_t CUDARTAPI __wrap_cudaDeviceEnablePeerAccess(int, unsigned int) {
    return cudaSuccess;
}

CUresult CUDAAPI __wrap_cuMemRetainAllocationHandle(
    CUmemGenericAllocationHandle* handle, void* addr) {
    const uint64_t target = reinterpret_cast<uint64_t>(addr);
    for (const auto& seg : mockCuda().segments) {
        if (!seg.freed && seg.is_vmm && target >= seg.base &&
            target - seg.base < seg.size) {
            if (handle) *handle = static_cast<CUmemGenericAllocationHandle>(1);
            return CUDA_SUCCESS;
        }
    }
    return CUDA_ERROR_INVALID_VALUE;
}

CUresult CUDAAPI __wrap_cuMemRelease(CUmemGenericAllocationHandle) {
    ++mockCuda().release_vmm_calls;
    return CUDA_SUCCESS;
}

CUresult CUDAAPI __wrap_cuMemGetAddressRange_v2(CUdeviceptr* pbase,
                                                size_t* psize,
                                                CUdeviceptr dptr) {
    auto& state = mockCuda();
    ++state.addr_range_calls;
    const uint64_t target = static_cast<uint64_t>(dptr);
    for (const auto& seg : state.segments) {
        if (!seg.freed && target >= seg.base && target - seg.base < seg.size) {
            if (pbase) *pbase = static_cast<CUdeviceptr>(seg.base);
            if (psize) *psize = seg.size;
            return CUDA_SUCCESS;
        }
    }
    return CUDA_ERROR_INVALID_VALUE;
}

cudaError_t CUDARTAPI __wrap_cudaIpcGetMemHandle(cudaIpcMemHandle_t* handle,
                                                 void* dev_ptr) {
    auto& state = mockCuda();
    ++state.get_handle_calls;
    state.last_get_handle_ptr = dev_ptr;
    state.last_get_handle_device = state.current_device;
    const uint64_t target = reinterpret_cast<uint64_t>(dev_ptr);
    for (const auto& seg : state.segments) {
        if (!seg.freed && !seg.is_vmm && seg.base == target) {
            *handle = seg.handle;
            return cudaSuccess;
        }
    }
    return cudaErrorInvalidValue;
}

cudaError_t CUDARTAPI __wrap_cudaIpcOpenMemHandle(void** dev_ptr,
                                                  cudaIpcMemHandle_t,
                                                  unsigned int) {
    auto& state = mockCuda();
    ++state.open_handle_calls;
    if (state.next_open_error != cudaSuccess) {
        return std::exchange(state.next_open_error, cudaSuccess);
    }
    const uint64_t mapped = state.next_mapped_base;
    state.next_mapped_base += 0x10000000ULL;
    *dev_ptr = reinterpret_cast<void*>(mapped);
    state.last_mapped_by_dev[state.current_device] = mapped;
    state.opened_ptrs.insert(*dev_ptr);
    return cudaSuccess;
}

cudaError_t CUDARTAPI __wrap_cudaIpcCloseMemHandle(void* dev_ptr) {
    mockCuda().closed_ptrs.insert(dev_ptr);
    return cudaSuccess;
}
}

namespace mooncake {
namespace tent {

class NVLinkTransportTestPeer {
   public:
    static Status relocateOnDevice(NVLinkTransport& transport,
                                   uint64_t& address, uint64_t length,
                                   SegmentID target_id, int src_device) {
        return transport.relocateSharedMemoryAddress(address, length, target_id,
                                                     src_device);
    }

    static size_t mappingCount(NVLinkTransport& transport,
                               SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        return it == transport.relocate_map_.end() ? 0 : it->second.size();
    }

    static size_t deviceMappingCount(NVLinkTransport& transport,
                                     SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        if (it == transport.relocate_map_.end()) return 0;
        size_t total = 0;
        for (const auto& by_addr : it->second) total += by_addr.second.size();
        return total;
    }
};

namespace {

constexpr uint64_t kSeg1Base = 0x700000000000ULL;
constexpr uint64_t kSeg2Base = 0x710000000000ULL;
constexpr size_t kSegSize = 2 * 1024 * 1024;

class NVLinkIpcAttrTest : public ::testing::Test {
   protected:
    void SetUp() override {
        mockCuda().reset();
        metadata_ = std::make_shared<ControlService>("p2p", "", nullptr);
        ASSERT_TRUE(publish(*metadata_, {}).ok());
        std::string name = "local_seg";
        ASSERT_TRUE(
            transport_
                .install(name, metadata_, nullptr, std::make_shared<Config>())
                .ok());
    }

    void TearDown() override {
        (void)transport_.uninstall();
        mockCuda().reset();
    }

    static Status publish(ControlService& meta, std::vector<BufferDesc> buffers,
                          const std::string& machine_id = "machine-a") {
        return meta.segmentManager().updateLocal(
            [&](SegmentDesc& seg) -> Status {
                seg.name = "nvlink_attr_test_seg";
                seg.machine_id = machine_id;
                seg.type = SegmentType::Memory;
                std::get<MemorySegmentDesc>(seg.detail).buffers =
                    std::move(buffers);
                return Status::OK();
            });
    }

    static BufferDesc makeBuffer(uint64_t addr, uint64_t len,
                                 std::string loc = "cuda:0") {
        BufferDesc desc{};
        desc.addr = addr;
        desc.length = len;
        desc.location = std::move(loc);
        return desc;
    }

    static std::string makeIpcAttrJson(uint64_t base, uint64_t size,
                                       const cudaIpcMemHandle_t& handle) {
        return json{{"version", 1},
                    {"kind", "ipc"},
                    {"base_ptr", base},
                    {"alloc_size", size},
                    {"handle",
                     serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t))}}
            .dump();
    }

    Status relocate(uint64_t& addr, uint64_t len, int dev = 0,
                    SegmentID id = LOCAL_SEGMENT_ID) {
        return NVLinkTransportTestPeer::relocateOnDevice(transport_, addr, len,
                                                         id, dev);
    }

    size_t mappings(SegmentID id = LOCAL_SEGMENT_ID) {
        return NVLinkTransportTestPeer::mappingCount(transport_, id);
    }

    size_t devMappings(SegmentID id = LOCAL_SEGMENT_ID) {
        return NVLinkTransportTestPeer::deviceMappingCount(transport_, id);
    }

    std::shared_ptr<ControlService> metadata_;
    NVLinkTransport transport_;
};

TEST_F(NVLinkIpcAttrTest, SubBuffersShareEncodedAttrAndPreserveLogicalRange) {
    const auto handle = makeHandle(0x21);
    mockCuda().segments.push_back({kSeg1Base, kSegSize, handle, false, false});
    mockCuda().segments.push_back(
        {kSeg2Base, kSegSize, makeHandle(0x22), true, false});

    MemoryOptions opts;
    auto first = makeBuffer(kSeg1Base + 4096, 64 * 1024, "cuda:1");
    auto second = makeBuffer(kSeg1Base + 1024 * 1024, 128 * 1024, "cuda:1");
    ASSERT_TRUE(transport_.addMemoryBuffer(first, opts).ok());
    ASSERT_TRUE(transport_.addMemoryBuffer(second, opts).ok());

    EXPECT_EQ(first.addr, kSeg1Base + 4096);
    EXPECT_EQ(first.length, 64u * 1024u);
    EXPECT_EQ(second.addr, kSeg1Base + 1024u * 1024u);
    EXPECT_EQ(second.length, 128u * 1024u);
    EXPECT_TRUE(first.shm_path.empty());
    EXPECT_TRUE(second.shm_path.empty());

    ASSERT_TRUE(first.transport_attrs.count(TransportType::NVLINK));
    EXPECT_EQ(first.transport_attrs.at(TransportType::NVLINK),
              second.transport_attrs.at(TransportType::NVLINK));
    json parsed = json::parse(first.transport_attrs.at(TransportType::NVLINK));
    EXPECT_EQ(parsed.at("version").get<uint64_t>(), 1u);
    EXPECT_EQ(parsed.at("kind").get<std::string>(), "ipc");
    EXPECT_EQ(parsed.at("base_ptr").get<uint64_t>(), kSeg1Base);
    EXPECT_EQ(parsed.at("alloc_size").get<uint64_t>(), kSegSize);
    EXPECT_EQ(parsed.at("handle").get<std::string>(),
              serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t)));

    EXPECT_EQ(mockCuda().get_handle_calls, 1u);
    EXPECT_EQ(mockCuda().last_get_handle_ptr,
              reinterpret_cast<void*>(kSeg1Base));
    EXPECT_EQ(mockCuda().last_get_handle_device, 1);
    EXPECT_EQ(mockCuda().current_device, 0);

    ASSERT_TRUE(transport_.addMemoryBuffer(first, opts).ok());
    EXPECT_EQ(std::count(first.transports.begin(), first.transports.end(),
                         TransportType::NVLINK),
              1);
    EXPECT_EQ(mockCuda().get_handle_calls, 1u);

    auto bad_zero = makeBuffer(0, 4096);
    auto bad_len = makeBuffer(kSeg1Base, 0);
    auto bad_overflow = makeBuffer(UINT64_MAX - 16, 32);
    auto bad_overrun = makeBuffer(kSeg1Base + kSegSize - 1024, 4096, "cuda:1");
    EXPECT_TRUE(transport_.addMemoryBuffer(bad_zero, opts).IsInvalidArgument());
    EXPECT_TRUE(transport_.addMemoryBuffer(bad_len, opts).IsInvalidArgument());
    EXPECT_TRUE(
        transport_.addMemoryBuffer(bad_overflow, opts).IsInvalidArgument());
    EXPECT_TRUE(
        transport_.addMemoryBuffer(bad_overrun, opts).IsInvalidArgument());
    EXPECT_TRUE(bad_overrun.transport_attrs.empty());
    EXPECT_EQ(mockCuda().current_device, 0);

    auto vmm = makeBuffer(kSeg2Base + 4096, 4096, "cuda:1");
    ASSERT_TRUE(transport_.addMemoryBuffer(vmm, opts).ok());
    EXPECT_TRUE(vmm.transport_attrs.empty());
    EXPECT_TRUE(vmm.transports.empty());
    EXPECT_EQ(mockCuda().release_vmm_calls, 1u);
}

TEST_F(NVLinkIpcAttrTest,
       RelocateOffsetsFromEncodedBaseAndEnforcesLogicalBounds) {
    const auto handle = makeHandle(0x51);
    auto first = makeBuffer(kSeg1Base + 4096, 64 * 1024, "cuda:99");
    auto second = makeBuffer(kSeg1Base + 1024 * 1024, 128 * 1024, "cuda:99");
    const std::string attr = makeIpcAttrJson(kSeg1Base, kSegSize, handle);
    first.transport_attrs[TransportType::NVLINK] = attr;
    second.transport_attrs[TransportType::NVLINK] = attr;
    ASSERT_TRUE(publish(*metadata_, {first, second}).ok());

    uint64_t dest1 = first.addr + 128;
    uint64_t dest2 = second.addr + 512;
    ASSERT_TRUE(relocate(dest1, 4096, 0).ok());
    ASSERT_TRUE(relocate(dest2, 8192, 0).ok());
    const uint64_t mapped_dev0 = mockCuda().last_mapped_by_dev.at(0);
    EXPECT_EQ(mockCuda().open_handle_calls, 1u);
    EXPECT_EQ(dest1, mapped_dev0 + (first.addr + 128 - kSeg1Base));
    EXPECT_EQ(dest2, mapped_dev0 + (second.addr + 512 - kSeg1Base));

    uint64_t repeat = first.addr + 128;
    uint64_t cpu_src = second.addr + 1024;
    ASSERT_TRUE(relocate(repeat, 4096, 0).ok());
    ASSERT_TRUE(relocate(cpu_src, 2048, -1).ok());
    EXPECT_EQ(repeat, dest1);
    EXPECT_EQ(cpu_src, mapped_dev0 + (second.addr + 1024 - kSeg1Base));
    EXPECT_EQ(mockCuda().open_handle_calls, 1u);

    uint64_t dev1_first = first.addr + 64;
    uint64_t dev1_second = second.addr + 256;
    ASSERT_TRUE(relocate(dev1_first, 4096, 1).ok());
    ASSERT_TRUE(relocate(dev1_second, 4096, 1).ok());
    const uint64_t mapped_dev1 = mockCuda().last_mapped_by_dev.at(1);
    EXPECT_NE(mapped_dev0, mapped_dev1);
    EXPECT_EQ(mockCuda().open_handle_calls, 2u);
    EXPECT_EQ(mockCuda().current_device, 0);
    EXPECT_EQ(mappings(), 1u);
    EXPECT_EQ(devMappings(), 2u);
    EXPECT_EQ(dev1_first, mapped_dev1 + (first.addr + 64 - kSeg1Base));
    EXPECT_EQ(dev1_second, mapped_dev1 + (second.addr + 256 - kSeg1Base));

    uint64_t gap_before = kSeg1Base + 128;
    uint64_t gap_middle = kSeg1Base + 512 * 1024;
    uint64_t straddle = first.addr + first.length - 64;
    uint64_t overflow = UINT64_MAX - 16;
    EXPECT_TRUE(relocate(gap_before, 256).IsNeedsRefreshCache());
    EXPECT_TRUE(relocate(gap_middle, 4096).IsNeedsRefreshCache());
    EXPECT_TRUE(relocate(straddle, 128).IsNeedsRefreshCache());
    EXPECT_TRUE(relocate(overflow, 32).IsInvalidArgument());
    EXPECT_EQ(mockCuda().open_handle_calls, 2u);
}

TEST_F(NVLinkIpcAttrTest, LegacyShmPathAndVmmBoundary) {
    const auto handle = makeHandle(0x81);
    mockCuda().segments.push_back({kSeg1Base, kSegSize, handle, false, false});

    auto legacy = makeBuffer(0x50000000ULL, 64 * 1024, "cuda:99");
    legacy.shm_path = serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
    auto cpu_shm = makeBuffer(0x60000000ULL, 4096, "cpu:0");
    cpu_shm.shm_path = "/mooncake_shm_cpu_buf";
    auto bad_hex = makeBuffer(0x61000000ULL, 4096, "cuda:0");
    bad_hex.shm_path = "not-valid-hex!";
    ASSERT_TRUE(publish(*metadata_, {legacy, cpu_shm, bad_hex}).ok());

    uint64_t dest = legacy.addr + 256;
    ASSERT_TRUE(relocate(dest, 1024).ok());
    EXPECT_EQ(dest, mockCuda().last_mapped_by_dev.at(0) + 256);

    uint64_t cpu_dest = cpu_shm.addr;
    uint64_t bad_dest = bad_hex.addr;
    EXPECT_TRUE(relocate(cpu_dest, 1024).IsInvalidArgument());
    EXPECT_TRUE(relocate(bad_dest, 1024).IsInvalidArgument());
    EXPECT_EQ(mockCuda().open_handle_calls, 1u);

    auto peer_meta = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t peer_port = 0;
    ASSERT_TRUE(peer_meta->start(peer_port, false, 1).ok());

    auto peer_buf = makeBuffer(kSeg1Base + 4096, 64 * 1024);
    ASSERT_TRUE(transport_.addMemoryBuffer(peer_buf, MemoryOptions{}).ok());
    ASSERT_TRUE(publish(*peer_meta, {peer_buf}).ok());

    SegmentID peer_id = 0;
    ASSERT_TRUE(
        metadata_->segmentManager()
            .openRemote(peer_id, "127.0.0.1:" + std::to_string(peer_port))
            .ok());
    EXPECT_TRUE(transport_
                    .mapRemoteMemory(peer_id,
                                     reinterpret_cast<void*>(peer_buf.addr),
                                     peer_buf.length)
                    .IsNotImplemented());

    uint64_t remote_dest = peer_buf.addr + 128;
    ASSERT_TRUE(relocate(remote_dest, 1024, 0, peer_id).ok());
    EXPECT_EQ(remote_dest, mockCuda().last_mapped_by_dev.at(0) +
                               (peer_buf.addr + 128 - kSeg1Base));
}

TEST_F(NVLinkIpcAttrTest, RejectsMalformedAttrsAndOpenFailure) {
    const auto handle = makeHandle(0xA1);
    const std::string hex =
        serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));
    constexpr uint64_t kAddr = kSeg1Base + 4096;
    constexpr uint64_t kLen = 64 * 1024;

    auto withField = [&](const char* key, json value) {
        json j = {{"version", 1},
                  {"kind", "ipc"},
                  {"base_ptr", kSeg1Base},
                  {"alloc_size", kSegSize},
                  {"handle", hex}};
        j[key] = std::move(value);
        return j.dump();
    };

    struct BadCase {
        const char* name;
        std::string loc;
        uint64_t addr;
        uint64_t len;
        std::string attr;
    };
    const std::vector<BadCase> cases = {
        {"bad_json", "cuda:0", kAddr, kLen, "{broken"},
        {"bad_version", "cuda:0", kAddr, kLen, withField("version", 2)},
        {"bad_kind", "cuda:0", kAddr, kLen, withField("kind", "vmm")},
        {"zero_size", "cuda:0", kAddr, kLen, withField("alloc_size", 0)},
        {"overflow", "cuda:0", kAddr, kLen,
         withField("base_ptr", UINT64_MAX - 16)},
        {"bad_handle", "cuda:0", kAddr, kLen,
         withField("handle", std::string(hex.size(), 'Z'))},
        {"non_cuda", "cpu:0", kAddr, kLen,
         makeIpcAttrJson(kSeg1Base, kSegSize, handle)},
        {"outside_seg", "cuda:0", kSeg1Base + kSegSize - 1024, 4096,
         makeIpcAttrJson(kSeg1Base, kSegSize, handle)},
    };

    for (const auto& tc : cases) {
        SCOPED_TRACE(tc.name);
        auto buf = makeBuffer(tc.addr, tc.len, tc.loc);
        buf.transport_attrs[TransportType::NVLINK] = tc.attr;
        ASSERT_TRUE(publish(*metadata_, {buf}).ok());
        uint64_t dest = tc.addr;
        EXPECT_TRUE(relocate(dest, 64).IsInvalidArgument());
        EXPECT_EQ(dest, tc.addr);
    }
    EXPECT_EQ(mockCuda().open_handle_calls, 0u);

    auto valid = makeBuffer(kAddr, kLen);
    valid.transport_attrs[TransportType::NVLINK] =
        makeIpcAttrJson(kSeg1Base, kSegSize, handle);
    ASSERT_TRUE(publish(*metadata_, {valid}).ok());
    mockCuda().next_open_error = cudaErrorInvalidResourceHandle;
    uint64_t dest = kAddr;
    EXPECT_FALSE(relocate(dest, 64).ok());
    EXPECT_EQ(mappings(), 0u);
}

TEST_F(NVLinkIpcAttrTest, RemoveRefCountsAfterFreeAndUninstallInvalidates) {
    const auto h1 = makeHandle(0xB1);
    const auto h2 = makeHandle(0xB2);
    mockCuda().segments.push_back({kSeg1Base, kSegSize, h1, false, false});

    MemoryOptions opts;
    auto first = makeBuffer(kSeg1Base + 4096, 64 * 1024);
    auto second = makeBuffer(kSeg1Base + 1024 * 1024, 64 * 1024);
    ASSERT_TRUE(transport_.addMemoryBuffer(first, opts).ok());
    ASSERT_TRUE(transport_.addMemoryBuffer(second, opts).ok());
    EXPECT_EQ(mockCuda().get_handle_calls, 1u);

    first.shm_path = "/other_transport";
    ASSERT_TRUE(transport_.removeMemoryBuffer(first).ok());
    EXPECT_EQ(first.transport_attrs.count(TransportType::NVLINK), 0u);
    EXPECT_EQ(first.shm_path, "/other_transport");
    ASSERT_TRUE(transport_.removeMemoryBuffer(first).ok());

    auto third = makeBuffer(kSeg1Base + 512 * 1024, 32 * 1024);
    ASSERT_TRUE(transport_.addMemoryBuffer(third, opts).ok());
    EXPECT_EQ(mockCuda().get_handle_calls, 1u);
    ASSERT_TRUE(transport_.removeMemoryBuffer(third).ok());

    mockCuda().segments[0].freed = true;
    const size_t addr_range_before = mockCuda().addr_range_calls;
    ASSERT_TRUE(transport_.removeMemoryBuffer(second).ok());
    EXPECT_EQ(mockCuda().addr_range_calls, addr_range_before);

    mockCuda().segments[0].freed = false;
    mockCuda().segments[0].handle = h2;
    auto fresh = makeBuffer(kSeg1Base + 4096, 64 * 1024);
    ASSERT_TRUE(transport_.addMemoryBuffer(fresh, opts).ok());
    EXPECT_EQ(mockCuda().get_handle_calls, 2u);
    EXPECT_EQ(json::parse(fresh.transport_attrs.at(TransportType::NVLINK))
                  .at("handle")
                  .get<std::string>(),
              serializeBinaryData(&h2, sizeof(cudaIpcMemHandle_t)));

    ASSERT_TRUE(publish(*metadata_, {fresh}).ok());
    uint64_t a0 = fresh.addr + 16;
    uint64_t a1 = fresh.addr + 16;
    ASSERT_TRUE(relocate(a0, 1024, 0).ok());
    ASSERT_TRUE(relocate(a1, 1024, 1).ok());
    const auto opened = mockCuda().opened_ptrs;
    ASSERT_EQ(opened.size(), 2u);

    ASSERT_TRUE(transport_.uninstall().ok());
    EXPECT_EQ(mockCuda().closed_ptrs, opened);
    EXPECT_EQ(mappings(), 0u);

    std::string name = "local_seg";
    ASSERT_TRUE(
        transport_.install(name, metadata_, nullptr, std::make_shared<Config>())
            .ok());
    uint64_t reopened = fresh.addr + 16;
    ASSERT_TRUE(relocate(reopened, 1024, 0).ok());
    EXPECT_NE(reopened, a0);
    auto after_reinstall = makeBuffer(kSeg1Base + 4096, 64 * 1024);
    ASSERT_TRUE(transport_.addMemoryBuffer(after_reinstall, opts).ok());
    EXPECT_EQ(mockCuda().get_handle_calls, 3u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
