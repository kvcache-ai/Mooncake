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

// Real TCP sessions with a CPU-backed ACL mock: no NPU or driver is required.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#include <acl/acl.h>
#include "transfer_engine.h"

namespace {
struct DeviceRange {
    uintptr_t base;
    size_t size;
};
std::mutex range_mutex;
std::vector<DeviceRange> device_ranges;
std::atomic<int> singles{0}, set_device_error{0};
std::atomic<int> fail_kind{-1};
std::atomic<size_t> max_copy_size{0};
thread_local int current_device = -1;
constexpr uint32_t kDevice = 2;
constexpr int kUserDevice = 2;
constexpr aclError kCopyError = 100001;

bool isDevice(const void* ptr) {
    const auto addr = reinterpret_cast<uintptr_t>(ptr);
    std::lock_guard<std::mutex> lock(range_mutex);
    for (auto range : device_ranges)
        if (addr >= range.base && addr - range.base < range.size) return true;
    return false;
}
}  // namespace

extern "C" aclError aclrtPointerGetAttributes(const void* ptr,
                                              aclrtPtrAttributes* attr) {
    *attr = {};
    attr->location = {kDevice, isDevice(ptr)
                                   ? ACL_MEM_LOCATION_TYPE_DEVICE
                                   : ACL_MEM_LOCATION_TYPE_UNREGISTERED};
    return ACL_SUCCESS;
}

extern "C" aclError aclrtSetDevice(int32_t device) {
    if (set_device_error.load()) return set_device_error.load();
    current_device = device;
    return device == kUserDevice ? ACL_SUCCESS : kCopyError;
}

extern "C" aclError aclrtMemcpy(void* dst, size_t capacity, const void* src,
                                size_t size, aclrtMemcpyKind kind) {
    ++singles;
    if (current_device != kUserDevice || capacity < size) return kCopyError;
    if (static_cast<int>(kind) == fail_kind.load()) return kCopyError;
    max_copy_size.store(std::max(max_copy_size.load(), size));
    std::memcpy(dst, src, size);
    return ACL_SUCCESS;
}

namespace {
using namespace mooncake;

class TcpAscendStagingTest : public ::testing::Test {
   protected:
    void SetUp() override {
        singles = set_device_error = 0;
        fail_kind = -1;
        max_copy_size = 0;
        setenv("MC_TCP_SLICE_SIZE", "65536", 1);
        setenv("MC_TCP_LANES_PER_PEER", "8", 1);
        setenv("MC_NUM_TCP_IO_THREADS", "1", 1);
    }
    void TearDown() override {
        std::lock_guard<std::mutex> lock(range_mutex);
        device_ranges.clear();
        unsetenv("MC_TCP_SLICE_SIZE");
        unsetenv("MC_TCP_LANES_PER_PEER");
        unsetenv("MC_NUM_TCP_IO_THREADS");
    }
    void device(std::vector<char>& buffer) {
        std::lock_guard<std::mutex> lock(range_mutex);
        device_ranges.push_back(
            {reinterpret_cast<uintptr_t>(buffer.data()), buffer.size()});
    }
    void roundTrip(bool local_device, bool remote_device,
                   bool inject_h2d_failure = false,
                   bool inject_write_failure = false) {
        constexpr size_t length = 3 * 65536 + 17;
        constexpr size_t count = 8;
        std::vector<char> source(count * length), remote(count * length),
            result(count * length);
        for (size_t i = 0; i < source.size(); ++i)
            source[i] = static_cast<char>((i * 37 + 11) & 255);
        if (local_device) {
            device(source);
            device(result);
        }
        if (remote_device) device(remote);

        TransferEngine engine(false);
        // Pin the engine to the host transports: the staging cases below
        // install TCP explicitly, and an implicit Ascend Direct install fails
        // on a host whose stages are backed by the CPU ACL mock.
        ASSERT_EQ(
            engine.init(P2PHANDSHAKE, "127.0.0.2:0", "127.0.0.2", 0, "tcp"), 0);
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
        ASSERT_EQ(
            engine.registerLocalMemory(remote.data(), remote.size(), "cpu"), 0);
        const auto segment = engine.openSegment(engine.getLocalIpAndPort());
        auto run = [&](TransferRequest::OpCode opcode, char* local) {
            std::vector<TransferRequest> requests(count);
            for (size_t i = 0; i < count; ++i) {
                requests[i].opcode = opcode;
                requests[i].source = local + i * length;
                requests[i].length = length;
                requests[i].target_id = segment;
                requests[i].target_offset =
                    reinterpret_cast<uintptr_t>(remote.data() + i * length);
            }
            auto batch = engine.allocateBatchID(count);
            EXPECT_TRUE(engine.submitTransfer(batch, requests).ok());
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(10);
            size_t finished = 0;
            bool failed = false;
            do {
                finished = 0;
                for (size_t i = 0; i < count; ++i) {
                    TransferStatus status;
                    EXPECT_TRUE(
                        engine.getTransferStatus(batch, i, status).ok());
                    if (status.s == TransferStatusEnum::FAILED) failed = true;
                    if (status.s == TransferStatusEnum::COMPLETED ||
                        status.s == TransferStatusEnum::FAILED)
                        ++finished;
                }
                if (finished != count)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } while (finished != count &&
                     std::chrono::steady_clock::now() < deadline);
            EXPECT_EQ(finished, count);
            EXPECT_TRUE(engine.freeBatchID(batch).ok());
            return failed;
        };
        if (inject_write_failure) {
            if (!set_device_error.load())
                fail_kind = local_device ? ACL_MEMCPY_DEVICE_TO_HOST
                                         : ACL_MEMCPY_HOST_TO_DEVICE;
            EXPECT_TRUE(run(TransferRequest::WRITE, source.data()));
            EXPECT_EQ(engine.unregisterLocalMemory(remote.data()), 0);
            return;
        }
        ASSERT_FALSE(run(TransferRequest::WRITE, source.data()));
        ASSERT_EQ(remote, source);
        if (inject_h2d_failure) fail_kind = ACL_MEMCPY_HOST_TO_DEVICE;
        EXPECT_EQ(run(TransferRequest::READ, result.data()),
                  inject_h2d_failure);
        if (!inject_h2d_failure) {
            EXPECT_EQ(result, source);
        }
        if (local_device || remote_device) {
            EXPECT_GT(singles.load(), 0);
            EXPECT_LE(max_copy_size.load(), 65536u);
        } else {
            EXPECT_EQ(singles.load(), 0);
        }
        EXPECT_EQ(engine.unregisterLocalMemory(remote.data()), 0);
    }
};

TEST_F(TcpAscendStagingTest, HostToHost) { roundTrip(false, false); }
TEST_F(TcpAscendStagingTest, DeviceToHost) { roundTrip(true, false); }
TEST_F(TcpAscendStagingTest, HostToDevice) { roundTrip(false, true); }
TEST_F(TcpAscendStagingTest, DeviceToDevice) { roundTrip(true, true); }
TEST_F(TcpAscendStagingTest, ReadCopyFailureIsNotCompleted) {
    roundTrip(true, false, true);
}
TEST_F(TcpAscendStagingTest, WriteD2HFailureIsNotCompleted) {
    roundTrip(true, false, false, true);
}
TEST_F(TcpAscendStagingTest, WriteH2DFailureIsNotCompleted) {
    roundTrip(false, true, false, true);
}
TEST_F(TcpAscendStagingTest, DeviceSelectionFailureIsNotCompleted) {
    set_device_error = kCopyError;
    roundTrip(true, false, false, true);
    EXPECT_EQ(singles.load(), 0);
}
}  // namespace
