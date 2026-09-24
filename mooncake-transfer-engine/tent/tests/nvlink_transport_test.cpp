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

#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/config.h"
#include "tent/common/utils/string_builder.h"
#include "tent/runtime/control_plane.h"
#include "tent/transport/nvlink/nvlink_transport.h"

namespace mooncake {
namespace tent {

class NVLinkTransportTestPeer {
   public:
    static Status relocate(NVLinkTransport& transport, uint64_t& address,
                           uint64_t length, SegmentID target_id) {
        return transport.relocateSharedMemoryAddress(address, length,
                                                     target_id);
    }

    static size_t mappingCount(NVLinkTransport& transport,
                               SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        return it == transport.relocate_map_.end() ? 0 : it->second.size();
    }
};

namespace {

// A peer BufferDesc can carry a device ordinal that is invalid (or refers to
// a different physical GPU) in the local process: ordinals are relative to
// the PEER's CUDA_VISIBLE_DEVICES. Regression guard for the bug where
// relocateSharedMemoryAddress switched to the peer ordinal before opening
// the IPC handle.
constexpr uint64_t kRemoteAddress = 0x10000000;
constexpr size_t kBufferLength = 4096;
constexpr uint32_t kPattern = 0x5a3c69e7;

// CUDA IPC handles cannot be opened by the process that exported them, so
// these tests obtain a handle from a forked child: the child allocates a
// device buffer, fills it with a known pattern, exports the IPC handle and
// stays alive (holding the allocation) until the parent is done.
//
// The parent must not call into CUDA before fork(): the child would inherit
// the initialized runtime and its own CUDA calls would be unreliable. The
// parent's first CUDA call therefore happens inside
// relocateSharedMemoryAddress(), well after fork().
class PeerExporter {
   public:
    PeerExporter() {
        if (pipe(ready_pipe_) != 0 || pipe(stop_pipe_) != 0) return;

        pid_t child = fork();
        if (child < 0) return;
        if (child == 0) {
            // Child: allocate, fill, export, then block until the parent is
            // done so the exported allocation stays alive.
            close(ready_pipe_[0]);
            close(stop_pipe_[1]);
            _exit(childMain(ready_pipe_[1], stop_pipe_[0]));
        }

        close(ready_pipe_[1]);
        close(stop_pipe_[0]);
        child_pid_ = child;
        valid_ = readExportResult();
    }

    ~PeerExporter() {
        if (child_pid_ > 0) {
            char done = 1;
            (void)write(stop_pipe_[1], &done, sizeof(done));
            (void)waitpid(child_pid_, nullptr, 0);
        }
        if (ready_pipe_[0] >= 0) close(ready_pipe_[0]);
        if (stop_pipe_[1] >= 0) close(stop_pipe_[1]);
    }

    bool forkOk() const { return child_pid_ > 0; }
    // 0 = handle exported fine; 1 = no CUDA device in the child.
    int childStatus() const { return child_status_; }
    const std::string& serializedHandle() const { return serialized_handle_; }

   private:
    static int childMain(int ready_fd, int stop_fd) {
        int device_count = 0;
        if (cudaGetDeviceCount(&device_count) != cudaSuccess ||
            device_count <= 0) {
            int32_t status = 1;
            (void)write(ready_fd, &status, sizeof(status));
            return 0;
        }

        void* ptr = nullptr;
        if (cudaMalloc(&ptr, kBufferLength) != cudaSuccess) return 2;
        std::vector<uint32_t> pattern(kBufferLength / sizeof(uint32_t),
                                      kPattern);
        if (cudaMemcpy(ptr, pattern.data(), kBufferLength,
                       cudaMemcpyHostToDevice) != cudaSuccess)
            return 3;
        cudaIpcMemHandle_t handle;
        if (cudaIpcGetMemHandle(&handle, ptr) != cudaSuccess) return 4;

        int32_t status = 0;
        size_t length = kBufferLength;
        if (write(ready_fd, &status, sizeof(status)) != sizeof(status))
            return 5;
        if (write(ready_fd, &length, sizeof(length)) != sizeof(length))
            return 6;
        if (write(ready_fd, &handle, sizeof(handle)) != sizeof(handle))
            return 7;

        // Hold the allocation until the parent is done with the mapping.
        char done = 0;
        while (read(stop_fd, &done, sizeof(done)) < 0 && errno == EINTR) {
        }
        return 0;
    }

    bool readExportResult() {
        int32_t status = -1;
        if (read(ready_pipe_[0], &status, sizeof(status)) !=
            (ssize_t)sizeof(status))
            return false;
        child_status_ = status;
        if (status != 0) return true;  // no CUDA in child; skip, not fail

        size_t length = 0;
        cudaIpcMemHandle_t handle{};
        if (read(ready_pipe_[0], &length, sizeof(length)) !=
            (ssize_t)sizeof(length))
            return false;
        if (read(ready_pipe_[0], &handle, sizeof(handle)) !=
            (ssize_t)sizeof(handle))
            return false;
        serialized_handle_ = serializeBinaryData(&handle, sizeof(handle));
        return true;
    }

    int ready_pipe_[2]{-1, -1};
    int stop_pipe_[2]{-1, -1};
    pid_t child_pid_{-1};
    int child_status_{-1};
    std::string serialized_handle_;
    bool valid_{false};

   public:
    bool valid() const { return valid_; }
};

Status installLocalSegmentWithIpcHandle(ControlService& metadata,
                                        const std::string& serialized_handle,
                                        const std::string& peer_location,
                                        uint64_t remote_addr, size_t length) {
    return metadata.segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            segment.name = "nvlink_test_segment";
            segment.machine_id = "nvlink_test_machine";
            segment.type = SegmentType::Memory;
            auto& memory = std::get<MemorySegmentDesc>(segment.detail);
            memory.buffers.clear();
            BufferDesc buffer;
            buffer.addr = remote_addr;
            buffer.length = length;
            buffer.location = peer_location;
            buffer.shm_path = serialized_handle;
            memory.buffers.push_back(std::move(buffer));
            return Status::OK();
        });
}

void verifyRelocatedData(uint64_t address, size_t length) {
    ASSERT_LE(length, kBufferLength);
    std::vector<uint32_t> readback(length / sizeof(uint32_t));
    ASSERT_EQ(cudaMemcpy(readback.data(), reinterpret_cast<void*>(address),
                         length, cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (uint32_t value : readback) {
        EXPECT_EQ(value, kPattern);
    }
}

}  // namespace

TEST(NVLinkTransportTest, RelocateToleratesOutOfRangePeerDeviceOrdinal) {
    PeerExporter exporter;
    if (!exporter.forkOk() || !exporter.valid()) {
        GTEST_SKIP() << "Peer export failed (fork/pipe error)";
    }
    if (exporter.childStatus() != 0) {
        GTEST_SKIP() << "No CUDA device available";
    }

    // The peer claims its buffer lives on cuda:99 — an ordinal that is out
    // of range in this process, exactly what a peer with a larger
    // CUDA_VISIBLE_DEVICES produces.
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithIpcHandle(
                    *metadata, exporter.serializedHandle(), "cuda:99",
                    kRemoteAddress, kBufferLength)
                    .ok());

    NVLinkTransport transport;
    std::string local_segment_name = "nvlink_test_segment";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());

    uint64_t dest_addr = kRemoteAddress + 64;
    ASSERT_TRUE(NVLinkTransportTestPeer::relocate(
                    transport, dest_addr, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());
    EXPECT_EQ(
        NVLinkTransportTestPeer::mappingCount(transport, LOCAL_SEGMENT_ID), 1u);
    // The relocated address must alias the child's allocation: dest_addr is
    // now mapped_base + 64.
    verifyRelocatedData(dest_addr, kBufferLength - 64);
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(NVLinkTransportTest,
     RelocateCachesSingleMappingForConcurrentPeersWithBogusOrdinal) {
    PeerExporter exporter;
    if (!exporter.forkOk() || !exporter.valid()) {
        GTEST_SKIP() << "Peer export failed (fork/pipe error)";
    }
    if (exporter.childStatus() != 0) {
        GTEST_SKIP() << "No CUDA device available";
    }

    int device_count = 0;
    ASSERT_EQ(cudaGetDeviceCount(&device_count), cudaSuccess);
    const std::string bogus_location =
        "cuda:" + std::to_string(device_count + 8);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithIpcHandle(
                    *metadata, exporter.serializedHandle(), bogus_location,
                    kRemoteAddress, kBufferLength)
                    .ok());

    NVLinkTransport transport;
    std::string local_segment_name = "nvlink_test_segment";
    ASSERT_TRUE(transport
                    .install(local_segment_name, metadata, nullptr,
                             std::make_shared<Config>())
                    .ok());

    constexpr size_t kThreadCount = 8;
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<uint64_t> relocated(kThreadCount, kRemoteAddress);
    std::vector<uint8_t> succeeded(kThreadCount, 0);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);
    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            succeeded[i] =
                NVLinkTransportTestPeer::relocate(
                    transport, relocated[i], kBufferLength, LOCAL_SEGMENT_ID)
                    .ok();
        });
    }
    while (ready.load(std::memory_order_acquire) != kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();

    for (uint8_t success : succeeded) EXPECT_TRUE(success);
    for (uint64_t address : relocated) EXPECT_EQ(address, relocated.front());
    EXPECT_EQ(
        NVLinkTransportTestPeer::mappingCount(transport, LOCAL_SEGMENT_ID), 1u);
    verifyRelocatedData(relocated.front(), kBufferLength);
    ASSERT_TRUE(transport.uninstall().ok());
}

}  // namespace tent
}  // namespace mooncake
