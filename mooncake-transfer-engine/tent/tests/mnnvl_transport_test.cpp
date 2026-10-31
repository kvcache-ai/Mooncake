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

#include <cerrno>
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

class MnnvlTransportTestPeer {
   public:
    static Status relocate(MnnvlTransport& transport, uint64_t& address,
                           uint64_t length, SegmentID target_id) {
        return transport.relocateSharedMemoryAddress(address, length,
                                                     target_id);
    }
};

namespace {

constexpr uint64_t kRemoteAddress = 0x10000000;
constexpr size_t kBufferLength = 4096;
constexpr uint32_t kPattern = 0x5a3c69e7;

// POSIX-FD handles are imported through pidfd_getfd(), so the exporter must
// be another process: the child allocates a VMM buffer with the
// POSIX_FILE_DESCRIPTOR handle type, fills it, exports the fd and stays
// alive (keeping the allocation and the fd open) until the parent is done.
//
// The parent must not touch CUDA before fork(); its first CUDA call happens
// in MnnvlTransport::install(), after the child has been spawned.
class PeerFdExporter {
   public:
    PeerFdExporter() {
        if (pipe(ready_pipe_) != 0 || pipe(stop_pipe_) != 0) return;

        pid_t child = fork();
        if (child < 0) return;
        if (child == 0) {
            close(ready_pipe_[0]);
            close(stop_pipe_[1]);
            _exit(childMain(ready_pipe_[1], stop_pipe_[0]));
        }

        close(ready_pipe_[1]);
        close(stop_pipe_[0]);
        child_pid_ = child;
        valid_ = readExportResult();
    }

    ~PeerFdExporter() {
        if (child_pid_ > 0) {
            char done = 1;
            (void)write(stop_pipe_[1], &done, sizeof(done));
            (void)waitpid(child_pid_, nullptr, 0);
        }
        if (ready_pipe_[0] >= 0) close(ready_pipe_[0]);
        if (stop_pipe_[1] >= 0) close(stop_pipe_[1]);
    }

    bool forkOk() const { return child_pid_ > 0; }
    bool valid() const { return valid_; }
    // 0 = exported fine; 1 = no CUDA device or no POSIX-FD VMM support.
    int childStatus() const { return child_status_; }
    // "<pid>-<fd>", the format MnnvlTransport publishes in mnnvl_handle.
    std::string mnnvlHandle() const {
        return std::to_string(child_pid_) + "-" + std::to_string(exported_fd_);
    }
    // Granularity-rounded size of the child's mapping. addMemoryBuffer
    // publishes this (not the requested length) in BufferDesc::length, and
    // cuMemMap on the importing side requires a granularity multiple.
    size_t mappedSize() const { return mapped_size_; }

   private:
    static int childMain(int ready_fd, int stop_fd) {
        int32_t status = 1;
        int device_count = 0;
        if (cudaGetDeviceCount(&device_count) != cudaSuccess ||
            device_count <= 0) {
            (void)write(ready_fd, &status, sizeof(status));
            return 0;
        }
        if (cudaSetDevice(0) != cudaSuccess || cudaFree(0) != cudaSuccess)
            return 2;

        CUdevice device;
        if (cuDeviceGet(&device, 0) != CUDA_SUCCESS) return 3;
        int fd_supported = 0;
        cuDeviceGetAttribute(
            &fd_supported,
            CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED,
            device);
        if (!fd_supported) {
            (void)write(ready_fd, &status, sizeof(status));
            return 0;
        }

        CUmemAllocationProp prop = {};
        prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
        prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        prop.location.id = device;
        prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
        size_t granularity = 0;
        if (cuMemGetAllocationGranularity(&granularity, &prop,
                                          CU_MEM_ALLOC_GRANULARITY_MINIMUM) !=
            CUDA_SUCCESS)
            return 4;
        size_t size =
            ((kBufferLength + granularity - 1) / granularity) * granularity;

        CUmemGenericAllocationHandle handle;
        CUdeviceptr ptr = 0;
        if (cuMemCreate(&handle, size, &prop, 0) != CUDA_SUCCESS) return 5;
        if (cuMemAddressReserve(&ptr, size, 0, 0, 0) != CUDA_SUCCESS) return 6;
        if (cuMemMap(ptr, size, 0, handle, 0) != CUDA_SUCCESS) return 7;
        CUmemAccessDesc access = {};
        access.location = prop.location;
        access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        if (cuMemSetAccess(ptr, size, &access, 1) != CUDA_SUCCESS) return 8;

        std::vector<uint32_t> pattern(kBufferLength / sizeof(uint32_t),
                                      kPattern);
        if (cudaMemcpy(reinterpret_cast<void*>(ptr), pattern.data(),
                       kBufferLength, cudaMemcpyHostToDevice) != cudaSuccess)
            return 9;

        int shared_fd = -1;
        if (cuMemExportToShareableHandle(
                &shared_fd, handle, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                0) != CUDA_SUCCESS)
            return 10;

        status = 0;
        if (write(ready_fd, &status, sizeof(status)) != sizeof(status))
            return 11;
        if (write(ready_fd, &shared_fd, sizeof(shared_fd)) != sizeof(shared_fd))
            return 12;
        if (write(ready_fd, &size, sizeof(size)) != sizeof(size)) return 13;

        // Hold the allocation and the fd until the parent is done.
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
        if (status != 0) return true;  // unsupported in child; skip, not fail
        if (read(ready_pipe_[0], &exported_fd_, sizeof(exported_fd_)) !=
            (ssize_t)sizeof(exported_fd_))
            return false;
        if (read(ready_pipe_[0], &mapped_size_, sizeof(mapped_size_)) !=
            (ssize_t)sizeof(mapped_size_))
            return false;
        return exported_fd_ >= 0 && mapped_size_ >= kBufferLength;
    }

    int ready_pipe_[2]{-1, -1};
    int stop_pipe_[2]{-1, -1};
    pid_t child_pid_{-1};
    int child_status_{-1};
    int exported_fd_{-1};
    size_t mapped_size_{0};
    bool valid_{false};
};

Status installLocalSegmentWithMnnvlHandle(ControlService& metadata,
                                          const std::string& mnnvl_handle,
                                          uint64_t remote_addr, size_t length) {
    return metadata.segmentManager().updateLocal(
        [&](SegmentDesc& segment) -> Status {
            segment.name = "mnnvl_test_segment";
            segment.machine_id = "mnnvl_test_machine";
            segment.type = SegmentType::Memory;
            auto& memory = std::get<MemorySegmentDesc>(segment.detail);
            memory.buffers.clear();
            BufferDesc buffer;
            buffer.addr = remote_addr;
            buffer.length = length;
            buffer.location = "cuda:0";
            buffer.mnnvl_handle = mnnvl_handle;
            memory.buffers.push_back(std::move(buffer));
            return Status::OK();
        });
}

std::shared_ptr<Config> makePosixFdConfig() {
    auto conf = std::make_shared<Config>();
    conf->set("transports/nvlink/handle_type", std::string("posix_fd"));
    return conf;
}

// pid of a process that has already exited (reaped, so the pid is dead).
pid_t exitedChildPid() {
    pid_t child = fork();
    if (child == 0) _exit(0);
    if (child > 0) (void)waitpid(child, nullptr, 0);
    return child;
}

}  // namespace

// Regression guard: cuMemImportFromShareableHandle() takes the POSIX fd
// value itself cast to void*, not a pointer to it. Passing &fd failed with
// CUDA_ERROR_UNKNOWN, so the POSIX-FD handle type could never import a peer
// buffer.
TEST(MnnvlTransportTest, RelocateImportsPosixFdHandle) {
    PeerFdExporter exporter;
    if (!exporter.forkOk() || !exporter.valid()) {
        GTEST_SKIP() << "Peer export failed (fork/pipe error)";
    }
    if (exporter.childStatus() != 0) {
        GTEST_SKIP() << "No CUDA device with POSIX-FD VMM support";
    }

    // First CUDA touch in the parent, after fork(). install() alone does not
    // create a context, and the driver calls in relocate need one.
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, exporter.mnnvlHandle(), kRemoteAddress,
                    exporter.mappedSize())
                    .ok());

    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_test_segment";
    auto status = transport.install(local_segment_name, metadata, nullptr,
                                    makePosixFdConfig());
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    uint64_t dest_addr = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport, dest_addr, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());

    // dest_addr now aliases the child's allocation at offset 64.
    std::vector<uint32_t> readback((kBufferLength - 64) / sizeof(uint32_t));
    ASSERT_EQ(cudaMemcpy(readback.data(), reinterpret_cast<void*>(dest_addr),
                         kBufferLength - 64, cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (uint32_t value : readback) {
        EXPECT_EQ(value, kPattern);
    }
    ASSERT_TRUE(transport.uninstall().ok());
}

// A peer that is gone (or never exported the fd) must surface as a failed
// relocate, not a crash: pidfd_open fails on the dead pid and
// importFdFromProcess returns -1.
TEST(MnnvlTransportTest, RelocateFailsCleanlyWhenPeerIsGone) {
    pid_t dead_pid = exitedChildPid();
    if (dead_pid <= 0) {
        GTEST_SKIP() << "fork failed";
    }
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    std::string handle = std::to_string(dead_pid) + "-3";
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(*metadata, handle,
                                                   kRemoteAddress, 2 << 20)
                    .ok());

    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_test_segment";
    auto status = transport.install(local_segment_name, metadata, nullptr,
                                    makePosixFdConfig());
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    uint64_t dest_addr = kRemoteAddress;
    EXPECT_FALSE(MnnvlTransportTestPeer::relocate(
                     transport, dest_addr, kBufferLength, LOCAL_SEGMENT_ID)
                     .ok());
    ASSERT_TRUE(transport.uninstall().ok());
}

}  // namespace tent
}  // namespace mooncake
