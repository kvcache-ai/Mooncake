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

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
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

    // Seed cache-only tests without creating a CUDA context or importing a
    // real mapping. The transport stays uninstalled, so teardown never tries
    // to unmap these synthetic addresses.
    static void seed(MnnvlTransport& transport,
                     std::shared_ptr<ControlService> metadata,
                     uint64_t remote_addr, size_t length,
                     const std::string& handle, uint64_t mapped_addr) {
        transport.metadata_ = std::move(metadata);
        transport.handle_type_ = CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
        MnnvlTransport::OpenedMnnvlEntry entry;
        entry.mnnvl_addr = reinterpret_cast<void*>(mapped_addr);
        entry.length = length;
        entry.mnnvl_handle = handle;
        transport.relocate_map_[LOCAL_SEGMENT_ID][remote_addr] =
            std::move(entry);
        transport.relocate_generation_.fetch_add(1);
    }

    // Own ordinary pipe FDs without CUDA initialization. No batches, host
    // registrations or mappings exist, so uninstall only closes these FDs.
    static void seedExportedFd(MnnvlTransport& transport,
                               const BufferDesc& desc, int fd) {
        transport.installed_ = true;
        transport.exported_fds_.emplace(desc.mnnvl_handle, fd);
    }

    static void start(MnnvlTransport& transport, MnnvlSubBatch& batch) {
        std::vector<MnnvlTask*> tasks;
        for (auto& task : batch.task_list) tasks.push_back(&task);
        transport.startTransfer(tasks, &batch);
    }

    static Status importHandle(MnnvlTransport& transport,
                               CUmemAllocationHandleType type,
                               const std::string& handle) {
        transport.handle_type_ = type;
        BufferDesc buffer;
        buffer.length = 2 << 20;
        buffer.mnnvl_handle = handle;
        MnnvlTransport::OpenedMnnvlEntry entry;
        auto status = transport.importRemoteBuffer(buffer, entry);
        if (status.ok()) transport.releaseImportedEntry(entry);
        return status;
    }

    static Status ensureContext() {
        return MnnvlTransport::ensureCudaContext();
    }

    static Status setupPeerAccess(MnnvlTransport& transport) {
        return transport.setPeerAccess();
    }

    // Synthetic allocation records exercise bounds checks without CUDA.
    // The uninstalled transport never attempts to free these addresses.
    static void seedAllocation(MnnvlTransport& transport, uint64_t base,
                               size_t size, bool mapped = true) {
        auto* addr = reinterpret_cast<void*>(base);
        transport.allocate_set_[addr] = {addr, size, 0, mapped};
    }

    static Status allocationRange(MnnvlTransport& transport, uint64_t address,
                                  size_t length, uint64_t& base, size_t& size) {
        return transport.getLocalAllocationRange(address, length, base, size);
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

TEST(MnnvlTransportTest, OwnedVmmRangeIncludesSubranges) {
    MnnvlTransport transport;
    constexpr size_t mapped_size = 48 << 20;
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           mapped_size);
    for (uint64_t address : {kRemoteAddress, kRemoteAddress + 64,
                             kRemoteAddress + mapped_size - 64}) {
        uint64_t base = 0;
        size_t size = 0;
        ASSERT_TRUE(MnnvlTransportTestPeer::allocationRange(transport, address,
                                                            64, base, size)
                        .ok());
        EXPECT_EQ(base, kRemoteAddress);
        EXPECT_EQ(size, mapped_size);
    }
}

TEST(MnnvlTransportTest, OwnedVmmRangeRejectsOverflowAndOverrun) {
    MnnvlTransport transport;
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           kBufferLength);
    uint64_t base = 0;
    size_t size = 0;
    EXPECT_FALSE(
        MnnvlTransportTestPeer::allocationRange(
            transport, kRemoteAddress + kBufferLength - 32, 64, base, size)
            .ok());
    EXPECT_EQ(base, 0u);
    EXPECT_EQ(size, 0u);
    EXPECT_FALSE(MnnvlTransportTestPeer::allocationRange(
                     transport, UINT64_MAX - 31, 64, base, size)
                     .ok());
    EXPECT_FALSE(MnnvlTransportTestPeer::allocationRange(
                     transport, kRemoteAddress, 0, base, size)
                     .ok());
    EXPECT_FALSE(
        MnnvlTransportTestPeer::allocationRange(transport, 0, 64, base, size)
            .ok());

    // Reject before exporting or initializing CUDA, even in non-strict mode.
    BufferDesc desc;
    desc.addr = kRemoteAddress + kBufferLength - 32;
    desc.length = 64;
    desc.location = "cpu:0";
    MemoryOptions options;
    options.type = MNNVL;
    EXPECT_FALSE(transport.addMemoryBuffer(desc, options).ok());
    EXPECT_TRUE(desc.mnnvl_handle.empty());
    EXPECT_TRUE(desc.transports.empty());
}

TEST(MnnvlTransportTest, OwnedVmmRangeIgnoresUnknownAndUnmappedBuffers) {
    MnnvlTransport transport;
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           kBufferLength);
    for (uint64_t address :
         {kRemoteAddress - 64, kRemoteAddress + kBufferLength}) {
        uint64_t base = 1;
        size_t size = 1;
        EXPECT_TRUE(MnnvlTransportTestPeer::allocationRange(transport, address,
                                                            64, base, size)
                        .ok());
        EXPECT_EQ(base, 0u);
        EXPECT_EQ(size, 0u);
    }
    MnnvlTransportTestPeer::seedAllocation(transport, kRemoteAddress,
                                           kBufferLength, false);
    uint64_t base = 1;
    size_t size = 1;
    EXPECT_TRUE(MnnvlTransportTestPeer::allocationRange(
                    transport, kRemoteAddress, 64, base, size)
                    .ok());
    EXPECT_EQ(base, 0u);
    EXPECT_EQ(size, 0u);
    MnnvlTransportTestPeer::seedAllocation(transport, UINT64_MAX - 31, 64);
    EXPECT_TRUE(MnnvlTransportTestPeer::allocationRange(
                    transport, UINT64_MAX - 31, 16, base, size)
                    .ok());
    EXPECT_EQ(base, 0u);
    EXPECT_EQ(size, 0u);
}

TEST(MnnvlTransportTest, RemovingSameBaseRegistrationKeepsOtherExportOpen) {
    for (int removed_index : {0, 1}) {
        SCOPED_TRACE(removed_index);
        int fds[2];
        ASSERT_EQ(pipe(fds), 0);
        MnnvlTransport transport;
        BufferDesc descs[2];
        for (int i = 0; i < 2; ++i) {
            descs[i].addr = kRemoteAddress;
            descs[i].length = kBufferLength;
            descs[i].mnnvl_handle =
                std::to_string(getpid()) + "-" + std::to_string(fds[i]);
            MnnvlTransportTestPeer::seedExportedFd(transport, descs[i], fds[i]);
        }
        const int survivor_index = 1 - removed_index;
        const auto removed_desc = descs[removed_index];
        const auto survivor_handle = descs[survivor_index].mnnvl_handle;
        EXPECT_TRUE(transport.removeMemoryBuffer(descs[removed_index]).ok());
        EXPECT_TRUE(descs[removed_index].mnnvl_handle.empty());
        EXPECT_EQ(fcntl(fds[removed_index], F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
        EXPECT_GE(fcntl(fds[survivor_index], F_GETFD), 0);
        EXPECT_EQ(descs[survivor_index].mnnvl_handle, survivor_handle);

        // Failed/foreign registration cleanup at the same base owns no FD.
        auto unowned = descs[survivor_index];
        unowned.mnnvl_handle.clear();
        EXPECT_TRUE(transport.removeMemoryBuffer(unowned).ok());
        unowned.mnnvl_handle = "unowned-handle";
        EXPECT_TRUE(transport.removeMemoryBuffer(unowned).ok());
        EXPECT_GE(fcntl(fds[survivor_index], F_GETFD), 0);

        // A repeated removal must not close an unrelated reused FD number.
        int reused_fd = dup(fds[survivor_index]);
        ASSERT_GE(reused_fd, 0);
        auto stale_desc = removed_desc;
        EXPECT_TRUE(transport.removeMemoryBuffer(stale_desc).ok());
        EXPECT_GE(fcntl(reused_fd, F_GETFD), 0);
        EXPECT_EQ(close(reused_fd), 0);
        EXPECT_TRUE(transport.removeMemoryBuffer(descs[removed_index]).ok());
        EXPECT_GE(fcntl(fds[survivor_index], F_GETFD), 0);

        EXPECT_TRUE(transport.removeMemoryBuffer(descs[survivor_index]).ok());
        EXPECT_EQ(fcntl(fds[survivor_index], F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
    }
}

TEST(MnnvlTransportTest, UninstallClosesAllSameBaseExports) {
    int fds[2];
    ASSERT_EQ(pipe(fds), 0);
    MnnvlTransport transport;
    for (int fd : fds) {
        BufferDesc desc;
        desc.addr = kRemoteAddress;
        desc.length = kBufferLength;
        desc.mnnvl_handle = std::to_string(getpid()) + "-" + std::to_string(fd);
        MnnvlTransportTestPeer::seedExportedFd(transport, desc, fd);
    }
    EXPECT_TRUE(transport.uninstall().ok());
    for (int fd : fds) {
        EXPECT_EQ(fcntl(fd, F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
    }
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, MalformedHandlesReturnErrorsWithoutThrowing) {
    MnnvlTransport transport;
    for (const std::string handle : {"not-a-pid-fd", "123-fd", "0-3",
                                     "123-3junk", "99999999999999999999-3"}) {
        SCOPED_TRACE(handle);
        EXPECT_FALSE(
            MnnvlTransportTestPeer::importHandle(
                transport, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR, handle)
                .ok());
    }
    for (const std::string handle : {"f", "zz", "00"}) {
        SCOPED_TRACE(handle);
        EXPECT_FALSE(MnnvlTransportTestPeer::importHandle(
                         transport, CU_MEM_HANDLE_TYPE_FABRIC, handle)
                         .ok());
    }
}

TEST(MnnvlTransportTest, RelocationCacheDetectsChangedPeerHandle) {
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, "cached-handle", kRemoteAddress, kBufferLength)
                    .ok());
    MnnvlTransport transport;
    constexpr uint64_t mapped_addr = 0x20000000;
    MnnvlTransportTestPeer::seed(transport, metadata, kRemoteAddress,
                                 kBufferLength, "cached-handle", mapped_addr);
    uint64_t address = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(transport, address, 64,
                                                 LOCAL_SEGMENT_ID)
                    .ok());
    EXPECT_EQ(address, mapped_addr + 64);

    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, "not-a-pid-fd", kRemoteAddress, kBufferLength)
                    .ok());
    address = kRemoteAddress + 64;
    EXPECT_FALSE(MnnvlTransportTestPeer::relocate(transport, address, 64,
                                                  LOCAL_SEGMENT_ID)
                     .ok());
    EXPECT_EQ(address, kRemoteAddress + 64);
}

TEST(MnnvlTransportTest, RelocationCacheIsIsolatedBetweenInstances) {
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(
                    *metadata, "cached-handle", kRemoteAddress, kBufferLength)
                    .ok());
    MnnvlTransport first, second;
    MnnvlTransportTestPeer::seed(first, metadata, kRemoteAddress, kBufferLength,
                                 "cached-handle", 0x20000000);
    MnnvlTransportTestPeer::seed(second, metadata, kRemoteAddress,
                                 kBufferLength, "cached-handle", 0x30000000);
    for (auto* transport : {&first, &second, &first}) {
        uint64_t address = kRemoteAddress;
        ASSERT_TRUE(MnnvlTransportTestPeer::relocate(*transport, address, 64,
                                                     LOCAL_SEGMENT_ID)
                        .ok());
        EXPECT_EQ(address, transport == &first ? 0x20000000u : 0x30000000u);
    }
}

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
    // The same thread must not reuse a mapping after uninstall/reinstall.
    const auto old_mapping = dest_addr;
    ASSERT_TRUE(transport.uninstall().ok());
    unsigned long long flags = 0;
    CUmemLocation location{};
    location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    location.id = 0;
    EXPECT_NE(cuMemGetAccess(&flags, &location, (CUdeviceptr)old_mapping),
              CUDA_SUCCESS);
    ASSERT_TRUE(
        transport
            .install(local_segment_name, metadata, nullptr, makePosixFdConfig())
            .ok());
    dest_addr = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport, dest_addr, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());
    ASSERT_EQ(cudaMemcpy(readback.data(), reinterpret_cast<void*>(dest_addr),
                         kBufferLength - 64, cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (uint32_t value : readback) EXPECT_EQ(value, kPattern);
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

// A malformed POSIX-FD handle published by a peer must fail the relocate
// instead of escaping parsePidFd() as an exception.
TEST(MnnvlTransportTest, RelocateRejectsMalformedPosixFdHandle) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    ASSERT_TRUE(installLocalSegmentWithMnnvlHandle(*metadata, "not-a-pid-fd",
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
    EXPECT_EQ(dest_addr, kRemoteAddress);
    ASSERT_TRUE(transport.uninstall().ok());
}

// transports/mnnvl/egm_strict without transports/mnnvl/egm must refuse host
// allocations instead of silently handing out memory peers cannot address.
TEST(MnnvlTransportTest, StrictEgmRejectsHostAllocationWithoutEgm) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    auto conf = std::make_shared<Config>();
    conf->set("transports/mnnvl/egm", false);
    conf->set("transports/mnnvl/egm_strict", true);

    MnnvlTransport transport;
    std::string local_segment_name = "mnnvl_test_segment";
    auto status =
        transport.install(local_segment_name, metadata, nullptr, conf);
    if (!status.ok()) {
        GTEST_SKIP() << "MnnvlTransport::install failed: " << status.ToString();
    }

    MemoryOptions options;
    options.location = "cpu:0";
    void* addr = nullptr;
    EXPECT_FALSE(transport.allocateLocalMemory(&addr, 2 << 20, options).ok());
    EXPECT_EQ(addr, nullptr);
    ASSERT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, StrictRegistrationCleanupPreservesCallerPinning) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    auto conf = std::make_shared<Config>();
    conf->set("transports/mnnvl/egm", true);
    conf->set("transports/mnnvl/egm_strict", true);
    MnnvlTransport transport;
    std::string segment_name = "mnnvl_test_segment";
    auto status = transport.install(segment_name, metadata, nullptr, conf);
    if (!status.ok()) GTEST_SKIP() << status.ToString();

    std::vector<uint8_t> buffer(kBufferLength);
    ASSERT_EQ(
        cudaHostRegister(buffer.data(), buffer.size(), cudaHostRegisterDefault),
        cudaSuccess);
    BufferDesc desc;
    desc.addr = reinterpret_cast<uint64_t>(buffer.data());
    desc.length = buffer.size();
    desc.location = "cpu:0";
    MemoryOptions options;
    options.type = MNNVL;
    EXPECT_FALSE(transport.addMemoryBuffer(desc, options).ok());
    EXPECT_TRUE(desc.mnnvl_handle.empty());
    EXPECT_FALSE(transport.tracksLocalBuffer(desc));
    EXPECT_TRUE(transport.removeMemoryBuffer(desc).ok());
    // Caller ownership remains intact after defensive engine cleanup.
    EXPECT_EQ(cudaHostUnregister(buffer.data()), cudaSuccess);
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, CopyWaitsForRequestProducerEvent) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    CUDAStreamPool pool;
    CUDAStreamHandle producer_stream;
    ASSERT_TRUE(pool.acquire(producer_stream, 0).ok());
    MnnvlSubBatch batch;
    ASSERT_TRUE(pool.acquire(batch.async_stream, 0).ok());
    batch.stream_device_id = 0;
    void* source = nullptr;
    void* target = nullptr;
    ASSERT_EQ(cudaMalloc(&source, kBufferLength), cudaSuccess);
    ASSERT_EQ(cudaMalloc(&target, kBufferLength), cudaSuccess);
    ASSERT_EQ(cudaMemset(source, 0, kBufferLength), cudaSuccess);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
    cudaEvent_t producer_event = nullptr;
    ASSERT_EQ(cudaEventCreateWithFlags(&producer_event, cudaEventDisableTiming),
              cudaSuccess);

    struct Gate {
        std::atomic<bool> released{false};
        cudaStream_t stream;
        ~Gate() {
            released.store(true);
            cudaStreamSynchronize(stream);
        }
    } gate{{false}, producer_stream.get()};
    ASSERT_EQ(cudaLaunchHostFunc(
                  producer_stream.get(),
                  [](void* data) {
                      auto& gate = *static_cast<Gate*>(data);
                      // Bound the wait even if the test exits unexpectedly.
                      auto deadline = std::chrono::steady_clock::now() +
                                      std::chrono::seconds(5);
                      while (!gate.released.load() &&
                             std::chrono::steady_clock::now() < deadline)
                          std::this_thread::yield();
                  },
                  &gate),
              cudaSuccess);
    ASSERT_EQ(
        cudaMemsetAsync(source, 0x5a, kBufferLength, producer_stream.get()),
        cudaSuccess);
    ASSERT_EQ(cudaEventRecord(producer_event, producer_stream.get()),
              cudaSuccess);

    batch.task_list.emplace_back();
    auto& task = batch.task_list.back();
    task.request.opcode = Request::WRITE;
    task.request.source = source;
    task.request.length = kBufferLength;
    task.request.producer_event = producer_event;
    task.target_addr = reinterpret_cast<uint64_t>(target);
    task.status_word = PENDING;
    task.transferred_bytes = 0;
    MnnvlTransport transport;
    MnnvlTransportTestPeer::start(transport, batch);
    TransferStatus before_release{};
    EXPECT_TRUE(transport.getTransferStatus(&batch, 0, before_release).ok());
    EXPECT_EQ(before_release.s, PENDING);
    gate.released.store(true);
    ASSERT_EQ(cudaStreamSynchronize(batch.async_stream.get()), cudaSuccess);
    TransferStatus status{};
    ASSERT_TRUE(transport.getTransferStatus(&batch, 0, status).ok());
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kBufferLength);
    std::vector<uint8_t> readback(kBufferLength);
    ASSERT_EQ(cudaMemcpy(readback.data(), target, kBufferLength,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (auto value : readback) EXPECT_EQ(value, 0x5a);
    EXPECT_EQ(cudaEventDestroy(producer_event), cudaSuccess);
    EXPECT_EQ(cudaFree(source), cudaSuccess);
    EXPECT_EQ(cudaFree(target), cudaSuccess);
}

// Exercise actual export bookkeeping after two subranges expand to one VMM
// allocation. Unlike the pipe tests, this requires a CUDA-capable host.
TEST(MnnvlTransportTest, SameVmmBaseRegistrationsKeepIndependentExports) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);
    CUdevice device;
    ASSERT_EQ(cuDeviceGet(&device, 0), CUDA_SUCCESS);
    int fd_supported = 0;
    ASSERT_EQ(
        cuDeviceGetAttribute(
            &fd_supported,
            CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED,
            device),
        CUDA_SUCCESS);
    if (!fd_supported) GTEST_SKIP() << "No POSIX-FD VMM support";

    auto metadata = std::make_shared<ControlService>("p2p", "", nullptr);
    MnnvlTransport transport;
    std::string segment_name = "mnnvl_test_segment";
    auto conf = makePosixFdConfig();
    conf->set("transports/mnnvl/egm_strict", true);
    auto status = transport.install(segment_name, metadata, nullptr, conf);
    if (!status.ok()) GTEST_SKIP() << status.ToString();

    MemoryOptions options;
    options.type = MNNVL;
    options.location = "cuda:0";
    void* ptr = nullptr;
    ASSERT_TRUE(
        transport.allocateLocalMemory(&ptr, kBufferLength, options).ok());
    auto free_buffer = [&transport](void* addr) {
        EXPECT_TRUE(transport.freeLocalMemory(addr, kBufferLength).ok());
    };
    std::unique_ptr<void, decltype(free_buffer)> allocation(ptr, free_buffer);
    BufferDesc first;
    first.addr = reinterpret_cast<uint64_t>(ptr);
    first.length = kBufferLength / 2;
    first.location = options.location;
    BufferDesc second = first;
    second.addr += kBufferLength / 2;
    uint64_t mapped_base = 0;
    size_t mapped_size = 0;
    ASSERT_TRUE(
        MnnvlTransportTestPeer::allocationRange(
            transport, first.addr, first.length, mapped_base, mapped_size)
            .ok());
    ASSERT_EQ(mapped_base, reinterpret_cast<uint64_t>(ptr));
    ASSERT_GE(mapped_size, kBufferLength);
    ASSERT_TRUE(transport.addMemoryBuffer(first, options).ok());
    ASSERT_FALSE(first.mnnvl_handle.empty());
    std::thread registration_thread([&] {
        CUcontext context = nullptr;
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        ASSERT_EQ(context, nullptr);
        auto registered = transport.addMemoryBuffer(second, options);
        ASSERT_TRUE(registered.ok()) << registered.ToString();
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        EXPECT_NE(context, nullptr);
    });
    registration_thread.join();
    ASSERT_FALSE(second.mnnvl_handle.empty());
    EXPECT_EQ(first.addr, mapped_base);
    EXPECT_EQ(first.length, mapped_size);
    EXPECT_EQ(first.addr, second.addr);
    EXPECT_EQ(first.length, second.length);
    EXPECT_NE(first.mnnvl_handle, second.mnnvl_handle);
    const int first_fd = std::stoi(
        first.mnnvl_handle.substr(first.mnnvl_handle.find_last_of('-') + 1));
    const int second_fd = std::stoi(
        second.mnnvl_handle.substr(second.mnnvl_handle.find_last_of('-') + 1));
    EXPECT_GE(fcntl(first_fd, F_GETFD), 0);
    EXPECT_GE(fcntl(second_fd, F_GETFD), 0);

    EXPECT_TRUE(transport.removeMemoryBuffer(first).ok());
    EXPECT_EQ(fcntl(first_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_GE(fcntl(second_fd, F_GETFD), 0);
    // The surviving published FD must remain importable, not merely open.
    CUmemGenericAllocationHandle imported;
    ASSERT_EQ(cuMemImportFromShareableHandle(
                  &imported,
                  reinterpret_cast<void*>(static_cast<uintptr_t>(second_fd)),
                  CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR),
              CUDA_SUCCESS);
    EXPECT_EQ(cuMemRelease(imported), CUDA_SUCCESS);
    EXPECT_TRUE(transport.removeMemoryBuffer(second).ok());
    EXPECT_EQ(fcntl(second_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    allocation.reset();
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(MnnvlTransportTest, PeerAccessInitializesCudaContextOnFreshThread) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    ASSERT_EQ(cuInit(0), CUDA_SUCCESS);
    std::thread worker([] {
        CUcontext context = nullptr;
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        ASSERT_EQ(context, nullptr);
        MnnvlTransport transport;
        auto status = MnnvlTransportTestPeer::setupPeerAccess(transport);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(cuCtxGetCurrent(&context), CUDA_SUCCESS);
        EXPECT_NE(context, nullptr);
    });
    worker.join();
}

TEST(MnnvlTransportTest, PreservesCurrentCudaContext) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0)
        GTEST_SKIP() << "No CUDA device available";
    // Also exercise a nonzero selected device when more than one is visible.
    ASSERT_EQ(cudaSetDevice(device_count - 1), cudaSuccess);
    ASSERT_EQ(cudaFree(nullptr), cudaSuccess);
    CUcontext before = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&before), CUDA_SUCCESS);
    ASSERT_NE(before, nullptr);
    auto status = MnnvlTransportTestPeer::ensureContext();
    ASSERT_TRUE(status.ok()) << status.ToString();
    CUcontext after = nullptr;
    ASSERT_EQ(cuCtxGetCurrent(&after), CUDA_SUCCESS);
    EXPECT_EQ(after, before);
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
}

}  // namespace tent
}  // namespace mooncake
