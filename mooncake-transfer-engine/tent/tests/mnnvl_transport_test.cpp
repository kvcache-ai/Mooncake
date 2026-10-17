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
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/transfer_engine.h"
#include "tent/transport/mnnvl/mnnvl_transport.h"

namespace mooncake {
namespace tent {

class MnnvlTransportTestPeer {
   public:
    using Entry = std::shared_ptr<MnnvlTransport::OpenedMnnvlEntry>;

    // `pinned` non-null: the returned entry is pinned (as a submitted task
    // would pin it) and must be released with unpin().
    static Status relocate(MnnvlTransport& transport, uint64_t& address,
                           uint64_t length, SegmentID target_id,
                           Entry* pinned = nullptr) {
        return transport.relocateSharedMemoryAddress(address, length, target_id,
                                                     pinned);
    }

    static void unpin(MnnvlTransport& transport, Entry& entry) {
        transport.unpin(entry);
    }

    static size_t mappingCount(MnnvlTransport& transport, SegmentID target_id) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        auto it = transport.relocate_map_.find(target_id);
        return it == transport.relocate_map_.end() ? 0 : it->second.size();
    }

    // Entries that were retired while still pinned and await release.
    static size_t retiredCount(MnnvlTransport& transport) {
        RWSpinlock::ReadGuard guard(transport.relocate_lock_);
        return transport.retired_.size();
    }
};

namespace {

constexpr uint64_t kRemoteAddress = 0x10000000;
constexpr size_t kBufferLength = 4096;
constexpr uint32_t kPatternA = 0x5a3c69e7;
constexpr uint32_t kPatternB = 0xa5c3961e;
// Set in the peer process: "<ready_fd>,<ctl_fd>" for the raw VMM exporter,
// "engine:<ready_fd>,<ctl_fd>,<port>" for the full TransferEngine provider.
constexpr char kPeerEnv[] = "TENT_MNNVL_TEST_PEER";

// ---------------------------------------------------------------- peer side
//
// POSIX-FD handles are imported through pidfd_getfd(), so the exporter must
// be another process. The peer is this same binary re-executed with kPeerEnv
// set (fork + exec, so no process ever forks after initializing CUDA; gtest
// runs several cases in one process and the parent has a live context from
// the second case on). The peer allocates a VMM buffer with the
// POSIX_FILE_DESCRIPTOR handle type, fills it with a pattern, exports the fd
// and reports status, fd and mapped size on the ready pipe. On the control
// pipe it accepts
// 'r' (release the current allocation, allocate a new one with the other
// pattern, report it) and 'x' (exit). It holds the current allocation and fd
// open until told otherwise, exactly like a provider that re-registers the
// same address.

struct PeerGeneration {
    CUmemGenericAllocationHandle handle = 0;
    CUdeviceptr ptr = 0;
    size_t size = 0;
    int fd = -1;
};

bool writeAll(int fd, const void* data, size_t len) {
    return write(fd, data, len) == (ssize_t)len;
}

bool readAll(int fd, void* data, size_t len) {
    auto* p = static_cast<char*>(data);
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        p += n;
        len -= n;
    }
    return true;
}

int peerAllocate(PeerGeneration& gen, uint32_t pattern) {
    CUdevice device;
    if (cuDeviceGet(&device, 0) != CUDA_SUCCESS) return 3;
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
    gen.size = ((kBufferLength + granularity - 1) / granularity) * granularity;
    if (cuMemCreate(&gen.handle, gen.size, &prop, 0) != CUDA_SUCCESS) return 5;
    if (cuMemAddressReserve(&gen.ptr, gen.size, 0, 0, 0) != CUDA_SUCCESS)
        return 6;
    if (cuMemMap(gen.ptr, gen.size, 0, gen.handle, 0) != CUDA_SUCCESS) return 7;
    CUmemAccessDesc access = {};
    access.location = prop.location;
    access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    if (cuMemSetAccess(gen.ptr, gen.size, &access, 1) != CUDA_SUCCESS) return 8;
    std::vector<uint32_t> fill(kBufferLength / sizeof(uint32_t), pattern);
    if (cudaMemcpy(reinterpret_cast<void*>(gen.ptr), fill.data(), kBufferLength,
                   cudaMemcpyHostToDevice) != cudaSuccess)
        return 9;
    if (cuMemExportToShareableHandle(&gen.fd, gen.handle,
                                     CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                                     0) != CUDA_SUCCESS)
        return 10;
    return 0;
}

void peerRelease(PeerGeneration& gen) {
    // Same order as MnnvlTransport::freeLocalMemory: unmap, release, free VA.
    cuMemUnmap(gen.ptr, gen.size);
    cuMemRelease(gen.handle);
    cuMemAddressFree(gen.ptr, gen.size);
    if (gen.fd >= 0) close(gen.fd);
    gen = PeerGeneration{};
}

bool peerReport(int ready_fd, const PeerGeneration& gen) {
    int32_t status = 0;
    return writeAll(ready_fd, &status, sizeof(status)) &&
           writeAll(ready_fd, &gen.fd, sizeof(gen.fd)) &&
           writeAll(ready_fd, &gen.size, sizeof(gen.size));
}

int peerMain(int ready_fd, int ctl_fd) {
    int device_count = 0;
    int32_t unsupported = 1;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        (void)writeAll(ready_fd, &unsupported, sizeof(unsupported));
        return 0;
    }
    if (cudaSetDevice(0) != cudaSuccess || cudaFree(0) != cudaSuccess) return 2;
    CUdevice device;
    if (cuDeviceGet(&device, 0) != CUDA_SUCCESS) return 3;
    int fd_supported = 0;
    cuDeviceGetAttribute(
        &fd_supported,
        CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED,
        device);
    if (!fd_supported) {
        (void)writeAll(ready_fd, &unsupported, sizeof(unsupported));
        return 0;
    }

    PeerGeneration gen;
    uint32_t pattern = kPatternA;
    int rc = peerAllocate(gen, pattern);
    if (rc) return rc;
    if (!peerReport(ready_fd, gen)) return 11;

    char cmd = 0;
    while (readAll(ctl_fd, &cmd, 1)) {
        if (cmd == 'x') break;
        if (cmd != 'r') return 12;
        // Keep the old fd open until the new allocation is exported, as
        // MnnvlTransport::addMemoryBuffer does (it never closes exported
        // fds). Closing first would hand the next export the same fd
        // number, and "<pid>-<fd>" would no longer tell the two
        // generations apart.
        int retired_fd = gen.fd;
        gen.fd = -1;
        peerRelease(gen);
        pattern = (pattern == kPatternA) ? kPatternB : kPatternA;
        rc = peerAllocate(gen, pattern);
        if (retired_fd >= 0) close(retired_fd);
        if (rc) return rc;
        if (!peerReport(ready_fd, gen)) return 11;
    }
    peerRelease(gen);
    return 0;
}

// ---------------------------------------------------- engine peer (end-to-end)
//
// A complete TransferEngine provider over the public API, the shape of the
// #4592 reproduction: allocate + register a buffer on cuda:0, fill it, and on
// 'r' unregister + free + allocate + register again with the other pattern.
// Reports "<segment name> <address>\n" lines on the ready pipe.

std::shared_ptr<Config> makeEngineConfig(int port) {
    auto conf = std::make_shared<Config>();
    conf->set("metadata_type", std::string("p2p"));
    conf->set("rpc_server_hostname", std::string("127.0.0.1"));
    if (port) conf->set("rpc_server_port", port);
    conf->set("transports/tcp/enable", false);
    conf->set("transports/rdma/enable", false);
    conf->set("transports/io_uring/enable", false);
    conf->set("transports/nvlink/handle_type", std::string("posix_fd"));
    conf->set("log_level", std::string("warning"));
    return conf;
}

bool writeLine(int fd, const std::string& line) {
    std::string buf = line + "\n";
    return writeAll(fd, buf.data(), buf.size());
}

bool readLine(int fd, std::string& line) {
    line.clear();
    char c;
    while (true) {
        ssize_t n = read(fd, &c, 1);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        if (c == '\n') return true;
        line += c;
    }
}

int engineMain(int ready_fd, int ctl_fd, int port) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count <= 0) {
        (void)writeLine(ready_fd, "UNSUPPORTED");
        return 0;
    }
    if (cudaSetDevice(0) != cudaSuccess) return 2;
    TransferEngine engine(makeEngineConfig(port));
    if (!engine.available()) return 20;

    void* addr = nullptr;
    uint32_t pattern = kPatternA;
    auto allocate = [&]() -> int {
        if (!engine.allocateLocalMemory(&addr, kBufferLength, "cuda:0").ok())
            return 21;
        if (!engine.registerLocalMemory(addr, kBufferLength).ok()) return 22;
        std::vector<uint32_t> fill(kBufferLength / sizeof(uint32_t), pattern);
        if (cudaMemcpy(addr, fill.data(), kBufferLength,
                       cudaMemcpyHostToDevice) != cudaSuccess)
            return 23;
        cudaDeviceSynchronize();
        return 0;
    };
    auto release = [&]() {
        (void)engine.unregisterLocalMemory(addr, kBufferLength);
        (void)engine.freeLocalMemory(addr);
        addr = nullptr;
    };
    auto report = [&]() {
        return writeLine(ready_fd, engine.getSegmentName() + " " +
                                       std::to_string((uint64_t)addr));
    };

    int rc = allocate();
    if (rc) return rc;
    if (!report()) return 11;
    char cmd = 0;
    while (readAll(ctl_fd, &cmd, 1)) {
        if (cmd == 'x') break;
        if (cmd != 'r') return 12;
        release();
        pattern = (pattern == kPatternA) ? kPatternB : kPatternA;
        rc = allocate();
        if (rc) return rc;
        if (!report()) return 11;
    }
    release();
    return 0;
}

// Called from main() when this binary is the re-executed peer: returns the
// peer's exit code, or -1 when kPeerEnv is not set. Kept out of static
// initialization on purpose: engineMain() builds a TransferEngine, which
// depends on globals in other translation units.
int runPeerIfRequested() {
    const char* spec = getenv(kPeerEnv);
    if (!spec) return -1;
    int ready_fd = -1, ctl_fd = -1, port = 0;
    if (sscanf(spec, "engine:%d,%d,%d", &ready_fd, &ctl_fd, &port) == 3)
        return engineMain(ready_fd, ctl_fd, port);
    if (sscanf(spec, "%d,%d", &ready_fd, &ctl_fd) != 2) return 100;
    return peerMain(ready_fd, ctl_fd);
}

// fork + exec this binary as a peer; kPeerEnv becomes
// "<prefix><ready_fd>,<ctl_fd><suffix>". Returns the child pid, or -1.
pid_t spawnPeer(const std::string& spec_prefix, const std::string& spec_suffix,
                int ready_pipe[2], int ctl_pipe[2]) {
    if (pipe(ready_pipe) != 0 || pipe(ctl_pipe) != 0) return -1;
    pid_t child = fork();
    if (child < 0) return -1;
    if (child == 0) {
        close(ready_pipe[0]);
        close(ctl_pipe[1]);
        std::string spec = spec_prefix + std::to_string(ready_pipe[1]) + "," +
                           std::to_string(ctl_pipe[0]) + spec_suffix;
        setenv(kPeerEnv, spec.c_str(), 1);
        char self[] = "/proc/self/exe";
        char* argv[] = {self, nullptr};
        execv(self, argv);
        _exit(127);
    }
    close(ready_pipe[1]);
    close(ctl_pipe[0]);
    return child;
}

// -------------------------------------------------------------- parent side

class PeerFdExporter {
   public:
    PeerFdExporter() {
        child_pid_ = spawnPeer("", "", ready_pipe_, ctl_pipe_);
        if (child_pid_ <= 0) return;
        valid_ = readReport();
    }

    ~PeerFdExporter() { stop(); }

    bool forkOk() const { return child_pid_ > 0; }
    bool valid() const { return valid_; }
    // 0 = exported fine; 1 = no CUDA device or no POSIX-FD VMM support.
    int childStatus() const { return child_status_; }
    pid_t pid() const { return child_pid_; }
    // "<pid>-<fd>" for the CURRENT generation, the format MnnvlTransport
    // publishes in mnnvl_handle.
    std::string mnnvlHandle() const {
        return std::to_string(child_pid_) + "-" + std::to_string(exported_fd_);
    }
    // Granularity-rounded size of the peer's mapping. addMemoryBuffer
    // publishes this (not the requested length) in BufferDesc::length, and
    // cuMemMap on the importing side requires a granularity multiple.
    size_t mappedSize() const { return mapped_size_; }

    // Peer frees its buffer and allocates a new one filled with the other
    // pattern; mnnvlHandle() then names the new fd.
    bool reallocate() {
        if (!writeAll(ctl_pipe_[1], "r", 1)) return false;
        return readReport();
    }

    // Peer exits (releasing its allocation and fd). Idempotent.
    void stop() {
        if (child_pid_ > 0) {
            (void)writeAll(ctl_pipe_[1], "x", 1);
            (void)waitpid(child_pid_, nullptr, 0);
            child_pid_ = -1;
        }
        if (ready_pipe_[0] >= 0) close(ready_pipe_[0]);
        if (ctl_pipe_[1] >= 0) close(ctl_pipe_[1]);
        ready_pipe_[0] = ctl_pipe_[1] = -1;
    }

   private:
    bool readReport() {
        int32_t status = -1;
        if (!readAll(ready_pipe_[0], &status, sizeof(status))) return false;
        child_status_ = status;
        if (status != 0) return true;  // unsupported in peer; skip, not fail
        if (!readAll(ready_pipe_[0], &exported_fd_, sizeof(exported_fd_)))
            return false;
        if (!readAll(ready_pipe_[0], &mapped_size_, sizeof(mapped_size_)))
            return false;
        return exported_fd_ >= 0 && mapped_size_ >= kBufferLength;
    }

    int ready_pipe_[2]{-1, -1};
    int ctl_pipe_[2]{-1, -1};
    pid_t child_pid_{-1};
    int child_status_{-1};
    int exported_fd_{-1};
    size_t mapped_size_{0};
    bool valid_{false};
};

// Reads kBufferLength bytes through a local mapping and returns the uniform
// 32-bit pattern found there, or 0 when the buffer is not uniform or the
// copy fails.
uint32_t readPattern(uint64_t local_addr) {
    std::vector<uint32_t> readback(kBufferLength / sizeof(uint32_t));
    if (cudaMemcpy(readback.data(), reinterpret_cast<void*>(local_addr),
                   kBufferLength, cudaMemcpyDeviceToHost) != cudaSuccess)
        return 0;
    for (uint32_t value : readback)
        if (value != readback[0]) return 0;
    return readback[0];
}

class EnginePeer {
   public:
    explicit EnginePeer(int port) {
        child_pid_ = spawnPeer("engine:", "," + std::to_string(port),
                               ready_pipe_, ctl_pipe_);
        if (child_pid_ <= 0) return;
        valid_ = readReport();
    }
    ~EnginePeer() { stop(); }

    bool forkOk() const { return child_pid_ > 0; }
    bool valid() const { return valid_; }
    bool unsupported() const { return unsupported_; }
    const std::string& segmentName() const { return segment_name_; }
    uint64_t address() const { return address_; }

    bool reallocate() {
        if (!writeAll(ctl_pipe_[1], "r", 1)) return false;
        return readReport();
    }

    void stop() {
        if (child_pid_ > 0) {
            (void)writeAll(ctl_pipe_[1], "x", 1);
            (void)waitpid(child_pid_, nullptr, 0);
            child_pid_ = -1;
        }
        if (ready_pipe_[0] >= 0) close(ready_pipe_[0]);
        if (ctl_pipe_[1] >= 0) close(ctl_pipe_[1]);
        ready_pipe_[0] = ctl_pipe_[1] = -1;
    }

   private:
    bool readReport() {
        std::string line;
        if (!readLine(ready_pipe_[0], line)) return false;
        if (line == "UNSUPPORTED") {
            unsupported_ = true;
            return true;
        }
        auto space = line.find(' ');
        if (space == std::string::npos) return false;
        segment_name_ = line.substr(0, space);
        address_ = std::stoull(line.substr(space + 1));
        return address_ != 0;
    }

    int ready_pipe_[2]{-1, -1};
    int ctl_pipe_[2]{-1, -1};
    pid_t child_pid_{-1};
    bool valid_{false};
    bool unsupported_{false};
    std::string segment_name_;
    uint64_t address_{0};
};

// READ kBufferLength bytes from (segment, remote) into `local` through the
// public API. Returns the uniform pattern read, 0 when the buffer is not
// uniform or the transfer did not complete.
uint32_t readRemote(TransferEngine& engine, SegmentID segment, uint64_t remote,
                    void* local, TransferStatusEnum& result) {
    cudaMemset(local, 0, kBufferLength);
    cudaDeviceSynchronize();
    auto batch = engine.allocateBatch(1);
    Request request{};
    request.opcode = Request::READ;
    request.source = local;
    request.target_id = segment;
    request.target_offset = remote;
    request.length = kBufferLength;
    TransferStatus status{PENDING, 0};
    if (engine.submitTransfer(batch, {request}).ok()) {
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        do {
            if (!engine.getTransferStatus(batch, 0, status).ok()) break;
        } while (status.s == PENDING &&
                 std::chrono::steady_clock::now() < deadline);
    } else {
        status.s = FAILED;
    }
    engine.freeBatch(batch);
    result = status.s;
    if (status.s != COMPLETED) return 0;
    return readPattern(reinterpret_cast<uint64_t>(local));
}

Status publishBuffer(ControlService& metadata, const std::string& mnnvl_handle,
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

// Common setup: a live peer, a local segment that publishes the peer's
// buffer at kRemoteAddress, and an installed transport in posix_fd mode.
class MnnvlTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        exporter_ = std::make_unique<PeerFdExporter>();
        if (!exporter_->forkOk() || !exporter_->valid()) {
            GTEST_SKIP() << "Peer export failed (fork/pipe/exec error)";
        }
        if (exporter_->childStatus() != 0) {
            GTEST_SKIP() << "No CUDA device with POSIX-FD VMM support";
        }
        // First CUDA touch in the parent, after the peer was spawned.
        // install() alone does not create a context, and the driver calls
        // in relocate need one.
        ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
        ASSERT_EQ(cudaFree(0), cudaSuccess);

        metadata_ = std::make_shared<ControlService>("p2p", "", nullptr);
        ASSERT_TRUE(publishCurrentGeneration().ok());

        std::string local_segment_name = "mnnvl_test_segment";
        auto status = transport_.install(local_segment_name, metadata_, nullptr,
                                         makePosixFdConfig());
        if (!status.ok()) {
            GTEST_SKIP() << "MnnvlTransport::install failed: "
                         << status.ToString();
        }
        installed_ = true;
    }

    void TearDown() override {
        if (installed_) (void)transport_.uninstall();
        exporter_.reset();
    }

    Status publishCurrentGeneration() {
        return publishBuffer(*metadata_, exporter_->mnnvlHandle(),
                             kRemoteAddress, exporter_->mappedSize());
    }

    // Relocates kRemoteAddress + 64 for kBufferLength - 64 bytes and returns
    // the local address, or 0 on failure.
    uint64_t relocateHead(MnnvlTransportTestPeer::Entry* pinned = nullptr) {
        uint64_t dest_addr = kRemoteAddress;
        auto status = MnnvlTransportTestPeer::relocate(
            transport_, dest_addr, kBufferLength, LOCAL_SEGMENT_ID, pinned);
        return status.ok() ? dest_addr : 0;
    }

    std::unique_ptr<PeerFdExporter> exporter_;
    std::shared_ptr<ControlService> metadata_;
    MnnvlTransport transport_;
    bool installed_ = false;
};

}  // namespace

// Regression guard for the POSIX-FD import call: it takes the fd value cast
// to void*, not a pointer to it (passing &fd failed with CUDA_ERROR_UNKNOWN).
TEST_F(MnnvlTransportTest, RelocateImportsPosixFdHandle) {
    uint64_t dest_addr = kRemoteAddress + 64;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport_, dest_addr, kBufferLength - 64, LOCAL_SEGMENT_ID)
                    .ok());
    // dest_addr now aliases the peer's allocation at offset 64.
    std::vector<uint32_t> readback((kBufferLength - 64) / sizeof(uint32_t));
    ASSERT_EQ(cudaMemcpy(readback.data(), reinterpret_cast<void*>(dest_addr),
                         kBufferLength - 64, cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (uint32_t value : readback) {
        EXPECT_EQ(value, kPatternA);
    }
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 1u);
}

// #4592: the peer frees its buffer and registers a new allocation at the
// SAME address. The consumer's descriptor now carries a different
// mnnvl_handle, so a relocate must import the new allocation, not serve the
// cached mapping of the old one.
TEST_F(MnnvlTransportTest, SameAddressNewHandleIsReimported) {
    uint64_t first = relocateHead();
    ASSERT_NE(first, 0u);
    EXPECT_EQ(readPattern(first), kPatternA);

    ASSERT_TRUE(exporter_->reallocate());
    ASSERT_TRUE(publishCurrentGeneration().ok());

    uint64_t second = relocateHead();
    ASSERT_NE(second, 0u);
    EXPECT_EQ(readPattern(second), kPatternB)
        << "relocate served the mapping of the old allocation";
    // The old mapping had no users, so it is gone, not merely shadowed.
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 1u);
    EXPECT_EQ(MnnvlTransportTestPeer::retiredCount(transport_), 0u);
}

// The peer re-registers at a DIFFERENT address (a restart with ASLR on).
// The old address is never looked up again, so address-keyed retirement
// alone would leave its mapping behind until uninstall(); the miss on the new
// address sweeps the stale one instead.
TEST_F(MnnvlTransportTest, UnpublishedAddressIsRetiredOnMiss) {
    uint64_t first = relocateHead();
    ASSERT_NE(first, 0u);
    EXPECT_EQ(readPattern(first), kPatternA);

    ASSERT_TRUE(exporter_->reallocate());
    constexpr uint64_t kMovedAddress = kRemoteAddress + 0x10000000;
    ASSERT_TRUE(publishBuffer(*metadata_, exporter_->mnnvlHandle(),
                              kMovedAddress, exporter_->mappedSize())
                    .ok());

    uint64_t dest_addr = kMovedAddress;
    ASSERT_TRUE(MnnvlTransportTestPeer::relocate(
                    transport_, dest_addr, kBufferLength, LOCAL_SEGMENT_ID)
                    .ok());
    EXPECT_EQ(readPattern(dest_addr), kPatternB);
    // Only the new address is mapped; the old mapping had no users and is
    // released, not merely retired.
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 1u);
    EXPECT_EQ(MnnvlTransportTestPeer::retiredCount(transport_), 0u);
}

// Same address, same handle: the cached mapping is reused, not re-imported.
TEST_F(MnnvlTransportTest, SameHandleHitsCache) {
    uint64_t first = relocateHead();
    ASSERT_NE(first, 0u);
    uint64_t second = relocateHead();
    EXPECT_EQ(second, first);
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 1u);
}

// A task that is in flight pins its mapping. When the peer re-registers,
// the old mapping is retired but stays readable until the last pin drops;
// new relocates already resolve to the new allocation.
TEST_F(MnnvlTransportTest, PinKeepsRetiredMappingAlive) {
    MnnvlTransportTestPeer::Entry pinned;
    uint64_t old_addr = relocateHead(&pinned);
    ASSERT_NE(old_addr, 0u);
    ASSERT_TRUE(pinned);

    ASSERT_TRUE(exporter_->reallocate());
    ASSERT_TRUE(publishCurrentGeneration().ok());

    uint64_t new_addr = relocateHead();
    ASSERT_NE(new_addr, 0u);
    EXPECT_NE(new_addr, old_addr);
    EXPECT_EQ(readPattern(new_addr), kPatternB);

    // Old mapping: retired, still mapped, still holding the old allocation
    // (the peer freed it, our import keeps it alive).
    EXPECT_TRUE(pinned->stale);
    EXPECT_EQ(MnnvlTransportTestPeer::retiredCount(transport_), 1u);
    EXPECT_EQ(readPattern(old_addr), kPatternA);
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 1u);

    // Even after the peer process is gone the pinned import keeps the old
    // allocation readable (and alive) until the pin drops.
    exporter_->stop();
    EXPECT_EQ(readPattern(old_addr), kPatternA);

    MnnvlTransportTestPeer::unpin(transport_, pinned);
    EXPECT_EQ(MnnvlTransportTestPeer::retiredCount(transport_), 0u);
}

// Descriptor replaced by one that cannot be imported (the peer is gone):
// relocate fails and does NOT fall back to the mapping of the old handle.
TEST_F(MnnvlTransportTest, UnimportableReplacementFailsClosed) {
    uint64_t first = relocateHead();
    ASSERT_NE(first, 0u);

    pid_t old_pid = exporter_->pid();
    exporter_->stop();
    ASSERT_TRUE(publishBuffer(*metadata_, std::to_string(old_pid) + "-3",
                              kRemoteAddress, kBufferLength)
                    .ok());

    EXPECT_EQ(relocateHead(), 0u) << "relocate must fail, not serve stale";
    EXPECT_EQ(relocateHead(), 0u) << "second attempt must fail as well";
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 0u);
    EXPECT_EQ(MnnvlTransportTestPeer::retiredCount(transport_), 0u);
}

// A handle that names a live peer but an fd it does not hold: the import
// fails before any driver resource is created and nothing is cached.
TEST_F(MnnvlTransportTest, ImportFailureDoesNotLeak) {
    ASSERT_TRUE(publishBuffer(*metadata_,
                              std::to_string(exporter_->pid()) + "-999999",
                              kRemoteAddress, exporter_->mappedSize())
                    .ok());
    EXPECT_EQ(relocateHead(), 0u);
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 0u);
    // Recovery: a correct descriptor imports normally afterwards.
    ASSERT_TRUE(publishCurrentGeneration().ok());
    uint64_t addr = relocateHead();
    ASSERT_NE(addr, 0u);
    EXPECT_EQ(readPattern(addr), kPatternA);
}

// A malformed mnnvl_handle is an error status, not an exception escaping
// submitTransfer.
TEST_F(MnnvlTransportTest, MalformedHandleIsAnError) {
    for (const char* bad : {"not-a-handle", "12345", "12-34-56", "x-1"}) {
        ASSERT_TRUE(publishBuffer(*metadata_, bad, kRemoteAddress,
                                  exporter_->mappedSize())
                        .ok());
        EXPECT_EQ(relocateHead(), 0u) << "handle: " << bad;
    }
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 0u);
}

// uninstall() releases every mapping instead of just forgetting it.
TEST_F(MnnvlTransportTest, UninstallReleasesMappings) {
    MnnvlTransportTestPeer::Entry pinned;
    ASSERT_NE(relocateHead(&pinned), 0u);
    MnnvlTransportTestPeer::unpin(transport_, pinned);
    ASSERT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 1u);

    ASSERT_TRUE(transport_.uninstall().ok());
    installed_ = false;
    EXPECT_EQ(
        MnnvlTransportTestPeer::mappingCount(transport_, LOCAL_SEGMENT_ID), 0u);
    EXPECT_EQ(MnnvlTransportTestPeer::retiredCount(transport_), 0u);
}

// A peer that is gone (or never exported the fd) must surface as a failed
// relocate, not a crash: pidfd_open fails on the dead pid and
// importFdFromProcess returns -1. No live peer needed.
TEST(MnnvlTransportStandaloneTest, RelocateFailsCleanlyWhenPeerIsGone) {
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
    ASSERT_TRUE(publishBuffer(*metadata, std::to_string(dead_pid) + "-3",
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

// The #4592 reproduction through the public API: provider and consumer are
// two TransferEngines (p2p metadata, MNNVL only). After the provider frees
// and re-registers its buffer, the consumer's READ must return the new
// contents. This exercises the pin lifecycle in submit / getTransferStatus
// / freeBatch and the descriptor refresh path for a remote segment, which
// the LOCAL_SEGMENT_ID cases above cannot reach.
TEST(MnnvlTransportEndToEndTest, ReadSeesNewAllocationAfterPeerReregisters) {
    setenv("MC_ENABLE_MNNVL", "1", 1);  // inherited by the peer as well
    const int port = 17000 + static_cast<int>(getpid() % 1000);
    EnginePeer peer(port);
    if (!peer.forkOk() || !peer.valid()) {
        GTEST_SKIP() << "Engine peer failed to start (fork/pipe/exec error)";
    }
    if (peer.unsupported()) {
        GTEST_SKIP() << "No CUDA device in the peer";
    }

    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    ASSERT_EQ(cudaFree(0), cudaSuccess);
    CUdevice device;
    ASSERT_EQ(cuDeviceGet(&device, 0), CUDA_SUCCESS);
    int fd_supported = 0;
    cuDeviceGetAttribute(
        &fd_supported,
        CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED,
        device);
    if (!fd_supported) {
        GTEST_SKIP() << "No POSIX-FD VMM support";
    }

    TransferEngine engine(makeEngineConfig(0));
    if (!engine.available()) {
        GTEST_SKIP() << "Consumer TransferEngine not available";
    }
    void* local = nullptr;
    ASSERT_TRUE(
        engine.allocateLocalMemory(&local, kBufferLength, "cuda:0").ok());
    ASSERT_TRUE(engine.registerLocalMemory(local, kBufferLength).ok());
    SegmentID segment;
    ASSERT_TRUE(engine.openSegment(segment, peer.segmentName()).ok());

    const uint64_t first_addr = peer.address();
    TransferStatusEnum status;
    EXPECT_EQ(readRemote(engine, segment, first_addr, local, status),
              kPatternA);
    EXPECT_EQ(status, COMPLETED);

    ASSERT_TRUE(peer.reallocate());
    const uint64_t second_addr = peer.address();
    // Same VA is the interesting case (the cache is keyed by address); a
    // different VA is a plain miss and still has to read the new contents.
    ::testing::Test::RecordProperty("same_address",
                                    second_addr == first_addr ? "yes" : "no");

    // The provider's update reaches the consumer by push notification; allow
    // it a moment, but never accept the old contents as final.
    uint32_t seen = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        seen = readRemote(engine, segment, second_addr, local, status);
        if (seen == kPatternB) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_EQ(seen, kPatternB)
        << "consumer still reads the old allocation (status " << status
        << ", same address: " << (second_addr == first_addr ? "yes" : "no")
        << ")";

    peer.stop();
    (void)engine.unregisterLocalMemory(local, kBufferLength);
    (void)engine.freeLocalMemory(local);
}

}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    int peer_rc = mooncake::tent::runPeerIfRequested();
    if (peer_rc >= 0) return peer_rc;
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
