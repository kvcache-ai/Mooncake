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

// nvlink_intra with virtual-memory (cuMemCreate) buffers. The exporter runs
// in a separate process (this binary re-executed), because CUDA cannot be
// used in a child forked after CUDA was initialized.

#include <cuda.h>
#include <cuda_runtime.h>
#include <dirent.h>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"

namespace mooncake {
namespace {

constexpr const char* kRoleEnv = "MC_VMM_TEST_EXPORTER";

uint8_t PatternA(size_t i) { return static_cast<uint8_t>(i * 7 + 3); }
uint8_t PatternB(size_t i) { return static_cast<uint8_t>(i * 13 + 5); }

// A VMM buffer made of chunks of different sizes mapped back to back, like
// a PyTorch expandable segment.
class VmmBuffer {
   public:
    // `chunk_units` are multiples of the allocation granularity.
    bool Create(int device, const std::vector<size_t>& chunk_units,
                bool shareable) {
        CUmemAllocationProp prop = {};
        prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
        prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        prop.location.id = device;
        prop.requestedHandleTypes =
            shareable ? CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR
                      : CU_MEM_HANDLE_TYPE_NONE;
        size_t granularity = 0;
        if (cuMemGetAllocationGranularity(&granularity, &prop,
                                          CU_MEM_ALLOC_GRANULARITY_MINIMUM) !=
            CUDA_SUCCESS) {
            return false;
        }
        for (size_t units : chunk_units) sizes_.push_back(units * granularity);
        for (size_t size : sizes_) length_ += size;
        if (cuMemAddressReserve(&va_, length_, 0, 0, 0) != CUDA_SUCCESS) {
            return false;
        }
        size_t offset = 0;
        for (size_t size : sizes_) {
            CUmemGenericAllocationHandle handle;
            if (cuMemCreate(&handle, size, &prop, 0) != CUDA_SUCCESS) {
                return false;
            }
            handles_.push_back(handle);
            if (cuMemMap(va_ + offset, size, 0, handle, 0) != CUDA_SUCCESS) {
                return false;
            }
            mapped_.push_back(offset);
            offset += size;
        }
        CUmemAccessDesc access = {};
        access.location = prop.location;
        access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
        return cuMemSetAccess(va_, length_, &access, 1) == CUDA_SUCCESS;
    }

    ~VmmBuffer() {
        for (size_t i = 0; i < mapped_.size(); ++i) {
            cuMemUnmap(va_ + mapped_[i], sizes_[i]);
        }
        for (auto handle : handles_) cuMemRelease(handle);
        if (va_) cuMemAddressFree(va_, length_);
    }

    void* ptr() const { return reinterpret_cast<void*>(va_); }
    size_t length() const { return length_; }
    const std::vector<size_t>& sizes() const { return sizes_; }

   private:
    CUdeviceptr va_ = 0;
    size_t length_ = 0;
    std::vector<size_t> sizes_;
    std::vector<size_t> mapped_;
    std::vector<CUmemGenericAllocationHandle> handles_;
};

bool VmmSupported(int device) {
    if (cuInit(0) != CUDA_SUCCESS) return false;
    CUdevice dev;
    if (cuDeviceGet(&dev, device) != CUDA_SUCCESS) return false;
    int vmm = 0, posix = 0;
    cuDeviceGetAttribute(
        &vmm, CU_DEVICE_ATTRIBUTE_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED, dev);
    cuDeviceGetAttribute(
        &posix, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED,
        dev);
    return vmm && posix;
}

int DeviceCount() {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess) return 0;
    return count;
}

std::string ServerName(int extra) {
    return "127.0.0.1:" + std::to_string(23000 + getpid() % 1000 + extra);
}

std::unique_ptr<TransferEngine> MakeEngine(const std::string& server_name) {
    auto engine = std::make_unique<TransferEngine>(false);
    auto host_port = parseHostNameWithPort(server_name);
    if (engine->init(P2PHANDSHAKE, server_name, host_port.first.c_str(),
                     host_port.second) != 0 ||
        !engine->installTransport("nvlink_intra", nullptr)) {
        return nullptr;
    }
    return engine;
}

size_t OpenFdCount() {
    size_t count = 0;
    if (DIR* dir = opendir("/proc/self/fd")) {
        while (readdir(dir)) ++count;
        closedir(dir);
    }
    return count;
}

// Exporter process. Commands arrive on `in`, replies go to `out`:
//   -> "<rc> <server> <addr> <length> <chunk sizes...>"
//   <- "verify <offset> <length>"  -> "ok" | "bad <index>"
//   <- "release"  unregister and free the buffer, then allocate and fill a
//                 new one of the same layout  -> "released"
//   <- "check_new"  -> "ok" | "bad <index>" (the new buffer is untouched)
//   <- "exit"
int RunExporter() {
    int in_fd = -1, out_fd = -1, device = 0;
    if (sscanf(getenv(kRoleEnv), "%d,%d,%d", &in_fd, &out_fd, &device) != 3) {
        return 2;
    }
    FILE* in = fdopen(in_fd, "r");
    FILE* out = fdopen(out_fd, "w");
    cudaSetDevice(device);
    auto make_buffer = [&]() {
        auto buffer = std::make_unique<VmmBuffer>();
        if (!buffer->Create(device, {1, 2, 3}, true)) return decltype(buffer)();
        std::vector<uint8_t> pattern(buffer->length());
        for (size_t i = 0; i < pattern.size(); ++i) pattern[i] = PatternA(i);
        cudaMemcpy(buffer->ptr(), pattern.data(), pattern.size(),
                   cudaMemcpyHostToDevice);
        return buffer;
    };
    auto buffer = make_buffer();
    if (!buffer) {
        fprintf(out, "-1\n");
        fflush(out);
        return 1;
    }
    std::vector<uint8_t> host(buffer->length());
    std::unique_ptr<VmmBuffer> fresh;

    auto engine = MakeEngine(ServerName(500));
    int rc = engine
                 ? engine->registerLocalMemory(buffer->ptr(), buffer->length(),
                                               "cuda:" + std::to_string(device))
                 : -1;
    fprintf(out, "%d %s %llu %zu", rc,
            engine ? engine->getLocalIpAndPort().c_str() : "-",
            (unsigned long long)(uintptr_t)buffer->ptr(), buffer->length());
    for (size_t size : buffer->sizes()) fprintf(out, " %zu", size);
    fprintf(out, "\n");
    fflush(out);

    char command[64];
    while (fscanf(in, "%63s", command) == 1) {
        if (strcmp(command, "verify") == 0) {
            size_t offset = 0, length = 0;
            if (fscanf(in, "%zu %zu", &offset, &length) != 2) break;
            cudaMemcpy(host.data(), buffer->ptr(), host.size(),
                       cudaMemcpyDeviceToHost);
            size_t bad = host.size();
            for (size_t i = 0; i < host.size() && bad == host.size(); ++i) {
                bool inside = i >= offset && i < offset + length;
                uint8_t want = inside ? PatternB(i - offset) : PatternA(i);
                if (host[i] != want) bad = i;
            }
            if (bad == host.size()) {
                fprintf(out, "ok\n");
            } else {
                fprintf(out, "bad %zu\n", bad);
            }
            fflush(out);
        } else if (strcmp(command, "release") == 0) {
            if (engine) engine->unregisterLocalMemory(buffer->ptr());
            buffer.reset();
            fresh = make_buffer();
            fprintf(out, fresh ? "released\n" : "failed\n");
            fflush(out);
        } else if (strcmp(command, "check_new") == 0) {
            cudaMemcpy(host.data(), fresh->ptr(), host.size(),
                       cudaMemcpyDeviceToHost);
            size_t bad = host.size();
            for (size_t i = 0; i < host.size() && bad == host.size(); ++i) {
                if (host[i] != PatternA(i)) bad = i;
            }
            if (bad == host.size()) {
                fprintf(out, "ok\n");
            } else {
                fprintf(out, "bad %zu\n", bad);
            }
            fflush(out);
        } else if (strcmp(command, "exit") == 0) {
            break;
        }
    }
    if (engine && buffer) engine->unregisterLocalMemory(buffer->ptr());
    return 0;
}

class Exporter {
   public:
    bool Start(int device) {
        int to_child[2], from_child[2];
        if (pipe(to_child) != 0 || pipe(from_child) != 0) return false;
        pid_ = fork();
        if (pid_ == 0) {
            close(to_child[1]);
            close(from_child[0]);
            std::string role = std::to_string(to_child[0]) + "," +
                               std::to_string(from_child[1]) + "," +
                               std::to_string(device);
            setenv(kRoleEnv, role.c_str(), 1);
            execl("/proc/self/exe", "/proc/self/exe", (char*)nullptr);
            _exit(127);
        }
        close(to_child[0]);
        close(from_child[1]);
        to_ = fdopen(to_child[1], "w");
        from_ = fdopen(from_child[0], "r");
        return pid_ > 0;
    }

    std::string ReadLine() {
        char line[512] = {};
        if (!fgets(line, sizeof(line), from_)) return "";
        std::string s(line);
        if (!s.empty() && s.back() == '\n') s.pop_back();
        return s;
    }

    void Send(const std::string& command) {
        fprintf(to_, "%s\n", command.c_str());
        fflush(to_);
    }

    int Finish() {
        if (pid_ <= 0) return -1;
        Send("exit");
        fclose(to_);
        fclose(from_);
        int status = 0;
        waitpid(pid_, &status, 0);
        pid_ = -1;
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }

    ~Exporter() {
        if (pid_ > 0) Finish();
    }

   private:
    pid_t pid_ = -1;
    FILE* to_ = nullptr;
    FILE* from_ = nullptr;
};

bool WaitCompleted(TransferEngine& engine, BatchID batch_id) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        if (!engine.getTransferStatus(batch_id, 0, status).ok()) return false;
        if (status.s == TransferStatusEnum::COMPLETED) return true;
        if (status.s == TransferStatusEnum::FAILED) return false;
        usleep(100);
    }
    return false;
}

bool Transfer(TransferEngine& engine, TransferRequest::OpCode opcode,
              void* local, Transport::SegmentHandle segment, uint64_t remote,
              size_t length) {
    auto batch = engine.allocateBatchID(1);
    TransferRequest request;
    request.opcode = opcode;
    request.source = local;
    request.target_id = segment;
    request.target_offset = remote;
    request.length = length;
    bool ok = engine.submitTransfer(batch, {request}).ok() &&
              WaitCompleted(engine, batch);
    engine.freeBatchID(batch);
    return ok;
}

class IntraNodeNvlinkVmmTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (DeviceCount() < 1 || !VmmSupported(0)) {
            GTEST_SKIP() << "needs a GPU with VMM and POSIX fd handles";
        }
    }
};

// A peer process reads and writes a VMM buffer of three chunks, crossing
// both chunk boundaries, from a cudaMalloc buffer on another GPU.
TEST_F(IntraNodeNvlinkVmmTest, CrossProcessReadWriteAcrossChunks) {
    const int importer_device = DeviceCount() > 1 ? 1 : 0;
    Exporter exporter;
    ASSERT_TRUE(exporter.Start(0));
    std::istringstream reply(exporter.ReadLine());
    int rc = -1;
    std::string server;
    unsigned long long remote = 0;
    size_t length = 0;
    std::vector<size_t> sizes(3);
    reply >> rc >> server >> remote >> length >> sizes[0] >> sizes[1] >>
        sizes[2];
    ASSERT_EQ(rc, 0) << "exporter failed to register its VMM buffer";

    ASSERT_EQ(cudaSetDevice(importer_device), cudaSuccess);
    void* local = nullptr;
    ASSERT_EQ(cudaMalloc(&local, length), cudaSuccess);
    auto engine = MakeEngine(ServerName(0));
    ASSERT_NE(engine, nullptr);
    ASSERT_EQ(engine->registerLocalMemory(
                  local, length, "cuda:" + std::to_string(importer_device)),
              0);
    auto segment = engine->openSegment(server);

    // Read the whole buffer.
    ASSERT_TRUE(Transfer(*engine, TransferRequest::READ, local, segment, remote,
                         length));
    std::vector<uint8_t> host(length);
    ASSERT_EQ(cudaMemcpy(host.data(), local, length, cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (size_t i = 0; i < length; ++i) {
        ASSERT_EQ(host[i], PatternA(i)) << "at " << i;
    }

    // Write a range that starts in chunk 0 and ends in chunk 2.
    const size_t offset = sizes[0] - 4096;
    const size_t span = sizes[1] + 8192;
    for (size_t i = 0; i < span; ++i) host[i] = PatternB(i);
    ASSERT_EQ(cudaMemcpy(local, host.data(), span, cudaMemcpyHostToDevice),
              cudaSuccess);
    ASSERT_TRUE(Transfer(*engine, TransferRequest::WRITE, local, segment,
                         remote + offset, span));
    exporter.Send("verify " + std::to_string(offset) + " " +
                  std::to_string(span));
    EXPECT_EQ(exporter.ReadLine(), "ok");

    engine->unregisterLocalMemory(local);
    engine.reset();
    cudaFree(local);
    EXPECT_EQ(exporter.Finish(), 0);
}

// The importer's mapping holds a reference to the physical memory, like a
// cudaIpc mapping: after the exporter unregisters and frees its buffer, a
// late write lands in the old memory, not in memory the exporter reuses.
TEST_F(IntraNodeNvlinkVmmTest, ExporterReleaseDoesNotCorruptNewAllocations) {
    const int importer_device = DeviceCount() > 1 ? 1 : 0;
    Exporter exporter;
    ASSERT_TRUE(exporter.Start(0));
    std::istringstream reply(exporter.ReadLine());
    int rc = -1;
    std::string server;
    unsigned long long remote = 0;
    size_t length = 0;
    reply >> rc >> server >> remote >> length;
    ASSERT_EQ(rc, 0);

    ASSERT_EQ(cudaSetDevice(importer_device), cudaSuccess);
    void* local = nullptr;
    ASSERT_EQ(cudaMalloc(&local, length), cudaSuccess);
    ASSERT_EQ(cudaMemset(local, 0xab, length), cudaSuccess);
    auto engine = MakeEngine(ServerName(4));
    ASSERT_NE(engine, nullptr);
    ASSERT_EQ(engine->registerLocalMemory(
                  local, length, "cuda:" + std::to_string(importer_device)),
              0);
    auto segment = engine->openSegment(server);
    ASSERT_TRUE(Transfer(*engine, TransferRequest::WRITE, local, segment,
                         remote, length));

    exporter.Send("release");
    ASSERT_EQ(exporter.ReadLine(), "released");
    // The cached mapping still points at the released memory.
    EXPECT_TRUE(Transfer(*engine, TransferRequest::WRITE, local, segment,
                         remote, length));
    exporter.Send("check_new");
    EXPECT_EQ(exporter.ReadLine(), "ok");

    engine->unregisterLocalMemory(local);
    engine.reset();
    cudaFree(local);
    EXPECT_EQ(exporter.Finish(), 0);
}

TEST_F(IntraNodeNvlinkVmmTest, RejectsNonShareableAllocation) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    VmmBuffer buffer;
    ASSERT_TRUE(buffer.Create(0, {1, 1}, false));
    auto engine = MakeEngine(ServerName(1));
    ASSERT_NE(engine, nullptr);
    EXPECT_NE(
        engine->registerLocalMemory(buffer.ptr(), buffer.length(), "cuda:0"),
        0);
}

TEST_F(IntraNodeNvlinkVmmTest, RejectsRangeBeyondMappedMemory) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    VmmBuffer buffer;
    ASSERT_TRUE(buffer.Create(0, {1}, true));
    auto engine = MakeEngine(ServerName(2));
    ASSERT_NE(engine, nullptr);
    EXPECT_NE(engine->registerLocalMemory(buffer.ptr(), buffer.length() * 2,
                                          "cuda:0"),
              0);
}

TEST_F(IntraNodeNvlinkVmmTest, RepeatedRegistrationDoesNotLeakFds) {
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    VmmBuffer buffer;
    ASSERT_TRUE(buffer.Create(0, {1, 2}, true));
    auto engine = MakeEngine(ServerName(3));
    ASSERT_NE(engine, nullptr);
    // The first registration starts the fd server.
    ASSERT_EQ(
        engine->registerLocalMemory(buffer.ptr(), buffer.length(), "cuda:0"),
        0);
    ASSERT_EQ(engine->unregisterLocalMemory(buffer.ptr()), 0);
    const size_t baseline = OpenFdCount();
    for (int i = 0; i < 20; ++i) {
        ASSERT_EQ(engine->registerLocalMemory(buffer.ptr(), buffer.length(),
                                              "cuda:0"),
                  0);
        ASSERT_EQ(engine->unregisterLocalMemory(buffer.ptr()), 0);
    }
    EXPECT_EQ(OpenFdCount(), baseline);
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    if (getenv(mooncake::kRoleEnv)) return mooncake::RunExporter();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
