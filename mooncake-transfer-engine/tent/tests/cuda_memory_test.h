// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <gtest/gtest.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "tent/transfer_engine.h"
#include "tent/platform/cuda.h"

namespace mooncake::tent::cuda_memory_test {

constexpr uint64_t kAddress = 1ULL << 48;
constexpr size_t kBytes = 4096;
constexpr uint32_t kInitial = 0x2a2a2a2a;
constexpr uint32_t kWritten = 0x35353535;

class TransportEnvironment {
   public:
    explicit TransportEnvironment(TransportType type) {
        const char* value = getenv("MC_ENABLE_MNNVL");
        present_ = value != nullptr;
        if (value) previous_ = value;
        if (type == MNNVL)
            setenv("MC_ENABLE_MNNVL", "1", 1);
        else
            unsetenv("MC_ENABLE_MNNVL");
    }
    ~TransportEnvironment() {
        if (present_)
            setenv("MC_ENABLE_MNNVL", previous_.c_str(), 1);
        else
            unsetenv("MC_ENABLE_MNNVL");
    }

   private:
    bool present_;
    std::string previous_;
};

inline std::shared_ptr<Config> config(TransportType type) {
    auto result = std::make_shared<Config>();
    result->set("metadata_type", "p2p");
    result->set("rpc_server_hostname", "127.0.0.1");
    result->set("rpc_server_port", 0);
    result->set("log_level", "warning");
    for (const char* name : {"tcp", "rdma", "shm", "mnnvl", "nvlink", "gds",
                             "io_uring", "hp_tcp", "mpcomm"})
        result->set(std::string("transports/") + name + "/enable", false);
    result->set(
        type == MNNVL ? "transports/mnnvl/enable" : "transports/nvlink/enable",
        true);
    result->set("transports/nvlink/handle_type", "posix_fd");
    // Exercise the existing async path, including NVLink's driver fallback.
    result->set("transports/nvlink/async_memcpy_threshold", 0);
    return result;
}

inline bool bytes(int fd, void* data, size_t size, bool sending) {
    auto* cursor = static_cast<char*>(data);
    while (size) {
        ssize_t count = sending ? send(fd, cursor, size, MSG_NOSIGNAL)
                                : recv(fd, cursor, size, 0);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        cursor += count;
        size -= count;
    }
    return true;
}

struct RegionInfo {
    int status = -1;
    char segment[256]{};
};

inline int exportRegion(int fd, TransportType type) {
    RegionInfo info;
    auto report = [&](int error) {
        info.status = error;
        bytes(fd, &info, sizeof(info), true);
        return error;
    };
    TransportEnvironment environment(type);
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || !count) return report(1);
    if (cudaSetDevice(count > 1 ? 1 : 0) != cudaSuccess) return report(2);
    TransferEngine engine(config(type));
    if (!engine.available()) return report(3);
    MemoryOptions options;
    options.type = type;
    options.location = count > 1 ? "cuda:1" : "cuda:0";
    options.global_address = kAddress;
    void* region = nullptr;
    auto status = engine.allocateLocalMemory(&region, kBytes, options);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return report(4);
    }
    if ((uint64_t)region != kAddress) return report(5);
    if (cudaMemset(region, 0x2a, kBytes) != cudaSuccess ||
        cudaDeviceSynchronize() != cudaSuccess)
        return report(6);
    if (engine.registerLocalMemory(region, kBytes + 1, options).ok())
        return report(15);
    if (!engine.registerLocalMemory(region, kBytes, options).ok())
        return report(7);
    if (engine.freeLocalMemory(region).ok()) return report(8);
    auto name = engine.getSegmentName();
    if (name.size() >= sizeof(info.segment)) return report(9);
    memcpy(info.segment, name.c_str(), name.size() + 1);
    info.status = 0;
    if (!bytes(fd, &info, sizeof(info), true)) return 10;
    char done = 0;
    if (!bytes(fd, &done, sizeof(done), false)) return 11;
    uint32_t value = 0;
    if (cudaMemcpy(&value, region, sizeof(value), cudaMemcpyDeviceToHost) !=
            cudaSuccess ||
        value != kWritten)
        return 12;
    if (!engine.unregisterLocalMemory(region, kBytes).ok() ||
        !engine.freeLocalMemory(region).ok())
        return 13;
    if (!engine.allocateLocalMemory(&region, kBytes, options).ok() ||
        !engine.freeLocalMemory(region).ok())
        return 14;
    return 0;
}

inline int childMain(int argc, char** argv) {
    if (argc != 4 || std::strcmp(argv[1], "--export-vmm-region")) return -1;
    return exportRegion(std::atoi(argv[2]),
                        std::strcmp(argv[3], "mnnvl") == 0 ? MNNVL : NVLINK);
}

class Exporter {
   public:
    bool start(TransportType type) {
        int sockets[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets)) return false;
        auto argument = std::to_string(sockets[1]);
        pid_ = fork();
        if (pid_ == 0) {
            close(sockets[0]);
            execl("/proc/self/exe", "cuda_memory_test", "--export-vmm-region",
                  argument.c_str(), type == MNNVL ? "mnnvl" : "nvlink",
                  nullptr);
            _exit(127);
        }
        close(sockets[1]);
        fd_ = sockets[0];
        timeval timeout{30, 0};
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        return pid_ > 0 && bytes(fd_, &info, sizeof(info), false);
    }
    int finish() {
        char done = 1;
        bytes(fd_, &done, 1, true);
        close(fd_);
        fd_ = -1;
        int status = 0;
        waitpid(pid_, &status, 0);
        pid_ = -1;
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }
    ~Exporter() {
        if (fd_ >= 0) close(fd_);
        if (pid_ > 0) waitpid(pid_, nullptr, 0);
    }
    RegionInfo info;

   private:
    pid_t pid_ = -1;
    int fd_ = -1;
};

inline void increment(void* address) {
    // An older PTX ISA keeps this test independent of the installed nvcc or
    // NVRTC version, while exercising a real kernel load/store at the GVA.
    constexpr char program[] = R"ptx(
.version 6.0
.target sm_60
.address_size 64
.visible .entry increment(.param .u64 address) {
    .reg .u64 pointer;
    .reg .u32 value;
    ld.param.u64 pointer, [address];
    ld.global.u32 value, [pointer];
    add.u32 value, value, 1;
    st.global.u32 [pointer], value;
    ret;
}
)ptx";
    CUmodule module;
    ASSERT_EQ(cuModuleLoadData(&module, program), CUDA_SUCCESS);
    CUfunction function;
    ASSERT_EQ(cuModuleGetFunction(&function, module, "increment"),
              CUDA_SUCCESS);
    void* arguments[] = {&address};
    ASSERT_EQ(cuLaunchKernel(function, 1, 1, 1, 1, 1, 1, 0, nullptr, arguments,
                             nullptr),
              CUDA_SUCCESS);
    ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
    ASSERT_EQ(cuModuleUnload(module), CUDA_SUCCESS);
}

inline void copy(TransferEngine& engine, SegmentID peer, void* local,
                 Request::OpCode operation) {
    Request request{};
    request.opcode = operation;
    request.source = local;
    request.target_id = peer;
    request.target_offset = kAddress;
    request.length = kBytes;
    request.transport_hint = NVLINK;
    auto batch = engine.allocateBatch(1);
    ASSERT_TRUE(engine.submitTransfer(batch, {request}).ok());
    TransferStatus status{};
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    do {
        ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
        if (status.s == COMPLETED || status.s == FAILED) break;
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_EQ(status.s, COMPLETED);
    EXPECT_EQ(status.transferred_bytes, kBytes);
    EXPECT_TRUE(engine.freeBatch(batch).ok());
}

inline void roundTrip(TransportType type) {
    int devices = 0;
    if (cudaGetDeviceCount(&devices) != cudaSuccess || !devices)
        GTEST_SKIP() << "Requires CUDA VMM";
    TransportEnvironment environment(type);
    Exporter exporter;
    ASSERT_TRUE(exporter.start(type));
    ASSERT_EQ(exporter.info.status, 0);
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    TransferEngine engine(config(type));
    ASSERT_TRUE(engine.available());
    SegmentID owner;
    ASSERT_TRUE(engine.openSegment(owner, exporter.info.segment).ok());
    void* remote = (void*)kAddress;
    EXPECT_FALSE(engine.mapRemoteMemory(owner, remote, kBytes + 1).ok());
    EXPECT_FALSE(engine.mapRemoteMemory(owner, (char*)remote + 64, 64).ok());
    MemoryOptions options;
    options.location = "cuda:0";
    options.global_address = kAddress;
    void* occupied = nullptr;
    ASSERT_TRUE(engine.allocateLocalMemory(&occupied, kBytes, options).ok());
    EXPECT_EQ(options.type, type);
    EXPECT_FALSE(engine.mapRemoteMemory(owner, remote, kBytes).ok());
    ASSERT_TRUE(engine.freeLocalMemory(occupied).ok());
    options.global_address = kAddress + 1;
    void* unchanged = (void*)1;
    EXPECT_FALSE(engine.allocateLocalMemory(&unchanged, kBytes, options).ok());
    EXPECT_EQ(unchanged, (void*)1);
    auto status = engine.mapRemoteMemory(owner, remote, kBytes);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(engine.mapRemoteMemory(owner, remote, kBytes).ok());
    increment(remote);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    uint32_t value = 0;
    ASSERT_EQ(cudaMemcpy(&value, remote, sizeof(value), cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(value, kInitial + 1);
    ASSERT_TRUE(engine.unmapRemoteMemory(owner, remote).ok());
    EXPECT_FALSE(engine.unmapRemoteMemory(owner, remote).ok());
    ASSERT_TRUE(engine.mapRemoteMemory(owner, remote, kBytes).ok());
    if (type == NVLINK) {
        // Calling device 0 differs from the source buffer's device. The VMM
        // copy path must grant source-device access to the existing mapping.
        options.global_address = 0;
        options.location = devices > 1 ? "cuda:1" : "cuda:0";
        void* local = nullptr;
        ASSERT_TRUE(engine.allocateLocalMemory(&local, kBytes, options).ok());
        ASSERT_TRUE(engine.registerLocalMemory(local, kBytes, options).ok());
        ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
        copy(engine, owner, local, Request::READ);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        ASSERT_EQ(
            cudaMemcpy(&value, local, sizeof(value), cudaMemcpyDeviceToHost),
            cudaSuccess);
        EXPECT_EQ(value, kInitial + 1);
        ASSERT_EQ(cudaMemset(local, 0x35, kBytes), cudaSuccess);
        ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
        copy(engine, owner, local, Request::WRITE);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        ASSERT_TRUE(engine.unregisterLocalMemory(local).ok());
        ASSERT_TRUE(engine.freeLocalMemory(local).ok());
    } else {
        // MNNVL's existing copy-submission implementation is unchanged. This
        // independently verifies async device access through its GVA mapping.
        cudaStream_t stream;
        ASSERT_EQ(cudaStreamCreate(&stream), cudaSuccess);
        ASSERT_EQ(cudaMemsetAsync(remote, 0x35, kBytes, stream), cudaSuccess);
        ASSERT_EQ(cudaStreamSynchronize(stream), cudaSuccess);
        ASSERT_EQ(cudaStreamDestroy(stream), cudaSuccess);
    }
    ASSERT_TRUE(engine.unmapRemoteMemory(owner, remote).ok());
    ASSERT_TRUE(engine.closeSegment(owner).ok());
    EXPECT_EQ(exporter.finish(), 0);
}

}  // namespace mooncake::tent::cuda_memory_test
