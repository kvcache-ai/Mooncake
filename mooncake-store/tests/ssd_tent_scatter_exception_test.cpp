// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated executable: allocation failure affects only this test's caller
// thread, only the first TENT Request allocation with an already live batch.
#include "transfer_engine.h"
#include "tent/common/types.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <array>
#include <chrono>
#include <cstdlib>
#include <new>
#include <thread>

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static size_t TentBatches(TransferEngine& engine) {
        size_t count = 0;
        for (auto& shard : engine.impl_tent_->impl_->batch_shards_) {
            std::lock_guard lock(shard.mtx);
            count += shard.active_batches.size();
        }
        return count;
    }
};
}  // namespace mooncake
namespace {
thread_local mooncake::TransferEngine* fault_engine = nullptr;
thread_local bool fault_enabled = false, fault_fired = false;
thread_local int fault_notify = -1;
}  // namespace
void* operator new(size_t bytes) {
    if (fault_enabled && bytes == sizeof(mooncake::tent::Request) &&
        mooncake::TransferEngineImplTestPeer::TentBatches(*fault_engine) == 1) {
        fault_enabled = false;
        fault_fired = true;
        const char proof = '1';
        (void)write(fault_notify, &proof, 1);
        throw std::bad_alloc();
    }
    if (void* pointer = std::malloc(bytes ? bytes : 1)) return pointer;
    throw std::bad_alloc();
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new[](size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new(size_t bytes, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t bytes, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void operator delete(void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}

namespace {
int RunConversionFailure(int notify) {
    using namespace mooncake;
    setenv("MC_USE_TENT", "1", 1);
    setenv("MC_FORCE_TCP", "1", 1);
    setenv("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1", 1);
    setenv(
        "MC_TENT_CONF",
        R"({"transports":{"tcp":{"enable":true},"rdma":{"enable":false},"shm":{"enable":false}},"metrics":{"enabled":false}})",
        1);
    TransferEngine engine(false);
    if (!engine.isUsingTent() ||
        engine.init("P2PHANDSHAKE", "tent-scatter") != 0)
        return 1;
    std::array<char, 4096> buffer{};
    if (engine.registerLocalMemory(buffer.data(), buffer.size(), "cpu:0", true,
                                   true) != 0)
        return 2;
    const auto segment = engine.openSegment("tent-scatter");
    if (segment == static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT)) return 3;
    std::array<size_t, 1> offsets{0}, lengths{16};
    size_t failed_callbacks = 0;
    TransferEngine::ScatterTransferRange range{
        TransferRequest::WRITE,
        "tent-scatter",
        reinterpret_cast<uint64_t>(buffer.data() + 32),
        16,
        buffer.data(),
        buffer.size(),
        offsets,
        offsets,
        lengths,
        [&](size_t, const Status& status) {
            if (!status.ok()) ++failed_callbacks;
        }};
    fault_engine = &engine;
    fault_notify = notify;
    fault_enabled = true;
    auto operation = engine.submitScatter({range});
    fault_enabled = false;
    if (!fault_fired || operation.wait().ok() || failed_callbacks != 1 ||
        TransferEngineImplTestPeer::TentBatches(engine) != 0)
        return 4;
    engine.closeSegment(segment);
    if (engine.unregisterLocalMemory(buffer.data(), true) != 0) return 5;
    return 0;
}
TEST(SsdTentScatterException,
     PrepublicationConversionFailureReleasesEmptyBatch) {
    google::InitGoogleLogging("ssd_tent_scatter_exception_test");
    FLAGS_logtostderr = 1;
    int pipe_fds[2];
    ASSERT_EQ(pipe(pipe_fds), 0);
    const auto child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(pipe_fds[0]);
        const int result = RunConversionFailure(pipe_fds[1]);
        close(pipe_fds[1]);
        _exit(result);
    }
    close(pipe_fds[1]);
    pollfd notification{pipe_fds[0], POLLIN, 0};
    const int ready = poll(&notification, 1, 10000);
    char proof = 0;
    const bool fired_after_allocation =
        ready == 1 && read(pipe_fds[0], &proof, 1) == 1 && proof == '1';
    close(pipe_fds[0]);
    int status = 0;
    pid_t ended = 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!(ended = waitpid(child, &status, WNOHANG)) &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (!ended) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
    }
    EXPECT_TRUE(fired_after_allocation)
        << "Fault must follow batch allocation and precede task publication";
    EXPECT_EQ(ended, child)
        << "An unpublished TENT batch must not wait for nonexistent I/O";
    EXPECT_TRUE(WIFEXITED(status));
    if (WIFEXITED(status)) {
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }
    google::ShutdownGoogleLogging();
}
}  // namespace
