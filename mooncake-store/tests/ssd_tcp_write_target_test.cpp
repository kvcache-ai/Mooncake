// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_destination_registry.h"
#include "ssd_read_pipeline.h"
#include "ssd_read_source.h"
#include "ssd_transfer_target.h"
#include "transfer_engine.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <memory>
#include <vector>

namespace mooncake::ssd_offload {
namespace {

constexpr PeerIdentity kRequester{1, 2, 3};
constexpr PeerIdentity kOwner{4, 5, 6};
constexpr size_t kPayloadBytes = 3 * 1024 * 1024 + 123;
constexpr size_t kCanaryBytes = 64;

std::byte Pattern(size_t offset) { return std::byte(offset % 251); }

std::shared_ptr<TransferEngine> MakeTcpEngine() {
    auto engine = std::make_shared<TransferEngine>(false);
    if (engine->init("P2PHANDSHAKE", "127.0.0.1:0", "127.0.0.1", 0) != 0 ||
        !engine->installTransport("tcp", nullptr)) {
        return {};
    }
    return engine;
}

struct Control {
    int status{0};
    uint64_t base{0};
    char endpoint[128]{};
};

bool Send(int fd, const Control& value) {
#ifdef MSG_NOSIGNAL
    constexpr int kFlags = MSG_NOSIGNAL;
#else
    constexpr int kFlags = 0;
#endif
    return send(fd, &value, sizeof(value), kFlags) == ssize_t(sizeof(value));
}

bool Receive(int fd, Control& value) {
    pollfd p{fd, POLLIN, 0};
    int ready;
    do {
        ready = poll(&p, 1, 30000);
    } while (ready < 0 && errno == EINTR);
    return ready == 1 &&
           recv(fd, &value, sizeof(value), 0) == ssize_t(sizeof(value));
}

int RequesterProcess(int fd, bool timeout) {
    auto engine = MakeTcpEngine();
    if (!engine) return 1;
    auto backing = std::make_shared<std::vector<std::byte>>(
        kPayloadBytes + 2 * kCanaryBytes, std::byte{0xA5});
    if (engine->registerLocalMemory(backing->data(), backing->size(), "cpu:0",
                                    true, true) != 0) {
        Send(fd, {2});
        return 2;
    }
    const auto base = reinterpret_cast<uint64_t>(backing->data());
    DestinationRegistry registry(kRequester, engine->getLocalIpAndPort());
    auto registered = registry.Register(
        {base + kCanaryBytes, kPayloadBytes, base, backing->size()}, {7, 8},
        backing);
    if (registered.error != DestinationError::kOk) {
        Send(fd, {3});
        return 3;
    }
    auto acquired = registry.Acquire({kOwner, 1, 1},
                                     {{registered.region, 0, kPayloadBytes}});
    if (acquired.error != DestinationError::kOk) return 4;
    std::vector<ResolvedDestination> views;
    if (acquired.attempt.Publish(views) != DestinationError::kOk) return 5;
    if (timeout &&
        (acquired.attempt.Timeout() != DestinationError::kOk ||
         registry.BeginRevoke(registered.region) != DestinationError::kOk ||
         registry.Retire(registered.region) != DestinationError::kBusy)) {
        return 6;
    }
    Control ready{0, views[0].address, {}};
    if (views[0].transport_endpoint.size() >= sizeof(ready.endpoint)) return 7;
    std::memcpy(ready.endpoint, views[0].transport_endpoint.c_str(),
                views[0].transport_endpoint.size() + 1);
    if (!Send(fd, ready)) return 8;
    Control command;
    if (!Receive(fd, command) || command.status != 10) return 9;
    for (size_t i = 0; i < backing->size(); ++i) {
        const auto expected =
            i < kCanaryBytes || i >= kCanaryBytes + kPayloadBytes
                ? std::byte{0xA5}
                : Pattern(i - kCanaryBytes);
        if ((*backing)[i] != expected) return 10;
    }
    if (acquired.attempt.Complete({{kOwner, 1, 1},
                                   AttemptOutcome::kSucceeded,
                                   kPayloadBytes,
                                   true,
                                   kRequester}) != DestinationError::kOk ||
        acquired.attempt.snapshot().publishable == timeout) {
        return 11;
    }
    if (!timeout &&
        registry.BeginRevoke(registered.region) != DestinationError::kOk) {
        return 12;
    }
    if (registry.Retire(registered.region) != DestinationError::kOk ||
        engine->unregisterLocalMemory(backing->data(), true) != 0) {
        return 13;
    }
    return Send(fd, {20}) ? 0 : 14;
}

class ChildProcess {
   public:
    ~ChildProcess() {
        if (fd >= 0) close(fd);
        if (pid > 0) {
            kill(pid, SIGKILL);
            waitpid(pid, nullptr, 0);
        }
    }
    int fd{-1};
    pid_t pid{-1};
};

class SsdTcpWriteTargetTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("ssd_tcp_write_target_test");
        FLAGS_logtostderr = 1;
        unsetenv("MC_TCP_PROTO");
        unsetenv("MC_USE_TENT");
        unsetenv("MC_USE_TEV1");
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void RunCrossProcessRestore(bool timeout) {
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sockets), 0);
        const pid_t pid = fork();
        ASSERT_GE(pid, 0);
        if (pid == 0) {
            close(sockets[0]);
            const int rc = RequesterProcess(sockets[1], timeout);
            close(sockets[1]);
            _exit(rc);
        }
        close(sockets[1]);
        ChildProcess child;
        child.fd = sockets[0];
        child.pid = pid;

        Control ready;
        ASSERT_TRUE(Receive(child.fd, ready));
        ASSERT_EQ(ready.status, 0);

        auto engine = MakeTcpEngine();
        ASSERT_TRUE(engine);
        char filename[] = "/tmp/mooncake-ssd-tcp-target-XXXXXX";
        const int file = mkstemp(filename);
        ASSERT_GE(file, 0);
        struct TempFileGuard {
            int fd;
            const char* path;
            ~TempFileGuard() {
                if (fd >= 0) close(fd);
                if (path) unlink(path);
            }
        } temp_guard{file, filename};
        std::vector<std::byte> expected(kPayloadBytes);
        for (size_t i = 0; i < expected.size(); ++i) expected[i] = Pattern(i);
        ASSERT_EQ(write(file, expected.data(), expected.size()),
                  ssize_t(expected.size()));
        auto opened = OpenPosixReadSource(filename, kPayloadBytes);
        close(temp_guard.fd);
        temp_guard.fd = -1;
        unlink(filename);
        temp_guard.path = nullptr;
        ASSERT_EQ(opened.error, Error::kOk);

        constexpr size_t kSlotBytes = 64 * 1024;
        std::array<std::byte, kSlotBytes> slot{};
        ASSERT_EQ(engine->registerLocalMemory(slot.data(), slot.size(), "cpu:0",
                                              false, false),
                  0);
        auto target = PrepareSsdTcpWriteTarget(
            engine, {{ready.endpoint, ready.base, kPayloadBytes,
                      DestinationMemory::kHost}});
        ASSERT_EQ(target.error, DestinationError::kOk);
        ASSERT_TRUE(target.target);
        auto plan =
            BuildReadPlan({kPayloadBytes, 0, 0, {{0, 0, kPayloadBytes}}},
                          {kSlotBytes, 1, 128});
        ASSERT_EQ(plan.error, Error::kOk);
        ASSERT_GT(plan.plan.chunks.size(), 1u);
        for (const auto& chunk : plan.plan.chunks) {
            const auto read = opened.source->ReadAt(
                std::span(slot).first(chunk.read_bytes), chunk.file_offset);
            ASSERT_EQ(read.error, Error::kOk);
            ASSERT_EQ(read.bytes, chunk.read_bytes);
            ASSERT_EQ(
                target.target->WriteAndDrain(chunk, {slot.data(), slot.size()}),
                Error::kOk);
        }
        EXPECT_TRUE(target.target->DestinationsDrained());
        target.target.reset();
        ASSERT_EQ(engine->unregisterLocalMemory(slot.data(), false), 0);

        ASSERT_TRUE(Send(child.fd, {10}));
        Control done;
        ASSERT_TRUE(Receive(child.fd, done));
        ASSERT_EQ(done.status, 20);
        int status = 0;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        child.pid = -1;
        ASSERT_TRUE(WIFEXITED(status));
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }
};

TEST_F(SsdTcpWriteTargetTest, RestoresRealFileAcrossProcessesThroughSlots) {
    RunCrossProcessRestore(false);
}

TEST_F(SsdTcpWriteTargetTest, TimedOutLeaseRetainsRegionUntilDelayedDrainAck) {
    RunCrossProcessRestore(true);
}

TEST_F(SsdTcpWriteTargetTest, PreflightsEveryFragmentBeforeTransfer) {
    auto source = MakeTcpEngine();
    auto peer = MakeTcpEngine();
    ASSERT_TRUE(source);
    ASSERT_TRUE(peer);
    std::array<std::byte, 128> backing;
    backing.fill(std::byte{0xA5});
    ASSERT_EQ(peer->registerLocalMemory(backing.data(), backing.size(), "cpu:0",
                                        true, true),
              0);
    const auto base = reinterpret_cast<uint64_t>(backing.data());
    auto prepared = PrepareSsdTcpWriteTarget(
        source,
        {{peer->getLocalIpAndPort(), base + 16, 32, DestinationMemory::kHost}});
    ASSERT_EQ(prepared.error, DestinationError::kOk);

    std::array<std::byte, 32> slot;
    slot.fill(std::byte{0x42});
    ASSERT_EQ(source->registerLocalMemory(slot.data(), slot.size(), "cpu:0",
                                          false, false),
              0);
    auto plan = BuildReadPlan({32, 0, 0, {{0, 0, 32}}}, {4096, 1, 4});
    ASSERT_EQ(plan.error, Error::kOk);

    auto bad = plan.plan.chunks[0];
    bad.fragments.push_back({0, {1, 0, 1}});
    EXPECT_EQ(prepared.target->WriteAndDrain(bad, {slot.data(), slot.size()}),
              Error::kInvalidArgument);
    EXPECT_TRUE(prepared.target->DestinationsDrained());
    EXPECT_TRUE(std::all_of(backing.begin(), backing.end(),
                            [](auto b) { return b == std::byte{0xA5}; }));

    ASSERT_EQ(prepared.target->WriteAndDrain(plan.plan.chunks[0],
                                             {slot.data(), slot.size()}),
              Error::kOk);
    EXPECT_TRUE(prepared.target->DestinationsDrained());
    EXPECT_TRUE(std::all_of(backing.begin() + 16, backing.begin() + 48,
                            [](auto b) { return b == std::byte{0x42}; }));

    prepared.target.reset();
    EXPECT_EQ(source->unregisterLocalMemory(slot.data(), false), 0);
    EXPECT_EQ(peer->unregisterLocalMemory(backing.data(), true), 0);
}

TEST_F(SsdTcpWriteTargetTest, RejectsUnsafeViewsAndLegacyPeersBeforeWrite) {
    auto source = MakeTcpEngine();
    auto peer = MakeTcpEngine();
    ASSERT_TRUE(source);
    ASSERT_TRUE(peer);
    std::array<std::byte, 128> backing{};
    ASSERT_EQ(peer->registerLocalMemory(backing.data(), backing.size(), "cpu:0",
                                        true, true),
              0);
    const auto base = reinterpret_cast<uint64_t>(backing.data());
    const ResolvedDestination view{peer->getLocalIpAndPort(), base + 16, 32,
                                   DestinationMemory::kHost};

    EXPECT_EQ(PrepareSsdTcpWriteTarget(source, {view}, 0).error,
              DestinationError::kLimit);
    auto bad = view;
    bad.address = UINT64_MAX;
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source, {bad}).error,
              DestinationError::kOverflow);
    bad = view;
    bad.memory = DestinationMemory::kDevice;
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source, {bad}).error,
              DestinationError::kNotSupported);
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source, {view, view}).error,
              DestinationError::kOverlap);
    bad = view;
    bad.address = base + backing.size();
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source, {bad}).error,
              DestinationError::kInvalidArgument);
    EXPECT_EQ(peer->unregisterLocalMemory(backing.data(), true), 0);
}

TEST_F(SsdTcpWriteTargetTest, RejectsLegacyTcpProtoBeforeEngineStart) {
    struct ScopedEnv {
        explicit ScopedEnv(const char* value) {
            setenv("MC_TCP_PROTO", value, 1);
        }
        ~ScopedEnv() { unsetenv("MC_TCP_PROTO"); }
    };
    // Guard declared before TransferEngine instances so every background
    // engine thread is joined before restoring the process environment.
    ScopedEnv legacy_env("1");
    auto source = MakeTcpEngine();
    auto peer = MakeTcpEngine();
    ASSERT_TRUE(source);
    ASSERT_TRUE(peer);
    std::array<std::byte, 128> backing{};
    ASSERT_EQ(peer->registerLocalMemory(backing.data(), backing.size(), "cpu:0",
                                        true, true),
              0);
    const auto base = reinterpret_cast<uint64_t>(backing.data());
    const ResolvedDestination view{peer->getLocalIpAndPort(), base + 16, 32,
                                   DestinationMemory::kHost};
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source, {view}).error,
              DestinationError::kNotSupported);
    EXPECT_EQ(peer->unregisterLocalMemory(backing.data(), true), 0);
}

TEST_F(SsdTcpWriteTargetTest, MergesAdjacentFragmentsWithinSingleBuffer) {
    auto source = MakeTcpEngine();
    auto peer = MakeTcpEngine();
    ASSERT_TRUE(source);
    ASSERT_TRUE(peer);
    std::array<std::byte, 64> staging{};
    for (size_t i = 0; i < staging.size(); ++i) staging[i] = Pattern(i + 17);
    std::array<std::byte, 128> destination;
    destination.fill(std::byte{0xA5});
    ASSERT_EQ(source->registerLocalMemory(staging.data(), staging.size(),
                                          "cpu:0", false, true),
              0);
    ASSERT_EQ(peer->registerLocalMemory(destination.data(), destination.size(),
                                        "cpu:0", true, true),
              0);
    const auto base = reinterpret_cast<uint64_t>(destination.data());
    auto prepared = PrepareSsdTcpWriteTarget(
        source,
        {{peer->getLocalIpAndPort(), base + 16, 32, DestinationMemory::kHost},
         {peer->getLocalIpAndPort(), base + 48, 32, DestinationMemory::kHost}});
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    // Two adjacent fragments in the same destination MR and contiguous in the
    // staging slot should coalesce into one transfer and preserve all bytes.
    ReadChunk chunk{0, 0, 64, 0, 64, {{0, {0, 0, 32}}, {32, {1, 0, 32}}}};
    ASSERT_EQ(
        prepared.target->WriteAndDrain(chunk, {staging.data(), staging.size()}),
        Error::kOk);
    EXPECT_TRUE(prepared.target->DestinationsDrained());
    EXPECT_TRUE(std::all_of(destination.begin(), destination.begin() + 16,
                            [](auto b) { return b == std::byte{0xA5}; }));
    EXPECT_EQ(std::memcmp(destination.data() + 16, staging.data(), 64), 0);
    EXPECT_TRUE(std::all_of(destination.begin() + 80, destination.end(),
                            [](auto b) { return b == std::byte{0xA5}; }));
    prepared.target.reset();
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data(), true), 0);
    EXPECT_EQ(source->unregisterLocalMemory(staging.data(), true), 0);
}

TEST_F(SsdTcpWriteTargetTest, WriteFailureClearsDestinationDrainProof) {
    auto source = MakeTcpEngine();
    auto peer = MakeTcpEngine();
    ASSERT_TRUE(source);
    ASSERT_TRUE(peer);
    std::array<std::byte, 128> backing;
    backing.fill(std::byte{0xA5});
    ASSERT_EQ(peer->registerLocalMemory(backing.data(), backing.size(), "cpu:0",
                                        true, true),
              0);
    const auto base = reinterpret_cast<uint64_t>(backing.data());
    auto prepared = PrepareSsdTcpWriteTarget(
        source,
        {{peer->getLocalIpAndPort(), base + 16, 32, DestinationMemory::kHost}});
    ASSERT_EQ(prepared.error, DestinationError::kOk);

    std::array<std::byte, 32> slot;
    slot.fill(std::byte{0x42});
    ASSERT_EQ(source->registerLocalMemory(slot.data(), slot.size(), "cpu:0",
                                          false, false),
              0);
    auto plan = BuildReadPlan({32, 0, 0, {{0, 0, 32}}}, {4096, 1, 4});
    ASSERT_EQ(plan.error, Error::kOk);

    ASSERT_EQ(peer->unregisterLocalMemory(backing.data(), true), 0);
    EXPECT_EQ(prepared.target->WriteAndDrain(plan.plan.chunks[0],
                                             {slot.data(), slot.size()}),
              Error::kTransferError);
    EXPECT_FALSE(prepared.target->DestinationsDrained());
    EXPECT_TRUE(std::all_of(backing.begin(), backing.end(),
                            [](auto b) { return b == std::byte{0xA5}; }));
    prepared.target.reset();
    EXPECT_EQ(source->unregisterLocalMemory(slot.data(), false), 0);
}

TEST_F(SsdTcpWriteTargetTest, RejectsLoopbackSourceAliasBeforeSubmission) {
    auto engine = MakeTcpEngine();
    ASSERT_TRUE(engine);
    std::array<std::byte, 64> slot{};
    ASSERT_EQ(engine->registerLocalMemory(slot.data(), slot.size(), "cpu:0",
                                          true, true),
              0);
    auto prepared = PrepareSsdTcpWriteTarget(
        engine,
        {{engine->getLocalIpAndPort(), reinterpret_cast<uint64_t>(slot.data()),
          slot.size(), DestinationMemory::kHost}});
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    auto plan =
        BuildReadPlan({slot.size(), 0, 0, {{0, 0, slot.size()}}}, {4096, 1, 4});
    ASSERT_EQ(plan.error, Error::kOk);
    EXPECT_EQ(prepared.target->WriteAndDrain(plan.plan.chunks[0],
                                             {slot.data(), slot.size()}),
              Error::kInvalidArgument);
    EXPECT_TRUE(prepared.target->DestinationsDrained());
    prepared.target.reset();
    EXPECT_EQ(engine->unregisterLocalMemory(slot.data(), true), 0);
}

TEST_F(SsdTcpWriteTargetTest, AdjacentMrsNeverBypassSingleTcpBufferPreflight) {
    auto source = MakeTcpEngine();
    auto peer = MakeTcpEngine();
    ASSERT_TRUE(source);
    ASSERT_TRUE(peer);
    std::array<std::byte, 64> staging{}, destination{};
    ASSERT_EQ(source->registerLocalMemory(staging.data(), staging.size(),
                                          "cpu:0", false, true),
              0);
    ASSERT_EQ(
        peer->registerLocalMemory(destination.data(), 32, "cpu:0", true, true),
        0);
    ASSERT_EQ(peer->registerLocalMemory(destination.data() + 32, 32, "cpu:0",
                                        true, true),
              0);
    auto made = PrepareSsdTcpWriteTarget(
        source, {{peer->getLocalIpAndPort(),
                  reinterpret_cast<uint64_t>(destination.data()),
                  destination.size(), DestinationMemory::kHost}});
    EXPECT_EQ(made.error, DestinationError::kInvalidArgument);
    EXPECT_FALSE(made.target);

    staging.fill(std::byte{0x47});
    made = PrepareSsdTcpWriteTarget(
        source, {{peer->getLocalIpAndPort(),
                  reinterpret_cast<uint64_t>(destination.data()), 32,
                  DestinationMemory::kHost},
                 {peer->getLocalIpAndPort(),
                  reinterpret_cast<uint64_t>(destination.data() + 32), 32,
                  DestinationMemory::kHost}});
    ASSERT_EQ(made.error, DestinationError::kOk);
    ReadChunk chunk{0, 0, 64, 0, 64, {{32, {1, 0, 32}}, {0, {0, 0, 32}}}};
    ASSERT_EQ(
        made.target->WriteAndDrain(chunk, {staging.data(), staging.size()}),
        Error::kOk);
    EXPECT_EQ(destination, staging);
    made.target.reset();
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data() + 32, true), 0);
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data(), true), 0);
    EXPECT_EQ(source->unregisterLocalMemory(staging.data(), true), 0);
}

TEST_F(SsdTcpWriteTargetTest,
       MultiWaveScatterSucceedsAndWaveFailureClearsDrain) {
    auto source = MakeTcpEngine();
    auto peer = MakeTcpEngine();
    ASSERT_TRUE(source);
    ASSERT_TRUE(peer);

    // 10 non-mergeable fragments with guard gaps between each destination span
    // 3 bounded waves (kMaxWaveEntries = 4 -> 4 + 4 + 2).
    constexpr size_t kFragments = 10;
    constexpr size_t kFragBytes = 128;
    constexpr size_t kStride = 256;
    std::vector<std::byte> staging(kFragments * kFragBytes);
    for (size_t i = 0; i < staging.size(); ++i) {
        staging[i] = static_cast<std::byte>((i * 53 + 11) % 251);
    }
    std::vector<std::byte> destination(kFragments * kStride, std::byte{0xCC});
    ASSERT_EQ(source->registerLocalMemory(staging.data(), staging.size(),
                                          "cpu:0", false, true),
              0);
    ASSERT_EQ(peer->registerLocalMemory(destination.data(), destination.size(),
                                        "cpu:0", true, true),
              0);

    std::vector<ResolvedDestination> views;
    views.reserve(kFragments);
    ReadChunk chunk{0, 0, staging.size(), 0, staging.size(), {}};
    chunk.fragments.reserve(kFragments);
    const auto dst_base = reinterpret_cast<uint64_t>(destination.data());
    for (size_t i = 0; i < kFragments; ++i) {
        views.push_back({peer->getLocalIpAndPort(), dst_base + i * kStride,
                         kFragBytes, DestinationMemory::kHost});
        chunk.fragments.push_back({i * kFragBytes, {i, 0, kFragBytes}});
    }

    auto made = PrepareSsdTcpWriteTarget(source, views);
    ASSERT_EQ(made.error, DestinationError::kOk);
    ASSERT_TRUE(made.target);
    ASSERT_EQ(
        made.target->WriteAndDrain(chunk, {staging.data(), staging.size()}),
        Error::kOk);
    EXPECT_TRUE(made.target->DestinationsDrained());
    for (size_t i = 0; i < kFragments; ++i) {
        EXPECT_EQ(std::memcmp(destination.data() + i * kStride,
                              staging.data() + i * kFragBytes, kFragBytes),
                  0);
        for (size_t g = kFragBytes; g < kStride; ++g) {
            EXPECT_EQ(destination[i * kStride + g], std::byte{0xCC});
        }
    }

    // Tear down the peer so wave 0 fails; WriteAndDrain must abort remaining
    // waves and clear DestinationsDrained().
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data(), true), 0);
    peer.reset();
    EXPECT_NE(
        made.target->WriteAndDrain(chunk, {staging.data(), staging.size()}),
        Error::kOk);
    EXPECT_FALSE(made.target->DestinationsDrained());

    made.target.reset();
    EXPECT_EQ(source->unregisterLocalMemory(staging.data(), true), 0);
}

}  // namespace
}  // namespace mooncake::ssd_offload
