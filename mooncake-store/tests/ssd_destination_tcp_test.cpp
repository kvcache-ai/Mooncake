// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_transfer_registration.h"
#include "ssd_transfer_target.h"
#include "ssd_read_source.h"
#include "transfer_engine.h"

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <poll.h>
#include <unistd.h>
#include <signal.h>
#include <array>
#include <algorithm>
#include <cstring>
#include <vector>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
constexpr size_t payload_bytes = 3 * 1024 * 1024 + 123;
constexpr size_t canary_bytes = 64;
std::byte Pattern(size_t offset) { return std::byte(offset % 251); }
std::shared_ptr<TransferEngine> Engine() {
    auto engine = std::make_shared<TransferEngine>(false);
    if (engine->init("P2PHANDSHAKE", "127.0.0.1:0", "127.0.0.1", 0) != 0 ||
        !engine->installTransport("tcp", nullptr))
        return {};
    return engine;
}
// Fixed-size test coordination over a local Unix socket, not the Store RPC
// protocol. Destination views come only from the requester's published lease.
struct Control {
    int status{0};
    uint64_t base{0};
    char endpoint[128]{};
};
bool Send(int fd, const Control& value) {
    return send(fd, &value, sizeof(value), MSG_NOSIGNAL) ==
           ssize_t(sizeof(value));
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
    auto engine = Engine();
    if (!engine) return 1;
    auto backing = std::make_shared<std::vector<std::byte>>(
        payload_bytes + 2 * canary_bytes, std::byte{0xA5});
    const auto base = reinterpret_cast<uint64_t>(backing->data());
    auto transport =
        std::make_shared<TransferEngineDestinationRegistration>(engine);
    DestinationRegistration registration(requester, engine->getLocalIpAndPort(),
                                         transport);
    auto region = registration.Register(
        {base + canary_bytes, payload_bytes, base, backing->size()}, "cpu:0",
        backing);
    if (region.error != DestinationError::kOk) {
        Send(fd, {2});
        return 2;
    }
    auto attempt = registration.Acquire({owner, 1, 1},
                                        {{region.region, 0, payload_bytes}});
    if (attempt.error != DestinationError::kOk) return 3;
    std::vector<ResolvedDestination> views;
    if (attempt.attempt.Publish(views) != DestinationError::kOk) return 4;
    if (timeout &&
        (attempt.attempt.Timeout() != DestinationError::kOk ||
         registration.Revoke(region.region) != DestinationError::kBusy ||
         registration.pinned_regions() != 1))
        return 8;
    Control ready{0, views[0].address, {}};
    if (views[0].transport_endpoint.size() >= sizeof(ready.endpoint)) return 5;
    std::memcpy(ready.endpoint, views[0].transport_endpoint.c_str(),
                views[0].transport_endpoint.size() + 1);
    if (!Send(fd, ready)) return 6;
    Control command;
    if (!Receive(fd, command) || command.status != 10) return 7;
    if (timeout) {
        if (registration.Revoke(region.region) != DestinationError::kBusy ||
            registration.pinned_regions() != 1)
            return 8;
        if (!Send(fd, {11}) || !Receive(fd, command) || command.status != 12)
            return 9;
    }
    for (size_t i = 0; i < backing->size(); ++i) {
        const auto expected =
            i < canary_bytes || i >= canary_bytes + payload_bytes
                ? std::byte{0xA5}
                : Pattern(i - canary_bytes);
        if ((*backing)[i] != expected) return 10;
    }
    // Parent confirms that every accepted v2 WRITE has completed. A timeout
    // command alone never fabricates this terminal physical-drain ACK.
    if (registration.Complete({{owner, 1, 1},
                               AttemptOutcome::kSucceeded,
                               payload_bytes,
                               true,
                               requester}) != DestinationError::kOk ||
        registration.Inspect({owner, 1, 1}).publishable == timeout ||
        registration.Revoke(region.region) != DestinationError::kOk ||
        registration.pinned_regions() != 0)
        return 11;
    return Send(fd, {20}) ? 0 : 12;
}
class Child {
   public:
    ~Child() {
        if (fd >= 0) close(fd);
        if (pid > 0) {
            kill(pid, SIGKILL);
            waitpid(pid, nullptr, 0);
        }
    }
    int fd{-1};
    pid_t pid{-1};
};
class SsdDestinationTcp : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("ssd_destination_tcp_test");
        FLAGS_logtostderr = 1;
        // The test explicitly checks acknowledged v2 TCP, not the legacy
        // escape hatch where sender completion need not mean remote visibility.
        unsetenv("MC_TCP_PROTO");
        unsetenv("MC_USE_TENT");
        unsetenv("MC_USE_TEV1");
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }
    void Restore(bool timeout) {
        int sockets[2];
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sockets), 0);
        const pid_t pid = fork();
        ASSERT_GE(pid, 0);
        if (pid == 0) {
            close(sockets[0]);
            int result = RequesterProcess(sockets[1], timeout);
            close(sockets[1]);
            _exit(result);
        }
        close(sockets[1]);
        Child child;
        child.fd = sockets[0];
        child.pid = pid;
        Control ready;
        ASSERT_TRUE(Receive(child.fd, ready));
        ASSERT_EQ(ready.status, 0);
        auto engine = Engine();
        ASSERT_TRUE(engine);
        const auto segment = engine->openSegment(ready.endpoint);
        ASSERT_NE(segment, static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT));
        auto desc = engine->getMetadata()->getSegmentDescByID(segment);
        ASSERT_TRUE(desc);
        ASSERT_GE(desc->tcp_proto_version, 2);
        ASSERT_EQ(engine->closeSegment(segment), 0);
        char filename[] = "/tmp/mooncake-ssd-tcp-XXXXXX";
        int file = mkstemp(filename);
        ASSERT_GE(file, 0);
        std::vector<std::byte> expected(payload_bytes);
        for (size_t i = 0; i < expected.size(); ++i) expected[i] = Pattern(i);
        ASSERT_EQ(write(file, expected.data(), expected.size()),
                  ssize_t(expected.size()));
        auto opened = OpenPosixReadSource(filename, {payload_bytes, 0, 1});
        close(file);
        unlink(filename);
        ASSERT_EQ(opened.error, Error::kOk);
        constexpr size_t slot_bytes = 64 * 1024;
        std::array<std::byte, slot_bytes> slot;
        ASSERT_EQ(engine->registerLocalMemory(slot.data(), slot.size(), "cpu:0",
                                              false, false),
                  0);
        auto target = PrepareSsdTcpWriteTarget(
            engine, {{ready.endpoint, ready.base, payload_bytes,
                      DestinationMemory::kHost}});
        ASSERT_EQ(target.error, DestinationError::kOk);
        ASSERT_TRUE(target.target);
        auto plan =
            BuildReadPlan({payload_bytes, 0, {{0, {{1, 1, 0, payload_bytes}}}}},
                          {slot_bytes, 1, 128});
        ASSERT_EQ(plan.error, Error::kOk);
        ASSERT_GT(plan.plan.chunks.size(), 1);
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
        ASSERT_EQ(engine->unregisterLocalMemory(slot.data(), false), 0);
        ASSERT_TRUE(Send(child.fd, {10}));
        Control done;
        if (timeout) {
            ASSERT_TRUE(Receive(child.fd, done));
            ASSERT_EQ(done.status, 11);
            ASSERT_TRUE(Send(child.fd, {12}));
        }
        ASSERT_TRUE(Receive(child.fd, done));
        ASSERT_EQ(done.status, 20);
        int status;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        child.pid = -1;
        ASSERT_TRUE(WIFEXITED(status));
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }
};
TEST_F(SsdDestinationTcp, RestoresRealFileThroughBoundedSlotsAcrossProcesses) {
    Restore(false);
}
TEST_F(SsdDestinationTcp,
       TimedOutLeaseRetainsMrThroughWritesAndDelayedDrainConfirmation) {
    Restore(true);
}

TEST_F(SsdDestinationTcp, RejectsDeviceAndDoesNotRemoveAnExistingRegistration) {
    auto engine = Engine();
    ASSERT_TRUE(engine);
    std::array<std::byte, 4096> buffer{};
    const auto base = reinterpret_cast<uint64_t>(buffer.data());
    TransferEngineDestinationRegistration transport(engine);
    EXPECT_EQ(transport
                  .RegisterWritable({base, buffer.size(), base, buffer.size(),
                                     DestinationMemory::kDevice},
                                    "npu")
                  .error,
              DestinationError::kNotSupported);
    ASSERT_EQ(engine->registerLocalMemory(buffer.data(), buffer.size(), "cpu:0",
                                          false, true),
              0);
    const auto rejected = transport.RegisterWritable(
        {base, buffer.size(), base, buffer.size()}, "cpu:0");
    EXPECT_EQ(rejected.error, DestinationError::kOverlap);
    EXPECT_FALSE(rejected.cleanup_required);
    const auto segment = engine->openSegment(engine->getLocalIpAndPort());
    std::vector<SegmentBufferInfo> buffers;
    ASSERT_EQ(engine->getSegmentBuffers(segment, buffers), 0);
    ASSERT_EQ(buffers.size(), 1);
    EXPECT_EQ(buffers[0].addr, base);
    EXPECT_EQ(engine->closeSegment(segment), 0);
    EXPECT_EQ(engine->unregisterLocalMemory(buffer.data(), true), 0);
}

TEST_F(SsdDestinationTcp,
       RequiredWriteAckRejectsLegacyFramingBeforeAnyDataWrite) {
    auto source_engine = Engine(), destination_engine = Engine();
    ASSERT_TRUE(source_engine);
    ASSERT_TRUE(destination_engine);
    std::array<std::byte, 128> destination;
    destination.fill(std::byte{0xA5});
    std::array<std::byte, 32> source;
    source.fill(std::byte{0x42});
    ASSERT_EQ(destination_engine->registerLocalMemory(
                  destination.data(), destination.size(), "cpu:0", true, true),
              0);
    ASSERT_EQ(source_engine->registerLocalMemory(source.data(), source.size(),
                                                 "cpu:0", false, false),
              0);
    std::array<size_t, 1> offsets{0}, lengths{source.size()};
    TransferEngine::ScatterTransferRange range{
        TransferRequest::WRITE,
        destination_engine->getLocalIpAndPort(),
        reinterpret_cast<uint64_t>(destination.data() + 16),
        source.size(),
        source.data(),
        source.size(),
        offsets,
        offsets,
        lengths,
        {}};
    range.require_tcp_write_ack = true;
    setenv("MC_TCP_PROTO", "1", 1);
    auto transfer = source_engine->submitScatter({range});
    const auto status = transfer.wait();
    unsetenv("MC_TCP_PROTO");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(std::all_of(destination.begin(), destination.end(),
                            [](auto byte) { return byte == std::byte{0xA5}; }));
    EXPECT_EQ(source_engine->unregisterLocalMemory(source.data(), false), 0);
    EXPECT_EQ(
        destination_engine->unregisterLocalMemory(destination.data(), true), 0);
}

TEST_F(SsdDestinationTcp, WriteTargetPreflightsEveryFragmentBeforeTransfer) {
    auto source_engine = Engine(), destination_engine = Engine();
    ASSERT_TRUE(source_engine);
    ASSERT_TRUE(destination_engine);
    auto backing = std::make_shared<std::array<std::byte, 128>>();
    backing->fill(std::byte{0xA5});
    const auto base = reinterpret_cast<uint64_t>(backing->data());
    DestinationRegistration registration(
        requester, destination_engine->getLocalIpAndPort(),
        std::make_shared<TransferEngineDestinationRegistration>(
            destination_engine));
    auto registered = registration.Register(
        {base + 16, 96, base, backing->size()}, "cpu:0", backing);
    ASSERT_EQ(registered.error, DestinationError::kOk);
    auto acquired =
        registration.Acquire({owner, 1, 1}, {{registered.region, 0, 32}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    auto prepared = PrepareSsdTcpWriteTarget(source_engine, views);
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    ASSERT_TRUE(prepared.target);
    std::array<std::byte, 32> slot;
    slot.fill(std::byte{0x42});
    ASSERT_EQ(source_engine->registerLocalMemory(slot.data(), slot.size(),
                                                 "cpu:0", false, false),
              0);
    auto plan = BuildReadPlan({32, 0, {{0, {{1, 1, 0, 32}}}}}, {4096, 1, 4});
    ASSERT_EQ(plan.error, Error::kOk);
    auto bad = plan.plan.chunks[0];
    bad.fragments.push_back({0, {2, 1, 0, 1}});
    EXPECT_EQ(prepared.target->WriteAndDrain(bad, {slot.data(), slot.size()}),
              Error::kInvalidArgument);
    EXPECT_TRUE(prepared.target->DestinationsDrained());
    EXPECT_TRUE(std::all_of(backing->begin(), backing->end(),
                            [](auto b) { return b == std::byte{0xA5}; }));
    ASSERT_EQ(prepared.target->WriteAndDrain(plan.plan.chunks[0],
                                             {slot.data(), slot.size()}),
              Error::kOk);
    EXPECT_TRUE(prepared.target->DestinationsDrained());
    EXPECT_TRUE(std::all_of(backing->begin() + 16, backing->begin() + 48,
                            [](auto b) { return b == std::byte{0x42}; }));
    EXPECT_EQ(
        registration.Complete(
            {{owner, 1, 1}, AttemptOutcome::kSucceeded, 32, true, requester}),
        DestinationError::kOk);
    EXPECT_EQ(registration.Revoke(registered.region), DestinationError::kOk);
    EXPECT_EQ(source_engine->unregisterLocalMemory(slot.data(), false), 0);
}

TEST_F(SsdDestinationTcp,
       WriteTargetRejectsUnsafeViewsAndLegacyPeerBeforeWrite) {
    auto source_engine = Engine(), destination_engine = Engine();
    ASSERT_TRUE(source_engine);
    ASSERT_TRUE(destination_engine);
    std::array<std::byte, 128> backing{};
    const auto base = reinterpret_cast<uint64_t>(backing.data());
    ASSERT_EQ(destination_engine->registerLocalMemory(
                  backing.data(), backing.size(), "cpu:0", true, true),
              0);
    const ResolvedDestination view{destination_engine->getLocalIpAndPort(),
                                   base + 16, 32, DestinationMemory::kHost};
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source_engine, {view}, 0).error,
              DestinationError::kLimit);
    auto bad = view;
    bad.address = UINT64_MAX;
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source_engine, {bad}).error,
              DestinationError::kOverflow);
    bad = view;
    bad.memory = DestinationMemory::kDevice;
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source_engine, {bad}).error,
              DestinationError::kNotSupported);
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source_engine, {view, view}).error,
              DestinationError::kOverlap);
    bad = view;
    bad.address = base + backing.size();
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source_engine, {bad}).error,
              DestinationError::kInvalidArgument);
    const auto segment = source_engine->openSegment(view.transport_endpoint);
    auto desc = source_engine->getMetadata()->getSegmentDescByID(segment);
    ASSERT_TRUE(desc);
    desc->tcp_proto_version = 1;
    EXPECT_EQ(PrepareSsdTcpWriteTarget(source_engine, {view}).error,
              DestinationError::kNotSupported);
    desc->tcp_proto_version = 2;
    EXPECT_EQ(source_engine->closeSegment(segment), 0);
    EXPECT_EQ(destination_engine->unregisterLocalMemory(backing.data(), true),
              0);
}

TEST_F(SsdDestinationTcp, WriteFailureDoesNotProveDestinationDrain) {
    auto source_engine = Engine(), destination_engine = Engine();
    ASSERT_TRUE(source_engine);
    ASSERT_TRUE(destination_engine);
    std::array<std::byte, 128> backing;
    backing.fill(std::byte{0xA5});
    const auto base = reinterpret_cast<uint64_t>(backing.data());
    ASSERT_EQ(destination_engine->registerLocalMemory(
                  backing.data(), backing.size(), "cpu:0", true, true),
              0);
    auto prepared = PrepareSsdTcpWriteTarget(
        source_engine, {{destination_engine->getLocalIpAndPort(), base + 16, 32,
                         DestinationMemory::kHost}});
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    std::array<std::byte, 32> slot;
    slot.fill(std::byte{0x42});
    ASSERT_EQ(source_engine->registerLocalMemory(slot.data(), slot.size(),
                                                 "cpu:0", false, false),
              0);
    auto plan = BuildReadPlan({32, 0, {{0, {{1, 1, 0, 32}}}}}, {4096, 1, 4});
    ASSERT_EQ(plan.error, Error::kOk);
    // Simulate a peer losing its MR after trusted setup. The actual server
    // rejects this WRITE; an error completion is still no remote drain proof.
    ASSERT_EQ(destination_engine->unregisterLocalMemory(backing.data(), true),
              0);
    EXPECT_EQ(prepared.target->WriteAndDrain(plan.plan.chunks[0],
                                             {slot.data(), slot.size()}),
              Error::kTransferError);
    EXPECT_FALSE(prepared.target->DestinationsDrained());
    EXPECT_TRUE(std::all_of(backing.begin(), backing.end(),
                            [](auto b) { return b == std::byte{0xA5}; }));
    EXPECT_EQ(source_engine->unregisterLocalMemory(slot.data(), false), 0);
}

TEST_F(SsdDestinationTcp,
       WriteTargetRejectsLoopbackSourceAliasBeforeSubmission) {
    auto engine = Engine();
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
    auto plan = BuildReadPlan({slot.size(), 0, {{0, {{1, 1, 0, slot.size()}}}}},
                              {4096, 1, 4});
    ASSERT_EQ(plan.error, Error::kOk);
    EXPECT_EQ(prepared.target->WriteAndDrain(plan.plan.chunks[0],
                                             {slot.data(), slot.size()}),
              Error::kInvalidArgument);
    EXPECT_TRUE(prepared.target->DestinationsDrained());
    EXPECT_EQ(engine->unregisterLocalMemory(slot.data(), true), 0);
}

#ifdef USE_TENT
TEST_F(SsdDestinationTcp, RejectsTentReferenceCountedRegistration) {
    setenv("MC_USE_TENT", "1", 1);
    auto engine = std::make_shared<TransferEngine>(false);
    unsetenv("MC_USE_TENT");
    ASSERT_TRUE(engine);
    ASSERT_TRUE(engine->isUsingTent());
    ASSERT_EQ(engine->init("P2PHANDSHAKE", "127.0.0.1:0", "127.0.0.1", 0), 0);
    auto backing = std::make_shared<std::array<std::byte, 4096>>();
    const auto base = reinterpret_cast<uint64_t>(backing->data());
    TransferEngineDestinationRegistration transport(engine);
    const auto result = transport.RegisterWritable(
        {base, backing->size(), base, backing->size()}, "cpu:0");
    EXPECT_EQ(result.error, DestinationError::kNotSupported);
    EXPECT_FALSE(result.cleanup_required);
    EXPECT_EQ(transport.Unregister(base), DestinationError::kNotSupported);
}
#endif
TEST_F(SsdDestinationTcp, AdjacentMRSNeverBypassSingleTcpBufferPreflight) {
    auto source = Engine(), peer = Engine();
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
    made.target.reset();
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data() + 32, true), 0);
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data(), true), 0);
    EXPECT_EQ(source->unregisterLocalMemory(staging.data(), true), 0);
}
}  // namespace
}  // namespace mooncake::ssd_offload
