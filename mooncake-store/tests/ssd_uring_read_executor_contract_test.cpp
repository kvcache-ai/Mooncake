// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Real-kernel contract tests for UringReadExecutor and UringPipelineDriver.
#include "ssd_uring_pipeline.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <liburing.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iterator>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace {
struct Observation {
    std::atomic<size_t> init_calls{0}, init_ok{0}, exits{0}, eventfds{0};
    size_t fail_init_at{0}, fail_eventfd_at{0};
};
std::atomic<Observation*> observed{nullptr};
struct Observe {
    explicit Observe(Observation& value) { observed.store(&value); }
    ~Observe() { observed.store(nullptr); }
};
}  // namespace

extern "C" int __real_io_uring_queue_init(unsigned, io_uring*, unsigned);
extern "C" void __real_io_uring_queue_exit(io_uring*);
extern "C" int __real_eventfd(unsigned, int);

extern "C" int __wrap_io_uring_queue_init(unsigned depth, io_uring* ring,
                                          unsigned flags) {
    auto* observation = observed.load();
    if (observation && ++observation->init_calls == observation->fail_init_at)
        return -ENOMEM;
    const int result = __real_io_uring_queue_init(depth, ring, flags);
    if (observation && result == 0) ++observation->init_ok;
    return result;
}
extern "C" void __wrap_io_uring_queue_exit(io_uring* ring) {
    __real_io_uring_queue_exit(ring);
    if (auto* observation = observed.load()) ++observation->exits;
}
extern "C" int __wrap_eventfd(unsigned initial, int flags) {
    auto* observation = observed.load();
    if (observation &&
        ++observation->eventfds == observation->fail_eventfd_at) {
        errno = EMFILE;
        return -1;
    }
    return __real_eventfd(initial, flags);
}

namespace mooncake::ssd_offload {
namespace {
using namespace std::chrono_literals;

constexpr size_t kSlot = 4096;
constexpr size_t kPayload = 30000;
constexpr size_t kPrefix = 37;
constexpr size_t kFirst = 7777;

struct File {
    char path[64] = "/tmp/mooncake_uring_executor_XXXXXX";
    std::vector<std::byte> bytes;
    explicit File(size_t salt) : bytes(kPayload + kPrefix) {
        for (size_t i = 0; i < bytes.size(); ++i)
            bytes[i] = std::byte((i * 37 + i / 251 + salt) % 256);
        const int fd = mkstemp(path);
        if (fd < 0) throw std::runtime_error("mkstemp");
        size_t done = 0;
        while (done < bytes.size()) {
            const auto result =
                ::write(fd, bytes.data() + done, bytes.size() - done);
            if (result < 0 && errno == EINTR) continue;
            if (result <= 0) {
                close(fd);
                unlink(path);
                throw std::runtime_error("fixture write");
            }
            done += size_t(result);
        }
        close(fd);
    }
    ~File() { unlink(path); }
};

struct Arena {
    std::byte* data{nullptr};
    size_t count;
    explicit Arena(size_t count = 2) : count(count) {
        void* value = nullptr;
        if (posix_memalign(&value, kSlot, count * kSlot) != 0)
            throw std::bad_alloc();
        data = static_cast<std::byte*>(value);
        std::memset(data, 0xA5, count * kSlot);
    }
    ~Arena() { free(data); }
    std::vector<SlotBuffer> buffers() const {
        std::vector<SlotBuffer> result;
        for (size_t i = 0; i < count; ++i)
            result.push_back({data + i * kSlot, kSlot});
        return result;
    }
};

struct GateState {
    std::mutex mutex;
    std::condition_variable changed;
    size_t entered{0};
    bool open{false};
    void Block() {
        std::unique_lock lock(mutex);
        ++entered;
        changed.notify_all();
        changed.wait(lock, [&] { return open; });
    }
    bool Wait(size_t count = 1) {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, 10s, [&] { return entered >= count; });
    }
    void Open() {
        std::lock_guard lock(mutex);
        open = true;
        changed.notify_all();
    }
};

struct ReleaseGate {
    std::shared_ptr<GateState> state;
    ~ReleaseGate() { state->Open(); }
};

struct Target final : WriteTarget {
    std::vector<std::byte> bytes =
        std::vector<std::byte>(64 + kPayload + 97 + 64, std::byte{0xA5});
    std::shared_ptr<GateState> gate;
    Error Write(const ReadChunk& chunk, SlotBuffer slot) noexcept override {
        if (gate) gate->Block();
        for (const auto& fragment : chunk.fragments) {
            std::memcpy(bytes.data() + fragment.destination.offset,
                        slot.data + fragment.buffer_offset,
                        size_t(fragment.destination.length));
        }
        return Error::kOk;
    }
};

ReadPlan MakePlan() {
    return BuildReadPlan(
               {kPayload,
                kPrefix,
                0,
                {{0, 64, kFirst}, {0, 64 + kFirst + 97, kPayload - kFirst}}},
               {kSlot, 1, 32})
        .plan;
}

std::shared_ptr<ReadSource> OpenSource(const File& file) {
    return OpenPosixReadSource(file.path, file.bytes.size()).source;
}

std::vector<std::byte> Expected(const File& file) {
    std::vector<std::byte> result(64 + kPayload + 97 + 64, std::byte{0xA5});
    std::copy_n(file.bytes.begin() + kPrefix, kFirst, result.begin() + 64);
    std::copy(file.bytes.begin() + kPrefix + kFirst, file.bytes.end(),
              result.begin() + 64 + kFirst + 97);
    return result;
}

bool Drain(ReadPipeline& pipeline, PipelineDriver& driver) {
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (pipeline.inflight() && std::chrono::steady_clock::now() < until) {
        IoCompletion completion;
        if (!driver.WaitForEvent(completion, 50ms)) continue;
        if (completion.stage == SlotStage::kReading) {
            EXPECT_EQ(pipeline.ReadDrained(completion.token, completion.bytes,
                                           completion.result),
                      Error::kOk);
        } else {
            EXPECT_EQ(completion.stage, SlotStage::kWriting);
            EXPECT_EQ(
                pipeline.WriteDrained(completion.token, completion.result),
                Error::kOk);
        }
    }
    return pipeline.inflight() == 0;
}

struct DriverBeforePlanRelease {
    std::unique_ptr<UringPipelineDriver>& driver;
    ~DriverBeforePlanRelease() { driver.reset(); }
};

size_t OpenFdCount() {
    return size_t(
        std::distance(std::filesystem::directory_iterator("/proc/self/fd"),
                      std::filesystem::directory_iterator{}));
}

bool KernelUringAvailable() {
    io_uring ring{};
    if (__real_io_uring_queue_init(2, &ring, 0) < 0) return false;
    io_uring_probe* probe = io_uring_get_probe_ring(&ring);
    const bool supported =
        probe && io_uring_opcode_supported(probe, IORING_OP_READ) != 0;
    if (probe) io_uring_free_probe(probe);
    __real_io_uring_queue_exit(&ring);
    return supported;
}

class PersistentKernel : public testing::TestWithParam<size_t> {};

TEST_P(PersistentKernel, ReusesWorkerRingsAcrossRequestsAndVerifiesPayload) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    const size_t workers = GetParam();
    Observation observation;
    Observe scope(observation);
    auto reader = UringReadExecutor::Create(workers, 8);
    ASSERT_TRUE(reader);
    EXPECT_TRUE(reader->healthy());
    BoundedExecutor writes(2, 8);

    for (size_t request = 1; request <= 3; ++request) {
        File file(request * 71);
        auto source = OpenSource(file);
        ASSERT_TRUE(source);
        auto plan = MakePlan();
        std::vector<size_t> chunk_sources(plan.chunks.size(), 0);
        auto target = std::make_shared<Target>();
        Arena arena;
        auto made = UringPipelineDriver::Create(
            {source}, chunk_sources, target, reader, writes, arena.buffers());
        ASSERT_EQ(made.error, Error::kOk);
        ASSERT_TRUE(made.driver);
        ReadPipeline pipeline(std::move(plan), arena.buffers(), *made.driver);
        DriverBeforePlanRelease cleanup{made.driver};
        ASSERT_EQ(pipeline.Start(), Error::kOk);
        ASSERT_TRUE(Drain(pipeline, *made.driver));
        EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
        EXPECT_EQ(target->bytes, Expected(file));
        EXPECT_EQ(observation.init_ok.load(), workers);
        EXPECT_EQ(observation.exits.load(), 0u);
    }
    reader->Shutdown();
    reader.reset();
    EXPECT_EQ(observation.init_calls.load(), workers);
    EXPECT_EQ(observation.exits.load(), workers);
}

INSTANTIATE_TEST_SUITE_P(OneTwoFourWorkers, PersistentKernel,
                         testing::Values(size_t{1}, size_t{2}, size_t{4}));

TEST(SsdUringReadExecutorContract,
     ConcurrentDriversWithIdenticalTokensDoNotCrossDeliver) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File first(11), second(191);
    auto source_a = OpenSource(first), source_b = OpenSource(second);
    ASSERT_TRUE(source_a && source_b);
    Arena arena_a(1), arena_b(1);
    auto target_a = std::make_shared<Target>(),
         target_b = std::make_shared<Target>();
    BoundedExecutor writes(2, 8);
    auto reader = UringReadExecutor::Create(2, 8);
    ASSERT_TRUE(reader);
    auto a = UringPipelineDriver::Create({source_a}, {0}, target_a, reader,
                                         writes, arena_a.buffers());
    auto b = UringPipelineDriver::Create({source_b}, {0}, target_b, reader,
                                         writes, arena_b.buffers());
    ASSERT_TRUE(a.driver && b.driver);
    auto chunk_a = MakePlan().chunks.front();
    auto chunk_b = chunk_a;
    const SlotToken token{0, 1, 0};
    DriverBeforePlanRelease ca{a.driver}, cb{b.driver};
    ASSERT_TRUE(a.driver->SubmitRead(token, chunk_a, arena_a.buffers()[0]));
    ASSERT_TRUE(b.driver->SubmitRead(token, chunk_b, arena_b.buffers()[0]));
    for (auto* pair : {&b, &a}) {
        IoCompletion event;
        ASSERT_TRUE(pair->driver->WaitForEvent(event, 10s));
        EXPECT_EQ(event.stage, SlotStage::kReading);
        EXPECT_EQ(event.result, Error::kOk);
    }
    EXPECT_EQ(std::memcmp(arena_a.data, first.bytes.data() + kPrefix, kSlot),
              0);
    EXPECT_EQ(std::memcmp(arena_b.data, second.bytes.data() + kPrefix, kSlot),
              0);
}

TEST(SsdUringReadExecutorContract,
     BlockedWriteHoldsSlotAndDestructorWhileSiblingCompletes) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File first(27), second(163);
    Arena arena_a(1), arena_b(1);
    BoundedExecutor writes(2, 8);
    auto reader = UringReadExecutor::Create(2, 8);
    ASSERT_TRUE(reader);
    auto gate = std::make_shared<GateState>();
    auto target_a = std::make_shared<Target>(),
         target_b = std::make_shared<Target>();
    target_a->gate = gate;
    auto plan_b = MakePlan();
    std::vector<size_t> chunk_sources_b(plan_b.chunks.size(), 0);
    auto a = UringPipelineDriver::Create({OpenSource(first)}, {0}, target_a,
                                         reader, writes, arena_a.buffers());
    auto b = UringPipelineDriver::Create({OpenSource(second)}, chunk_sources_b,
                                         target_b, reader, writes,
                                         arena_b.buffers());
    ASSERT_TRUE(a.driver && b.driver);
    auto chunk = MakePlan().chunks.front();
    const SlotToken token{0, 1, 0};
    ReadPipeline pipeline_b(std::move(plan_b), arena_b.buffers(), *b.driver);
    DriverBeforePlanRelease ca{a.driver}, cb{b.driver};
    ReleaseGate release{gate};

    ASSERT_TRUE(a.driver->SubmitRead(token, chunk, arena_a.buffers()[0]));
    IoCompletion event;
    ASSERT_TRUE(a.driver->WaitForEvent(event, 10s));
    ASSERT_EQ(event.result, Error::kOk);
    ASSERT_TRUE(a.driver->SubmitWrite(token, chunk, arena_a.buffers()[0]));
    ASSERT_TRUE(gate->Wait());
    EXPECT_FALSE(a.driver->SubmitRead({0, 2, 0}, chunk, arena_a.buffers()[0]));
    ASSERT_EQ(pipeline_b.Start(), Error::kOk);
    ASSERT_TRUE(Drain(pipeline_b, *b.driver));
    EXPECT_EQ(pipeline_b.state(), PipelineState::kSucceeded);
    EXPECT_EQ(target_b->bytes, Expected(second));

    auto closing = std::async(std::launch::async, [&] { a.driver.reset(); });
    EXPECT_EQ(closing.wait_for(20ms), std::future_status::timeout);
    gate->Open();
    ASSERT_EQ(closing.wait_for(10s), std::future_status::ready);
    closing.get();
}

TEST(SsdUringReadExecutorContract,
     BackpressureAndShutdownCoverClosureDestructionAndCallbackExceptions) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File file(51);
    auto source = OpenSource(file);
    ASSERT_TRUE(source);
    Arena arena(1);
    auto reader = UringReadExecutor::Create(1, 1);
    ASSERT_TRUE(reader);

    auto gate = std::make_shared<GateState>();
    ReleaseGate release{gate};
    auto callback_gate = std::make_shared<GateState>();
    ReleaseGate release_callback{callback_gate};
    struct Held {
        std::shared_ptr<GateState> gate;
        bool armed{false};
        ~Held() {
            if (armed) gate->Block();
        }
    };
    auto held = std::make_shared<Held>();
    held->gate = gate;
    ASSERT_TRUE(reader->WaitSubmit(
        source, *source->native_read_fd(), {arena.data, kSlot}, 0,
        [held, callback_gate](ReadResult) {
            callback_gate->Block();
            throw std::runtime_error("caller callback");
        }));
    held->armed = true;
    held.reset();
    callback_gate->Open();
    ASSERT_TRUE(gate->Wait());

    // Credit is still held while the callback closure is destroying.
    auto done = std::make_shared<std::promise<ReadResult>>();
    auto finished = done->get_future();
    auto waiter = std::async(std::launch::async, [&] {
        return reader->WaitSubmit(
            source, *source->native_read_fd(), {arena.data, kSlot}, 0,
            [done](ReadResult value) { done->set_value(value); });
    });
    EXPECT_EQ(waiter.wait_for(20ms), std::future_status::timeout);
    gate->Open();
    ASSERT_EQ(waiter.wait_for(10s), std::future_status::ready);
    EXPECT_TRUE(waiter.get());
    ASSERT_EQ(finished.wait_for(10s), std::future_status::ready);
    EXPECT_EQ(finished.get().error, Error::kOk);

    reader->Shutdown();
    EXPECT_FALSE(reader->healthy());
    EXPECT_FALSE(reader->WaitSubmit(source, *source->native_read_fd(),
                                    {arena.data, kSlot}, 0, [](ReadResult) {}));
}

TEST(SsdUringReadExecutorContract,
     InvalidArgsAndPartialStartupFailureLeakNoFdsOrRings) {
    Observation observation;
    {
        Observe scope(observation);
        EXPECT_FALSE(UringReadExecutor::Create(0, 1));
        EXPECT_FALSE(UringReadExecutor::Create(1, 0));
        EXPECT_FALSE(UringReadExecutor::Create(65, 8));
        EXPECT_FALSE(UringReadExecutor::Create(2, 65537));
        EXPECT_EQ(observation.init_calls.load(), 0u);
        EXPECT_EQ(observation.eventfds.load(), 0u);
    }
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    for (bool fail_wakeup : {false, true}) {
        Observation partial;
        Observe scope(partial);
        if (fail_wakeup)
            partial.fail_eventfd_at = 2;
        else
            partial.fail_init_at = 2;
        const auto before = OpenFdCount();
        EXPECT_FALSE(UringReadExecutor::Create(4, 8));
        EXPECT_EQ(partial.init_ok.load(), partial.exits.load());
        EXPECT_GE(partial.init_ok.load(), 1u);
        EXPECT_EQ(OpenFdCount(), before);
    }
}

}  // namespace
}  // namespace mooncake::ssd_offload
