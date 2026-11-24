// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// This executable observes real liburing/kernel operations; no model reader.
#include "ssd_uring_pipeline.h"

#include <gtest/gtest.h>
#include <liburing.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
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
#include <tuple>

namespace {
struct ObservedSqe {
    unsigned opcode, flags;
    int fd_flags;
};
struct Observation {
    std::atomic<size_t> init_calls{0}, init_ok{0}, exits{0}, registrations{0};
    std::atomic<size_t> eventfds{0};
    size_t fail_init_at{0}, fail_eventfd_at{0};
    std::mutex mutex;
    std::vector<ObservedSqe> reads;
};
std::atomic<Observation*> observed{nullptr};
struct Observe {
    explicit Observe(Observation& value) { observed.store(&value); }
    ~Observe() { observed.store(nullptr); }
};
}  // namespace

extern "C" int __real_io_uring_queue_init(unsigned, io_uring*, unsigned);
extern "C" void __real_io_uring_queue_exit(io_uring*);
extern "C" int __real_io_uring_submit(io_uring*);
extern "C" int __real_io_uring_register_buffers(io_uring*, const iovec*,
                                                unsigned);
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
extern "C" int __wrap_io_uring_register_buffers(io_uring* ring,
                                                const iovec* buffers,
                                                unsigned count) {
    if (auto* observation = observed.load()) ++observation->registrations;
    return __real_io_uring_register_buffers(ring, buffers, count);
}
extern "C" int __wrap_io_uring_submit(io_uring* ring) {
    if (auto* observation = observed.load()) {
        std::lock_guard lock(observation->mutex);
        for (unsigned i = ring->sq.sqe_head; i != ring->sq.sqe_tail; ++i) {
            const auto& sqe = ring->sq.sqes[i & *ring->sq.kring_mask];
            observation->reads.push_back(
                {sqe.opcode, sqe.flags, fcntl(sqe.fd, F_GETFL)});
        }
    }
    return __real_io_uring_submit(ring);
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
    size_t count;
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
        return changed.wait_for(lock, 2s, [&] { return entered >= count; });
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

struct Target final : DrainedWriteTarget {
    std::vector<std::byte> bytes =
        std::vector<std::byte>(64 + kPayload + 97 + 64, std::byte{0xA5});
    std::shared_ptr<GateState> gate;
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer slot) noexcept override {
        if (gate) gate->Block();
        for (const auto& fragment : chunk.fragments)
            std::memcpy(bytes.data() + fragment.destination.offset,
                        slot.data + fragment.buffer_offset,
                        size_t(fragment.destination.length));
        return Error::kOk;
    }
};

ReadPlan Plan(bool direct) {
    return BuildReadPlan({kPayload,
                          kPrefix,
                          {{0,
                            {{1, 1, 64, kFirst},
                             {1, 1, 64 + kFirst + 97, kPayload - kFirst}}}}},
                         {kSlot, direct ? kSlot : 1, 32})
        .plan;
}
std::shared_ptr<ReadSource> Source(const File& file, bool direct = false) {
    return OpenPosixReadSource(
               file.path,
               {kPayload, kPrefix, direct ? kSlot : 1,
                direct ? ReadIoMode::kDirect : ReadIoMode::kBuffered})
        .source;
}
std::vector<std::byte> Expected(const File& file) {
    std::vector<std::byte> result(64 + kPayload + 97 + 64, std::byte{0xA5});
    std::copy_n(file.bytes.begin() + kPrefix, kFirst, result.begin() + 64);
    std::copy(file.bytes.begin() + kPrefix + kFirst, file.bytes.end(),
              result.begin() + 64 + kFirst + 97);
    return result;
}
void Deliver(ReadPipeline& pipeline, const IoCompletion& event) {
    if (event.stage == SlotStage::kReading)
        EXPECT_EQ(pipeline.ReadDrained(event.token, event.bytes, event.result),
                  Error::kOk);
    else {
        EXPECT_EQ(event.stage, SlotStage::kWriting);
        EXPECT_EQ(pipeline.WriteDrained(event.token, event.result), Error::kOk);
    }
}
bool Drain(ReadPipeline& pipeline, EventPipelineDriver& driver) {
    const auto until = std::chrono::steady_clock::now() + 5s;
    while (pipeline.inflight() && std::chrono::steady_clock::now() < until) {
        IoCompletion completion;
        if (driver.WaitForEvent(completion, 50ms))
            Deliver(pipeline, completion);
    }
    return pipeline.inflight() == 0;
}
struct DriverBeforePlanRelease {
    std::unique_ptr<UringPipelineDriver>& driver;
    ~DriverBeforePlanRelease() { driver.reset(); }
};
bool Quiescent(const std::shared_ptr<UringReadExecutor>& reader) {
    const auto until = std::chrono::steady_clock::now() + 2s;
    while (reader->outstanding() && std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(1ms);
    return reader->outstanding() == 0;
}
size_t Fds() {
    return size_t(
        std::distance(std::filesystem::directory_iterator("/proc/self/fd"),
                      std::filesystem::directory_iterator{}));
}
bool KernelUringAvailable() {
    io_uring ring{};
    const int rc = __real_io_uring_queue_init(2, &ring, 0);
    if (rc == 0) {
        io_uring_probe* probe = io_uring_get_probe_ring(&ring);
        const bool supported =
            probe && io_uring_opcode_supported(probe, IORING_OP_READ) != 0;
        if (probe) io_uring_free_probe(probe);
        __real_io_uring_queue_exit(&ring);
        return supported;
    }
    return rc != -ENOSYS && rc != -EPERM && rc != -EACCES && rc != -ENOMEM;
}

class PersistentKernel
    : public testing::TestWithParam<std::tuple<size_t, bool>> {};
TEST_P(PersistentKernel,
       ReusesWorkerRingsAcrossRequestsAndChecksEveryDestinationByte) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    const auto [workers, direct] = GetParam();
    if (direct) {
        File probe_file(1);
        if (!Source(probe_file, true)) {
            GTEST_SKIP() << "O_DIRECT unsupported on /tmp filesystem";
        }
    }
    Observation observation;
    Observe scope(observation);
    auto reader = UringReadExecutor::Create(workers, 8);
    ASSERT_TRUE(reader);
    ASSERT_EQ(reader->worker_count(), workers);
    BoundedExecutor writes(2, 8);
    for (uint64_t request = 1; request <= 3; ++request) {
        File file(size_t(request * 71));
        auto source = Source(file, direct);
        ASSERT_TRUE(source);
        auto target = std::make_shared<Target>();
        Arena arena;
        auto made = UringPipelineDriver::Create(
            source, target, writes, arena.buffers(), {false, reader});
        ASSERT_EQ(made.error, Error::kOk);
        ASSERT_TRUE(made.driver);
        ReadPipeline pipeline(request, 1, Plan(direct), arena.buffers(),
                              *made.driver);
        DriverBeforePlanRelease cleanup{made.driver};
        ASSERT_EQ(pipeline.Start(), Error::kOk);
        ASSERT_TRUE(Drain(pipeline, *made.driver));
        EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
        EXPECT_EQ(pipeline.completed_bytes(), kPayload);
        EXPECT_EQ(target->bytes, Expected(file));
        EXPECT_EQ(observation.init_ok.load(), workers);
        EXPECT_EQ(observation.exits.load(), 0u);
        EXPECT_EQ(observation.registrations.load(), 0u);
    }
    ASSERT_TRUE(Quiescent(reader));
    reader->Shutdown();
    reader.reset();
    EXPECT_EQ(observation.init_calls.load(), workers);
    EXPECT_EQ(observation.exits.load(), workers);
    std::lock_guard lock(observation.mutex);
    ASSERT_FALSE(observation.reads.empty());
    for (const auto& read : observation.reads) {
        EXPECT_EQ(read.opcode, unsigned(IORING_OP_READ));
        EXPECT_EQ(read.flags, 0u);
        EXPECT_EQ(bool(read.fd_flags & O_DIRECT), direct);
    }
}
INSTANTIATE_TEST_SUITE_P(OneTwoFourWorkersBufferedAndDirect, PersistentKernel,
                         testing::Combine(testing::Values(size_t{1}, size_t{2},
                                                          size_t{4}),
                                          testing::Bool()));

TEST(SsdUringReadExecutorContract,
     DistinctRequestsWithSameSlotGenerationDoNotCrossDeliver) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File first(11), second(191);
    auto source_a = Source(first), source_b = Source(second);
    ASSERT_TRUE(source_a && source_b);
    Arena arena_a(1), arena_b(1);
    auto target_a = std::make_shared<Target>(),
         target_b = std::make_shared<Target>();
    BoundedExecutor writes(2, 8);
    auto reader = UringReadExecutor::Create(2, 8);
    ASSERT_TRUE(reader);
    auto a = UringPipelineDriver::Create(source_a, target_a, writes,
                                         arena_a.buffers(), {false, reader});
    auto b = UringPipelineDriver::Create(source_b, target_b, writes,
                                         arena_b.buffers(), {false, reader});
    ASSERT_TRUE(a.driver && b.driver);
    auto chunk_a = Plan(false).chunks.front();
    auto chunk_b = chunk_a;
    const SlotToken token_a{91, 7, 0, 1, 0}, token_b{92, 8, 0, 1, 0};
    DriverBeforePlanRelease ca{a.driver}, cb{b.driver};
    ASSERT_TRUE(a.driver->SubmitRead(token_a, chunk_a, arena_a.buffers()[0]));
    ASSERT_TRUE(b.driver->SubmitRead(token_b, chunk_b, arena_b.buffers()[0]));
    for (auto* pair : {&b, &a}) {
        const auto& token = pair == &a ? token_a : token_b;
        IoCompletion event;
        ASSERT_TRUE(pair->driver->WaitForEvent(event, 2s));
        EXPECT_EQ(event.token.request_id, token.request_id);
        EXPECT_EQ(event.token.attempt_id, token.attempt_id);
        EXPECT_EQ(event.token.slot_index, token.slot_index);
        EXPECT_EQ(event.token.generation, token.generation);
        EXPECT_EQ(event.token.chunk_index, token.chunk_index);
        EXPECT_EQ(event.stage, SlotStage::kReading);
        EXPECT_EQ(event.result, Error::kOk);
    }
    EXPECT_EQ(std::memcmp(arena_a.data, first.bytes.data() + kPrefix, kSlot),
              0);
    EXPECT_EQ(std::memcmp(arena_b.data, second.bytes.data() + kPrefix, kSlot),
              0);
    a.driver.reset();
    b.driver.reset();
}

TEST(SsdUringReadExecutorContract,
     CancelledRequestDoesNotCancelAnotherRequest) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File first(9), second(109);
    Arena arena_a, arena_b;
    auto target_a = std::make_shared<Target>(),
         target_b = std::make_shared<Target>();
    BoundedExecutor writes(2, 8);
    auto reader = UringReadExecutor::Create(4, 8);
    ASSERT_TRUE(reader);
    auto a = UringPipelineDriver::Create(Source(first), target_a, writes,
                                         arena_a.buffers(), {false, reader});
    auto b = UringPipelineDriver::Create(Source(second), target_b, writes,
                                         arena_b.buffers(), {false, reader});
    ASSERT_TRUE(a.driver && b.driver);
    ReadPipeline pa(71, 1, Plan(false), arena_a.buffers(), *a.driver);
    ReadPipeline pb(72, 1, Plan(false), arena_b.buffers(), *b.driver);
    DriverBeforePlanRelease ca{a.driver}, cb{b.driver};
    ASSERT_EQ(pa.Start(), Error::kOk);
    pa.Cancel();
    ASSERT_EQ(pb.Start(), Error::kOk);
    ASSERT_TRUE(Drain(pa, *a.driver));
    ASSERT_TRUE(Drain(pb, *b.driver));
    EXPECT_EQ(pa.state(), PipelineState::kCancelled);
    EXPECT_EQ(pa.completed_bytes(), 0u);
    EXPECT_TRUE(std::all_of(target_a->bytes.begin(), target_a->bytes.end(),
                            [](std::byte b) { return b == std::byte{0xA5}; }));
    EXPECT_EQ(pb.state(), PipelineState::kSucceeded);
    EXPECT_EQ(target_b->bytes, Expected(second));
}

TEST(SsdUringReadExecutorContract,
     BlockedWriteRetainsItsSlotWhileAnotherRequestCompletes) {
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
    auto a = UringPipelineDriver::Create(Source(first), target_a, writes,
                                         arena_a.buffers(), {false, reader});
    auto b = UringPipelineDriver::Create(Source(second), target_b, writes,
                                         arena_b.buffers(), {false, reader});
    ASSERT_TRUE(a.driver && b.driver);
    auto chunk = Plan(false).chunks.front();
    const SlotToken token{31, 2, 0, 1, 0};
    ReadPipeline pipeline_b(32, 2, Plan(false), arena_b.buffers(), *b.driver);
    DriverBeforePlanRelease ca{a.driver}, cb{b.driver};
    ReleaseGate release{gate};
    ASSERT_TRUE(a.driver->SubmitRead(token, chunk, arena_a.buffers()[0]));
    IoCompletion event;
    ASSERT_TRUE(a.driver->WaitForEvent(event, 2s));
    ASSERT_EQ(event.stage, SlotStage::kReading);
    ASSERT_EQ(event.result, Error::kOk);
    ASSERT_TRUE(a.driver->SubmitWrite(token, chunk, arena_a.buffers()[0]));
    ASSERT_TRUE(gate->Wait());
    EXPECT_FALSE(
        a.driver->SubmitRead({31, 2, 0, 2, 1}, chunk, arena_a.buffers()[0]));
    EXPECT_EQ(std::memcmp(arena_a.data, first.bytes.data() + kPrefix, kSlot),
              0);
    ASSERT_EQ(pipeline_b.Start(), Error::kOk);
    ASSERT_TRUE(Drain(pipeline_b, *b.driver));
    EXPECT_EQ(pipeline_b.state(), PipelineState::kSucceeded);
    EXPECT_EQ(target_b->bytes, Expected(second));
    auto closing = std::async(std::launch::async, [&] { a.driver.reset(); });
    EXPECT_EQ(closing.wait_for(20ms), std::future_status::timeout);
    gate->Open();
    EXPECT_EQ(closing.wait_for(2s), std::future_status::ready);
    closing.get();
    std::vector<std::byte> expected(target_a->bytes.size(), std::byte{0xA5});
    std::copy_n(first.bytes.begin() + kPrefix, kSlot, expected.begin() + 64);
    EXPECT_EQ(target_a->bytes, expected);
}

TEST(SsdUringReadExecutorContract, FixedOptionRetainsTheRequestLocalPath) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    Observation observation;
    Observe scope(observation);
    File file(59);
    Arena arena;
    BoundedExecutor writes(2, 8);
    auto reader = UringReadExecutor::Create(1, 8);
    ASSERT_TRUE(reader);
    auto target = std::make_shared<Target>();
    auto made = UringPipelineDriver::Create(Source(file), target, writes,
                                            arena.buffers(), {true, reader});
    ASSERT_TRUE(made.driver);
    EXPECT_EQ(observation.init_ok.load(), 2u);
    EXPECT_EQ(observation.registrations.load(), 1u);
    ReadPipeline pipeline(64, 1, Plan(false), arena.buffers(), *made.driver);
    DriverBeforePlanRelease cleanup{made.driver};
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    ASSERT_TRUE(Drain(pipeline, *made.driver));
    EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    EXPECT_EQ(target->bytes, Expected(file));
    EXPECT_EQ(reader->outstanding(), 0u);
    made.driver.reset();
    EXPECT_EQ(observation.exits.load(), 1u);
    reader.reset();
    EXPECT_EQ(observation.exits.load(), 2u);
}

class SharedCredit : public testing::TestWithParam<size_t> {};
TEST_P(SharedCredit, GlobalLimitIncludesQueuedRunningAndBlockedCallbacks) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File file(33);
    auto source = Source(file);
    ASSERT_TRUE(source);
    Arena arena(3);
    auto reader = UringReadExecutor::Create(GetParam(), 2);
    ASSERT_TRUE(reader);
    auto gate = std::make_shared<GateState>();
    ReleaseGate release{gate};
    std::array<std::shared_ptr<std::promise<ReadResult>>, 2> done{
        std::make_shared<std::promise<ReadResult>>(),
        std::make_shared<std::promise<ReadResult>>()};
    std::array<std::future<ReadResult>, 2> futures{done[0]->get_future(),
                                                   done[1]->get_future()};
    for (size_t i = 0; i < 2; ++i)
        ASSERT_TRUE(reader->TrySubmit(
            source, *source->native_read_fd(), {arena.data + i * kSlot, kSlot},
            i * kSlot, [gate, promise = done[i]](ReadResult result) {
                gate->Block();
                promise->set_value(result);
            }));
    ASSERT_TRUE(gate->Wait(GetParam() == 1 ? 1 : 2));
    EXPECT_EQ(reader->outstanding(), 2u);
    auto rejected_callbacks = std::make_shared<std::atomic<size_t>>(0);
    EXPECT_FALSE(reader->TrySubmit(
        source, *source->native_read_fd(), {arena.data + 2 * kSlot, kSlot}, 0,
        [rejected_callbacks](ReadResult) { ++*rejected_callbacks; }));
    EXPECT_TRUE(std::all_of(arena.data + 2 * kSlot, arena.data + 3 * kSlot,
                            [](std::byte b) { return b == std::byte{0xA5}; }));
    gate->Open();
    for (auto& future : futures) {
        ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
        EXPECT_EQ(future.get().error, Error::kOk);
    }
    ASSERT_TRUE(Quiescent(reader));
    auto again = std::make_shared<std::promise<ReadResult>>();
    auto finished = again->get_future();
    ASSERT_TRUE(reader->TrySubmit(
        source, *source->native_read_fd(), {arena.data + 2 * kSlot, kSlot}, 0,
        [again](ReadResult result) { again->set_value(result); }));
    ASSERT_EQ(finished.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(finished.get().bytes, kSlot);
    reader->Shutdown();
    reader->Shutdown();
    EXPECT_EQ(reader->outstanding(), 0u);
    EXPECT_EQ(rejected_callbacks->load(), 0u);
    EXPECT_FALSE(reader->TrySubmit(
        source, *source->native_read_fd(), {arena.data, kSlot}, 0,
        [rejected_callbacks](ReadResult) { ++*rejected_callbacks; }));
    EXPECT_EQ(rejected_callbacks->load(), 0u);
}
INSTANTIATE_TEST_SUITE_P(OneTwoFourWorkers, SharedCredit,
                         testing::Values(size_t{1}, size_t{2}, size_t{4}));

TEST(SsdUringReadExecutorContract,
     CreditAndShutdownIncludeCompletionClosureDestruction) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File file(51);
    auto source = Source(file);
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
    ASSERT_TRUE(reader->TrySubmit(
        source, *source->native_read_fd(), {arena.data, kSlot}, 0,
        [held, callback_gate](ReadResult) { callback_gate->Block(); }));
    // The callback cannot finish until the test drops its last external copy.
    // Otherwise the blocking destructor could run on the test thread itself.
    held->armed = true;
    held.reset();
    callback_gate->Open();
    ASSERT_TRUE(gate->Wait());
    EXPECT_EQ(reader->outstanding(), 1u);
    EXPECT_FALSE(reader->TrySubmit(source, *source->native_read_fd(),
                                   {arena.data, kSlot}, 0, [](ReadResult) {}));
    auto closing = std::async(std::launch::async, [&] { reader->Shutdown(); });
    EXPECT_EQ(closing.wait_for(20ms), std::future_status::timeout);
    gate->Open();
    EXPECT_EQ(closing.wait_for(2s), std::future_status::ready);
    closing.get();
    EXPECT_EQ(reader->outstanding(), 0u);
}

TEST(SsdUringReadExecutorContract,
     ThrowingCallbackDoesNotKillWorkerOrLeakCredit) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File file(89);
    auto source = Source(file);
    ASSERT_TRUE(source);
    Arena arena(1);
    auto reader = UringReadExecutor::Create(1, 1);
    ASSERT_TRUE(reader);
    auto called = std::make_shared<std::atomic<size_t>>(0);
    ASSERT_TRUE(reader->TrySubmit(source, *source->native_read_fd(),
                                  {arena.data, kSlot}, 0, [called](ReadResult) {
                                      ++*called;
                                      throw std::runtime_error(
                                          "caller callback");
                                  }));
    ASSERT_TRUE(Quiescent(reader));
    EXPECT_EQ(called->load(), 1u);
    auto result = std::make_shared<std::promise<ReadResult>>();
    auto finished = result->get_future();
    ASSERT_TRUE(reader->TrySubmit(
        source, *source->native_read_fd(), {arena.data, kSlot}, 0,
        [result](ReadResult value) { result->set_value(value); }));
    ASSERT_EQ(finished.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(finished.get().error, Error::kOk);
    reader->Shutdown();
    EXPECT_EQ(std::memcmp(arena.data, file.bytes.data(), kSlot), 0);
}

TEST(SsdUringReadExecutorContract, InvalidAdmissionAcceptsNoAccessOrCallback) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    File file(17);
    auto source = Source(file);
    ASSERT_TRUE(source);
    Arena arena(1);
    auto reader = UringReadExecutor::Create(1, 4);
    ASSERT_TRUE(reader);
    auto callbacks = std::make_shared<std::atomic<size_t>>(0);
    auto callback = [callbacks](ReadResult) { ++*callbacks; };
    const int fd = *source->native_read_fd();
    EXPECT_FALSE(
        reader->TrySubmit(nullptr, fd, {arena.data, kSlot}, 0, callback));
    EXPECT_FALSE(
        reader->TrySubmit(source, -1, {arena.data, kSlot}, 0, callback));
    EXPECT_FALSE(
        reader->TrySubmit(source, fd + 1, {arena.data, kSlot}, 0, callback));
    EXPECT_FALSE(reader->TrySubmit(source, fd, {}, 0, callback));
    EXPECT_FALSE(reader->TrySubmit(source, fd, {arena.data, kSlot},
                                   std::numeric_limits<uint64_t>::max(),
                                   callback));
    EXPECT_FALSE(reader->TrySubmit(source, fd, {arena.data, kSlot}, 0, {}));
    EXPECT_EQ(reader->outstanding(), 0u);
    reader->Shutdown();
    EXPECT_EQ(callbacks->load(), 0u);
    EXPECT_TRUE(std::all_of(arena.data, arena.data + kSlot,
                            [](std::byte b) { return b == std::byte{0xA5}; }));
}

TEST(SsdUringReadExecutorContract, InvalidConfigurationCreatesNoRing) {
    Observation observation;
    Observe scope(observation);
    EXPECT_FALSE(UringReadExecutor::Create(0, 1));
    EXPECT_FALSE(UringReadExecutor::Create(1, 0));
    EXPECT_FALSE(UringReadExecutor::Create(65, 8));
    EXPECT_FALSE(UringReadExecutor::Create(2, 65537));
    EXPECT_EQ(observation.init_calls.load(), 0u);
    EXPECT_EQ(observation.eventfds.load(), 0u);
}

TEST(SsdUringReadExecutorContract,
     PartialStartupFailureClosesEverySuccessfulRing) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    for (bool fail_wakeup : {false, true}) {
        Observation observation;
        Observe scope(observation);
        if (fail_wakeup)
            observation.fail_eventfd_at = 2;
        else
            observation.fail_init_at = 2;
        const auto before = Fds();
        EXPECT_FALSE(UringReadExecutor::Create(4, 8));
        EXPECT_EQ(observation.init_ok.load(), observation.exits.load());
        EXPECT_GE(observation.init_ok.load(), 1u);
        EXPECT_EQ(Fds(), before);
    }
}
}  // namespace
}  // namespace mooncake::ssd_offload
