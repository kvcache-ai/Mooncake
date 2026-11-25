// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Real-kernel contract tests for UringReadExecutor and UringPipelineDriver.
#include "ssd_uring_pipeline.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <liburing.h>
#include <poll.h>
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
    std::atomic<size_t> submit_calls{0}, submit_ok{0}, poll_calls{0};
    size_t fail_init_at{0}, fail_eventfd_at{0};
    std::atomic<size_t> fail_submit_at{0};
    std::atomic<int> fail_submit_rc{0};
    std::atomic<size_t> fail_poll_at{0};
    std::atomic<int> fail_poll_errno{0};
    std::atomic<short> inject_ring_revents{0};
    std::atomic<short> inject_signal_revents{0};
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
extern "C" int __real_poll(pollfd*, nfds_t, int);
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
extern "C" int __wrap_io_uring_submit(io_uring* ring) {
    if (auto* observation = observed.load()) {
        const size_t call = ++observation->submit_calls;
        const size_t target = observation->fail_submit_at.load();
        if (target != 0 && call >= target) {
            return observation->fail_submit_rc.load();
        }
        const int rc = __real_io_uring_submit(ring);
        if (rc > 0) observation->submit_ok.fetch_add(size_t(rc));
        return rc;
    }
    return __real_io_uring_submit(ring);
}
extern "C" int __wrap_poll(pollfd* fds, nfds_t nfds, int timeout) {
    if (auto* observation = observed.load()) {
        if (nfds == 2) {
            const size_t call = ++observation->poll_calls;
            const size_t target = observation->fail_poll_at.load();
            if (target != 0 && call >= target) {
                const int err = observation->fail_poll_errno.load();
                if (err != 0) {
                    errno = err;
                    return -1;
                }
                const short ring_ev = observation->inject_ring_revents.load();
                const short sig_ev = observation->inject_signal_revents.load();
                if (ring_ev != 0 && fds[0].fd >= 0) {
                    fds[0].revents = ring_ev;
                    fds[1].revents = 0;
                    return 1;
                }
                if (sig_ev != 0 && fds[1].fd >= 0) {
                    fds[0].revents = 0;
                    fds[1].revents = sig_ev;
                    return 1;
                }
            }
        }
    }
    return __real_poll(fds, nfds, timeout);
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
    bool supported =
        probe && io_uring_opcode_supported(probe, IORING_OP_READ) != 0;
    if (probe) io_uring_free_probe(probe);
    if (supported) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            supported = false;
        } else {
            io_uring_prep_nop(sqe);
            int enter_rc;
            do {
                enter_rc = io_uring_submit_and_wait(&ring, 1);
            } while (enter_rc == -EINTR);
            io_uring_cqe* cqe = nullptr;
            if (enter_rc < 0 || io_uring_peek_cqe(&ring, &cqe) != 0) {
                supported = false;
            } else {
                supported = cqe->res >= 0;
                io_uring_cqe_seen(&ring, cqe);
            }
        }
    }
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

struct PipeReadSource final : ReadSource {
    int rfd{-1};
    int wfd{-1};
    PipeReadSource() {
        int fds[2] = {-1, -1};
        if (pipe2(fds, O_CLOEXEC) != 0) throw std::runtime_error("pipe2");
        rfd = fds[0];
        wfd = fds[1];
    }
    ~PipeReadSource() override {
        if (rfd >= 0) close(rfd);
        if (wfd >= 0) close(wfd);
    }
    uint64_t file_bytes() const noexcept override { return 4096; }
    ReadResult ReadAt(std::span<std::byte>, uint64_t) noexcept override {
        return {0, Error::kNotSupported, 0};
    }
    std::optional<int> native_read_fd() const noexcept override { return rfd; }
    void WriteBytes(std::byte fill, size_t n) const {
        std::vector<std::byte> buf(n, fill);
        size_t off = 0;
        while (off < n) {
            const ssize_t written = ::write(wfd, buf.data() + off, n - off);
            if (written < 0 && errno == EINTR) continue;
            ASSERT_GT(written, 0);
            off += size_t(written);
        }
    }
};

template <typename Predicate>
bool WaitUntil(Predicate&& pred, std::chrono::milliseconds timeout = 5000ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

TEST(SsdUringReadExecutorContract,
     PermanentSubmitFailureFailsUnsubmittedAndDrainsSubmittedSafely) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";

    // Case 1: permanent io_uring_submit error with no prior submitted ops.
    {
        File file(91);
        auto source = OpenSource(file);
        ASSERT_TRUE(source);
        Arena arena(1);
        Observation observation;
        Observe scope(observation);
        auto reader = UringReadExecutor::Create(1, 4);
        ASSERT_TRUE(reader);
        observation.fail_submit_rc.store(-EACCES);
        observation.fail_submit_at.store(1);

        auto done = std::make_shared<std::promise<ReadResult>>();
        auto future = done->get_future();
        ASSERT_TRUE(reader->WaitSubmit(
            source, *source->native_read_fd(), {arena.data, kSlot}, 0,
            [done](ReadResult r) { done->set_value(r); }));
        ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
        const auto res = future.get();
        EXPECT_EQ(res.error, Error::kIoError);
        EXPECT_EQ(res.sys_errno, EACCES);
        EXPECT_FALSE(reader->healthy());
        reader->Shutdown();
        reader.reset();
        EXPECT_EQ(observation.exits.load(), 1u);
    }

    // Case 2: one op is actively in flight in the kernel when a second op hits
    // a permanent io_uring_submit failure. The unsubmitted op must fail
    // immediately while the already-submitted op stays pinned (and the ring
    // stays alive) until its real CQE arrives, preventing kernel DMA UAF.
    {
        auto pipe_source = std::make_shared<PipeReadSource>();
        File file(97);
        auto file_source = OpenSource(file);
        ASSERT_TRUE(file_source);
        Arena arena(2);
        Observation observation;
        Observe scope(observation);
        auto reader = UringReadExecutor::Create(1, 4);
        ASSERT_TRUE(reader);

        auto op1_done = std::make_shared<std::promise<ReadResult>>();
        auto op1_future = op1_done->get_future();
        ASSERT_TRUE(reader->WaitSubmit(
            pipe_source, *pipe_source->native_read_fd(), {arena.data, 16}, 0,
            [op1_done](ReadResult r) { op1_done->set_value(r); }));
        ASSERT_TRUE(
            WaitUntil([&] { return observation.submit_ok.load() >= 1u; }));
        // If kernel immediately rejected pipe read at offset 0, skip only this
        // sub-case cleanly rather than asserting on a completed op.
        if (op1_future.wait_for(20ms) == std::future_status::timeout) {
            observation.fail_submit_rc.store(-EACCES);
            observation.fail_submit_at.store(observation.submit_calls.load() +
                                             1);

            auto op2_done = std::make_shared<std::promise<ReadResult>>();
            auto op2_future = op2_done->get_future();
            ASSERT_TRUE(reader->WaitSubmit(
                file_source, *file_source->native_read_fd(),
                {arena.data + kSlot, kSlot}, 0,
                [op2_done](ReadResult r) { op2_done->set_value(r); }));
            ASSERT_EQ(op2_future.wait_for(5s), std::future_status::ready);
            const auto res2 = op2_future.get();
            EXPECT_EQ(res2.error, Error::kIoError);
            EXPECT_EQ(res2.sys_errno, EACCES);
            EXPECT_FALSE(reader->healthy());
            // The in-flight op1 must NOT be prematurely failed or its ring
            // torn down while the kernel still holds op1's buffer.
            EXPECT_EQ(op1_future.wait_for(50ms), std::future_status::timeout);
            EXPECT_EQ(observation.exits.load(), 0u);

            pipe_source->WriteBytes(std::byte{0x42}, 16);
            ASSERT_EQ(op1_future.wait_for(5s), std::future_status::ready);
            const auto res1 = op1_future.get();
            EXPECT_EQ(res1.error, Error::kOk);
            EXPECT_EQ(res1.bytes, 16u);
            for (size_t i = 0; i < 16; ++i) {
                EXPECT_EQ(arena.data[i], std::byte{0x42});
            }
        } else {
            pipe_source->WriteBytes(std::byte{0x42}, 16);
        }
        reader->Shutdown();
        reader.reset();
        EXPECT_EQ(observation.exits.load(), 1u);
    }
}

TEST(SsdUringReadExecutorContract,
     PollErrorsDrainInFlightSubmittedOpsWithoutRingTeardownUaf) {
    if (!KernelUringAvailable()) GTEST_SKIP() << "io_uring unavailable";
    enum class PollFault { kErrnoEio, kEventfdErr, kRingFdErr };
    for (PollFault fault : {PollFault::kErrnoEio, PollFault::kEventfdErr,
                            PollFault::kRingFdErr}) {
        auto pipe_source = std::make_shared<PipeReadSource>();
        Arena arena(1);
        Observation observation;
        Observe scope(observation);
        auto reader = UringReadExecutor::Create(1, 4);
        ASSERT_TRUE(reader);

        auto op_done = std::make_shared<std::promise<ReadResult>>();
        auto op_future = op_done->get_future();
        ASSERT_TRUE(reader->WaitSubmit(
            pipe_source, *pipe_source->native_read_fd(), {arena.data, 16}, 0,
            [op_done](ReadResult r) { op_done->set_value(r); }));
        ASSERT_TRUE(
            WaitUntil([&] { return observation.submit_ok.load() >= 1u; }));
        if (op_future.wait_for(20ms) != std::future_status::timeout) {
            pipe_source->WriteBytes(std::byte{0x5A}, 16);
            reader->Shutdown();
            reader.reset();
            continue;
        }

        // Wait until worker enters poll() for the in-flight op, then inject
        // the poll error on the next poll call and wake via Shutdown/eventfd.
        if (fault == PollFault::kErrnoEio) {
            observation.fail_poll_errno.store(EIO);
        } else if (fault == PollFault::kEventfdErr) {
            observation.inject_signal_revents.store(POLLERR);
        } else {
            observation.inject_ring_revents.store(POLLERR);
        }
        observation.fail_poll_at.store(observation.poll_calls.load() + 1);
        // Submit a second op (or let 100ms poll timeout expire) so the worker
        // invokes poll() and hits the injected fault.
        ASSERT_TRUE(WaitUntil([&] { return !reader->healthy(); }));
        // The ring must NOT have exited and op must NOT have completed before
        // the kernel finishes writing into arena.data.
        EXPECT_EQ(op_future.wait_for(50ms), std::future_status::timeout);
        EXPECT_EQ(observation.exits.load(), 0u);

        pipe_source->WriteBytes(std::byte{0x5A}, 16);
        ASSERT_EQ(op_future.wait_for(5s), std::future_status::ready);
        const auto res = op_future.get();
        EXPECT_EQ(res.error, Error::kOk);
        EXPECT_EQ(res.bytes, 16u);
        for (size_t i = 0; i < 16; ++i) {
            EXPECT_EQ(arena.data[i], std::byte{0x5A});
        }
        reader->Shutdown();
        reader.reset();
        EXPECT_EQ(observation.exits.load(), 1u);
    }
}

}  // namespace
}  // namespace mooncake::ssd_offload
