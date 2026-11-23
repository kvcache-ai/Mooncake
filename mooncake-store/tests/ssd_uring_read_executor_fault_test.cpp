// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Deterministic kernel-return fixture, linked to the unchanged production
// executor and driver. It is not evidence of real io_uring/device behavior.
#include "ssd_uring_pipeline.h"

#include <gtest/gtest.h>
#include <liburing.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <future>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace fake_uring {
using namespace std::chrono_literals;
struct Submission {
    int returned;
    unsigned consumed;
};
struct Ring {
    explicit Ring(unsigned depth) : sqes(depth) {
        fd = int(syscall(SYS_eventfd2, 0, EFD_CLOEXEC | EFD_NONBLOCK));
    }
    std::mutex mutex;
    std::condition_variable changed;
    int fd{-1};
    unsigned head{0}, tail{0};
    std::vector<io_uring_sqe> sqes;
    std::map<uint64_t, io_uring_sqe> accepted;
    std::vector<io_uring_sqe> history;
    std::deque<std::unique_ptr<io_uring_cqe>> completions;
    std::vector<uint64_t> seen;
    std::deque<Submission> outcomes;
    std::vector<Submission> submissions;
    std::deque<int> peek_errors;
    bool paused{false}, submit_entered{false}, auto_drain{false};
    bool exited{false}, exit_with_unfinished{false}, invalid_fixture{false};

    void PostLocked(uint64_t id, int result) {
        auto completion = std::make_unique<io_uring_cqe>();
        completion->user_data = id;
        completion->res = result;
        completion->flags = 0;
        completions.push_back(std::move(completion));
        uint64_t one = 1;
        (void)syscall(SYS_write, fd, &one, sizeof(one));
        changed.notify_all();
    }
    bool CompleteLocked(uint64_t id, int result) {
        const auto found = accepted.find(id);
        if (found == accepted.end()) return false;
        const auto sqe = found->second;
        if (result > int(sqe.len)) {
            invalid_fixture = true;
            return false;
        }
        // Memory is touched only now, immediately before its physical CQE.
        // Until then the fixture holds only raw fd/address, not a source lease.
        if (result > 0) {
            ssize_t copied;
            do {
                copied = ::pread(sqe.fd, reinterpret_cast<void*>(sqe.addr),
                                 size_t(result), off_t(sqe.off));
            } while (copied < 0 && errno == EINTR);
            if (copied != result) {
                invalid_fixture = true;
                result = copied < 0 ? -errno : int(copied);
            }
        }
        accepted.erase(found);
        PostLocked(id, result);
        return true;
    }
    bool Complete(uint64_t id, int result) {
        std::lock_guard lock(mutex);
        return CompleteLocked(id, result);
    }
    void PostUnmatched(uint64_t id) {
        std::lock_guard lock(mutex);
        PostLocked(id, 0);
    }
    bool WaitAccepted(size_t count) {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, 2s,
                                [&] { return history.size() >= count; });
    }
    bool WaitSubmit() {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, 2s, [&] { return submit_entered; });
    }
    bool WaitSeen(uint64_t id) {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, 2s, [&] {
            return std::find(seen.begin(), seen.end(), id) != seen.end();
        });
    }
    std::vector<io_uring_sqe> History() {
        std::lock_guard lock(mutex);
        return history;
    }
    void Resume() {
        std::lock_guard lock(mutex);
        paused = false;
        changed.notify_all();
    }
    void AutoDrain() {
        std::lock_guard lock(mutex);
        auto_drain = true;
        paused = false;
        outcomes.clear();
        peek_errors.clear();
        while (!accepted.empty())
            CompleteLocked(accepted.begin()->first,
                           int(accepted.begin()->second.len));
        changed.notify_all();
    }
};

struct Kernel;
std::atomic<Kernel*> current{nullptr};
struct Kernel {
    Kernel() { current.store(this); }
    ~Kernel() { current.store(nullptr); }
    std::mutex mutex;
    std::condition_variable changed;
    std::vector<std::shared_ptr<Ring>> rings;
    std::set<int> wake_fds;
    size_t poll_entries{0}, poll_interruptions{0}, poll_eintr_delivered{0},
        fail_wakes{0};
    size_t registrations{0};
    std::shared_ptr<Ring> First() {
        std::lock_guard lock(mutex);
        return rings.at(0);
    }
    bool WaitPoll() {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, 2s, [&] { return poll_entries != 0; });
    }
    void AutoDrain() {
        std::vector<std::shared_ptr<Ring>> snapshot;
        {
            std::lock_guard lock(mutex);
            fail_wakes = poll_interruptions = 0;
            snapshot = rings;
        }
        for (auto& ring : snapshot) ring->AutoDrain();
    }
};
struct DrainOnExit {
    Kernel& kernel;
    ~DrainOnExit() { kernel.AutoDrain(); }
};
Ring& State(const io_uring* ring) {
    return *static_cast<Ring*>(ring->test_state);
}
}  // namespace fake_uring

extern "C" int io_uring_queue_init(unsigned depth, io_uring* ring, unsigned) {
    auto* kernel = fake_uring::current.load();
    if (!kernel || !depth) return -EINVAL;
    auto state = std::make_shared<fake_uring::Ring>(depth);
    if (state->fd < 0) return -errno;
    ring->ring_fd = state->fd;
    ring->test_state = state.get();
    std::lock_guard lock(kernel->mutex);
    kernel->rings.push_back(std::move(state));
    return 0;
}
extern "C" void io_uring_queue_exit(io_uring* ring) {
    auto& state = fake_uring::State(ring);
    std::lock_guard lock(state.mutex);
    state.exit_with_unfinished = !state.accepted.empty() ||
                                 !state.completions.empty() ||
                                 state.head != state.tail;
    state.exited = true;
    close(state.fd);
    state.fd = -1;
}
extern "C" io_uring_sqe* io_uring_get_sqe(io_uring* ring) {
    auto& state = fake_uring::State(ring);
    std::lock_guard lock(state.mutex);
    if (state.tail - state.head == state.sqes.size()) return nullptr;
    auto* sqe = &state.sqes[state.tail++ % state.sqes.size()];
    *sqe = {};
    return sqe;
}
extern "C" unsigned io_uring_sq_ready(const io_uring* ring) {
    auto& state = fake_uring::State(ring);
    std::lock_guard lock(state.mutex);
    return state.tail - state.head;
}
extern "C" unsigned io_uring_sq_space_left(const io_uring* ring) {
    auto& state = fake_uring::State(ring);
    std::lock_guard lock(state.mutex);
    return unsigned(state.sqes.size()) - (state.tail - state.head);
}
extern "C" int io_uring_submit(io_uring* ring) {
    auto& state = fake_uring::State(ring);
    std::unique_lock lock(state.mutex);
    state.submit_entered = true;
    state.changed.notify_all();
    state.changed.wait(lock, [&] { return !state.paused; });
    const unsigned available = state.tail - state.head;
    fake_uring::Submission outcome{int(available), available};
    if (!state.outcomes.empty()) {
        outcome = state.outcomes.front();
        state.outcomes.pop_front();
    }
    if (outcome.consumed > available) {
        state.invalid_fixture = true;
        return -EINVAL;
    }
    for (unsigned i = 0; i < outcome.consumed; ++i) {
        const auto sqe = state.sqes[state.head++ % state.sqes.size()];
        if (!state.accepted.emplace(sqe.user_data, sqe).second)
            state.invalid_fixture = true;
        state.history.push_back(sqe);
        if (state.auto_drain) state.CompleteLocked(sqe.user_data, int(sqe.len));
    }
    state.submissions.push_back(outcome);
    state.changed.notify_all();
    return outcome.returned;
}
extern "C" int io_uring_peek_cqe(io_uring* ring, io_uring_cqe** cqe) {
    auto& state = fake_uring::State(ring);
    std::lock_guard lock(state.mutex);
    if (state.completions.empty()) return -EAGAIN;
    if (!state.peek_errors.empty()) {
        const int error = state.peek_errors.front();
        state.peek_errors.pop_front();
        return error;
    }
    *cqe = state.completions.front().get();
    return 0;
}
extern "C" void io_uring_cqe_seen(io_uring* ring, io_uring_cqe* cqe) {
    auto& state = fake_uring::State(ring);
    std::lock_guard lock(state.mutex);
    if (state.completions.empty() || state.completions.front().get() != cqe) {
        state.invalid_fixture = true;
        return;
    }
    state.seen.push_back(cqe->user_data);
    state.completions.pop_front();
    if (state.completions.empty()) {
        uint64_t value;
        (void)syscall(SYS_read, state.fd, &value, sizeof(value));
    }
    state.changed.notify_all();
}
extern "C" int io_uring_register_buffers(io_uring*, const iovec*, unsigned) {
    if (auto* kernel = fake_uring::current.load()) {
        std::lock_guard lock(kernel->mutex);
        ++kernel->registrations;
    }
    return -ENOTSUP;
}
extern "C" int io_uring_unregister_buffers(io_uring*) { return 0; }
extern "C" int eventfd(unsigned initial, int flags) noexcept {
    const int fd = int(syscall(SYS_eventfd2, initial, flags));
    if (auto* kernel = fake_uring::current.load()) {
        std::lock_guard lock(kernel->mutex);
        kernel->wake_fds.insert(fd);
    }
    return fd;
}
extern "C" ssize_t write(int fd, const void* buffer, size_t bytes) {
    if (auto* kernel = fake_uring::current.load()) {
        std::lock_guard lock(kernel->mutex);
        if (kernel->fail_wakes && kernel->wake_fds.count(fd)) {
            --kernel->fail_wakes;
            errno = EIO;
            return -1;
        }
    }
    return ssize_t(syscall(SYS_write, fd, buffer, bytes));
}
extern "C" int poll(pollfd* descriptors, nfds_t count, int timeout) {
    if (auto* kernel = fake_uring::current.load()) {
        std::lock_guard lock(kernel->mutex);
        ++kernel->poll_entries;
        kernel->changed.notify_all();
        if (kernel->poll_interruptions) {
            --kernel->poll_interruptions;
            ++kernel->poll_eintr_delivered;
            errno = EINTR;
            return -1;
        }
    }
#if defined(SYS_poll)
    return int(syscall(SYS_poll, descriptors, count, timeout));
#else
    timespec duration{timeout / 1000, (timeout % 1000) * 1000000L};
    return int(syscall(SYS_ppoll, descriptors, count,
                       timeout < 0 ? nullptr : &duration, nullptr, 0));
#endif
}

namespace mooncake::ssd_offload {
namespace {
using namespace std::chrono_literals;
constexpr size_t kSlot = 4096;
struct File {
    char path[64] = "/tmp/mooncake_uring_fault_XXXXXX";
    std::vector<std::byte> bytes;
    explicit File(size_t salt, size_t size = 4 * kSlot) : bytes(size) {
        for (size_t i = 0; i < size; ++i)
            bytes[i] = std::byte((i * 37 + i / 251 + salt) % 256);
        const int fd = mkstemp(path);
        if (fd < 0) throw std::runtime_error("mkstemp");
        size_t done = 0;
        while (done < size) {
            const auto n = ::write(fd, bytes.data() + done, size - done);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) {
                close(fd);
                unlink(path);
                throw std::runtime_error("write fixture");
            }
            done += size_t(n);
        }
        close(fd);
    }
    ~File() { unlink(path); }
};
struct Arena {
    std::byte* data{nullptr};
    size_t count;
    explicit Arena(size_t count) : count(count) {
        void* p = nullptr;
        if (posix_memalign(&p, kSlot, count * kSlot)) throw std::bad_alloc();
        data = static_cast<std::byte*>(p);
        std::memset(data, 0xA5, count * kSlot);
    }
    ~Arena() { free(data); }
    SlotBuffer slot(size_t i) { return {data + i * kSlot, kSlot}; }
};
struct Results {
    std::mutex mutex;
    std::condition_variable changed;
    std::vector<std::pair<size_t, ReadResult>> values;
    void Add(size_t id, ReadResult result) {
        std::lock_guard lock(mutex);
        values.emplace_back(id, result);
        changed.notify_all();
    }
    bool Wait(size_t count) {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, 2s,
                                [&] { return values.size() >= count; });
    }
    size_t Count() {
        std::lock_guard lock(mutex);
        return values.size();
    }
};
std::shared_ptr<ReadSource> Source(const File& file) {
    return OpenPosixReadSource(file.path, {file.bytes.size(), 0, 1}).source;
}
bool Pending(const std::shared_ptr<UringReadExecutor>& pool, size_t count) {
    const auto until = std::chrono::steady_clock::now() + 2s;
    while (pool->outstanding() != count &&
           std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(1ms);
    return pool->outstanding() == count;
}
struct Target final : DrainedWriteTarget {
    std::vector<std::byte> bytes =
        std::vector<std::byte>(kSlot + 128, std::byte{0xA5});
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer slot) noexcept override {
        for (const auto& fragment : chunk.fragments)
            std::memcpy(bytes.data() + fragment.destination.offset,
                        slot.data + fragment.buffer_offset,
                        size_t(fragment.destination.length));
        return Error::kOk;
    }
};
ReadChunk Chunk() { return {0, 0, kSlot, 0, kSlot, {{0, {1, 1, 64, kSlot}}}}; }
struct DriverBeforeChunkRelease {
    std::unique_ptr<UringPipelineDriver>& driver;
    ~DriverBeforeChunkRelease() { driver.reset(); }
};

TEST(SsdUringReadExecutorFault,
     ZeroEintrPartialAndRetryableErrorsCompleteExactlyOnce) {
    fake_uring::Kernel kernel;
    File file(17);
    auto source = Source(file);
    ASSERT_TRUE(source);
    Arena arena(4);
    auto pool = UringReadExecutor::Create(1, 4);
    ASSERT_TRUE(pool);
    fake_uring::DrainOnExit cleanup{kernel};
    auto ring = kernel.First();
    {
        std::lock_guard lock(kernel.mutex);
        kernel.poll_interruptions = 1;
    }
    {
        std::lock_guard lock(ring->mutex);
        ring->paused = true;
        ring->outcomes = {{0, 0},       {-EINTR, 0},  {2, 2},
                          {-EAGAIN, 0}, {-ENOMEM, 0}, {-EBUSY, 0}};
        ring->peek_errors = {-EINTR};
    }
    auto results = std::make_shared<Results>();
    for (size_t i = 0; i < 4; ++i) {
        ASSERT_TRUE(pool->TrySubmit(
            source, *source->native_read_fd(), {arena.slot(i).data, kSlot},
            i * kSlot,
            [results, i](ReadResult result) { results->Add(i, result); }));
        if (i == 0) {
            ASSERT_TRUE(ring->WaitSubmit());
        }
    }
    ring->Resume();
    ASSERT_TRUE(ring->WaitAccepted(4));
    EXPECT_EQ(results->Count(), 0u);
    const auto submitted = ring->History();
    std::set<uint64_t> ids;
    for (const auto& sqe : submitted) {
        EXPECT_EQ(sqe.opcode, IORING_OP_READ);
        EXPECT_EQ(sqe.flags, 0u);
        EXPECT_TRUE(ids.insert(sqe.user_data).second);
    }
    for (auto it = submitted.rbegin(); it != submitted.rend(); ++it)
        ASSERT_TRUE(ring->Complete(it->user_data, int(it->len)));
    ASSERT_TRUE(results->Wait(4));
    pool->Shutdown();
    EXPECT_EQ(results->Count(), 4u);
    std::set<size_t> completed_ids;
    for (size_t i = 0; i < 4; ++i)
        EXPECT_EQ(std::memcmp(arena.slot(i).data, file.bytes.data() + i * kSlot,
                              kSlot),
                  0);
    for (const auto& [id, result] : results->values) {
        EXPECT_LT(id, 4u);
        EXPECT_TRUE(completed_ids.insert(id).second);
        EXPECT_EQ(result.error, Error::kOk);
        EXPECT_EQ(result.bytes, kSlot);
    }
    EXPECT_EQ(completed_ids, (std::set<size_t>{0, 1, 2, 3}));
    pool.reset();
    std::lock_guard lock(ring->mutex);
    ASSERT_GE(ring->submissions.size(), 6u);
    EXPECT_EQ(ring->submissions[0].returned, 0);
    EXPECT_EQ(ring->submissions[1].returned, -EINTR);
    EXPECT_EQ(ring->submissions[2].returned, 2);
    EXPECT_EQ(ring->submissions[3].returned, -EAGAIN);
    EXPECT_EQ(ring->submissions[4].returned, -ENOMEM);
    EXPECT_EQ(ring->submissions[5].returned, -EBUSY);
    EXPECT_FALSE(ring->invalid_fixture);
    EXPECT_FALSE(ring->exit_with_unfinished);
    EXPECT_TRUE(ring->peek_errors.empty());
    EXPECT_EQ(kernel.poll_eintr_delivered, 1u);
    EXPECT_EQ(kernel.registrations, 0u);
}

TEST(SsdUringReadExecutorFault,
     SameRingReverseCqesPreserveDifferentRequestsWithSlotZero) {
    fake_uring::Kernel kernel;
    File first(7), second(103);
    Arena a(1), b(1);
    BoundedExecutor writes(2, 4);
    auto pool = UringReadExecutor::Create(1, 2);
    ASSERT_TRUE(pool);
    auto ta = std::make_shared<Target>(), tb = std::make_shared<Target>();
    auto da = UringPipelineDriver::Create(Source(first), ta, writes,
                                          {a.slot(0)}, {false, pool});
    auto db = UringPipelineDriver::Create(Source(second), tb, writes,
                                          {b.slot(0)}, {false, pool});
    ASSERT_TRUE(da.driver && db.driver);
    auto chunk = Chunk();
    const SlotToken token_a{101, 3, 0, 1, 0}, token_b{202, 4, 0, 1, 0};
    DriverBeforeChunkRelease ga{da.driver}, gb{db.driver};
    fake_uring::DrainOnExit cleanup{kernel};
    ASSERT_TRUE(da.driver->SubmitRead(token_a, chunk, a.slot(0)));
    ASSERT_TRUE(db.driver->SubmitRead(token_b, chunk, b.slot(0)));
    auto ring = kernel.First();
    ASSERT_TRUE(ring->WaitAccepted(2));
    const auto submitted = ring->History();
    ASSERT_NE(submitted[0].user_data, submitted[1].user_data);
    ASSERT_TRUE(ring->Complete(submitted[1].user_data, kSlot));
    ASSERT_TRUE(ring->Complete(submitted[0].user_data, kSlot));
    for (auto* pair : {&db, &da}) {
        const auto& token = pair == &da ? token_a : token_b;
        IoCompletion completion;
        ASSERT_TRUE(pair->driver->WaitForEvent(completion, 2s));
        EXPECT_EQ(completion.stage, SlotStage::kReading);
        EXPECT_EQ(completion.result, Error::kOk);
        EXPECT_EQ(completion.token.request_id, token.request_id);
        EXPECT_EQ(completion.token.attempt_id, token.attempt_id);
        EXPECT_EQ(completion.token.slot_index, 0u);
        EXPECT_EQ(completion.token.generation, 1u);
        EXPECT_EQ(completion.token.chunk_index, 0u);
    }
    EXPECT_EQ(std::memcmp(a.data, first.bytes.data(), kSlot), 0);
    EXPECT_EQ(std::memcmp(b.data, second.bytes.data(), kSlot), 0);
    da.driver.reset();
    db.driver.reset();
    pool.reset();
    EXPECT_FALSE(ring->exit_with_unfinished);
}

TEST(SsdUringReadExecutorFault,
     CancelAndDriverDestructionWaitForPhysicalCqeAndSourceLease) {
    fake_uring::Kernel kernel;
    File file(61);
    Arena arena(1);
    BoundedExecutor writes(1, 2);
    auto source = Source(file);
    ASSERT_TRUE(source);
    const int fd = *source->native_read_fd();
    std::weak_ptr<ReadSource> lease = source;
    auto pool = UringReadExecutor::Create(1, 2);
    ASSERT_TRUE(pool);
    auto target = std::make_shared<Target>();
    auto made = UringPipelineDriver::Create(source, target, writes,
                                            {arena.slot(0)}, {false, pool});
    ASSERT_TRUE(made.driver);
    auto chunk = Chunk();
    DriverBeforeChunkRelease driver_guard{made.driver};
    fake_uring::DrainOnExit cleanup{kernel};
    const SlotToken token{1, 2, 0, 1, 0};
    ASSERT_TRUE(made.driver->SubmitRead(token, chunk, arena.slot(0)));
    source.reset();
    auto ring = kernel.First();
    ASSERT_TRUE(ring->WaitAccepted(1));
    made.driver->RequestCancel(token, SlotStage::kReading);
    auto closing = std::async(std::launch::async, [&] { made.driver.reset(); });
    EXPECT_EQ(closing.wait_for(20ms), std::future_status::timeout);
    EXPECT_FALSE(lease.expired());
    EXPECT_GE(fcntl(fd, F_GETFD), 0);
    EXPECT_TRUE(std::all_of(arena.data, arena.data + kSlot,
                            [](std::byte b) { return b == std::byte{0xA5}; }));
    const auto submitted = ring->History();
    EXPECT_TRUE(ring->Complete(submitted[0].user_data, kSlot));
    EXPECT_EQ(closing.wait_for(2s), std::future_status::ready);
    closing.get();
    EXPECT_TRUE(lease.expired());
    EXPECT_EQ(fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(std::memcmp(arena.data, file.bytes.data(), kSlot), 0);
    EXPECT_TRUE(std::all_of(target->bytes.begin(), target->bytes.end(),
                            [](std::byte b) { return b == std::byte{0xA5}; }));
}

TEST(SsdUringReadExecutorFault,
     FatalSubmitFailsQueuedWorkButRetainsPublishedReadAndStickyAdmission) {
    fake_uring::Kernel kernel;
    File file(29);
    Arena arena(33);
    auto source = Source(file);
    ASSERT_TRUE(source);
    std::weak_ptr<ReadSource> lease = source;
    const int fd = *source->native_read_fd();
    auto pool = UringReadExecutor::Create(1, 33);
    ASSERT_TRUE(pool);
    fake_uring::DrainOnExit cleanup{kernel};
    auto ring = kernel.First();
    {
        std::lock_guard lock(ring->mutex);
        ring->paused = true;
        // A failed enter does not establish that no SQE was consumed.
        ring->outcomes = {{-EIO, 1}};
    }
    auto results = std::make_shared<Results>();
    for (size_t i = 0; i < 33; ++i) {
        ASSERT_TRUE(pool->TrySubmit(
            source, fd, {arena.slot(i).data, kSlot}, 0,
            [results, i](ReadResult result) { results->Add(i, result); }));
        if (!i) {
            ASSERT_TRUE(ring->WaitSubmit());
        }
    }
    source.reset();
    ring->Resume();
    ASSERT_TRUE(ring->WaitAccepted(1));
    ASSERT_TRUE(results->Wait(32));
    ASSERT_TRUE(Pending(pool, 1));
    EXPECT_FALSE(lease.expired());
    auto rejected = std::make_shared<std::atomic<size_t>>(0);
    auto probe_source = Source(file);
    EXPECT_FALSE(pool->TrySubmit(probe_source, *probe_source->native_read_fd(),
                                 {arena.slot(0).data, kSlot}, 0,
                                 [rejected](ReadResult) { ++*rejected; }));
    for (const auto& [id, result] : results->values) {
        EXPECT_NE(id, 0u);
        EXPECT_EQ(result.error, Error::kIoError);
        EXPECT_EQ(result.bytes, 0u);
    }
    auto closing = std::async(std::launch::async, [&] { pool->Shutdown(); });
    EXPECT_EQ(closing.wait_for(20ms), std::future_status::timeout);
    EXPECT_GE(fcntl(fd, F_GETFD), 0);
    EXPECT_TRUE(ring->Complete(ring->History()[0].user_data, kSlot));
    EXPECT_EQ(closing.wait_for(2s), std::future_status::ready);
    closing.get();
    EXPECT_TRUE(lease.expired());
    EXPECT_EQ(results->Count(), 33u);
    EXPECT_EQ(rejected->load(), 0u);
    EXPECT_EQ(std::memcmp(arena.data, file.bytes.data(), kSlot), 0);
    for (size_t i = 1; i < 33; ++i)
        EXPECT_TRUE(
            std::all_of(arena.slot(i).data, arena.slot(i).data + kSlot,
                        [](std::byte b) { return b == std::byte{0xA5}; }));
    pool.reset();
    EXPECT_FALSE(ring->exit_with_unfinished);
}

TEST(SsdUringReadExecutorFault, WakeFailureAfterAcceptanceNeverReturnsFalse) {
    fake_uring::Kernel kernel;
    File file(43);
    auto source = Source(file);
    ASSERT_TRUE(source);
    Arena arena(1);
    auto pool = UringReadExecutor::Create(1, 2);
    ASSERT_TRUE(pool);
    fake_uring::DrainOnExit cleanup{kernel};
    ASSERT_TRUE(kernel.WaitPoll());
    {
        std::lock_guard lock(kernel.mutex);
        // Fail both the enqueue wake and Fail()'s subsequent wake attempts.
        // An idle worker must still progress through its bounded backup poll.
        kernel.fail_wakes = 100;
    }
    auto results = std::make_shared<Results>();
    EXPECT_TRUE(pool->TrySubmit(
        source, *source->native_read_fd(), {arena.data, kSlot}, 0,
        [results](ReadResult result) { results->Add(0, result); }));
    EXPECT_FALSE(pool->TrySubmit(
        source, *source->native_read_fd(), {arena.data, kSlot}, 0,
        [results](ReadResult result) { results->Add(1, result); }));
    // The worker may have prepared the accepted job before Wake reports its
    // failure. Both outcomes are legal: queued work fails safely, while an
    // already published read must still wait for its real CQE.
    auto ring = kernel.First();
    const auto until = std::chrono::steady_clock::now() + 2s;
    while (!results->Count() && ring->History().empty() &&
           std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(1ms);
    const auto submitted = ring->History();
    if (!submitted.empty()) {
        ASSERT_TRUE(ring->Complete(submitted[0].user_data, kSlot));
    }
    ASSERT_TRUE(results->Wait(1));
    ASSERT_TRUE(Pending(pool, 0));
    {
        std::lock_guard lock(kernel.mutex);
        EXPECT_LE(kernel.fail_wakes, 98u);
        EXPECT_GT(kernel.fail_wakes, 0u);
    }
    EXPECT_EQ(results->values[0].second.error,
              submitted.empty() ? Error::kIoError : Error::kOk);
    pool->Shutdown();
    EXPECT_EQ(results->Count(), 1u);
    if (submitted.empty())
        EXPECT_TRUE(
            std::all_of(arena.data, arena.data + kSlot,
                        [](std::byte b) { return b == std::byte{0xA5}; }));
    else
        EXPECT_EQ(std::memcmp(arena.data, file.bytes.data(), kSlot), 0);
}

TEST(SsdUringReadExecutorFault, OldCqeCannotReleaseANewOperationInTheSameRing) {
    fake_uring::Kernel kernel;
    File first(3), second(211);
    Arena arena(2);
    auto source = Source(first), next = Source(second);
    ASSERT_TRUE(source && next);
    std::weak_ptr<ReadSource> lease = next;
    auto pool = UringReadExecutor::Create(1, 1);
    ASSERT_TRUE(pool);
    fake_uring::DrainOnExit cleanup{kernel};
    auto results = std::make_shared<Results>();
    ASSERT_TRUE(pool->TrySubmit(
        source, *source->native_read_fd(), {arena.slot(0).data, kSlot}, 0,
        [results](ReadResult result) { results->Add(0, result); }));
    auto ring = kernel.First();
    ASSERT_TRUE(ring->WaitAccepted(1));
    const auto old_id = ring->History()[0].user_data;
    ASSERT_TRUE(ring->Complete(old_id, kSlot));
    ASSERT_TRUE(results->Wait(1));
    ASSERT_TRUE(Pending(pool, 0));
    ASSERT_TRUE(pool->TrySubmit(
        next, *next->native_read_fd(), {arena.slot(1).data, kSlot}, 0,
        [results](ReadResult result) { results->Add(1, result); }));
    next.reset();
    ASSERT_TRUE(ring->WaitAccepted(2));
    const auto new_id = ring->History()[1].user_data;
    ASSERT_NE(old_id, new_id);
    ring->PostUnmatched(old_id);
    // The first old-id seen was the valid completion. Wait for this duplicate.
    const auto until = std::chrono::steady_clock::now() + 2s;
    bool duplicate_seen = false;
    while (!duplicate_seen && std::chrono::steady_clock::now() < until) {
        {
            std::lock_guard lock(ring->mutex);
            duplicate_seen =
                std::count(ring->seen.begin(), ring->seen.end(), old_id) == 2;
        }
        if (!duplicate_seen) std::this_thread::sleep_for(1ms);
    }
    ASSERT_TRUE(duplicate_seen);
    EXPECT_EQ(results->Count(), 1u);
    EXPECT_EQ(pool->outstanding(), 1u);
    EXPECT_FALSE(lease.expired());
    ASSERT_TRUE(ring->Complete(new_id, kSlot));
    ASSERT_TRUE(results->Wait(2));
    pool->Shutdown();
    EXPECT_TRUE(lease.expired());
    EXPECT_EQ(results->Count(), 2u);
    EXPECT_EQ(std::memcmp(arena.slot(1).data, second.bytes.data(), kSlot), 0);
    pool.reset();
    EXPECT_FALSE(ring->exit_with_unfinished);
}

class CompletionBoundary : public testing::TestWithParam<int> {};
class TransientReadCompletion : public testing::TestWithParam<bool> {};
TEST_P(TransientReadCompletion, RetriesWithFreshIdsAndRetainsSource) {
    fake_uring::Kernel kernel;
    File file(13);
    Arena arena(1);
    auto source = Source(file);
    ASSERT_TRUE(source);
    std::weak_ptr<ReadSource> lease = source;
    auto pool = UringReadExecutor::Create(1, 1);
    ASSERT_TRUE(pool);
    fake_uring::DrainOnExit cleanup{kernel};
    auto results = std::make_shared<Results>();
    ASSERT_TRUE(pool->TrySubmit(
        source, *source->native_read_fd(), {arena.slot(0).data, kSlot}, 0,
        [results](ReadResult result) { results->Add(0, result); }));
    source.reset();
    auto ring = kernel.First();
    uint64_t previous_id = 0;
    for (size_t attempt = 0; attempt < 4; ++attempt) {
        ASSERT_TRUE(ring->WaitAccepted(attempt + 1));
        const auto id = ring->History().back().user_data;
        EXPECT_GT(id, previous_id);
        previous_id = id;
        EXPECT_EQ(pool->outstanding(), 1u);
        EXPECT_FALSE(lease.expired());
        EXPECT_EQ(results->Count(), 0u);
        const bool success = GetParam() && attempt == 3;
        ASSERT_TRUE(ring->Complete(
            id, success ? int(kSlot) : (attempt % 2 ? -EAGAIN : -EINTR)));
    }
    ASSERT_TRUE(results->Wait(1));
    pool->Shutdown();
    EXPECT_EQ(pool->outstanding(), 0u);
    EXPECT_TRUE(lease.expired());
    EXPECT_EQ(results->Count(), 1u);
    EXPECT_EQ(results->values[0].second.error,
              GetParam() ? Error::kOk : Error::kIoError);
    EXPECT_EQ(results->values[0].second.bytes, GetParam() ? kSlot : 0u);
    if (GetParam()) {
        EXPECT_EQ(std::memcmp(arena.data, file.bytes.data(), kSlot), 0);
    }
    EXPECT_EQ(ring->History().size(), 4u);
    pool.reset();
    EXPECT_FALSE(ring->exit_with_unfinished);
}
INSTANTIATE_TEST_SUITE_P(RecoveredAndExhausted, TransientReadCompletion,
                         testing::Bool());

TEST_P(CompletionBoundary,
       RealPipelineContinuesShortReadsAndChecksPayloadCoverage) {
    fake_uring::Kernel kernel;
    File file(13, 1037);
    Arena arena(1);
    BoundedExecutor writes(1, 2);
    auto pool = UringReadExecutor::Create(1, 2);
    ASSERT_TRUE(pool);
    auto target = std::make_shared<Target>();
    auto made = UringPipelineDriver::Create(Source(file), target, writes,
                                            {arena.slot(0)}, {false, pool});
    ASSERT_TRUE(made.driver);
    auto plan =
        BuildReadPlan({1000, 37, {{0, {{1, 1, 64, 1000}}}}}, {kSlot, kSlot, 2});
    ASSERT_EQ(plan.error, Error::kOk);
    ReadPipeline pipeline(1, 1, std::move(plan.plan), {arena.slot(0)},
                          *made.driver);
    DriverBeforeChunkRelease driver_guard{made.driver};
    fake_uring::DrainOnExit cleanup{kernel};
    ASSERT_EQ(pipeline.Start(), Error::kOk);
    auto ring = kernel.First();
    ASSERT_TRUE(ring->WaitAccepted(1));
    const auto submitted = ring->History();
    ASSERT_EQ(submitted[0].len, kSlot);
    ASSERT_TRUE(ring->Complete(submitted[0].user_data, GetParam()));
    if (GetParam() == 127) {
        ASSERT_TRUE(ring->WaitAccepted(2));
        const auto suffix = ring->History().back();
        EXPECT_EQ(suffix.off, 127u);
        EXPECT_EQ(suffix.addr, submitted[0].addr + 127);
        EXPECT_EQ(suffix.len, kSlot - 127);
        ASSERT_TRUE(ring->Complete(suffix.user_data, 1037 - 127));
    }
    IoCompletion event;
    ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
    ASSERT_EQ(event.stage, SlotStage::kReading);
    EXPECT_EQ(event.bytes, GetParam() < 0      ? 0u
                           : GetParam() == 127 ? 1037u
                                               : size_t(GetParam()));
    EXPECT_EQ(event.result, GetParam() < 0 ? Error::kIoError : Error::kOk);
    EXPECT_EQ(pipeline.ReadDrained(event.token, event.bytes, event.result),
              Error::kOk);
    if (GetParam() == 1037 || GetParam() == 127) {
        ASSERT_TRUE(made.driver->WaitForEvent(event, 2s));
        EXPECT_EQ(event.stage, SlotStage::kWriting);
        EXPECT_EQ(pipeline.WriteDrained(event.token, event.result), Error::kOk);
        EXPECT_EQ(pipeline.state(), PipelineState::kSucceeded);
    } else {
        EXPECT_EQ(pipeline.state(), PipelineState::kFailed);
        EXPECT_EQ(pipeline.error(), Error::kIoError);
    }
    EXPECT_EQ(pipeline.inflight(), 0u);
    EXPECT_EQ(ring->History().size(), GetParam() == 127 ? 2u : 1u);
    std::vector<std::byte> expected(target->bytes.size(), std::byte{0xA5});
    if (GetParam() == 1037 || GetParam() == 127)
        std::copy(file.bytes.begin() + 37, file.bytes.end(),
                  expected.begin() + 64);
    EXPECT_EQ(target->bytes, expected);
    made.driver.reset();
    pool.reset();
    EXPECT_FALSE(ring->exit_with_unfinished);
    EXPECT_FALSE(ring->invalid_fixture);
}
INSTANTIATE_TEST_SUITE_P(EofShortNegativeAndCoveredPadding, CompletionBoundary,
                         testing::Values(0, 127, -EIO, 1037));
}  // namespace
}  // namespace mooncake::ssd_offload
