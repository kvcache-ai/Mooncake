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
#include <poll.h>
#include <sys/wait.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>

#include "tent/transport/rdma/shared_quota.h"

namespace {
std::atomic<int64_t> frozen_time_ns{-1};
}

extern "C" int __real_clock_gettime(clockid_t clock, struct timespec* ts);
extern "C" int __wrap_clock_gettime(clockid_t clock, struct timespec* ts) {
    const int64_t now = frozen_time_ns.load();
    if (clock == CLOCK_REALTIME && now >= 0) {
        ts->tv_sec = now / 1000000000;
        ts->tv_nsec = now % 1000000000;
        return 0;
    }
    return __real_clock_gettime(clock, ts);
}

namespace mooncake {
namespace tent {
namespace {

using Clock = std::chrono::steady_clock;
constexpr int kInterruptedSlot = NUM_SLOTS + 1;

struct SharedQuotaState {
    SharedSlotManager manager{nullptr};
    std::string name = "/tent_shared_quota_test_" + std::to_string(getpid());
    SharedHeader* header = nullptr;

    void initialize() {
        ASSERT_TRUE(manager.attach(name).ok());
        const int fd = shm_open(name.c_str(), O_RDWR, 0600);
        ASSERT_GE(fd, 0);
        void* ptr = mmap(nullptr, sizeof(SharedHeader), PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, 0);
        close(fd);
        ASSERT_NE(ptr, MAP_FAILED);
        header = static_cast<SharedHeader*>(ptr);
        // Fork only after the publisher has stopped. Keep a separate mapping
        // so the tests exercise the mutex initialized by the real manager.
        ASSERT_TRUE(manager.detach().ok());
    }

    ~SharedQuotaState() {
        manager.detach();
        if (header) munmap(header, sizeof(SharedHeader));
        shm_unlink(name.c_str());
    }
};

struct ChildOwner {
    pid_t pid = -1;

    ~ChildOwner() {
        if (pid > 0) {
            kill(pid, SIGKILL);
            waitpid(pid, nullptr, 0);
        }
    }

    void start(SharedHeader* header) {
        int ready[2];
        ASSERT_EQ(pipe(ready), 0);
        pid = fork();
        ASSERT_GE(pid, 0);
        if (pid == 0) {
            close(ready[0]);
            // Also bound the child lifetime if the test's watchdog fires.
            alarm(5);
            if (pthread_mutex_lock(&header->global_mutex) != 0) _exit(2);
            header->current_slot.store(kInterruptedSlot);
            const char marker = 'L';
            if (write(ready[1], &marker, 1) != 1) _exit(3);
            for (;;) pause();
        }
        close(ready[1]);
        pollfd fd{ready[0], POLLIN, 0};
        const int ready_result = poll(&fd, 1, 2000);
        char marker = 0;
        const auto count = ready_result == 1 ? read(ready[0], &marker, 1) : -1;
        close(ready[0]);
        ASSERT_EQ(count, 1);
        ASSERT_EQ(marker, 'L');
    }

    void killAndWait() {
        ASSERT_EQ(kill(pid, SIGKILL), 0);
        int status = 0;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        pid = -1;
        ASSERT_TRUE(WIFSIGNALED(status));
        ASSERT_EQ(WTERMSIG(status), SIGKILL);
    }
};

// Run each case in a subprocess: a regression in detach/join must fail the
// test instead of hanging the test runner. No RDMA hardware is needed.
void withinDeadline(void (*test)()) {
    ASSERT_EXIT(
        {
            alarm(8);
            test();
            const auto* result = ::testing::UnitTest::GetInstance()
                                     ->current_test_info()
                                     ->result();
            for (int i = 0; i < result->total_part_count(); ++i) {
                const auto& part = result->GetTestPartResult(i);
                if (part.failed()) std::cerr << part.message() << std::endl;
            }
            _exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST(SharedQuota, MutexReportsOwnerDeath) {
    withinDeadline([] {
        SharedQuotaState state;
        ASSERT_NO_FATAL_FAILURE(state.initialize());
        ChildOwner owner;
        ASSERT_NO_FATAL_FAILURE(owner.start(state.header));
        ASSERT_NO_FATAL_FAILURE(owner.killAndWait());
        // With the old preprocessor guard glibc returns EBUSY, not EOWNERDEAD.
        ASSERT_EQ(pthread_mutex_trylock(&state.header->global_mutex),
                  EOWNERDEAD);
        state.header->current_slot.store(0);
        ASSERT_EQ(pthread_mutex_consistent(&state.header->global_mutex), 0);
        ASSERT_EQ(pthread_mutex_unlock(&state.header->global_mutex), 0);
    });
}

TEST(SharedQuota, PublisherRepairsSlotAfterOwnerDeath) {
    withinDeadline([] {
        SharedQuotaState state;
        ASSERT_NO_FATAL_FAILURE(state.initialize());
        ChildOwner owner;
        ASSERT_NO_FATAL_FAILURE(owner.start(state.header));
        ASSERT_NO_FATAL_FAILURE(owner.killAndWait());
        ASSERT_TRUE(state.manager.attach(state.name).ok());
        const auto deadline = Clock::now() + std::chrono::seconds(2);
        while (state.header->current_slot.load() == kInterruptedSlot &&
               Clock::now() < deadline)
            usleep(1000);
        EXPECT_GE(state.header->current_slot.load(), 0);
        EXPECT_LT(state.header->current_slot.load(), NUM_SLOTS);
        EXPECT_TRUE(state.manager.canSend(PRIO_HIGH));
        const auto start = Clock::now();
        ASSERT_TRUE(state.manager.detach().ok());
        EXPECT_LT(Clock::now() - start, std::chrono::seconds(1));
        // Recovery must mark the mutex consistent, not merely unlock it.
        ASSERT_EQ(pthread_mutex_trylock(&state.header->global_mutex), 0);
        ASSERT_EQ(pthread_mutex_unlock(&state.header->global_mutex), 0);
    });
}

TEST(SharedQuota, ShutdownDoesNotWaitForAnotherOwner) {
    withinDeadline([] {
        SharedQuotaState state;
        ASSERT_NO_FATAL_FAILURE(state.initialize());
        ChildOwner owner;
        ASSERT_NO_FATAL_FAILURE(owner.start(state.header));
        ASSERT_TRUE(state.manager.attach(state.name).ok());
        usleep(20000);
        const auto start = Clock::now();
        ASSERT_TRUE(state.manager.detach().ok());
        EXPECT_LT(Clock::now() - start, std::chrono::seconds(1));
        // The publisher must not update the slot without owning the lock.
        EXPECT_EQ(state.header->current_slot.load(), kInterruptedSlot);
    });
}

TEST(SharedQuota, UnrecoverableMutexUsesLocalSlotAndReportsError) {
    withinDeadline([] {
        SharedQuotaState state;
        ASSERT_NO_FATAL_FAILURE(state.initialize());
        ChildOwner owner;
        ASSERT_NO_FATAL_FAILURE(owner.start(state.header));
        ASSERT_NO_FATAL_FAILURE(owner.killAndWait());
        ASSERT_EQ(pthread_mutex_trylock(&state.header->global_mutex),
                  EOWNERDEAD);
        // Deliberately abandon recovery to produce ENOTRECOVERABLE.
        ASSERT_EQ(pthread_mutex_unlock(&state.header->global_mutex), 0);
        // Freeze slot 1: 8 ms / 2 ms % NUM_SLOTS. Shared memory still holds
        // kInterruptedSlot, so only the local fallback can reject LOW here.
        state.manager.setRotationIntervalMs(2);
        frozen_time_ns.store(8000000);
        ASSERT_TRUE(state.manager.attach(state.name).ok());
        const auto deadline = Clock::now() + std::chrono::seconds(2);
        while (state.manager.canSend(PRIO_LOW) && Clock::now() < deadline)
            usleep(1000);
        EXPECT_TRUE(state.manager.canSend(PRIO_MEDIUM));
        EXPECT_FALSE(state.manager.canSend(PRIO_LOW));
        // The fallback keeps following the clock after the publisher exits.
        frozen_time_ns.store(10000000);  // Slot 2 admits all priorities.
        EXPECT_TRUE(state.manager.canSend(PRIO_LOW));
        EXPECT_EQ(state.header->current_slot.load(), kInterruptedSlot);
        const auto status = state.manager.detach();
        EXPECT_TRUE(status.IsInternalError());
        EXPECT_NE(status.ToString().find(std::strerror(ENOTRECOVERABLE)),
                  std::string::npos);
    });
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
