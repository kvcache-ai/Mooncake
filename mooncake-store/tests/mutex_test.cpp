#include "mutex.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>
#include <chrono>
#include <stdexcept>

namespace mooncake::test {

class SharedMutexTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("SharedMutexTest");
        FLAGS_logtostderr = true;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST(SharedMutexTest, CanLockExclusive) {
    SharedMutex mtx;
    EXPECT_NO_THROW({
        mtx.lock();
        mtx.unlock();
    });
}

TEST(SharedMutexTest, CanLockShared) {
    SharedMutex mtx;
    EXPECT_NO_THROW({
        mtx.lock_shared();
        mtx.unlock_shared();
    });
}

TEST(SharedMutexTest, ExclusiveAccessIsMutuallyExclusive) {
    SharedMutex mtx;
    std::atomic<int> counter{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&mtx, &counter]() {
            mtx.lock();
            int val = counter.load();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            counter.store(val + 1);
            mtx.unlock();
        });
    }

    for (auto& t : threads) t.join();
    EXPECT_EQ(counter.load(), 5);
}

TEST(SharedMutexTest, SharedAccessIsConcurrent) {
    SharedMutex mtx;
    std::atomic<int> active_readers{0};
    std::atomic<int> peak_readers{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&mtx, &active_readers, &peak_readers]() {
            mtx.lock_shared();
            int current = ++active_readers;
            if (current > peak_readers) peak_readers = current;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            --active_readers;
            mtx.unlock_shared();
        });
    }

    for (auto& t : threads) t.join();

    EXPECT_EQ(active_readers.load(), 0);  // All readers have exited
    EXPECT_GE(peak_readers.load(), 2);  // At least two readers ran concurrently
}

TEST(SharedMutexTest, WriterBlocksReaders) {
    SharedMutex mtx;
    std::atomic<bool> reader_started{false};
    std::atomic<bool> writer_proceeded{false};

    std::thread reader([&]() {
        mtx.lock_shared();
        reader_started = true;
        while (!writer_proceeded) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        mtx.unlock_shared();
    });

    // Give reader time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto try_write =
        mtx.try_lock();  // Should fail because reader holds shared lock
    EXPECT_FALSE(try_write);

    // Let reader finish
    writer_proceeded = true;
    reader.join();

    // Now writer should be able to acquire the lock
    mtx.lock();
    EXPECT_TRUE(true);  // No deadlock occurred
    mtx.unlock();
}

TEST(SharedMutexTest, LocksOnConstructionExclusive) {
    SharedMutex mtx;
    {
        SharedMutexLocker locker(&mtx);
        // Lock should still be held before destruction
        EXPECT_FALSE(mtx.try_lock());  // Cannot acquire another exclusive lock
        EXPECT_FALSE(mtx.try_lock_shared());  // Shared lock may also be blocked
                                              // (implementation-defined)
    }                             // Destructor automatically unlocks
    EXPECT_TRUE(mtx.try_lock());  // After destruction, lock should be available
}

TEST(SharedMutexTest, LocksOnConstructionShared) {
    SharedMutex mtx;
    {
        SharedMutexLocker locker(&mtx, shared_lock);
        EXPECT_TRUE(
            mtx.try_lock_shared());  // Multiple shared locks should be allowed
        // Note: This test does not attempt recursive locking (UB), just checks
        // concurrent shared access.
    }
    EXPECT_TRUE(
        mtx.try_lock_shared());  // Should still be available after unlock
}

TEST(SharedMutexTest, ManualLockUnlock) {
    SharedMutex mtx;
    SharedMutexLocker locker(nullptr);  // Initialize without a mutex
    EXPECT_NO_THROW(locker.unlock());  // Unlocking a null locker should be safe

    SharedMutexLocker temp(&mtx);
    temp.unlock();                // Manually release the lock
    EXPECT_TRUE(mtx.try_lock());  // Now we should be able to acquire it
}

TEST(SharedMutexTest, TryLockSuccess) {
    SharedMutex mtx;
    SharedMutexLocker locker(&mtx);
    locker.unlock();  // Ensure it's released first

    bool result = locker.try_lock();
    EXPECT_TRUE(result);
    EXPECT_FALSE(locker.try_lock());  // Should not allow re-locking
}

TEST(SharedMutexTest, TryLockSharedSuccess) {
    SharedMutex mtx;
    SharedMutexLocker locker(&mtx, shared_lock);
    locker.unlock();

    bool result = locker.try_lock_shared();
    EXPECT_TRUE(result);
    EXPECT_FALSE(locker.try_lock_shared());  // Should not allow re-locking
}

TEST(SharedMutexTest, HandlesNullptrSafely) {
    SharedMutexLocker locker(nullptr);
    EXPECT_NO_THROW(locker.lock());
    EXPECT_NO_THROW(locker.lock_shared());
    EXPECT_NO_THROW(locker.try_lock());
    EXPECT_NO_THROW(locker.try_lock_shared());
    EXPECT_NO_THROW(locker.unlock());
    // Should not crash under any operation
}

class SpinLockTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("SpinLockTest");
        FLAGS_logtostderr = true;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST(SpinLockTest, LockUnlockTest) {
    SpinLock lock;

    // The lock is initialized as unlocked.
    EXPECT_FALSE(lock.is_locked());

    // Lock it, should success.
    lock.lock();
    EXPECT_TRUE(lock.is_locked());
    // The lock is locked so try_lock should fail.
    EXPECT_FALSE(lock.try_lock());

    // Unlock the lock.
    lock.unlock();
    EXPECT_FALSE(lock.is_locked());

    // try_lock should success.
    EXPECT_TRUE(lock.try_lock());
    EXPECT_TRUE(lock.is_locked());

    // Start a background thread that is blocked.
    std::atomic<bool> thread_started(false);
    std::atomic<bool> thread_completed(false);
    std::thread blocked_thread([&]() {
        thread_started.store(true);
        lock.lock();
        thread_completed.store(true);
        lock.unlock();
    });

    // Wait for the thread to start.
    while (!thread_started.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Sleep for a while.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // The thread is still blocked.
    EXPECT_FALSE(thread_completed);

    // Unlock.
    lock.unlock();
    blocked_thread.join();

    // The thread is completed.
    EXPECT_TRUE(thread_completed);
    EXPECT_FALSE(lock.is_locked());
}

TEST(SpinLockTest, SpinLockerTest) {
    SpinLock lock;
    // The lock is initialized as unlocked.
    EXPECT_FALSE(lock.is_locked());

    {
        // The lock should be locked in this scope.
        SpinLocker locker(&lock);
        EXPECT_TRUE(lock.is_locked());
    }

    // The lock should be unlocked now.
    EXPECT_FALSE(lock.is_locked());
}

}  // namespace mooncake::test