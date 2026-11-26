#include "mutex.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>
#include <chrono>
#include <stdexcept>

namespace mooncake::test {

TEST(SharedMutexTest, LocksOnConstructionExclusive) {
    SharedMutex mtx;
    {
        SharedMutexLocker locker(&mtx);
        bool exclusive_acquired = false;
        bool shared_acquired = false;
        std::thread contender([&]() {
            exclusive_acquired = mtx.try_lock();
            if (exclusive_acquired) {
                mtx.unlock();
            }
            shared_acquired = mtx.try_lock_shared();
            if (shared_acquired) {
                mtx.unlock_shared();
            }
        });
        contender.join();

        EXPECT_FALSE(exclusive_acquired);
        EXPECT_FALSE(shared_acquired);
    }

    const bool exclusive_acquired = mtx.try_lock();
    EXPECT_TRUE(exclusive_acquired);
    if (exclusive_acquired) {
        mtx.unlock();
    }
}

TEST(SharedMutexTest, LocksOnConstructionShared) {
    SharedMutex mtx;
    {
        SharedMutexLocker locker(&mtx, shared_lock);
        bool shared_acquired = false;
        bool exclusive_acquired = false;
        std::thread contender([&]() {
            shared_acquired = mtx.try_lock_shared();
            if (shared_acquired) {
                mtx.unlock_shared();
            }
            exclusive_acquired = mtx.try_lock();
            if (exclusive_acquired) {
                mtx.unlock();
            }
        });
        contender.join();

        EXPECT_TRUE(shared_acquired);
        EXPECT_FALSE(exclusive_acquired);
    }

    const bool shared_acquired = mtx.try_lock_shared();
    EXPECT_TRUE(shared_acquired);
    if (shared_acquired) {
        mtx.unlock_shared();
    }
}

TEST(SharedMutexTest, ManualLockUnlock) {
    SharedMutex mtx;
    SharedMutexLocker locker(nullptr);  // Initialize without a mutex
    EXPECT_NO_THROW(locker.unlock());  // Unlocking a null locker should be safe

    SharedMutexLocker temp(&mtx);
    temp.unlock();
    const bool acquired = mtx.try_lock();
    EXPECT_TRUE(acquired);
    if (acquired) {
        mtx.unlock();
    }
}

TEST(SharedMutexTest, TryLockSuccess) {
    SharedMutex mtx;
    SharedMutexLocker locker(&mtx);
    locker.unlock();  // Ensure it's released first

    bool result = locker.try_lock();
    EXPECT_TRUE(result);
    EXPECT_FALSE(locker.try_lock());  // Should not allow re-locking
    locker.unlock();
}

TEST(SharedMutexTest, TryLockSharedSuccess) {
    SharedMutex mtx;
    SharedMutexLocker locker(&mtx, shared_lock);
    locker.unlock();

    bool result = locker.try_lock_shared();
    EXPECT_TRUE(result);
    EXPECT_FALSE(locker.try_lock_shared());  // Should not allow re-locking
    locker.unlock();
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
