#ifndef THREAD_SAFETY_ANALYSIS_MUTEX_H
#define THREAD_SAFETY_ANALYSIS_MUTEX_H

#include <mutex>
#include <shared_mutex>

// Enable thread safety attributes only with clang.
// The attributes can be safely erased when compiling with other compilers.
#if defined(__clang__) && (!defined(SWIG))
#define THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#else
#define THREAD_ANNOTATION_ATTRIBUTE__(x)  // no-op
#endif

#define CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(capability(x))

#define SCOPED_CAPABILITY THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

#define GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

#define PT_GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

#define ACQUIRED_BEFORE(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

#define ACQUIRED_AFTER(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))

#define REQUIRES(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(requires_capability(__VA_ARGS__))

#define ACQUIRE(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquire_capability(__VA_ARGS__))

#define ACQUIRE_SHARED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(acquire_shared_capability(__VA_ARGS__))

#define RELEASE(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(release_capability(__VA_ARGS__))

#define RELEASE_SHARED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(release_shared_capability(__VA_ARGS__))

#define TRY_ACQUIRE(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_capability(__VA_ARGS__))

#define TRY_ACQUIRE_SHARED(...) \
    THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_shared_capability(__VA_ARGS__))

#define EXCLUDES(...) THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define ASSERT_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(assert_capability(x))

#define RETURN_CAPABILITY(x) THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

#define NO_THREAD_SAFETY_ANALYSIS \
    THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

// Simple mutex implementation using std::mutex for exclusive locking only.
class CAPABILITY("mutex") Mutex {
   private:
    std::mutex mutex_;

   public:
    // Acquire/lock this mutex exclusively.
    void lock() ACQUIRE() { mutex_.lock(); }

    // Release/unlock the mutex.
    void unlock() RELEASE() { mutex_.unlock(); }

    // Try to acquire the mutex. Returns true on success, and false on failure.
    bool try_lock() TRY_ACQUIRE(true) { return mutex_.try_lock(); }

    // For negative capabilities.
    const Mutex& operator!() const { return *this; }
};

// Simple shared_mutex implementation using std::shared_mutex for exclusive
// locking only.
class CAPABILITY("shared_mutex") SharedMutex {
   private:
    std::shared_mutex mutex_;

   public:
    // Acquire/lock this mutex exclusively.
    void lock() ACQUIRE() { mutex_.lock(); }

    // Acquire/lock this mutex shared.
    void lock_shared() ACQUIRE_SHARED() { mutex_.lock_shared(); }

    // Release/unlock the mutex.
    void unlock() RELEASE() { mutex_.unlock(); }

    // Release/unlock a shared mutex.
    void unlock_shared() RELEASE_SHARED() { mutex_.unlock_shared(); }

    // Try to acquire the mutex. Returns true on success, and false on failure.
    bool try_lock() TRY_ACQUIRE(true) { return mutex_.try_lock(); }

    // Try to acquire the mutex for read operations.
    bool try_lock_shared() TRY_ACQUIRE_SHARED(true) {
        return mutex_.try_lock_shared();
    }

    // For negative capabilities.
    const SharedMutex& operator!() const { return *this; }
};

// MutexLocker is an RAII class that acquires a mutex in its constructor, and
// releases it in its destructor.
class SCOPED_CAPABILITY MutexLocker {
   private:
    Mutex* mut;
    bool locked;

   public:
    // Acquire mu, implicitly acquire *this and associate it with mu.
    MutexLocker(Mutex* mu) ACQUIRE(mu) : mut(mu), locked(true) { mu->lock(); }

    // Release *this and all associated mutexes, if they are still held.
    ~MutexLocker() RELEASE() {
        if (locked) {
            mut->unlock();
        }
    }

    // Acquire the mutex exclusively.
    void lock() ACQUIRE() {
        mut->lock();
        locked = true;
    }

    // Try to acquire the mutex exclusively.
    bool TryLock() TRY_ACQUIRE(true) { return locked = mut->try_lock(); }

    // Release the mutex.
    void unlock() RELEASE() {
        mut->unlock();
        locked = false;
    }
};

// Tag types for selecting a constructor.
struct shared_lock_t {
} inline constexpr shared_lock = {};

// SharedMutexLocker is an RAII class that acquires a shared mutex in its
// constructor, and releases it in its destructor.
class SCOPED_CAPABILITY SharedMutexLocker {
   private:
    SharedMutex* mut;
    bool is_exclusive;  // true if holding the lock in exclusive mode
    bool locked;

   public:
    // Constructor: Acquire the mutex in exclusive mode
    explicit SharedMutexLocker(SharedMutex* mu) ACQUIRE(mu)
        : mut(mu), is_exclusive(true), locked(true) {
        if (mut) {
            mut->lock();
        }
    }

    // Constructor: Acquire the mutex in shared mode
    SharedMutexLocker(SharedMutex* mu, const shared_lock_t&) ACQUIRE_SHARED(mu)
        : mut(mu), is_exclusive(false), locked(true) {
        if (mut) {
            mut->lock_shared();
        }
    }

    // Destructor: Automatically release the mutex
    ~SharedMutexLocker() RELEASE() {
        if (locked && mut) {
            if (is_exclusive) {
                mut->unlock();
            } else {
                mut->unlock_shared();
            }
        }
    }

    // Prevent copying and assignment
    SharedMutexLocker(const SharedMutexLocker&) = delete;
    SharedMutexLocker& operator=(const SharedMutexLocker&) = delete;

    // Acquire the mutex in exclusive mode
    void lock() ACQUIRE() {
        if (!mut || locked) return;
        mut->lock();
        is_exclusive = true;
        locked = true;
    }

    // Acquire the mutex in shared mode
    void lock_shared() ACQUIRE_SHARED() {
        if (!mut || locked) return;
        mut->lock_shared();
        is_exclusive = false;
        locked = true;
    }

    // Try to acquire the mutex in exclusive mode; returns true on success
    bool try_lock() TRY_ACQUIRE(true) {
        if (!mut || locked) return false;
        locked = mut->try_lock();
        if (locked) is_exclusive = true;
        return locked;
    }

    // Try to acquire the mutex in shared mode; returns true on success
    bool try_lock_shared() TRY_ACQUIRE_SHARED(true) {
        if (!mut || locked) return false;
        locked = mut->try_lock_shared();
        if (locked) is_exclusive = false;
        return locked;
    }

    // Release the mutex according to the current mode (exclusive or shared)
    void unlock() RELEASE() {
        if (!locked || !mut) return;
        if (is_exclusive) {
            mut->unlock();
        } else {
            mut->unlock_shared();
        }
        locked = false;
    }
};

#endif  // THREAD_SAFETY_ANALYSIS_MUTEX_H
