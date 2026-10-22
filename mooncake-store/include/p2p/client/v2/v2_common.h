#pragma once

// Small pieces shared by every V2 component: the injected clock, the lifecycle
// gate, and the allocation-source / allocation-failure vocabulary.
//
// Note on file layout: remake_kvbm/new_data_manager.md section 8 lists the V2
// headers by component. These types are used by several of them (Clock by the
// lease manager, the movement queue and AllocateWithPolicy; LifecycleGate by
// DataManagerV2 and the task handles it hands out), so putting them in any one
// component header would create a cycle. They live here instead.

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake::v2 {

/**
 * @enum AllocationSource
 * @brief Why a block is being allocated. It selects the candidate tiler set:
 *        every request-path source is restricted to TE-addressable tilers;
 *        kMigration is the only source allowed to target a slow tier.
 */
enum class AllocationSource : uint8_t {
    kPut,
    kPreWrite,
    kWriteRemoteData,
    kMigration,
    kOnboard,
    kCount,
};

const char* ToString(AllocationSource source);

/**
 * @struct AllocationFailurePolicyConfig
 * @brief What AllocateWithPolicy does after BlockPool::Allocate reports
 *        NO_AVAILABLE_HANDLE. Defaults reproduce V1's behaviour: at most one
 *        synchronous reclaim round, then one retry.
 */
struct AllocationFailurePolicyConfig {
    bool try_evict = true;
    uint32_t max_evict_rounds = 1;
    std::chrono::milliseconds evict_timeout{50};
    size_t reclaim_margin_bytes = 0;
};

/** Validates the ranges documented in section 5.1. */
tl::expected<void, ErrorCode> ValidateAllocationFailurePolicy(
    const AllocationFailurePolicyConfig& config);

/**
 * @struct AllocationFailureCounters
 * @brief Per (tiler, source) counters for the allocation-failure path. The
 *        five outcomes are mutually exclusive per failure, so together they
 *        say exactly what the policy did.
 */
struct AllocationFailureCounters {
    std::atomic<uint64_t> failures{0};
    std::atomic<uint64_t> evict_disabled{0};
    std::atomic<uint64_t> evict_attempted{0};
    std::atomic<uint64_t> retry_succeeded{0};
    std::atomic<uint64_t> retry_failed{0};
    std::atomic<uint64_t> evict_timed_out{0};
};

/**
 * @class AllocationFailureMetrics
 * @brief Fixed after construction, so the request path only ever does a const
 *        lookup and an atomic increment -- no allocation, no rehash, no lock.
 */
class AllocationFailureMetrics {
   public:
    explicit AllocationFailureMetrics(const std::vector<UUID>& tiler_ids);

    /**
     * @brief Counters for a (tiler, source) pair.
     *
     * An unknown tiler returns a shared fallback slot and logs once, rather
     * than inserting: inserting would break the "fixed after construction"
     * property that makes this lock-free.
     */
    AllocationFailureCounters& For(const UUID& tiler_id,
                                   AllocationSource source);

   private:
    std::unordered_map<
        UUID,
        std::array<AllocationFailureCounters,
                   static_cast<size_t>(AllocationSource::kCount)>,
        boost::hash<UUID>>
        counters_;
    std::array<AllocationFailureCounters,
               static_cast<size_t>(AllocationSource::kCount)>
        unknown_;
    std::atomic<bool> warned_unknown_{false};
};

/**
 * @class Clock
 * @brief Injected time source. Every V2 deadline (lease expiry, movement
 *        deadline, synchronous-reclaim deadline) reads it, so a test can make
 *        timeout behaviour deterministic instead of sleeping.
 */
class Clock {
   public:
    using time_point = std::chrono::steady_clock::time_point;
    virtual ~Clock() = default;
    virtual time_point Now() const = 0;
};

class SteadyClock final : public Clock {
   public:
    time_point Now() const override { return std::chrono::steady_clock::now(); }
};

/**
 * @enum LifecycleState
 * @brief Running -> Stopping -> Stopped -> Destroyed. Monotonic.
 */
enum class LifecycleState : uint8_t {
    kRunning,
    kStopping,
    kStopped,
    kDestroyed,
};

const char* ToString(LifecycleState state);

/**
 * @class LifecycleGate
 * @brief Admission control plus an in-flight counter for the public API.
 *
 * A Guard is move-only and may be moved into a TaskHandle: Put/Get do their
 * copy and commit inside TaskHandle::Wait(), on the caller's thread, so the
 * guard has to follow the handle rather than the API call stack. Dropping the
 * handle without ever calling Wait() releases the guard, which is why Stop()
 * can bound its wait: after `stop_drain_timeout` it switches to Cancel mode,
 * every outstanding Guard reports IsUsable() == false, and their holders must
 * fail with SHUTTING_DOWN without touching any tiler.
 */
class LifecycleGate : public std::enable_shared_from_this<LifecycleGate> {
   public:
    /**
     * @class Guard
     * @brief Move-only admission ticket.
     *
     * It keeps the gate alive by reference count rather than pointing at it.
     * That is not defensive style: Stop() bounds its wait and then cancels, so
     * a caller is explicitly allowed to still hold a TaskHandle -- and its
     * Guard -- when the DataManager is destroyed. A raw back-pointer would
     * then be dereferenced by ~Guard on freed memory.
     */
    class Guard {
       public:
        Guard() = default;
        Guard(Guard&& other) noexcept;
        Guard& operator=(Guard&& other) noexcept;
        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;
        ~Guard();

        /** False once Stop() gave up waiting and switched to Cancel mode. */
        bool IsUsable() const;
        explicit operator bool() const { return owner_ != nullptr; }

       private:
        friend class LifecycleGate;
        explicit Guard(std::shared_ptr<LifecycleGate> owner)
            : owner_(std::move(owner)) {}
        std::shared_ptr<LifecycleGate> owner_;
    };

    /** SHUTTING_DOWN once Stop() has begun. */
    tl::expected<Guard, ErrorCode> Acquire();

    /** @return false if a Stop was already in progress. */
    bool BeginStop();

    /** @return false on timeout; the caller should then BeginCancel(). */
    bool WaitForNoInflight(std::chrono::milliseconds timeout);

    void BeginCancel();
    bool IsCancelled() const;
    void MarkStopped();

    /** @return false if Destroy already ran (so callbacks fire only once). */
    bool MarkDestroyed();

    LifecycleState State() const;
    uint64_t InflightForTest() const;

   private:
    friend class Guard;
    void Release();

    std::atomic<LifecycleState> state_{LifecycleState::kRunning};
    std::atomic<bool> cancelled_{false};
    std::atomic<uint64_t> inflight_{0};
    mutable std::mutex wait_mu_;
    std::condition_variable wait_cv_;
};

}  // namespace mooncake::v2
