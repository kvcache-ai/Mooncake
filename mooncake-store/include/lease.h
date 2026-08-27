// lease.h
//
// Lock-free shared deadline for a group: every member holds a shared_ptr to the
// same Lease (CAS monotonic-max), so a read of any member extends one TTL.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace mooncake {

class Lease {
   public:
    // Unless the deadline is already over half a TTL out, extend it (max) to
    // now + ttl. The early-out keeps hot-group reads lock-free.
    void GrantReadLease(std::chrono::milliseconds ttl) {
        const auto ttl_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(ttl).count();
        const int64_t now = NowNs();
        const int64_t half_ttl_ns = ttl_ns / 2;
        int64_t cur = deadline_ns_.load(std::memory_order_relaxed);
        if (cur > now + half_ttl_ns) {
            return;  // Far enough; no contention.
        }
        const int64_t new_deadline = now + ttl_ns;
        while (cur < new_deadline &&
               !deadline_ns_.compare_exchange_weak(cur, new_deadline,
                                                   std::memory_order_relaxed)) {
        }
    }

    // Merge an externally-derived deadline (used when rebuilding a group's
    // lease from a snapshot so the group TTL is the max of member deadlines).
    void ExtendTo(const std::chrono::system_clock::time_point deadline) {
        const int64_t d = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              deadline.time_since_epoch())
                              .count();
        int64_t cur = deadline_ns_.load(std::memory_order_relaxed);
        while (cur < d && !deadline_ns_.compare_exchange_weak(
                              cur, d, std::memory_order_relaxed)) {
        }
    }

    // Overwrite the deadline with an exact value (may lower it). Unlike
    // GrantReadLease/ExtendTo (monotonic max), this lets test scaffolding
    // (e.g. the scenario DSL) force a lease to a precise expiry.
    void SetDeadline(const std::chrono::system_clock::time_point deadline) {
        deadline_ns_.store(NowNs(deadline), std::memory_order_relaxed);
    }

    // True when the group has not been read within the TTL at `now`.
    bool IsExpired(const std::chrono::system_clock::time_point now) const {
        return NowNs(now) >= deadline_ns_.load(std::memory_order_relaxed);
    }

    std::chrono::system_clock::time_point ExpiresAt() const {
        return std::chrono::system_clock::time_point(std::chrono::nanoseconds(
            deadline_ns_.load(std::memory_order_relaxed)));
    }

   private:
    static int64_t NowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }
    static int64_t NowNs(std::chrono::system_clock::time_point t) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   t.time_since_epoch())
            .count();
    }

    std::atomic<int64_t> deadline_ns_{0};
};

}  // namespace mooncake
