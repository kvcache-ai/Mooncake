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
    // Extend the deadline (monotonic max) to now + ttl. The read path
    // advertises the full ttl to clients, so the server must honor now + ttl
    // rather than keeping an earlier existing deadline (which would let the
    // server evict before the client-held lease expires).
    void GrantReadLease(std::chrono::milliseconds ttl) {
        const auto ttl_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(ttl).count();
        const int64_t now = NowNs();
        const int64_t new_deadline = now + ttl_ns;
        int64_t cur = deadline_ns_.load(std::memory_order_relaxed);
        while (cur < new_deadline &&
               !deadline_ns_.compare_exchange_weak(cur, new_deadline,
                                                   std::memory_order_relaxed)) {
        }
        // A read or write makes the object recently used, so it ranks by its
        // new deadline from now on, not by the order it was restored with.
        restored_order_ns_.store(0, std::memory_order_relaxed);
    }

    // Like GrantReadLease, but keeps the restored eviction order: remount
    // protects a restored object for ttl without making it look recently
    // used.
    void GrantRestoreLease(std::chrono::milliseconds ttl) {
        const int64_t new_deadline =
            NowNs() +
            std::chrono::duration_cast<std::chrono::nanoseconds>(ttl).count();
        int64_t cur = deadline_ns_.load(std::memory_order_relaxed);
        while (cur < new_deadline &&
               !deadline_ns_.compare_exchange_weak(cur, new_deadline,
                                                   std::memory_order_relaxed)) {
        }
    }

    // Record the eviction order an object had on the primary it was restored
    // from (monotonic max, so a group takes its latest member's order).
    void SetRestoredOrder(const std::chrono::system_clock::time_point order) {
        const int64_t o = NowNs(order);
        int64_t cur = restored_order_ns_.load(std::memory_order_relaxed);
        while (cur < o && !restored_order_ns_.compare_exchange_weak(
                              cur, o, std::memory_order_relaxed)) {
        }
    }

    // The time eviction ranks by: the restored order until the object is
    // read or written again, otherwise the deadline.
    std::chrono::system_clock::time_point EvictionOrder() const {
        const int64_t restored =
            restored_order_ns_.load(std::memory_order_relaxed);
        return restored != 0 ? FromNs(restored) : ExpiresAt();
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
        // system_clock is coarser than nanoseconds on some platforms (e.g.
        // microseconds with libc++), so the conversion must be explicit.
        return FromNs(deadline_ns_.load(std::memory_order_relaxed));
    }

   private:
    static std::chrono::system_clock::time_point FromNs(int64_t ns) {
        return std::chrono::system_clock::time_point(
            std::chrono::duration_cast<std::chrono::system_clock::duration>(
                std::chrono::nanoseconds(ns)));
    }
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
    // 0 unless the object was restored from a standby and not touched since.
    std::atomic<int64_t> restored_order_ns_{0};
};

}  // namespace mooncake
