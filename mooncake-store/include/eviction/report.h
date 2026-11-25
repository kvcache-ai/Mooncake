#pragma once

// What one eviction run did, object by object (Effect) and in total (Report).

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {
namespace eviction {

// What reclaiming an object's memory came to.
enum class Outcome : uint8_t {
    // The object lost its last valid replica and goes.
    kDropped,
    // The object lost memory and lives on through its other replicas.
    kDemoted,
    // One memory replica was queued for offload to local disk and is freed by
    // a later run once the copy lands; the rest were released now.
    kOffloadQueued,
    // Nothing was released: the object waits for a later run.
    kKept,
};
inline constexpr size_t kOutcomeCount = 4;

// Why an object went without the offload that was due for it.
enum class DropReason : uint8_t {
    kNone,
    // The run had queued as many offloads as it may.
    kOffloadCap,
    // The offload could not be queued.
    kOffloadRejected,
};

// Why an eviction run stopped scanning before meeting its goal.
enum class StopReason : uint8_t {
    kNone,
    // Reclaimer::Apply failed (e.g., OpLog write or provider delete error);
    // Report::stop_error carries the concrete ErrorCode.
    kReclaimError,
    // Non-forced offload reached its per-run cap.
    kOffloadCap,
    // Non-forced offload hit consecutive queue rejections.
    kOffloadSaturated,
};

inline const char* ToString(StopReason reason) noexcept {
    switch (reason) {
        case StopReason::kNone:
            return "none";
        case StopReason::kReclaimError:
            return "reclaim_error";
        case StopReason::kOffloadCap:
            return "offload_cap";
        case StopReason::kOffloadSaturated:
            return "offload_saturated";
    }
    return "unknown";
}

// What reclaiming one object did. Move-only: it carries the replicas taken off
// the object, which must outlive the object's lock.
struct Effect {
    Outcome outcome = Outcome::kKept;
    DropReason reason = DropReason::kNone;
    // Memory taken off the object: freed now, or once the OpLog entry that
    // records it is durable.
    uint64_t released_bytes = 0;
    // Memory pinned for an offload, freed by a later run.
    uint64_t offload_bytes = 0;
    // The object holds nothing valid any more and must be torn down.
    bool needs_teardown = false;
    // Non-kNone stops scanning more candidates in this run without reporting a
    // reclaim error (e.g., non-forced offload hit cap or saturated queues).
    StopReason stop_reason = StopReason::kNone;
    // The replicas taken off the object, freed when this is destroyed.
    std::vector<Replica> detached;

    Effect() = default;
    Effect(Effect&&) = default;
    Effect& operator=(Effect&&) = default;
    Effect(const Effect&) = delete;
    Effect& operator=(const Effect&) = delete;
};

// What a run did in total.
struct Report {
    // Keys the eviction indexes had filed when the run began.
    uint64_t filed = 0;
    // Objects that released memory, and the memory they released.
    uint64_t evicted_objects = 0;
    uint64_t released_bytes = 0;
    // Memory pinned for offloads, freed by a later run.
    uint64_t offload_bytes = 0;
    std::array<uint64_t, kOutcomeCount> outcomes{};
    // Objects dropped without the offload due for them, by DropReason.
    uint64_t offload_cap_drops = 0;
    uint64_t offload_rejected_drops = 0;
    // Non-kNone if the run stopped early before meeting its goal.
    StopReason stop_reason = StopReason::kNone;
    // Concrete ErrorCode from Reclaimer::Apply when stop_reason ==
    // StopReason::kReclaimError.
    ErrorCode stop_error = ErrorCode::OK;
    uint64_t visited = 0;
    uint64_t moved = 0;
    uint64_t busy =
        0;  // Busy or concurrently removed before key-lock admission.
    uint64_t lease_active = 0;
    uint64_t scanned_slots = 0;
    uint64_t scan_waves = 0;
    uint32_t workers = 1;  // Largest worker count used by this run.

    [[nodiscard]] uint64_t Count(Outcome outcome) const {
        return outcomes[static_cast<size_t>(outcome)];
    }

    // Something was released or queued for offload.
    [[nodiscard]] bool MadeProgress() const {
        return evicted_objects > 0 || Count(Outcome::kOffloadQueued) > 0;
    }

    [[nodiscard]] bool Stopped() const noexcept {
        return stop_reason != StopReason::kNone;
    }

    [[nodiscard]] bool StoppedByError() const noexcept {
        return stop_reason == StopReason::kReclaimError;
    }

    void RecordStop(StopReason reason,
                    ErrorCode error = ErrorCode::OK) noexcept {
        if (reason == StopReason::kNone) {
            return;
        }
        if (reason == StopReason::kReclaimError) {
            if (stop_reason != StopReason::kReclaimError) {
                stop_reason = StopReason::kReclaimError;
                stop_error = error;
            }
            return;
        }
        if (stop_reason == StopReason::kNone) {
            stop_reason = reason;
        }
    }

    void Merge(const Report& other) {
        evicted_objects += other.evicted_objects;
        released_bytes += other.released_bytes;
        offload_bytes += other.offload_bytes;
        offload_cap_drops += other.offload_cap_drops;
        offload_rejected_drops += other.offload_rejected_drops;
        RecordStop(other.stop_reason, other.stop_error);
        visited += other.visited;
        moved += other.moved;
        busy += other.busy;
        lease_active += other.lease_active;
        scanned_slots += other.scanned_slots;
        scan_waves += other.scan_waves;
        workers = std::max(workers, other.workers);
        for (size_t i = 0; i < outcomes.size(); ++i)
            outcomes[i] += other.outcomes[i];
    }

    void Add(const Effect& effect) {
        ++outcomes[static_cast<size_t>(effect.outcome)];
        released_bytes += effect.released_bytes;
        offload_bytes += effect.offload_bytes;
        if (effect.released_bytes > 0) {
            ++evicted_objects;
        }
        if (effect.reason == DropReason::kOffloadCap) {
            ++offload_cap_drops;
        } else if (effect.reason == DropReason::kOffloadRejected) {
            ++offload_rejected_drops;
        }
        RecordStop(effect.stop_reason);
    }
};

}  // namespace eviction
}  // namespace mooncake
