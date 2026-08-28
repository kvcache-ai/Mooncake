#pragma once

#include <boost/functional/hash.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "segment_admission_config.h"
#include "types.h"

namespace mooncake {

enum class SegmentAdmissionState : uint8_t {
    RAMPING = 0,
    ACTIVE = 1,
    QUARANTINED = 2,
};

std::string_view ToString(SegmentAdmissionState state) noexcept;
int64_t SegmentAdmissionStateMetricValue(SegmentAdmissionState state) noexcept;

enum class SegmentAdmissionRejectReason : uint8_t {
    NONE = 0,
    QUARANTINED,
    INFLIGHT_OP_LIMIT,
    INFLIGHT_BYTE_LIMIT,
};

std::string_view ToString(SegmentAdmissionRejectReason reason) noexcept;

class SegmentAdmissionClock {
   public:
    using TimePoint = std::chrono::steady_clock::time_point;

    virtual ~SegmentAdmissionClock() = default;
    virtual TimePoint Now() const = 0;
};

class SteadySegmentAdmissionClock final : public SegmentAdmissionClock {
   public:
    TimePoint Now() const override { return std::chrono::steady_clock::now(); }
};

struct SegmentAdmissionSnapshot {
    UUID segment_id{0, 0};
    UUID owner_client_id{0, 0};
    std::string segment_name;
    std::string segment_host_id;
    SegmentAdmissionState state{SegmentAdmissionState::RAMPING};
    double effective_ratio{0.0};
    uint64_t inflight_remote_write_ops{0};
    uint64_t inflight_remote_write_bytes{0};
    uint64_t successful_remote_writes_in_ramp{0};
    uint64_t recent_failures{0};
    uint64_t observed_remote_writes{0};
    uint64_t observed_would_reject{0};
    int64_t quarantine_remaining_ms{0};
    int64_t owner_heartbeat_age_ms{0};
};

struct SegmentAdmissionObservation {
    bool would_admit{true};
    SegmentAdmissionRejectReason reason{SegmentAdmissionRejectReason::NONE};
    SegmentAdmissionSnapshot snapshot;
};

// Owns the volatile admission state of mounted memory segments. PR1 only
// observes decisions: callers may inspect would_admit, but must not use it to
// alter placement until the reservation/result-reporting path is introduced.
class SegmentAdmissionController {
   public:
    explicit SegmentAdmissionController(
        SegmentAdmissionConfig config = {},
        std::shared_ptr<SegmentAdmissionClock> clock =
            std::make_shared<SteadySegmentAdmissionClock>());

    SegmentWriteAdmissionMode mode() const noexcept { return config_.mode; }
    const SegmentAdmissionConfig& config() const noexcept { return config_; }

    std::optional<SegmentAdmissionSnapshot> OnMount(
        const Segment& segment, const UUID& owner_client_id,
        bool restored_active = false);
    std::vector<SegmentAdmissionSnapshot> OnOwnerHeartbeat(
        const UUID& owner_client_id);
    std::optional<SegmentAdmissionSnapshot> OnUnmount(const UUID& segment_id);

    // These result hooks are intentionally controller-local in PR1. They make
    // the state machine testable and are wired to client reports in a later PR.
    std::optional<SegmentAdmissionSnapshot> RecordRemoteWriteSuccess(
        const UUID& segment_id);
    std::optional<SegmentAdmissionSnapshot> RecordRemoteWriteFailure(
        const UUID& segment_id);

    SegmentAdmissionObservation ObserveRemoteWrite(
        const std::string& segment_name, const std::string& writer_host_id,
        uint64_t bytes);

    std::optional<SegmentAdmissionSnapshot> GetSnapshot(const UUID& segment_id);
    std::vector<SegmentAdmissionSnapshot> GetSnapshots();
    size_t size() const;

   private:
    struct Runtime {
        UUID segment_id{0, 0};
        UUID owner_client_id{0, 0};
        std::string segment_name;
        std::string segment_host_id;
        uint64_t segment_capacity_bytes{0};
        SegmentAdmissionState state{SegmentAdmissionState::RAMPING};
        SegmentAdmissionClock::TimePoint state_since{};
        SegmentAdmissionClock::TimePoint quarantine_until{};
        SegmentAdmissionClock::TimePoint last_owner_heartbeat{};
        uint64_t inflight_remote_write_ops{0};
        uint64_t inflight_remote_write_bytes{0};
        uint64_t successful_remote_writes_in_ramp{0};
        std::deque<SegmentAdmissionClock::TimePoint> recent_failures;
        uint64_t observed_remote_writes{0};
        uint64_t observed_would_reject{0};
    };

    double EffectiveRatioLocked(
        const Runtime& runtime,
        SegmentAdmissionClock::TimePoint now) const noexcept;
    uint64_t FullByteLimitLocked(const Runtime& runtime) const noexcept;
    uint64_t EffectiveByteLimitLocked(const Runtime& runtime,
                                      double ratio) const noexcept;
    uint64_t EffectiveOpLimitLocked(double ratio) const noexcept;
    void AdvanceStateLocked(Runtime& runtime,
                            SegmentAdmissionClock::TimePoint now);
    void PruneFailuresLocked(Runtime& runtime,
                             SegmentAdmissionClock::TimePoint now) const;
    SegmentAdmissionSnapshot SnapshotLocked(
        const Runtime& runtime, SegmentAdmissionClock::TimePoint now) const;
    void LogTransitionLocked(const Runtime& runtime,
                             SegmentAdmissionState old_state,
                             std::string_view reason,
                             SegmentAdmissionClock::TimePoint now) const;

    SegmentAdmissionConfig config_;
    std::shared_ptr<SegmentAdmissionClock> clock_;
    mutable std::mutex mutex_;
    std::unordered_map<UUID, Runtime, boost::hash<UUID>> runtimes_;
    std::unordered_map<std::string, UUID> segment_name_index_;
    std::unordered_map<UUID, std::unordered_set<UUID, boost::hash<UUID>>,
                       boost::hash<UUID>>
        owner_segments_;
};

}  // namespace mooncake
