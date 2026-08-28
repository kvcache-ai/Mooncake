#include "segment_admission_controller.h"

#include <glog/logging.h>
#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>

namespace mooncake {
namespace {

constexpr uint64_t kMiB = 1024ULL * 1024ULL;
constexpr uint64_t kMinAutoByteLimit = 64ULL * kMiB;
constexpr uint64_t kMaxAutoByteLimit = 1024ULL * kMiB;

uint64_t ScaleLimit(uint64_t limit, double ratio) noexcept {
    if (limit == 0) {
        return 0;
    }
    return std::max<uint64_t>(1, static_cast<uint64_t>(std::floor(
                                     static_cast<long double>(limit) * ratio)));
}

}  // namespace

std::string_view ToString(SegmentAdmissionState state) noexcept {
    switch (state) {
        case SegmentAdmissionState::RAMPING:
            return "RAMPING";
        case SegmentAdmissionState::ACTIVE:
            return "ACTIVE";
        case SegmentAdmissionState::QUARANTINED:
            return "QUARANTINED";
    }
    return "UNKNOWN";
}

int64_t SegmentAdmissionStateMetricValue(SegmentAdmissionState state) noexcept {
    return static_cast<int64_t>(state);
}

std::string_view ToString(SegmentAdmissionRejectReason reason) noexcept {
    switch (reason) {
        case SegmentAdmissionRejectReason::NONE:
            return "none";
        case SegmentAdmissionRejectReason::QUARANTINED:
            return "quarantined";
        case SegmentAdmissionRejectReason::INFLIGHT_OP_LIMIT:
            return "inflight_op_limit";
        case SegmentAdmissionRejectReason::INFLIGHT_BYTE_LIMIT:
            return "inflight_byte_limit";
    }
    return "unknown";
}

SegmentAdmissionController::SegmentAdmissionController(
    SegmentAdmissionConfig config, std::shared_ptr<SegmentAdmissionClock> clock)
    : config_(std::move(config)), clock_(std::move(clock)) {
    config_.Validate();
    if (!clock_) {
        throw std::invalid_argument("segment admission clock must not be null");
    }
}

std::optional<SegmentAdmissionSnapshot> SegmentAdmissionController::OnMount(
    const Segment& segment, const UUID& owner_client_id, bool restored_active) {
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return std::nullopt;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = clock_->Now();
    auto existing = runtimes_.find(segment.id);
    if (existing != runtimes_.end()) {
        auto& runtime = existing->second;
        if (runtime.owner_client_id != owner_client_id) {
            auto old_owner = owner_segments_.find(runtime.owner_client_id);
            if (old_owner != owner_segments_.end()) {
                old_owner->second.erase(segment.id);
                if (old_owner->second.empty()) {
                    owner_segments_.erase(old_owner);
                }
            }
            runtime.owner_client_id = owner_client_id;
            owner_segments_[owner_client_id].insert(segment.id);
        }
        runtime.segment_host_id = segment.host_id;
        runtime.segment_capacity_bytes = segment.size;
        runtime.last_owner_heartbeat = now;
        AdvanceStateLocked(runtime, now);
        return SnapshotLocked(runtime, now);
    }

    const bool first_segment = runtimes_.empty();
    Runtime runtime;
    runtime.segment_id = segment.id;
    runtime.owner_client_id = owner_client_id;
    runtime.segment_name = segment.name;
    runtime.segment_host_id = segment.host_id;
    runtime.segment_capacity_bytes = segment.size;
    runtime.state = (first_segment || restored_active)
                        ? SegmentAdmissionState::ACTIVE
                        : SegmentAdmissionState::RAMPING;
    runtime.state_since = now;
    runtime.last_owner_heartbeat = now;

    auto stale_name = segment_name_index_.find(segment.name);
    if (stale_name != segment_name_index_.end() &&
        stale_name->second != segment.id) {
        LOG(WARNING) << "segment_name=" << segment.name
                     << ", old_segment_id=" << stale_name->second
                     << ", new_segment_id=" << segment.id
                     << ", action=replace_stale_admission_name_index";
    }
    segment_name_index_[segment.name] = segment.id;
    owner_segments_[owner_client_id].insert(segment.id);
    auto [it, inserted] = runtimes_.emplace(segment.id, std::move(runtime));
    (void)inserted;
    LOG(INFO) << "segment_name=" << it->second.segment_name
              << ", segment_id=" << it->second.segment_id
              << ", owner_client_id=" << owner_client_id
              << ", admission_state=" << ToString(it->second.state)
              << ", action=segment_admission_mount";
    return SnapshotLocked(it->second, now);
}

std::vector<SegmentAdmissionSnapshot>
SegmentAdmissionController::OnOwnerHeartbeat(const UUID& owner_client_id) {
    std::vector<SegmentAdmissionSnapshot> snapshots;
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return snapshots;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = clock_->Now();
    auto owner = owner_segments_.find(owner_client_id);
    if (owner == owner_segments_.end()) {
        return snapshots;
    }
    snapshots.reserve(owner->second.size());
    for (const auto& segment_id : owner->second) {
        auto runtime = runtimes_.find(segment_id);
        if (runtime == runtimes_.end()) {
            continue;
        }
        runtime->second.last_owner_heartbeat = now;
        AdvanceStateLocked(runtime->second, now);
        snapshots.push_back(SnapshotLocked(runtime->second, now));
    }
    return snapshots;
}

std::optional<SegmentAdmissionSnapshot> SegmentAdmissionController::OnUnmount(
    const UUID& segment_id) {
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return std::nullopt;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto runtime = runtimes_.find(segment_id);
    if (runtime == runtimes_.end()) {
        return std::nullopt;
    }
    const auto now = clock_->Now();
    auto snapshot = SnapshotLocked(runtime->second, now);
    auto name_index = segment_name_index_.find(runtime->second.segment_name);
    if (name_index != segment_name_index_.end() &&
        name_index->second == segment_id) {
        segment_name_index_.erase(name_index);
    }
    auto owner = owner_segments_.find(runtime->second.owner_client_id);
    if (owner != owner_segments_.end()) {
        owner->second.erase(segment_id);
        if (owner->second.empty()) {
            owner_segments_.erase(owner);
        }
    }
    LOG(INFO) << "segment_name=" << runtime->second.segment_name
              << ", segment_id=" << segment_id
              << ", admission_state=" << ToString(runtime->second.state)
              << ", action=segment_admission_unmount";
    runtimes_.erase(runtime);
    return snapshot;
}

std::optional<SegmentAdmissionSnapshot>
SegmentAdmissionController::RecordRemoteWriteSuccess(const UUID& segment_id) {
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto runtime = runtimes_.find(segment_id);
    if (runtime == runtimes_.end()) {
        return std::nullopt;
    }
    const auto now = clock_->Now();
    if (runtime->second.state == SegmentAdmissionState::RAMPING) {
        ++runtime->second.successful_remote_writes_in_ramp;
    }
    AdvanceStateLocked(runtime->second, now);
    return SnapshotLocked(runtime->second, now);
}

std::optional<SegmentAdmissionSnapshot>
SegmentAdmissionController::RecordRemoteWriteFailure(const UUID& segment_id) {
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto runtime = runtimes_.find(segment_id);
    if (runtime == runtimes_.end()) {
        return std::nullopt;
    }
    const auto now = clock_->Now();
    auto& value = runtime->second;
    value.recent_failures.push_back(now);
    PruneFailuresLocked(value, now);
    if (value.state != SegmentAdmissionState::QUARANTINED &&
        value.recent_failures.size() >= config_.failure_threshold) {
        const auto old_state = value.state;
        value.state = SegmentAdmissionState::QUARANTINED;
        value.state_since = now;
        value.quarantine_until =
            now + std::chrono::seconds(config_.quarantine_duration_sec);
        LogTransitionLocked(value, old_state, "failure_threshold", now);
    }
    return SnapshotLocked(value, now);
}

SegmentAdmissionObservation SegmentAdmissionController::ObserveRemoteWrite(
    const std::string& segment_name, const std::string& writer_host_id,
    uint64_t bytes) {
    SegmentAdmissionObservation observation;
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return observation;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto index = segment_name_index_.find(segment_name);
    if (index == segment_name_index_.end()) {
        return observation;
    }
    auto runtime = runtimes_.find(index->second);
    if (runtime == runtimes_.end()) {
        return observation;
    }

    const auto now = clock_->Now();
    auto& value = runtime->second;
    AdvanceStateLocked(value, now);
    const bool local_write = !writer_host_id.empty() &&
                             !value.segment_host_id.empty() &&
                             writer_host_id == value.segment_host_id;
    if (!local_write) {
        ++value.observed_remote_writes;
        if (value.state == SegmentAdmissionState::QUARANTINED) {
            observation.would_admit = false;
            observation.reason = SegmentAdmissionRejectReason::QUARANTINED;
        } else {
            const double ratio = EffectiveRatioLocked(value, now);
            const uint64_t op_limit = EffectiveOpLimitLocked(ratio);
            const uint64_t full_byte_limit = FullByteLimitLocked(value);
            const uint64_t byte_limit = EffectiveByteLimitLocked(value, ratio);
            if (op_limit != 0 && value.inflight_remote_write_ops >= op_limit) {
                observation.would_admit = false;
                observation.reason =
                    SegmentAdmissionRejectReason::INFLIGHT_OP_LIMIT;
            } else if (full_byte_limit != 0 && bytes > full_byte_limit) {
                observation.would_admit = false;
                observation.reason =
                    SegmentAdmissionRejectReason::INFLIGHT_BYTE_LIMIT;
            } else if (byte_limit != 0 && bytes > byte_limit &&
                       (value.inflight_remote_write_ops != 0 ||
                        value.inflight_remote_write_bytes != 0)) {
                observation.would_admit = false;
                observation.reason =
                    SegmentAdmissionRejectReason::INFLIGHT_BYTE_LIMIT;
            } else if (byte_limit != 0 &&
                       value.inflight_remote_write_bytes >
                           byte_limit - std::min(bytes, byte_limit)) {
                observation.would_admit = false;
                observation.reason =
                    SegmentAdmissionRejectReason::INFLIGHT_BYTE_LIMIT;
            }
        }
        if (!observation.would_admit) {
            ++value.observed_would_reject;
        }
    }
    observation.snapshot = SnapshotLocked(value, now);
    return observation;
}

std::optional<SegmentAdmissionSnapshot> SegmentAdmissionController::GetSnapshot(
    const UUID& segment_id) {
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto runtime = runtimes_.find(segment_id);
    if (runtime == runtimes_.end()) {
        return std::nullopt;
    }
    const auto now = clock_->Now();
    AdvanceStateLocked(runtime->second, now);
    return SnapshotLocked(runtime->second, now);
}

std::vector<SegmentAdmissionSnapshot>
SegmentAdmissionController::GetSnapshots() {
    std::vector<SegmentAdmissionSnapshot> snapshots;
    if (config_.mode == SegmentWriteAdmissionMode::DISABLED) {
        return snapshots;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = clock_->Now();
    snapshots.reserve(runtimes_.size());
    for (auto& [_, runtime] : runtimes_) {
        AdvanceStateLocked(runtime, now);
        snapshots.push_back(SnapshotLocked(runtime, now));
    }
    return snapshots;
}

size_t SegmentAdmissionController::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return runtimes_.size();
}

double SegmentAdmissionController::EffectiveRatioLocked(
    const Runtime& runtime,
    SegmentAdmissionClock::TimePoint now) const noexcept {
    if (runtime.state == SegmentAdmissionState::ACTIVE) {
        return 1.0;
    }
    if (runtime.state == SegmentAdmissionState::QUARANTINED) {
        return 0.0;
    }
    const double elapsed_seconds =
        std::chrono::duration<double>(now - runtime.state_since).count();
    const double time_progress = std::clamp(
        elapsed_seconds / static_cast<double>(config_.ramp_up_duration_sec),
        0.0, 1.0);
    const double time_factor =
        config_.ramp_initial_ratio +
        (1.0 - config_.ramp_initial_ratio) * time_progress;
    const double success_progress =
        config_.ramp_min_successful_remote_writes == 0
            ? 1.0
            : std::clamp(static_cast<double>(
                             runtime.successful_remote_writes_in_ramp) /
                             static_cast<double>(
                                 config_.ramp_min_successful_remote_writes),
                         0.0, 1.0);
    const double success_factor =
        config_.ramp_initial_ratio +
        (1.0 - config_.ramp_initial_ratio) * success_progress;
    return std::min(time_factor, success_factor);
}

uint64_t SegmentAdmissionController::EffectiveByteLimitLocked(
    const Runtime& runtime, double ratio) const noexcept {
    return ScaleLimit(FullByteLimitLocked(runtime), ratio);
}

uint64_t SegmentAdmissionController::FullByteLimitLocked(
    const Runtime& runtime) const noexcept {
    if (config_.max_inflight_remote_write_bytes != 0) {
        return config_.max_inflight_remote_write_bytes;
    }
    return std::clamp(runtime.segment_capacity_bytes / 1024, kMinAutoByteLimit,
                      kMaxAutoByteLimit);
}

uint64_t SegmentAdmissionController::EffectiveOpLimitLocked(
    double ratio) const noexcept {
    return ScaleLimit(config_.max_inflight_remote_write_ops, ratio);
}

void SegmentAdmissionController::AdvanceStateLocked(
    Runtime& runtime, SegmentAdmissionClock::TimePoint now) {
    PruneFailuresLocked(runtime, now);
    if (runtime.state == SegmentAdmissionState::QUARANTINED) {
        if (now >= runtime.quarantine_until &&
            runtime.last_owner_heartbeat > runtime.state_since) {
            const auto old_state = runtime.state;
            runtime.state = SegmentAdmissionState::RAMPING;
            runtime.state_since = now;
            runtime.successful_remote_writes_in_ramp = 0;
            runtime.recent_failures.clear();
            LogTransitionLocked(runtime, old_state, "cooldown_and_heartbeat",
                                now);
        }
        return;
    }
    if (runtime.state == SegmentAdmissionState::RAMPING &&
        EffectiveRatioLocked(runtime, now) >= 1.0) {
        const auto old_state = runtime.state;
        runtime.state = SegmentAdmissionState::ACTIVE;
        runtime.state_since = now;
        LogTransitionLocked(runtime, old_state, "ramp_complete", now);
    }
}

void SegmentAdmissionController::PruneFailuresLocked(
    Runtime& runtime, SegmentAdmissionClock::TimePoint now) const {
    const auto earliest =
        now - std::chrono::seconds(config_.failure_window_sec);
    while (!runtime.recent_failures.empty() &&
           runtime.recent_failures.front() < earliest) {
        runtime.recent_failures.pop_front();
    }
}

SegmentAdmissionSnapshot SegmentAdmissionController::SnapshotLocked(
    const Runtime& runtime, SegmentAdmissionClock::TimePoint now) const {
    SegmentAdmissionSnapshot snapshot;
    snapshot.segment_id = runtime.segment_id;
    snapshot.owner_client_id = runtime.owner_client_id;
    snapshot.segment_name = runtime.segment_name;
    snapshot.segment_host_id = runtime.segment_host_id;
    snapshot.state = runtime.state;
    snapshot.effective_ratio = EffectiveRatioLocked(runtime, now);
    snapshot.inflight_remote_write_ops = runtime.inflight_remote_write_ops;
    snapshot.inflight_remote_write_bytes = runtime.inflight_remote_write_bytes;
    snapshot.successful_remote_writes_in_ramp =
        runtime.successful_remote_writes_in_ramp;
    snapshot.recent_failures = runtime.recent_failures.size();
    snapshot.observed_remote_writes = runtime.observed_remote_writes;
    snapshot.observed_would_reject = runtime.observed_would_reject;
    if (runtime.state == SegmentAdmissionState::QUARANTINED &&
        runtime.quarantine_until > now) {
        snapshot.quarantine_remaining_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                runtime.quarantine_until - now)
                .count();
    }
    snapshot.owner_heartbeat_age_ms = std::max<int64_t>(
        0, std::chrono::duration_cast<std::chrono::milliseconds>(
               now - runtime.last_owner_heartbeat)
               .count());
    return snapshot;
}

void SegmentAdmissionController::LogTransitionLocked(
    const Runtime& runtime, SegmentAdmissionState old_state,
    std::string_view reason, SegmentAdmissionClock::TimePoint now) const {
    const auto snapshot = SnapshotLocked(runtime, now);
    LOG(INFO) << "segment_name=" << runtime.segment_name
              << ", segment_id=" << runtime.segment_id
              << ", admission_state_from=" << ToString(old_state)
              << ", admission_state_to=" << ToString(runtime.state)
              << ", reason=" << reason
              << ", effective_ratio=" << snapshot.effective_ratio
              << ", inflight_ops=" << snapshot.inflight_remote_write_ops
              << ", inflight_bytes=" << snapshot.inflight_remote_write_bytes
              << ", recent_failures=" << snapshot.recent_failures
              << ", owner_heartbeat_age_ms=" << snapshot.owner_heartbeat_age_ms
              << ", action=segment_admission_transition";
}

}  // namespace mooncake
