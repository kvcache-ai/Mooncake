#pragma once

#include <cmath>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace mooncake {

enum class SegmentWriteAdmissionMode : uint8_t {
    DISABLED = 0,
    OBSERVE = 1,
    ENFORCE = 2,
};

inline std::string_view ToString(SegmentWriteAdmissionMode mode) noexcept {
    switch (mode) {
        case SegmentWriteAdmissionMode::DISABLED:
            return "disabled";
        case SegmentWriteAdmissionMode::OBSERVE:
            return "observe";
        case SegmentWriteAdmissionMode::ENFORCE:
            return "enforce";
    }
    return "unknown";
}

inline std::optional<SegmentWriteAdmissionMode> ParseSegmentWriteAdmissionMode(
    std::string_view value) noexcept {
    if (value == "disabled") {
        return SegmentWriteAdmissionMode::DISABLED;
    }
    if (value == "observe") {
        return SegmentWriteAdmissionMode::OBSERVE;
    }
    if (value == "enforce") {
        return SegmentWriteAdmissionMode::ENFORCE;
    }
    return std::nullopt;
}

struct SegmentAdmissionConfig {
    SegmentWriteAdmissionMode mode{SegmentWriteAdmissionMode::OBSERVE};
    uint64_t ramp_up_duration_sec{60};
    double ramp_initial_ratio{0.05};
    uint64_t ramp_min_successful_remote_writes{16};
    uint64_t max_inflight_remote_write_ops{64};
    uint64_t max_inflight_remote_write_bytes{0};
    uint64_t failure_window_sec{5};
    uint64_t failure_threshold{3};
    uint64_t quarantine_duration_sec{10};
    uint64_t result_retention_sec{30};

    void Validate() const {
        if (mode != SegmentWriteAdmissionMode::DISABLED &&
            mode != SegmentWriteAdmissionMode::OBSERVE &&
            mode != SegmentWriteAdmissionMode::ENFORCE) {
            throw std::invalid_argument(
                "segment_write_admission_mode is invalid");
        }
        if (ramp_up_duration_sec == 0) {
            throw std::invalid_argument(
                "segment_ramp_up_duration_sec must be greater than 0");
        }
        if (!std::isfinite(ramp_initial_ratio) || ramp_initial_ratio <= 0.0 ||
            ramp_initial_ratio > 1.0) {
            throw std::invalid_argument(
                "segment_ramp_initial_ratio must be in (0, 1]");
        }
        if (failure_window_sec == 0) {
            throw std::invalid_argument(
                "segment_failure_window_sec must be greater than 0");
        }
        if (failure_threshold == 0) {
            throw std::invalid_argument(
                "segment_failure_threshold must be greater than 0");
        }
        if (quarantine_duration_sec == 0) {
            throw std::invalid_argument(
                "segment_quarantine_duration_sec must be greater than 0");
        }
        if (result_retention_sec == 0) {
            throw std::invalid_argument(
                "segment_result_retention_sec must be greater than 0");
        }
    }
};

inline SegmentAdmissionConfig BuildSegmentAdmissionConfig(
    std::string_view mode, uint64_t ramp_up_duration_sec,
    double ramp_initial_ratio, uint64_t ramp_min_successful_remote_writes,
    uint64_t max_inflight_remote_write_ops,
    uint64_t max_inflight_remote_write_bytes, uint64_t failure_window_sec,
    uint64_t failure_threshold, uint64_t quarantine_duration_sec,
    uint64_t result_retention_sec) {
    const auto parsed_mode = ParseSegmentWriteAdmissionMode(mode);
    if (!parsed_mode) {
        throw std::invalid_argument(
            "segment_write_admission_mode must be disabled, observe, or "
            "enforce");
    }
    SegmentAdmissionConfig config{
        .mode = *parsed_mode,
        .ramp_up_duration_sec = ramp_up_duration_sec,
        .ramp_initial_ratio = ramp_initial_ratio,
        .ramp_min_successful_remote_writes = ramp_min_successful_remote_writes,
        .max_inflight_remote_write_ops = max_inflight_remote_write_ops,
        .max_inflight_remote_write_bytes = max_inflight_remote_write_bytes,
        .failure_window_sec = failure_window_sec,
        .failure_threshold = failure_threshold,
        .quarantine_duration_sec = quarantine_duration_sec,
        .result_retention_sec = result_retention_sec,
    };
    config.Validate();
    return config;
}

}  // namespace mooncake
