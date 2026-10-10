#include "storage_device_metadata.h"

#include <algorithm>
#include <array>
#include <cctype>

namespace mooncake {

namespace {

constexpr std::array<std::pair<StorageDeviceHealth, std::string_view>, 5>
    kHealthNames{{
        {StorageDeviceHealth::UNKNOWN, "UNKNOWN"},
        {StorageDeviceHealth::HEALTHY, "HEALTHY"},
        {StorageDeviceHealth::DEGRADED, "DEGRADED"},
        {StorageDeviceHealth::FAILED, "FAILED"},
        {StorageDeviceHealth::UNMOUNTING, "UNMOUNTING"},
    }};

std::string ToUpperAscii(std::string_view text) {
    std::string result(text);
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return result;
}

}  // namespace

std::string_view StorageDeviceHealthToString(StorageDeviceHealth health) {
    for (const auto& [value, name] : kHealthNames) {
        if (value == health) return name;
    }
    return "UNKNOWN";
}

bool StorageDeviceHealthFromString(std::string_view text,
                                   StorageDeviceHealth& out) {
    const auto normalized = ToUpperAscii(text);
    for (const auto& [value, name] : kHealthNames) {
        if (name == normalized) {
            out = value;
            return true;
        }
    }
    return false;
}

StorageDeviceHealth DeriveStorageDeviceHealth(
    const StorageDeviceProbeState& state,
    const StorageDeviceHealthPolicy& policy) {
    if (state.unmounting) return StorageDeviceHealth::UNMOUNTING;
    if (!state.ever_probed) return StorageDeviceHealth::UNKNOWN;

    const uint32_t failed_at = std::max(policy.failed_failures, 1u);
    const uint32_t degraded_at =
        std::clamp(policy.degraded_failures, 1u, failed_at);

    if (state.consecutive_failures >= failed_at) {
        return StorageDeviceHealth::FAILED;
    }
    if (state.consecutive_failures >= degraded_at) {
        return StorageDeviceHealth::DEGRADED;
    }
    return StorageDeviceHealth::HEALTHY;
}

std::optional<std::string> StorageDeviceRecoveryReason(
    const StorageDeviceMetadata& device) {
    switch (device.health) {
        case StorageDeviceHealth::FAILED:
            return "probe_failed";
        case StorageDeviceHealth::DEGRADED:
            return "probe_degraded";
        case StorageDeviceHealth::UNKNOWN:
        case StorageDeviceHealth::HEALTHY:
        case StorageDeviceHealth::UNMOUNTING:
            break;
    }
    if (device.isolated && !device.draining) {
        return "device_isolated";
    }
    return std::nullopt;
}

std::optional<std::string> StorageDeviceGcReason(
    const StorageDeviceMetadata& device, double gc_high_watermark) {
    if (device.draining) return "draining";
    if (device.health == StorageDeviceHealth::UNMOUNTING) return "unmounting";
    if (gc_high_watermark <= 0.0 || gc_high_watermark > 1.0) {
        return std::nullopt;
    }
    if (device.used_bytes < 0 || device.capacity_bytes <= 0) {
        return std::nullopt;
    }
    const double ratio = static_cast<double>(device.used_bytes) /
                         static_cast<double>(device.capacity_bytes);
    if (ratio >= gc_high_watermark) return "high_usage";
    return std::nullopt;
}

StorageDeviceMaintenancePlan BuildStorageDeviceMaintenancePlan(
    const std::vector<StorageDeviceMetadata>& devices,
    double gc_high_watermark) {
    StorageDeviceMaintenancePlan plan;
    for (const auto& device : devices) {
        if (auto reason = StorageDeviceRecoveryReason(device)) {
            plan.recovery_candidates.push_back(
                StorageDeviceMaintenanceCandidate{device.device_id, device.name,
                                                  *reason});
        }
        if (auto reason = StorageDeviceGcReason(device, gc_high_watermark)) {
            plan.gc_candidates.push_back(StorageDeviceMaintenanceCandidate{
                device.device_id, device.name, *reason});
        }
    }
    return plan;
}

}  // namespace mooncake
