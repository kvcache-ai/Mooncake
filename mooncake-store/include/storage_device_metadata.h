#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake {

/**
 * @brief Health of a cold-tier storage device as tracked by the master.
 *
 * The master derives health from NoF heartbeat probe accounting plus explicit
 * administrative intent. HEALTHY -> DEGRADED -> FAILED mirrors escalating
 * probe failures, so operators can see a flapping device before the master
 * reaches the terminal action of unmounting it.
 */
enum class StorageDeviceHealth {
    UNKNOWN = 0,     ///< mounted but never probed yet
    HEALTHY = 1,     ///< probes succeeding
    DEGRADED = 2,    ///< consecutive probe failures, still serving reads
    FAILED = 3,      ///< probe failures reached the unmount threshold
    UNMOUNTING = 4,  ///< segment is draining or being unmounted
};

std::string_view StorageDeviceHealthToString(StorageDeviceHealth health);

/**
 * @brief Parse a health name as accepted by the admin API.
 * @return false when \p text does not name a declared health state.
 */
bool StorageDeviceHealthFromString(std::string_view text,
                                   StorageDeviceHealth& out);

/**
 * @brief Thresholds that turn probe accounting into a health state.
 *
 * \c degraded_failures is the number of consecutive probe failures after
 * which a device is reported as DEGRADED. \c failed_failures is the number
 * after which it is reported as FAILED. The master keeps its existing
 * alive-timeout rule as the sole authority for actually unmounting a device,
 * so these thresholds only affect what is reported.
 */
struct StorageDeviceHealthPolicy {
    uint32_t degraded_failures = 1;
    uint32_t failed_failures = 3;
};

/// Inputs the master feeds into DeriveStorageDeviceHealth().
struct StorageDeviceProbeState {
    bool unmounting = false;
    bool ever_probed = false;
    uint32_t consecutive_failures = 0;
};

/**
 * @brief Derive the health state of a device from its probe accounting.
 *
 * Pure function so the state machine can be unit tested without a master.
 * Mount lifecycle wins over probe accounting: an UNMOUNTING device is reported
 * as such even while its probes still succeed, because its data is on the way
 * out of the cluster either way.
 */
StorageDeviceHealth DeriveStorageDeviceHealth(
    const StorageDeviceProbeState& state,
    const StorageDeviceHealthPolicy& policy);

/// Inventory and health of one cold-tier storage device known to the master.
struct StorageDeviceMetadata {
    UUID device_id{0, 0};
    std::string name;
    std::string endpoint;
    UUID owner_client_id{0, 0};
    StorageDeviceHealth health = StorageDeviceHealth::UNKNOWN;
    bool schedulable = false;  ///< still eligible to receive new allocations
    int64_t capacity_bytes = 0;
    int64_t used_bytes = -1;  ///< -1 when no allocator is attached
    uint32_t consecutive_failures = 0;
    std::string last_error;
    int64_t last_success_unix_ms = -1;
};

/// A device that the maintenance plan says needs attention, and why.
struct StorageDeviceMaintenanceCandidate {
    UUID device_id{0, 0};
    std::string name;
    std::string reason;
};

/**
 * @brief Derived maintenance work for the cold tier.
 *
 * \c recovery_candidates are devices whose data is at risk and should be
 * rebuilt or migrated elsewhere. \c gc_candidates are devices that should
 * have space reclaimed, either because they are nearly full or because they
 * are on their way out of the cluster.
 */
struct StorageDeviceMaintenancePlan {
    std::vector<StorageDeviceMaintenanceCandidate> recovery_candidates;
    std::vector<StorageDeviceMaintenanceCandidate> gc_candidates;
};

/// Why a device needs recovery, or std::nullopt when it does not.
std::optional<std::string> StorageDeviceRecoveryReason(
    const StorageDeviceMetadata& device);

/**
 * @brief Why a device needs garbage collection, or std::nullopt when it does
 * not.
 * @param gc_high_watermark used/capacity ratio at or above which a device is
 *        reported as needing eviction. Values outside (0, 1] disable the
 *        usage-based rule.
 */
std::optional<std::string> StorageDeviceGcReason(
    const StorageDeviceMetadata& device, double gc_high_watermark);

/// Build a maintenance plan over a device inventory. Pure function.
StorageDeviceMaintenancePlan BuildStorageDeviceMaintenancePlan(
    const std::vector<StorageDeviceMetadata>& devices,
    double gc_high_watermark);

}  // namespace mooncake
