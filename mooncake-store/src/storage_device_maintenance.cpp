#include "storage_device_maintenance.h"

namespace mooncake {

std::optional<std::string> StorageDeviceRecoveryReason(
    const StorageDeviceMetadata& metadata) {
    if (metadata.health == StorageDeviceHealth::FAILED) return "health_failed";
    if (metadata.health == StorageDeviceHealth::DISABLED) return "disabled";
    if (metadata.health == StorageDeviceHealth::DEGRADED) return "degraded";
    if (!metadata.readable) return "not_readable";
    return std::nullopt;
}

std::optional<std::string> StorageDeviceGcReason(
    const StorageDeviceMetadata& metadata) {
    if (metadata.health == StorageDeviceHealth::UNMOUNTING) return "unmounting";
    if (!metadata.writable) return "not_writable";
    if (!metadata.schedulable) return "not_schedulable";
    if (metadata.capacity_total > 0 && metadata.capacity_available == 0) {
        return "full";
    }
    return std::nullopt;
}

void ApplyStorageDeviceMetadataUpdate(
    StorageDeviceMetadata& metadata,
    const StorageDeviceMetadataUpdate& update) {
    if (update.health.has_value()) metadata.health = *update.health;
    if (update.readable.has_value()) metadata.readable = *update.readable;
    if (update.writable.has_value()) metadata.writable = *update.writable;
    if (update.schedulable.has_value())
        metadata.schedulable = *update.schedulable;
    if (update.capacity_total.has_value())
        metadata.capacity_total = *update.capacity_total;
    if (update.capacity_used.has_value())
        metadata.capacity_used = *update.capacity_used;
    if (update.capacity_available.has_value())
        metadata.capacity_available = *update.capacity_available;
    if (update.consecutive_failures.has_value())
        metadata.consecutive_failures = *update.consecutive_failures;
    if (update.last_update_unix_ms.has_value())
        metadata.last_update_unix_ms = *update.last_update_unix_ms;
    if (update.last_error.has_value()) metadata.last_error = *update.last_error;
    if (update.opaque_provider_metadata.has_value())
        metadata.opaque_provider_metadata = *update.opaque_provider_metadata;
}

}  // namespace mooncake
