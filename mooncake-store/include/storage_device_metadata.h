#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {

enum class StorageDeviceKind {
    LOGICAL_POOL = 0,
    TARGET = 1,
    NAMESPACE = 2,
    PHYSICAL_DISK = 3,
    DISK_NODE = 4,
};

enum class StorageDeviceHealth {
    UNKNOWN = 0,
    HEALTHY = 1,
    DEGRADED = 2,
    FAILED = 3,
    REBUILDING = 4,
    DISABLED = 5,
    UNMOUNTING = 6,
};

struct StorageDeviceIdentity {
    StorageDeviceKind kind = StorageDeviceKind::LOGICAL_POOL;
    std::string provider;
    UUID device_id{0, 0};
    std::string target_id;
    std::string namespace_id;
    std::string disk_id;
    std::string node_id;
};

struct StorageDeviceMetadata {
    StorageDeviceIdentity identity;
    StorageDeviceHealth health = StorageDeviceHealth::UNKNOWN;
    bool readable = false;
    bool writable = false;
    bool schedulable = false;
    int64_t capacity_total = 0;
    int64_t capacity_used = 0;
    int64_t capacity_available = 0;
    uint32_t consecutive_failures = 0;
    std::string last_error;
    int64_t last_update_unix_ms = -1;
    std::string mount_endpoint;
    std::string transport;
    std::string opaque_provider_metadata;
};

struct StorageDeviceMetadataUpdate {
    StorageDeviceIdentity identity;
    std::optional<StorageDeviceHealth> health;
    std::optional<bool> readable;
    std::optional<bool> writable;
    std::optional<bool> schedulable;
    std::optional<int64_t> capacity_total;
    std::optional<int64_t> capacity_used;
    std::optional<int64_t> capacity_available;
    std::optional<uint32_t> consecutive_failures;
    std::optional<int64_t> last_update_unix_ms;
    std::optional<std::string> last_error;
    std::optional<std::string> opaque_provider_metadata;
};

struct StorageDeviceMaintenanceCandidate {
    StorageDeviceMetadata metadata;
    std::string reason;
};

struct StorageDeviceMaintenancePlan {
    std::vector<StorageDeviceMaintenanceCandidate> recovery_candidates;
    std::vector<StorageDeviceMaintenanceCandidate> gc_candidates;
};

void ApplyStorageDeviceMetadataUpdate(
    StorageDeviceMetadata& metadata, const StorageDeviceMetadataUpdate& update);

}  // namespace mooncake
