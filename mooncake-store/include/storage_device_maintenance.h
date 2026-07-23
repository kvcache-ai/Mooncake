#pragma once

#include <optional>
#include <string>

#include "storage_device_metadata.h"

namespace mooncake {

std::optional<std::string> StorageDeviceRecoveryReason(
    const StorageDeviceMetadata& metadata);
std::optional<std::string> StorageDeviceGcReason(
    const StorageDeviceMetadata& metadata);

}  // namespace mooncake
