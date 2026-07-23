#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "storage_device_metadata.h"

namespace mooncake {

class StorageBackendInterface;

// Read-only composite view over storage-backend-owned devices (e.g. NVMe KV
// namespaces). It only aggregates provider metadata and derives maintenance
// candidates; routing of mutations to the owning provider stays in the caller.
// NoF logical-pool devices are owned by MasterService and merged by the caller.
class StorageDeviceView {
   public:
    explicit StorageDeviceView(
        std::shared_ptr<StorageBackendInterface> storage_backend)
        : storage_backend_(std::move(storage_backend)) {}

    std::vector<StorageDeviceMetadata> ListDeviceMetadata() const;

    std::optional<StorageDeviceMetadata> FindDeviceMetadata(
        const UUID& device_id) const;

    StorageDeviceMaintenancePlan BuildMaintenancePlan() const;

    void AppendMaintenanceCandidates(StorageDeviceMaintenancePlan& plan) const;

   private:
    std::shared_ptr<StorageBackendInterface> storage_backend_;
};

}  // namespace mooncake
