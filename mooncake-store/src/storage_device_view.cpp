#include "storage_device_view.h"

#include "storage_backend.h"
#include "storage_device_maintenance.h"

namespace mooncake {

std::vector<StorageDeviceMetadata> StorageDeviceView::ListDeviceMetadata()
    const {
    if (!storage_backend_) {
        return {};
    }
    return storage_backend_->ListStorageDeviceMetadata();
}

std::optional<StorageDeviceMetadata> StorageDeviceView::FindDeviceMetadata(
    const UUID& device_id) const {
    for (const auto& metadata : ListDeviceMetadata()) {
        if (metadata.identity.device_id == device_id) {
            return metadata;
        }
    }
    return std::nullopt;
}

StorageDeviceMaintenancePlan StorageDeviceView::BuildMaintenancePlan() const {
    StorageDeviceMaintenancePlan plan;
    for (const auto& metadata : ListDeviceMetadata()) {
        if (auto reason = StorageDeviceRecoveryReason(metadata);
            reason.has_value()) {
            plan.recovery_candidates.push_back(
                StorageDeviceMaintenanceCandidate{metadata, *reason});
        }
        if (auto reason = StorageDeviceGcReason(metadata); reason.has_value()) {
            plan.gc_candidates.push_back(
                StorageDeviceMaintenanceCandidate{metadata, *reason});
        }
    }
    return plan;
}

void StorageDeviceView::AppendMaintenanceCandidates(
    StorageDeviceMaintenancePlan& plan) const {
    auto storage_plan = BuildMaintenancePlan();
    plan.recovery_candidates.insert(plan.recovery_candidates.end(),
                                    storage_plan.recovery_candidates.begin(),
                                    storage_plan.recovery_candidates.end());
    plan.gc_candidates.insert(plan.gc_candidates.end(),
                              storage_plan.gc_candidates.begin(),
                              storage_plan.gc_candidates.end());
}

}  // namespace mooncake
