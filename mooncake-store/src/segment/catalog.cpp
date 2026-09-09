#include "segment/catalog.h"

namespace mooncake {

const MountedRegion* RegionCatalog::Find(const UUID& segment_id) const {
    auto it = records_.find(segment_id);
    return it == records_.end() ? nullptr : &*it;
}

ErrorCode RegionCatalog::Register(const MountedRegion& mounted) {
    if (Find(mounted.segment.id)) return ErrorCode::SEGMENT_ALREADY_EXISTS;
    auto owner = FindOwnerClientId(mounted.segment.name);
    if (owner && *owner != mounted.client_id) return ErrorCode::INVALID_PARAMS;
    auto record = mounted;
    record.generation = ++next_generation_;
    records_.insert(std::move(record));
    return ErrorCode::OK;
}

bool RegionCatalog::Erase(const UUID& segment_id) {
    return records_.erase(segment_id) != 0;
}

bool RegionCatalog::SetStatus(const UUID& segment_id, SegmentStatus status) {
    auto it = records_.find(segment_id);
    if (it == records_.end()) return false;
    if (it->status == status) return true;
    return records_.modify(it, [this, status](MountedRegion& r) noexcept {
        r.status = status;
        r.generation = ++next_generation_;
    });
}

void RegionCatalog::Clear() { records_.clear(); }

std::optional<UUID> RegionCatalog::FindOwnerClientId(
    std::string_view name) const {
    const auto& names = records_.get<1>();
    auto it = names.find(name);
    return it == names.end() ? std::nullopt
                             : std::optional<UUID>(it->client_id);
}

}  // namespace mooncake
