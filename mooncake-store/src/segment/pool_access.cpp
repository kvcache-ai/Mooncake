#include "segment/pool_access.h"

#include <algorithm>
#include <numeric>

#include "master_metric_manager.h"
#include "segment/pool.h"

namespace mooncake {
namespace {

RegionKind ClassifyRegion(const Segment& segment) {
    return segment.protocol == "cxl" ? RegionKind::CXL
                                     : RegionKind::HOST_MEMORY;
}

RegionResourceSpec MakeResourceSpec(const Segment& segment) {
    return RegionResourceSpec{segment.id, segment.name, segment.base,
                              segment.size, segment.te_endpoint};
}

void AddHostRegion(HostRegionIndex& index, const Segment& segment) {
    if (!segment.host_id.empty()) {
        index[segment.host_id][segment.name].insert(segment.id);
    }
}

void RemoveHostRegion(HostRegionIndex& index, const Segment& segment) {
    if (segment.host_id.empty()) {
        return;
    }
    auto host = index.find(segment.host_id);
    if (host == index.end()) {
        return;
    }
    auto group = host->second.find(segment.name);
    if (group == host->second.end()) {
        return;
    }
    group->second.erase(segment.id);
    if (group->second.empty()) {
        host->second.erase(group);
    }
    if (host->second.empty()) {
        index.erase(host);
    }
}

bool SameSegment(const Segment& lhs, const Segment& rhs) {
    return lhs.id == rhs.id && lhs.name == rhs.name && lhs.base == rhs.base &&
           lhs.size == rhs.size && lhs.te_endpoint == rhs.te_endpoint &&
           lhs.protocol == rhs.protocol && lhs.host_id == rhs.host_id;
}

}  // namespace

tl::expected<PreparedMountedRegion, ErrorCode>
ScopedSegmentPoolAccess::PrepareMount(const Segment& segment,
                                      const UUID& client_id,
                                      const RegionInitialState& initial_state) {
    const RegionKind kind = ClassifyRegion(segment);
    RegionDriver* driver = segment_pool_->GetDriver(kind);
    if (!driver) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    auto existing = segment_pool_->mounted_regions_.find(segment.id);
    const bool existed = existing != segment_pool_->mounted_regions_.end();
    if (existed) {
        if (existing->second.status == SegmentStatus::UNMOUNTING) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        if (existing->second.client_id != client_id ||
            existing->second.kind != kind ||
            !SameSegment(existing->second.segment, segment)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!segment_pool_->GetResource(existing->second)) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    } else {
        auto owner = segment_pool_->client_by_name_.find(segment.name);
        if (owner != segment_pool_->client_by_name_.end() &&
            owner->second != client_id) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    auto resource =
        driver->PrepareOpen(MakeResourceSpec(segment), initial_state);
    if (!resource) {
        return tl::make_unexpected(resource.error());
    }
    MountedRegion mounted{segment, client_id, SegmentStatus::OK, kind};
    if (existed) {
        mounted.status = existing->second.status;
    }
    return PreparedMountedRegion(std::move(mounted), existed, true,
                                 std::move(*resource));
}

tl::expected<PreparedMountedRegion, ErrorCode>
ScopedSegmentPoolAccess::PrepareAdopt(
    MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator) {
    if (segment_pool_->mounted_regions_.contains(mounted.segment.id)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    mounted.kind = ClassifyRegion(mounted.segment);
    RegionDriver* driver = segment_pool_->GetDriver(mounted.kind);
    if (!driver) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    auto resource = driver->PrepareAdopt(MakeResourceSpec(mounted.segment),
                                         std::move(allocator));
    if (!resource) {
        return tl::make_unexpected(resource.error());
    }
    return PreparedMountedRegion(std::move(mounted), false, false,
                                 std::move(*resource));
}

void ScopedSegmentPoolAccess::CommitMount(
    PreparedMountedRegion& prepared) noexcept {
    RegionDriver* driver = segment_pool_->GetDriver(prepared.mounted_.kind);
    RegionResource* old_resource = nullptr;
    if (prepared.existed_) {
        old_resource = driver->GetResource(prepared.mounted_.segment.id);
    }
    RegionResource* new_resource = prepared.resource_.resource();
    prepared.resource_.Commit();

    if (prepared.existed_) {
        if (prepared.mounted_.status == SegmentStatus::OK) {
            const bool replaced = segment_pool_->placement_index_.ReplaceTarget(
                prepared.mounted_.segment.name, &old_resource->target,
                &new_resource->target);
            DCHECK(replaced);
        } else {
            (void)driver->Deactivate(prepared.mounted_.segment.id);
        }
        if (prepared.account_capacity_metrics_ &&
            prepared.mounted_.kind == RegionKind::HOST_MEMORY &&
            segment_pool_->capacity_accounted_regions_
                .insert(prepared.mounted_.segment.id)
                .second) {
            MasterMetricManager::instance().inc_total_mem_capacity(
                prepared.mounted_.segment.name, prepared.mounted_.segment.size);
        }
        return;
    }

    const auto& mounted = prepared.mounted_;
    segment_pool_->mounted_regions_.emplace(mounted.segment.id, mounted);
    segment_pool_->client_segments_[mounted.client_id].push_back(
        mounted.segment.id);
    segment_pool_->client_by_name_[mounted.segment.name] = mounted.client_id;
    segment_pool_->segment_id_by_name_[mounted.segment.name] =
        mounted.segment.id;
    if (mounted.status == SegmentStatus::OK) {
        const bool added = segment_pool_->placement_index_.AddTarget(
            mounted.segment.name, &new_resource->target);
        DCHECK(added);
        AddHostRegion(segment_pool_->regions_by_host_, mounted.segment);
    } else {
        (void)driver->Deactivate(mounted.segment.id);
    }
    if (prepared.account_capacity_metrics_ &&
        mounted.kind == RegionKind::HOST_MEMORY) {
        segment_pool_->capacity_accounted_regions_.insert(mounted.segment.id);
        MasterMetricManager::instance().inc_total_mem_capacity(
            mounted.segment.name, mounted.segment.size);
    }
}

ErrorCode ScopedSegmentPoolAccess::MountSegment(const Segment& segment,
                                                const UUID& client_id) {
    if (segment_pool_->mounted_regions_.contains(segment.id)) {
        const auto& mounted = segment_pool_->mounted_regions_.at(segment.id);
        return mounted.status == SegmentStatus::OK
                   ? ErrorCode::SEGMENT_ALREADY_EXISTS
                   : ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    auto prepared = PrepareMount(segment, client_id);
    if (!prepared) {
        return prepared.error();
    }
    CommitMount(*prepared);
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::ValidateRemountSegment(
    const Segment& segment, const UUID& client_id) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment.id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::OK;
    }
    return mounted->second.client_id == client_id &&
                   SameSegment(mounted->second.segment, segment)
               ? ErrorCode::OK
               : ErrorCode::INVALID_PARAMS;
}

bool ScopedSegmentPoolAccess::GetSegment(const UUID& segment_id,
                                         Segment& segment) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return false;
    }
    segment = mounted->second.segment;
    return true;
}

ErrorCode ScopedSegmentPoolAccess::PrepareUnmountSegment(
    const UUID& segment_id, size_t& metrics_dec_capacity) {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.status == SegmentStatus::UNMOUNTING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    RegionResource* resource = segment_pool_->GetResource(mounted->second);
    if (!resource) {
        return ErrorCode::INTERNAL_ERROR;
    }
    metrics_dec_capacity = mounted->second.segment.size;
    if (segment_pool_->placement_index_.Contains(mounted->second.segment.name,
                                                 &resource->target)) {
        segment_pool_->placement_index_.RemoveTarget(
            mounted->second.segment.name, &resource->target);
    }
    RemoveHostRegion(segment_pool_->regions_by_host_, mounted->second.segment);
    if (resource->active) {
        (void)segment_pool_->GetDriver(mounted->second.kind)
            ->Deactivate(segment_id);
    }
    mounted->second.status = SegmentStatus::UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::RollbackUnmountSegment(
    const UUID& segment_id) {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.status != SegmentStatus::UNMOUNTING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    RegionResource* resource = segment_pool_->GetResource(mounted->second);
    if (!resource || !segment_pool_->GetDriver(mounted->second.kind)
                          ->Reactivate(segment_id)) {
        return ErrorCode::INTERNAL_ERROR;
    }
    const bool added = segment_pool_->placement_index_.AddTarget(
        mounted->second.segment.name, &resource->target);
    DCHECK(added);
    AddHostRegion(segment_pool_->regions_by_host_, mounted->second.segment);
    mounted->second.status = SegmentStatus::OK;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::PrepareGracefulUnmountSegment(
    const UUID& segment_id) {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.status == SegmentStatus::GRACEFULLY_UNMOUNTING) {
        return ErrorCode::OK;
    }
    if (mounted->second.status != SegmentStatus::OK &&
        mounted->second.status != SegmentStatus::DRAINING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    RegionResource* resource = segment_pool_->GetResource(mounted->second);
    if (!resource) {
        return ErrorCode::INTERNAL_ERROR;
    }
    if (segment_pool_->placement_index_.Contains(mounted->second.segment.name,
                                                 &resource->target)) {
        segment_pool_->placement_index_.RemoveTarget(
            mounted->second.segment.name, &resource->target);
    }
    RemoveHostRegion(segment_pool_->regions_by_host_, mounted->second.segment);
    if (resource->active) {
        (void)segment_pool_->GetDriver(mounted->second.kind)
            ->Deactivate(segment_id);
    }
    mounted->second.status = SegmentStatus::GRACEFULLY_UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::CommitUnmountSegment(
    const UUID& segment_id, const UUID& client_id,
    const size_t& metrics_dec_capacity) {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    const MountedRegion record = mounted->second;
    std::optional<std::pair<UUID, UUID>> replacement;
    for (const auto& [id, other] : segment_pool_->mounted_regions_) {
        if (id != segment_id && other.segment.name == record.segment.name) {
            replacement.emplace(id, other.client_id);
            break;
        }
    }
    RegionDriver* driver = segment_pool_->GetDriver(record.kind);
    if (record.client_id != client_id || !driver ||
        !driver->GetResource(segment_id)) {
        return ErrorCode::INTERNAL_ERROR;
    }
    // Resource existence was validated above. Erase is the commit point; all
    // remaining catalog/index operations are non-failing.
    const bool erased = driver->Erase(segment_id);
    DCHECK(erased);

    auto client = segment_pool_->client_segments_.find(client_id);
    if (client != segment_pool_->client_segments_.end()) {
        auto id =
            std::find(client->second.begin(), client->second.end(), segment_id);
        if (id != client->second.end()) {
            client->second.erase(id);
        }
        if (client->second.empty()) {
            segment_pool_->client_segments_.erase(client);
        }
    }
    RemoveHostRegion(segment_pool_->regions_by_host_, record.segment);
    segment_pool_->mounted_regions_.erase(mounted);
    if (replacement) {
        auto segment_index =
            segment_pool_->segment_id_by_name_.find(record.segment.name);
        auto client_index =
            segment_pool_->client_by_name_.find(record.segment.name);
        DCHECK(segment_index != segment_pool_->segment_id_by_name_.end());
        DCHECK(client_index != segment_pool_->client_by_name_.end());
        segment_index->second = replacement->first;
        client_index->second = replacement->second;
    } else {
        segment_pool_->segment_id_by_name_.erase(record.segment.name);
        segment_pool_->client_by_name_.erase(record.segment.name);
    }
    if (record.kind == RegionKind::HOST_MEMORY &&
        segment_pool_->capacity_accounted_regions_.erase(segment_id) != 0) {
        MasterMetricManager::instance().dec_total_mem_capacity(
            record.segment.name, metrics_dec_capacity);
        MasterMetricManager::instance().remove_segment_metrics(
            record.segment.name);
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::GetClientSegments(
    const UUID& client_id, std::vector<Segment>& segments) const {
    auto client = segment_pool_->client_segments_.find(client_id);
    if (client == segment_pool_->client_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (const auto& id : client->second) {
        auto mounted = segment_pool_->mounted_regions_.find(id);
        if (mounted != segment_pool_->mounted_regions_.end()) {
            segments.push_back(mounted->second.segment);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::GetAllSegments(
    std::vector<std::string>& all_segments) {
    all_segments.clear();
    for (const auto& [_, mounted] : segment_pool_->mounted_regions_) {
        if (mounted.status == SegmentStatus::OK) {
            all_segments.push_back(mounted.segment.name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::GetAllSegments(
    std::vector<std::pair<Segment, UUID>>& all_segments) {
    all_segments.clear();
    for (const auto& [_, mounted] : segment_pool_->mounted_regions_) {
        all_segments.emplace_back(mounted.segment, mounted.client_id);
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::GetAllSegmentNames(
    std::vector<std::string>& all_segment_names) {
    all_segment_names.clear();
    for (const auto& [_, mounted] : segment_pool_->mounted_regions_) {
        all_segment_names.push_back(mounted.segment.name);
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::QuerySegments(const std::string& name,
                                                 size_t& used,
                                                 size_t& capacity) {
    auto* group = segment_pool_->placement_index_.GetView().Find(name);
    if (!group) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    used = 0;
    capacity = 0;
    for (const auto* target : group->targets) {
        used += target->Used();
        capacity += target->Capacity();
    }
    return capacity == 0 ? ErrorCode::SEGMENT_NOT_FOUND : ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::GetUnreadySegments(
    std::vector<std::pair<Segment, UUID>>& unready_segments) const {
    unready_segments.clear();
    for (const auto& [_, mounted] : segment_pool_->mounted_regions_) {
        if (mounted.status != SegmentStatus::OK) {
            unready_segments.emplace_back(mounted.segment, mounted.client_id);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::GetClientIdBySegmentName(
    const std::string& segment_name, UUID& client_id) const {
    auto owner = segment_pool_->client_by_name_.find(segment_name);
    if (owner == segment_pool_->client_by_name_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    client_id = owner->second;
    return ErrorCode::OK;
}

bool ScopedSegmentPoolAccess::ExistsSegmentName(
    const std::string& segment_name) const {
    return segment_pool_->client_by_name_.contains(segment_name);
}

bool ScopedSegmentPoolAccess::IsSegmentAllocatable(
    const std::string& segment_name) const {
    auto id = segment_pool_->segment_id_by_name_.find(segment_name);
    if (id == segment_pool_->segment_id_by_name_.end()) {
        return false;
    }
    auto mounted = segment_pool_->mounted_regions_.find(id->second);
    return mounted != segment_pool_->mounted_regions_.end() &&
           mounted->second.status == SegmentStatus::OK;
}

ErrorCode ScopedSegmentPoolAccess::GetSegmentStatusByName(
    const std::string& segment_name, SegmentStatus& status) const {
    auto id = segment_pool_->segment_id_by_name_.find(segment_name);
    return id == segment_pool_->segment_id_by_name_.end()
               ? ErrorCode::SEGMENT_NOT_FOUND
               : GetSegmentStatusById(id->second, status);
}

ErrorCode ScopedSegmentPoolAccess::GetSegmentStatusById(
    const UUID& segment_id, SegmentStatus& status) const {
    auto mounted = segment_pool_->mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    status = mounted->second.status;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolAccess::SetSegmentStatusByName(
    const std::string& segment_name, SegmentStatus status) {
    auto id = segment_pool_->segment_id_by_name_.find(segment_name);
    if (id == segment_pool_->segment_id_by_name_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    auto mounted = segment_pool_->mounted_regions_.find(id->second);
    if (mounted == segment_pool_->mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.status == SegmentStatus::UNMOUNTING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    if (mounted->second.status == status) {
        return ErrorCode::OK;
    }
    RegionResource* resource = segment_pool_->GetResource(mounted->second);
    if (!resource) {
        return ErrorCode::INTERNAL_ERROR;
    }
    const bool active = segment_pool_->placement_index_.Contains(
        segment_name, &resource->target);
    if (status == SegmentStatus::OK && !active) {
        if (!segment_pool_->GetDriver(mounted->second.kind)
                 ->Reactivate(id->second)) {
            return ErrorCode::INTERNAL_ERROR;
        }
        segment_pool_->placement_index_.AddTarget(segment_name,
                                                  &resource->target);
        AddHostRegion(segment_pool_->regions_by_host_, mounted->second.segment);
    } else if (status != SegmentStatus::OK && active) {
        segment_pool_->placement_index_.RemoveTarget(segment_name,
                                                     &resource->target);
        RemoveHostRegion(segment_pool_->regions_by_host_,
                         mounted->second.segment);
        (void)segment_pool_->GetDriver(mounted->second.kind)
            ->Deactivate(id->second);
    }
    mounted->second.status = status;
    return ErrorCode::OK;
}

void ScopedSegmentPoolAccess::Clear() noexcept {
    for (const auto& [id, mounted] : segment_pool_->mounted_regions_) {
        if (auto* driver = segment_pool_->GetDriver(mounted.kind)) {
            (void)driver->Erase(id);
        }
    }
    segment_pool_->releaseCapacityMetrics();
    segment_pool_->placement_index_.Clear();
    segment_pool_->mounted_regions_.clear();
    segment_pool_->client_segments_.clear();
    segment_pool_->client_by_name_.clear();
    segment_pool_->segment_id_by_name_.clear();
    segment_pool_->regions_by_host_.clear();
}

}  // namespace mooncake
