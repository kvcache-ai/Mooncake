#include "segment/pool_write_access.h"

#include <algorithm>
#include <unordered_set>
#include "master_metric_manager.h"
#include "segment/pool.h"
#include "segment/region_initial_state.h"

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

ScopedSegmentPoolWriteAccess::ScopedSegmentPoolWriteAccess(
    SegmentPool* segment_pool)
    : segment_pool_(segment_pool), lock_(segment_pool->pool_mutex_) {}

tl::expected<RegionMountTxn, ErrorCode>
ScopedSegmentPoolWriteAccess::PrepareMount(const Segment& segment,
                                           const UUID& client_id) {
    return PrepareWithInitialState(segment, client_id, {}, 0);
}

tl::expected<RegionMountTxn, ErrorCode>
ScopedSegmentPoolWriteAccess::PrepareRestore(
    const Segment& segment, const UUID& client_id,
    std::span<const AllocatedBuffer::Descriptor> descriptors) {
    if (std::any_of(descriptors.begin(), descriptors.end(),
                    [](const auto& descriptor) {
                        return descriptor.protocol_ == "cxl";
                    })) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    auto initial_state =
        BuildRegionInitialState(MakeResourceSpec(segment), descriptors);
    if (!initial_state) {
        return tl::make_unexpected(initial_state.error());
    }
    uint64_t requested_bytes = 0;
    for (const auto& allocation : initial_state->allocations) {
        requested_bytes += allocation.requested_bytes;
    }
    return PrepareWithInitialState(segment, client_id, *initial_state,
                                   requested_bytes);
}

tl::expected<RegionMountTxn, ErrorCode>
ScopedSegmentPoolWriteAccess::PrepareWithInitialState(
    const Segment& segment, const UUID& client_id,
    const RegionInitialState& initial_state,
    uint64_t imported_requested_bytes) {
    const RegionKind kind = ClassifyRegion(segment);
    RegionDriver* driver = segment_pool_->GetDriver(kind);
    if (!driver) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    auto existing = segment_pool_->catalog_.mounted_regions_.find(segment.id);
    const bool existed =
        existing != segment_pool_->catalog_.mounted_regions_.end();
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
        auto owner = segment_pool_->catalog_.owner_client_by_group_name_.find(
            segment.name);
        if (owner !=
                segment_pool_->catalog_.owner_client_by_group_name_.end() &&
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
    return RegionMountTxn(std::move(mounted), existed, true,
                          std::move(*resource), imported_requested_bytes);
}

tl::expected<RegionMountTxn, ErrorCode>
ScopedSegmentPoolWriteAccess::PrepareAdopt(
    MountedRegion mounted, std::shared_ptr<BufferAllocatorBase> allocator,
    bool account_capacity_metrics) {
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
    return RegionMountTxn(std::move(mounted), false, account_capacity_metrics,
                          std::move(*resource));
}

void ScopedSegmentPoolWriteAccess::CommitMount(
    RegionMountTxn& transaction) noexcept {
    DCHECK(!transaction.committed_);
    RegionDriver* driver = segment_pool_->GetDriver(transaction.mounted_.kind);
    RegionResource* old_resource = nullptr;
    if (transaction.existed_) {
        old_resource = driver->GetResource(transaction.mounted_.segment.id);
    }
    RegionResource* new_resource = transaction.resource_.resource();
    transaction.resource_.Commit();
    transaction.committed_ = true;

    if (transaction.existed_) {
        if (transaction.mounted_.status == SegmentStatus::OK) {
            const bool replaced = segment_pool_->placement_index_.ReplaceTarget(
                transaction.mounted_.segment.name, &old_resource->target,
                &new_resource->target);
            DCHECK(replaced);
        } else {
            (void)driver->Deactivate(transaction.mounted_.segment.id);
        }
        if (transaction.account_capacity_metrics_ &&
            transaction.mounted_.kind == RegionKind::HOST_MEMORY &&
            segment_pool_->catalog_.capacity_accounted_region_ids_
                .insert(transaction.mounted_.segment.id)
                .second) {
            MasterMetricManager::instance().inc_total_mem_capacity(
                transaction.mounted_.segment.name,
                transaction.mounted_.segment.size);
        }
        return;
    }

    const auto& mounted = transaction.mounted_;
    segment_pool_->catalog_.mounted_regions_.emplace(mounted.segment.id,
                                                     mounted);
    segment_pool_->catalog_.region_ids_by_client_[mounted.client_id].push_back(
        mounted.segment.id);
    segment_pool_->catalog_.owner_client_by_group_name_[mounted.segment.name] =
        mounted.client_id;
    segment_pool_->catalog_.region_ids_by_group_name_[mounted.segment.name]
        .push_back(mounted.segment.id);
    if (mounted.status == SegmentStatus::OK) {
        const bool added = segment_pool_->placement_index_.AddTarget(
            mounted.segment.name, &new_resource->target);
        DCHECK(added);
        AddHostRegion(segment_pool_->catalog_.regions_by_host_,
                      mounted.segment);
    } else {
        (void)driver->Deactivate(mounted.segment.id);
    }
    if (transaction.account_capacity_metrics_ &&
        mounted.kind == RegionKind::HOST_MEMORY) {
        segment_pool_->catalog_.capacity_accounted_region_ids_.insert(
            mounted.segment.id);
        MasterMetricManager::instance().inc_total_mem_capacity(
            mounted.segment.name, mounted.segment.size);
    }
}

ErrorCode ScopedSegmentPoolWriteAccess::MountSegment(const Segment& segment,
                                                     const UUID& client_id) {
    if (segment_pool_->catalog_.mounted_regions_.contains(segment.id)) {
        const auto& mounted =
            segment_pool_->catalog_.mounted_regions_.at(segment.id);
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

ErrorCode ScopedSegmentPoolWriteAccess::ValidateRemount(
    std::span<const Segment> segments, const UUID& client_id) const {
    std::unordered_set<UUID, boost::hash<UUID>> ids;
    ids.reserve(segments.size());
    for (const auto& segment : segments) {
        if (!ids.insert(segment.id).second) {
            return ErrorCode::INVALID_PARAMS;
        }
        auto mounted =
            segment_pool_->catalog_.mounted_regions_.find(segment.id);
        if (mounted != segment_pool_->catalog_.mounted_regions_.end() &&
            (mounted->second.client_id != client_id ||
             !SameSegment(mounted->second.segment, segment))) {
            return ErrorCode::INVALID_PARAMS;
        }
    }
    return ErrorCode::OK;
}

bool ScopedSegmentPoolWriteAccess::GetSegment(const UUID& segment_id,
                                              Segment& segment) const {
    auto mounted = segment_pool_->catalog_.mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
        return false;
    }
    segment = mounted->second.segment;
    return true;
}

tl::expected<RegionUnmountTxn, ErrorCode>
ScopedSegmentPoolWriteAccess::PrepareUnmount(const UUID& segment_id,
                                             const UUID& client_id) {
    auto mounted = segment_pool_->catalog_.mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (mounted->second.client_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (mounted->second.status == SegmentStatus::UNMOUNTING) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    RegionResource* resource = segment_pool_->GetResource(mounted->second);
    if (!resource) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    Segment segment = mounted->second.segment;
    (void)segment_pool_->placement_index_.RemoveTarget(
        mounted->second.segment.name, &resource->target);
    RemoveHostRegion(segment_pool_->catalog_.regions_by_host_,
                     mounted->second.segment);
    if (resource->active) {
        (void)segment_pool_->GetDriver(mounted->second.kind)
            ->Deactivate(segment_id);
    }
    mounted->second.status = SegmentStatus::UNMOUNTING;
    return RegionUnmountTxn(std::move(segment), client_id);
}

ErrorCode ScopedSegmentPoolWriteAccess::RollbackUnmount(
    RegionUnmountTxn& transaction) {
    if (transaction.finished_) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    const UUID& segment_id = transaction.segment_.id;
    auto mounted = segment_pool_->catalog_.mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.client_id != transaction.client_id_ ||
        mounted->second.status != SegmentStatus::UNMOUNTING) {
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
    AddHostRegion(segment_pool_->catalog_.regions_by_host_,
                  mounted->second.segment);
    mounted->second.status = SegmentStatus::OK;
    transaction.finished_ = true;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolWriteAccess::PrepareGracefulUnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    auto mounted = segment_pool_->catalog_.mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (mounted->second.client_id != client_id) {
        return ErrorCode::INVALID_PARAMS;
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
    (void)segment_pool_->placement_index_.RemoveTarget(
        mounted->second.segment.name, &resource->target);
    RemoveHostRegion(segment_pool_->catalog_.regions_by_host_,
                     mounted->second.segment);
    if (resource->active) {
        (void)segment_pool_->GetDriver(mounted->second.kind)
            ->Deactivate(segment_id);
    }
    mounted->second.status = SegmentStatus::GRACEFULLY_UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolWriteAccess::CommitUnmount(
    RegionUnmountTxn& transaction) {
    if (transaction.finished_) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    auto result =
        EraseUnmountedRegion(transaction.segment_.id, transaction.client_id_,
                             SegmentStatus::UNMOUNTING);
    if (result == ErrorCode::OK) {
        transaction.finished_ = true;
    }
    return result;
}

ErrorCode ScopedSegmentPoolWriteAccess::FinalizeGracefulUnmount(
    const UUID& segment_id, const UUID& client_id) {
    return EraseUnmountedRegion(segment_id, client_id,
                                SegmentStatus::GRACEFULLY_UNMOUNTING);
}

ErrorCode ScopedSegmentPoolWriteAccess::EraseUnmountedRegion(
    const UUID& segment_id, const UUID& client_id,
    SegmentStatus expected_status) {
    auto mounted = segment_pool_->catalog_.mounted_regions_.find(segment_id);
    if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    const MountedRegion record = mounted->second;
    RegionDriver* driver = segment_pool_->GetDriver(record.kind);
    if (record.client_id != client_id || record.status != expected_status ||
        !driver || !driver->GetResource(segment_id)) {
        return ErrorCode::INTERNAL_ERROR;
    }
    // Resource existence was validated above. Erase is the commit point; all
    // remaining catalog/index operations are non-failing.
    const bool erased = driver->Erase(segment_id);
    DCHECK(erased);

    auto client = segment_pool_->catalog_.region_ids_by_client_.find(client_id);
    if (client != segment_pool_->catalog_.region_ids_by_client_.end()) {
        auto id =
            std::find(client->second.begin(), client->second.end(), segment_id);
        if (id != client->second.end()) {
            client->second.erase(id);
        }
        if (client->second.empty()) {
            segment_pool_->catalog_.region_ids_by_client_.erase(client);
        }
    }
    RemoveHostRegion(segment_pool_->catalog_.regions_by_host_, record.segment);
    segment_pool_->catalog_.mounted_regions_.erase(mounted);
    auto group = segment_pool_->catalog_.region_ids_by_group_name_.find(
        record.segment.name);
    DCHECK(group != segment_pool_->catalog_.region_ids_by_group_name_.end());
    auto group_id =
        std::find(group->second.begin(), group->second.end(), segment_id);
    DCHECK(group_id != group->second.end());
    *group_id = group->second.back();
    group->second.pop_back();
    const bool removed_group = group->second.empty();
    if (removed_group) {
        segment_pool_->catalog_.region_ids_by_group_name_.erase(group);
        segment_pool_->catalog_.owner_client_by_group_name_.erase(
            record.segment.name);
    }
    if (record.kind == RegionKind::HOST_MEMORY &&
        segment_pool_->catalog_.capacity_accounted_region_ids_.erase(
            segment_id) != 0) {
        MasterMetricManager::instance().dec_total_mem_capacity(
            record.segment.name, record.segment.size);
        if (removed_group) {
            MasterMetricManager::instance().remove_segment_metrics(
                record.segment.name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolWriteAccess::GetClientSegments(
    const UUID& client_id, std::vector<Segment>& segments) const {
    auto client = segment_pool_->catalog_.region_ids_by_client_.find(client_id);
    if (client == segment_pool_->catalog_.region_ids_by_client_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (const auto& id : client->second) {
        auto mounted = segment_pool_->catalog_.mounted_regions_.find(id);
        if (mounted != segment_pool_->catalog_.mounted_regions_.end()) {
            segments.push_back(mounted->second.segment);
        }
    }
    return ErrorCode::OK;
}

bool ScopedSegmentPoolWriteAccess::ExistsSegmentName(
    std::string_view segment_name) const {
    return segment_pool_->catalog_.region_ids_by_group_name_.contains(
        segment_name);
}

bool ScopedSegmentPoolWriteAccess::IsSegmentAllocatable(
    std::string_view segment_name) const {
    return segment_pool_->placement_index_.GetView().Find(segment_name) !=
           nullptr;
}

ErrorCode ScopedSegmentPoolWriteAccess::GetSegmentStatusByName(
    std::string_view segment_name, SegmentStatus& status) const {
    auto group =
        segment_pool_->catalog_.region_ids_by_group_name_.find(segment_name);
    if (group == segment_pool_->catalog_.region_ids_by_group_name_.end() ||
        group->second.empty()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    status = SegmentStatus::UNDEFINED;
    for (const auto& id : group->second) {
        auto mounted = segment_pool_->catalog_.mounted_regions_.find(id);
        if (mounted == segment_pool_->catalog_.mounted_regions_.end()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (SegmentStatusAvailabilityRank(mounted->second.status) <
            SegmentStatusAvailabilityRank(status)) {
            status = mounted->second.status;
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentPoolWriteAccess::SetSegmentStatusByName(
    std::string_view segment_name, SegmentStatus status) {
    auto group =
        segment_pool_->catalog_.region_ids_by_group_name_.find(segment_name);
    if (group == segment_pool_->catalog_.region_ids_by_group_name_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    for (const auto& id : group->second) {
        auto mounted = segment_pool_->catalog_.mounted_regions_.find(id);
        if (mounted == segment_pool_->catalog_.mounted_regions_.end() ||
            !segment_pool_->GetDriver(mounted->second.kind) ||
            !segment_pool_->GetResource(mounted->second)) {
            return ErrorCode::INTERNAL_ERROR;
        }
        if (mounted->second.status == SegmentStatus::UNMOUNTING) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    }

    for (const auto& id : group->second) {
        auto mounted = segment_pool_->catalog_.mounted_regions_.find(id);
        RegionDriver* driver = segment_pool_->GetDriver(mounted->second.kind);
        RegionResource* resource = segment_pool_->GetResource(mounted->second);
        if (status == SegmentStatus::OK) {
            if (!resource->active) {
                const bool reactivated = driver->Reactivate(id);
                DCHECK(reactivated);
            }
            (void)segment_pool_->placement_index_.AddTarget(segment_name,
                                                            &resource->target);
            AddHostRegion(segment_pool_->catalog_.regions_by_host_,
                          mounted->second.segment);
        } else {
            (void)segment_pool_->placement_index_.RemoveTarget(
                segment_name, &resource->target);
            RemoveHostRegion(segment_pool_->catalog_.regions_by_host_,
                             mounted->second.segment);
            if (resource->active) {
                const bool deactivated = driver->Deactivate(id);
                DCHECK(deactivated);
            }
        }
        mounted->second.status = status;
    }
    return ErrorCode::OK;
}

void ScopedSegmentPoolWriteAccess::Clear() noexcept {
    for (const auto& [id, mounted] : segment_pool_->catalog_.mounted_regions_) {
        if (auto* driver = segment_pool_->GetDriver(mounted.kind)) {
            (void)driver->Erase(id);
        }
    }
    segment_pool_->ReleaseCapacityMetrics();
    segment_pool_->placement_index_.Clear();
    segment_pool_->catalog_.mounted_regions_.clear();
    segment_pool_->catalog_.region_ids_by_client_.clear();
    segment_pool_->catalog_.owner_client_by_group_name_.clear();
    segment_pool_->catalog_.region_ids_by_group_name_.clear();
    segment_pool_->catalog_.regions_by_host_.clear();
}

}  // namespace mooncake
