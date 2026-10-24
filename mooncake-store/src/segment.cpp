#include "segment.h"

#include "master_metric_manager.h"

#include <functional>
namespace mooncake {
namespace {

bool HasAllocatorRegistration(
    const AllocatorManager& allocator_manager, const std::string& segment_name,
    const std::shared_ptr<SegmentAllocatorRegistration>& registration) {
    const auto* allocators = allocator_manager.getAllocators(segment_name);
    if (allocators == nullptr) {
        return false;
    }
    return std::find(allocators->begin(), allocators->end(), registration) !=
           allocators->end();
}

void AddHostSegment(HostSegmentIndex& index, const Segment& segment) {
    if (!segment.host_id.empty()) {
        index[segment.host_id][segment.name].insert(segment.id);
    }
}

void RemoveHostSegment(HostSegmentIndex& index, const Segment& segment) {
    if (segment.host_id.empty()) {
        return;
    }
    auto host_it = index.find(segment.host_id);
    if (host_it == index.end()) {
        return;
    }
    auto name_it = host_it->second.find(segment.name);
    if (name_it == host_it->second.end()) {
        return;
    }
    name_it->second.erase(segment.id);
    if (name_it->second.empty()) {
        host_it->second.erase(name_it);
    }
    if (host_it->second.empty()) {
        index.erase(host_it);
    }
}

std::vector<std::string> BuildHostOrderedSegments(
    const HostSegmentIndex& segments_by_host, const std::string& writer_host_id,
    const std::string& key) {
    std::vector<std::string> ordered_segments;
    if (writer_host_id.empty() || segments_by_host.empty()) {
        return ordered_segments;
    }

    auto start_it = segments_by_host.find(writer_host_id);
    if (start_it == segments_by_host.end()) {
        start_it = segments_by_host.lower_bound(writer_host_id);
        if (start_it == segments_by_host.end()) {
            start_it = segments_by_host.begin();
        }
    }

    const size_t host_count = segments_by_host.size();
    auto host_it = start_it;
    for (size_t host_idx = 0; host_idx < host_count; ++host_idx) {
        const auto& segments_by_name = host_it->second;
        if (!segments_by_name.empty()) {
            std::vector<std::string> host_segments;
            host_segments.reserve(segments_by_name.size());
            for (const auto& [name, segment_ids] : segments_by_name) {
                if (!segment_ids.empty()) {
                    host_segments.push_back(name);
                }
            }
            const size_t start =
                std::hash<std::string>{}(key) % host_segments.size();
            for (size_t i = 0; i < host_segments.size(); ++i) {
                ordered_segments.push_back(
                    host_segments[(start + i) % host_segments.size()]);
            }
        }

        ++host_it;
        if (host_it == segments_by_host.end()) {
            host_it = segments_by_host.begin();
        }
    }

    return ordered_segments;
}

void AddAllocatorUsage(
    StorageUsageSnapshot& snapshot,
    const std::shared_ptr<BufferAllocatorBase>& allocator,
    std::unordered_set<const BufferAllocatorBase*>& counted_allocators) {
    if (!allocator || !counted_allocators.insert(allocator.get()).second) {
        return;
    }

    const size_t used_bytes = allocator->size();
    const size_t capacity_bytes = allocator->capacity();
    snapshot.used_bytes += used_bytes;
    snapshot.capacity_bytes += capacity_bytes;

    const std::string segment_name = allocator->getSegmentName();
    if (!segment_name.empty()) {
        auto& segment = snapshot.segments[segment_name];
        segment.used_bytes += used_bytes;
        segment.capacity_bytes += capacity_bytes;
    }
}

}  // namespace

std::vector<std::string> ScopedAllocatorAccess::GetHostOrderedSegments(
    const std::string& writer_host_id, const std::string& key) const {
    if (segments_by_host_ == nullptr) {
        return {};
    }
    return BuildHostOrderedSegments(*segments_by_host_, writer_host_id, key);
}

StorageUsageSnapshot SegmentManager::GetMemoryUsageSnapshot() const {
    std::shared_lock<std::shared_mutex> lock(segment_mutex_);
    StorageUsageSnapshot snapshot;
    std::unordered_set<const BufferAllocatorBase*> counted_allocators;

    // The CXL allocator is owned by SegmentManager independently of client
    // mounts and may be shared by multiple mounted segment records.
    AddAllocatorUsage(snapshot, cxl_global_allocator_, counted_allocators);

    for (const auto& [segment_id, mounted_segment] : mounted_segments_) {
        (void)segment_id;
        AddAllocatorUsage(snapshot, mounted_segment.buf_allocator,
                          counted_allocators);
    }
    return snapshot;
}

void SegmentManager::AttachMountedUsageTrackers() {
    std::unordered_set<const BufferAllocatorBase*> attached_allocators;
    if (cxl_global_allocator_) {
        cxl_global_allocator_->AttachUsageTracker(usage_tracker_);
        attached_allocators.insert(cxl_global_allocator_.get());
    }
    for (auto& [segment_id, mounted_segment] : mounted_segments_) {
        (void)segment_id;
        auto& allocator = mounted_segment.buf_allocator;
        if (allocator && attached_allocators.insert(allocator.get()).second) {
            allocator->AttachUsageTracker(usage_tracker_);
        }
    }
}

std::optional<UUID> ScopedAllocatorAccess::GetOwnerClientId(
    const std::string& segment_name) const {
    if (client_by_name_ == nullptr) {
        return std::nullopt;
    }
    auto it = client_by_name_->find(segment_name);
    if (it == client_by_name_->end()) {
        return std::nullopt;
    }
    return it->second;
}

ErrorCode ScopedSegmentAccess::MountSegment(
    const Segment& segment, const UUID& client_id,
    std::shared_ptr<ClientLivenessRecord> client_liveness) {
    if (segment.locality && segment.locality->numa_node < 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    const auto indexed_name =
        segment_manager_->segment_id_by_name_.find(segment.name);
    if (indexed_name != segment_manager_->segment_id_by_name_.end()) {
        const auto indexed_segment =
            segment_manager_->mounted_segments_.find(indexed_name->second);
        if (indexed_segment == segment_manager_->mounted_segments_.end()) {
            // Offboarding retains this name until its durable unmount record
            // has been accepted. Live same-name Segments remain supported.
            return ErrorCode::INVALID_PARAMS;
        }
        if (indexed_segment->second.status == SegmentStatus::UNMOUNTING) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    }
    // SEGMENT_ALREADY_EXISTS is a liveness signal only for a confirmed replay
    // of the same mount. Keep this common to every memory protocol, including
    // CXL, instead of letting protocol-specific branches bypass it.
    auto existing = segment_manager_->mounted_segments_.find(segment.id);
    if (existing != segment_manager_->mounted_segments_.end()) {
        if (existing->second.status != SegmentStatus::OK) {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
        const auto owner = segment_manager_->client_segments_.find(client_id);
        const bool owned = owner != segment_manager_->client_segments_.end() &&
                           std::find(owner->second.begin(), owner->second.end(),
                                     segment.id) != owner->second.end();
        auto& mounted = existing->second.segment;
        if (!owned || mounted.name != segment.name ||
            mounted.base != segment.base || mounted.size != segment.size ||
            mounted.te_endpoint != segment.te_endpoint ||
            mounted.protocol != segment.protocol ||
            (mounted.locality && segment.locality &&
             *mounted.locality != *segment.locality)) {
            return ErrorCode::INVALID_PARAMS;
        }
        if (!mounted.locality && segment.locality) {
            mounted.locality = segment.locality;
            if (existing->second.buf_allocator) {
                existing->second.buf_allocator->SetLocality(segment.locality);
            }
        }
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

    // Check if cxl storage is enable
    if (segment_manager_->enable_cxl_ && segment.protocol == "cxl") {
        LOG(INFO) << "Start Mounting CXL Segment.";
        if (segment_manager_->memory_allocator_ ==
            BufferAllocatorType::CACHELIB) {
            auto allocator = segment_manager_->cxl_global_allocator_;
            if (segment_manager_->cxl_global_allocator_ == nullptr) {
                LOG(ERROR) << "Cxl global allocator has not been initialized.";
                return ErrorCode::INTERNAL_ERROR;
            }
            auto registration =
                segment_manager_->allocator_manager_.addAllocator(
                    segment.name, allocator, std::move(client_liveness));
            segment_manager_->client_segments_[client_id].push_back(segment.id);
            segment_manager_->mounted_segments_[segment.id] = {
                segment, SegmentStatus::OK, allocator, registration};
            segment_manager_->client_by_name_[segment.name] = client_id;
            segment_manager_->segment_id_by_name_[segment.name] = segment.id;
            AddHostSegment(segment_manager_->segments_by_host_, segment);

            LOG(INFO) << "[CXL Segment Mounted Successfully] Segment name: "
                      << segment.name
                      << ", Mount size: " << (size / 1024 / 1024 / 1024)
                      << " GB";
            return ErrorCode::OK;
        }
        return ErrorCode::INTERNAL_ERROR;
    }
    // Check if parameters are valid before allocating memory.
    if (buffer == 0 || size == 0) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is invalid";
        return ErrorCode::INVALID_PARAMS;
    }

    if (segment_manager_->memory_allocator_ == BufferAllocatorType::CACHELIB &&
        (buffer % facebook::cachelib::Slab::kSize ||
         size % facebook::cachelib::Slab::kSize)) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize
                   << " as required by Cachelib";
        return ErrorCode::INVALID_PARAMS;
    }

    auto created =
        CreateBufferAllocator(segment_manager_->memory_allocator_, segment.name,
                              buffer, size, segment.te_endpoint);
    if (!created) {
        return created.error();
    }
    auto allocator = std::move(*created);
    allocator->SetLocality(segment.locality);

    allocator->AttachUsageTracker(segment_manager_->usage_tracker_);
    auto registration = segment_manager_->allocator_manager_.addAllocator(
        segment.name, allocator, std::move(client_liveness));
    segment_manager_->client_segments_[client_id].push_back(segment.id);
    segment_manager_->mounted_segments_[segment.id] = {
        segment, SegmentStatus::OK, std::move(allocator), registration};
    segment_manager_->client_by_name_[segment.name] = client_id;
    segment_manager_->segment_id_by_name_[segment.name] = segment.id;
    AddHostSegment(segment_manager_->segments_by_host_, segment);
    MasterMetricManager::instance().inc_total_mem_capacity(segment.name, size);

    return ErrorCode::OK;
}

void ScopedSegmentAccess::BindClientLiveness(
    const UUID& client_id,
    const std::shared_ptr<ClientLivenessRecord>& client_liveness) {
    const auto client_segments_it =
        segment_manager_->client_segments_.find(client_id);
    if (client_segments_it != segment_manager_->client_segments_.end()) {
        for (const auto& segment_id : client_segments_it->second) {
            auto mounted_it =
                segment_manager_->mounted_segments_.find(segment_id);
            if (mounted_it != segment_manager_->mounted_segments_.end() &&
                mounted_it->second.allocator_registration) {
                mounted_it->second.allocator_registration->BindClientLiveness(
                    client_liveness);
            }
        }
    }
}

void ScopedSegmentAccess::BindBufferToSegment(const UUID& segment_id,
                                              AllocatedBuffer& buffer) {
    segment_manager_->mounted_segments_.at(segment_id)
        .allocator_registration->BindBuffer(buffer);
}

bool ScopedSegmentAccess::RebindBufferToOwningSegment(AllocatedBuffer& buffer) {
    for (auto& [segment_id, mounted] : segment_manager_->mounted_segments_) {
        (void)segment_id;
        if (mounted.allocator_registration &&
            mounted.allocator_registration->OwnsBuffer(buffer)) {
            mounted.allocator_registration->BindBuffer(buffer);
            return true;
        }
    }
    return false;
}

ErrorCode ScopedSegmentAccess::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id,
    std::shared_ptr<ClientLivenessRecord> client_liveness) {
    for (const auto& segment : segments) {
        auto validation = ValidateRemountSegment(segment, client_id);
        if (validation != ErrorCode::OK) {
            return validation;
        }
        ErrorCode err = MountSegment(segment, client_id, client_liveness);
        if (err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
            err == ErrorCode::INTERNAL_ERROR) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=fail_to_remount_segment";
            return err;
        } else if (err == ErrorCode::INVALID_PARAMS) {
            // Ignore INVALID_PARAMS. This error cannot be solved by a new
            // remount request.
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=invalid_params";
        } else if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            // Segment already exists, no need to remount.
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=segment_already_exists";
        } else if (err != ErrorCode::OK) {
            // Ignore other errors. The error may not be solvable by a new
            // remount request.
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=unexpected_error (" << err << ")";
        }
    }

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::ValidateRemountSegment(
    const Segment& segment, const UUID& client_id) const {
    auto mounted = segment_manager_->mounted_segments_.find(segment.id);
    if (mounted == segment_manager_->mounted_segments_.end()) {
        return ErrorCode::OK;
    }
    const auto owner = segment_manager_->client_segments_.find(client_id);
    const bool owned = owner != segment_manager_->client_segments_.end() &&
                       std::find(owner->second.begin(), owner->second.end(),
                                 segment.id) != owner->second.end();
    const auto& authoritative = mounted->second.segment;
    if (!owned || authoritative.id != segment.id ||
        authoritative.name != segment.name ||
        authoritative.base != segment.base ||
        authoritative.size != segment.size ||
        authoritative.te_endpoint != segment.te_endpoint ||
        authoritative.protocol != segment.protocol ||
        authoritative.host_id != segment.host_id ||
        (authoritative.locality && segment.locality &&
         *authoritative.locality != *segment.locality)) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

bool ScopedSegmentAccess::GetSegment(const UUID& segment_id,
                                     Segment& segment) const {
    auto mounted = segment_manager_->mounted_segments_.find(segment_id);
    if (mounted == segment_manager_->mounted_segments_.end()) {
        return false;
    }
    segment = mounted->second.segment;
    return true;
}

bool ScopedSegmentAccess::ReplaceAllocators(
    const std::vector<AllocatorReplacement>& replacements) {
    std::vector<AllocatorManager::Replacement> manager_replacements;
    manager_replacements.reserve(replacements.size());
    for (const auto& replacement : replacements) {
        auto mounted =
            segment_manager_->mounted_segments_.find(replacement.segment_id);
        if (mounted == segment_manager_->mounted_segments_.end() ||
            !mounted->second.allocator_registration ||
            mounted->second.allocator_registration->GetAllocator() !=
                replacement.expected ||
            !replacement.replacement) {
            return false;
        }
        manager_replacements.push_back({mounted->second.segment.name,
                                        replacement.expected,
                                        replacement.replacement});
        if (replacement.expected != replacement.replacement) {
            replacement.replacement->SetLocality(
                mounted->second.segment.locality);
        }
    }
    if (!segment_manager_->allocator_manager_.replaceAllocators(
            manager_replacements)) {
        return false;
    }
    for (const auto& replacement : replacements) {
        if (replacement.expected != replacement.replacement) {
            replacement.replacement->AttachUsageTracker(
                segment_manager_->usage_tracker_);
        }
        segment_manager_->mounted_segments_.at(replacement.segment_id)
            .buf_allocator = replacement.replacement;
    }
    return true;
}

std::shared_ptr<BufferAllocatorBase> ScopedSegmentAccess::GetAllocator(
    const UUID& segment_id) const {
    auto mounted = segment_manager_->mounted_segments_.find(segment_id);
    return mounted == segment_manager_->mounted_segments_.end()
               ? nullptr
               : mounted->second.buf_allocator;
}

ErrorCode ScopedSegmentAccess::PrepareUnmountSegment(
    const UUID& segment_id, size_t& metrics_dec_capacity) {
    auto it = segment_manager_->mounted_segments_.find(segment_id);
    if (it == segment_manager_->mounted_segments_.end()) {
        LOG(WARNING) << "segment_id=" << segment_id
                     << ", warn=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (it->second.status == SegmentStatus::UNMOUNTING) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_is_unmounting";
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    auto& mounted_segment = it->second;
    auto& segment = mounted_segment.segment;
    metrics_dec_capacity = segment.size;

    // Remove the allocator from the segment manager
    auto registration = mounted_segment.allocator_registration;

    // 1. Remove from allocators if the segment is still allocatable.
    if (HasAllocatorRegistration(segment_manager_->allocator_manager_,
                                 segment.name, registration)) {
        segment_manager_->allocator_manager_.removeAllocator(segment.name,
                                                             registration);
    }
    RemoveHostSegment(segment_manager_->segments_by_host_, segment);

    // 2. Invalidate detached allocation snapshots and existing buffers, then
    // remove the registration from mounted_segment. Do not detach usage here:
    // in-flight deallocate calls hold a local shared_ptr and must finish first.
    // The allocator destructor RAII-detaches when the last shared_ptr is
    // dropped.
    registration->Invalidate();
    mounted_segment.allocator_registration.reset();
    mounted_segment.buf_allocator.reset();

    // Set the segment status to UNMOUNTING
    mounted_segment.status = SegmentStatus::UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::PrepareGracefulUnmountSegment(
    const UUID& segment_id) {
    auto it = segment_manager_->mounted_segments_.find(segment_id);
    if (it == segment_manager_->mounted_segments_.end()) {
        LOG(WARNING) << "segment_id=" << segment_id
                     << ", warn=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    auto status = it->second.status;
    if (status == SegmentStatus::UNMOUNTING) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_is_unmounting";
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    if (status == SegmentStatus::GRACEFULLY_UNMOUNTING) {
        // Idempotent: already in graceful unmount state
        return ErrorCode::OK;
    }
    if (status != SegmentStatus::OK && status != SegmentStatus::DRAINING) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=unavailable_in_current_status, status="
                   << status;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    auto& mounted_segment = it->second;
    auto& segment = mounted_segment.segment;

    // Remove the allocator from the segment manager
    auto registration = mounted_segment.allocator_registration;
    if (HasAllocatorRegistration(segment_manager_->allocator_manager_,
                                 segment.name, registration)) {
        segment_manager_->allocator_manager_.removeAllocator(segment.name,
                                                             registration);
    }
    registration->SetAllocatable(false);
    RemoveHostSegment(segment_manager_->segments_by_host_, segment);
    // Set the segment status to GRACEFULLY_UNMOUNTING
    mounted_segment.status = SegmentStatus::GRACEFULLY_UNMOUNTING;
    return ErrorCode::OK;
}

void ScopedSegmentAccess::ReindexSegmentNameAfterRemoval(
    const UUID& removed_segment_id, const std::string& segment_name) {
    const auto indexed =
        segment_manager_->segment_id_by_name_.find(segment_name);
    if (indexed == segment_manager_->segment_id_by_name_.end() ||
        indexed->second != removed_segment_id) {
        return;
    }

    segment_manager_->segment_id_by_name_.erase(indexed);
    segment_manager_->client_by_name_.erase(segment_name);

    const auto bind_remaining = [&](bool require_ok) {
        for (const auto& [candidate_id, mounted] :
             segment_manager_->mounted_segments_) {
            if (candidate_id == removed_segment_id ||
                mounted.segment.name != segment_name ||
                (require_ok && mounted.status != SegmentStatus::OK)) {
                continue;
            }
            for (const auto& [owner, segment_ids] :
                 segment_manager_->client_segments_) {
                if (std::find(segment_ids.begin(), segment_ids.end(),
                              candidate_id) != segment_ids.end()) {
                    segment_manager_->segment_id_by_name_[segment_name] =
                        candidate_id;
                    segment_manager_->client_by_name_[segment_name] = owner;
                    return true;
                }
            }
        }
        return false;
    };

    if (!bind_remaining(/*require_ok=*/true)) {
        (void)bind_remaining(/*require_ok=*/false);
    }
}

ErrorCode ScopedSegmentAccess::CommitUnmountSegment(
    const UUID& segment_id, const UUID& client_id,
    const size_t& metrics_dec_capacity, bool retain_name_registration) {
    auto mounted = segment_manager_->mounted_segments_.find(segment_id);
    if (mounted == segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    // Remove from client_segments_
    bool found_in_client_segments = false;
    auto client_it = segment_manager_->client_segments_.find(client_id);
    if (client_it != segment_manager_->client_segments_.end()) {
        auto& segments = client_it->second;
        auto segment_it =
            std::find(segments.begin(), segments.end(), segment_id);
        if (segment_it != segments.end()) {
            segments.erase(segment_it);
            found_in_client_segments = true;
        }
        if (segments.empty()) {
            segment_manager_->client_segments_.erase(client_it);
        }
    }
    if (!found_in_client_segments) {
        LOG(ERROR) << "segment_id=" << segment_id
                   << ", error=segment_not_found_in_client_segments";
    }

    // segment_id -> segment_name
    const std::string segment_name = mounted->second.segment.name;
    const bool is_cxl = mounted->second.segment.protocol == "cxl";
    RemoveHostSegment(segment_manager_->segments_by_host_,
                      mounted->second.segment);
    if (!retain_name_registration) {
        ReindexSegmentNameAfterRemoval(segment_id, segment_name);
    }
    // Remove from mounted_segments_
    segment_manager_->mounted_segments_.erase(mounted);

    // Decrease the total capacity
    if (!is_cxl) {
        MasterMetricManager::instance().dec_total_mem_capacity(
            segment_name, metrics_dec_capacity);
        // Remove per-segment metric labels entirely to avoid stale 0-value
        // entries persisting in Prometheus output (e.g. after snapshot
        // restore followed by client expiry / reaper cleanup).
        MasterMetricManager::instance().remove_segment_metrics(segment_name);
    }

    return ErrorCode::OK;
}

void ScopedSegmentAccess::ReleaseUnmountedSegmentName(
    const UUID& segment_id, const std::string& segment_name) {
    ReindexSegmentNameAfterRemoval(segment_id, segment_name);
}

ErrorCode ScopedSegmentAccess::GetClientSegments(
    const UUID& client_id, std::vector<Segment>& segments) const {
    auto it = segment_manager_->client_segments_.find(client_id);
    if (it == segment_manager_->client_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (auto& segment_id : it->second) {
        auto segment_it = segment_manager_->mounted_segments_.find(segment_id);
        if (segment_it != segment_manager_->mounted_segments_.end()) {
            segments.emplace_back(segment_it->second.segment);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetAllSegments(
    std::vector<std::string>& all_segments) {
    all_segments.clear();
    for (auto& segment : segment_manager_->mounted_segments_) {
        if (segment.second.status == SegmentStatus::OK) {
            all_segments.push_back(segment.second.segment.name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetAllSegments(
    std::vector<std::pair<Segment, UUID>>& all_segments) {
    all_segments.clear();

    for (auto& segment_pair : segment_manager_->mounted_segments_) {
        UUID client_id{0, 0};
        // client_by_name_ is intentionally lossy because several Clients may
        // mount Segments with the same hostname. Resolve exact ownership from
        // the canonical client_id -> segment_ids relation.
        for (const auto& [owner, segment_ids] :
             segment_manager_->client_segments_) {
            if (std::find(segment_ids.begin(), segment_ids.end(),
                          segment_pair.first) != segment_ids.end()) {
                client_id = owner;
                break;
            }
        }

        all_segments.emplace_back(segment_pair.second.segment, client_id);
    }
    return ErrorCode::OK;
}

std::vector<std::string> ScopedSegmentAccess::GetHostOrderedSegments(
    const std::string& writer_host_id, const std::string& key) const {
    return BuildHostOrderedSegments(segment_manager_->segments_by_host_,
                                    writer_host_id, key);
}

ErrorCode ScopedSegmentAccess::GetAllSegmentNames(
    std::vector<std::string>& all_segment_names) {
    all_segment_names.clear();
    for (auto& segment : segment_manager_->mounted_segments_) {
        all_segment_names.push_back(segment.second.segment.name);
    }
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::QuerySegments(const std::string& segment,
                                             size_t& used, size_t& capacity) {
    size_t total_used = 0, total_capacity = 0;
    const auto& allocator_manager = segment_manager_->allocator_manager_;
    const auto& allocators = allocator_manager.getAllocators(segment);
    if (allocators != nullptr) {
        for (const auto& registration : *allocators) {
            const auto allocator = registration->GetAllocator();
            total_used += allocator->size();
            total_capacity += allocator->capacity();
        }
    }

    if (total_capacity == 0) {
        VLOG(1) << "### DEBUG ### MasterService::QuerySegments(" << segment
                << ") not found!";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    used = total_used;
    capacity = total_capacity;

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetUnreadySegments(
    std::vector<std::pair<Segment, UUID>>& unready_segments) const {
    unready_segments.clear();

    for (const auto& pair : segment_manager_->mounted_segments_) {
        const auto& mounted_segment = pair.second;
        if (mounted_segment.status == SegmentStatus::OK) {
            continue;
        }

        UUID client_id{0, 0};
        auto client_it = segment_manager_->client_by_name_.find(
            mounted_segment.segment.name);
        if (client_it != segment_manager_->client_by_name_.end()) {
            client_id = client_it->second;
        }

        unready_segments.emplace_back(mounted_segment.segment, client_id);
    }

    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetClientIdBySegmentName(
    const std::string& segment_name, UUID& client_id) const {
    auto it = segment_manager_->client_by_name_.find(segment_name);
    if (it == segment_manager_->client_by_name_.end()) {
        LOG(ERROR) << "segment_name=" << segment_name
                   << ", error=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    client_id = it->second;
    return ErrorCode::OK;
}

bool ScopedSegmentAccess::ExistsSegmentName(
    const std::string& segment_name) const {
    auto it = segment_manager_->client_by_name_.find(segment_name);
    return it != segment_manager_->client_by_name_.end();
}

bool ScopedSegmentAccess::IsSegmentAllocatable(
    const std::string& segment_name) const {
    auto segment_id_it =
        segment_manager_->segment_id_by_name_.find(segment_name);
    if (segment_id_it == segment_manager_->segment_id_by_name_.end()) {
        return false;
    }
    auto mounted_segment_it =
        segment_manager_->mounted_segments_.find(segment_id_it->second);
    return mounted_segment_it != segment_manager_->mounted_segments_.end() &&
           mounted_segment_it->second.status == SegmentStatus::OK &&
           mounted_segment_it->second.allocator_registration->IsServing();
}

ErrorCode ScopedSegmentAccess::GetSegmentStatusByName(
    const std::string& segment_name, SegmentStatus& status) const {
    auto segment_id_it =
        segment_manager_->segment_id_by_name_.find(segment_name);
    if (segment_id_it == segment_manager_->segment_id_by_name_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    auto mounted_segment_it =
        segment_manager_->mounted_segments_.find(segment_id_it->second);
    if (mounted_segment_it == segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    status = mounted_segment_it->second.status;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::GetSegmentStatusById(
    const UUID& segment_id, SegmentStatus& status) const {
    auto mounted_segment_it =
        segment_manager_->mounted_segments_.find(segment_id);
    if (mounted_segment_it == segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    status = mounted_segment_it->second.status;
    return ErrorCode::OK;
}

ErrorCode ScopedSegmentAccess::SetSegmentStatusByName(
    const std::string& segment_name, SegmentStatus status) {
    auto segment_id_it =
        segment_manager_->segment_id_by_name_.find(segment_name);
    if (segment_id_it == segment_manager_->segment_id_by_name_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    auto mounted_segment_it =
        segment_manager_->mounted_segments_.find(segment_id_it->second);
    if (mounted_segment_it == segment_manager_->mounted_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    auto& mounted_segment = mounted_segment_it->second;
    if (mounted_segment.status == SegmentStatus::UNMOUNTING) {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    if (mounted_segment.status == status) {
        return ErrorCode::OK;
    }

    auto& allocator_manager = segment_manager_->allocator_manager_;
    const auto& registration = mounted_segment.allocator_registration;
    const auto& name = mounted_segment.segment.name;
    const bool should_be_allocatable = status == SegmentStatus::OK;
    const bool is_allocatable =
        HasAllocatorRegistration(allocator_manager, name, registration);
    if (should_be_allocatable && !is_allocatable && registration) {
        registration->SetAllocatable(true);
        allocator_manager.addRegistration(name, registration);
        AddHostSegment(segment_manager_->segments_by_host_,
                       mounted_segment.segment);
    } else if (!should_be_allocatable && is_allocatable) {
        allocator_manager.removeAllocator(name, registration);
        registration->SetAllocatable(false);
        RemoveHostSegment(segment_manager_->segments_by_host_,
                          mounted_segment.segment);
    }

    mounted_segment.status = status;
    return ErrorCode::OK;
}

/* ScopedNoFSegmentAccess Implementation */
ErrorCode ScopedNoFSegmentAccess::MountSegment(const NoFSegment& segment,
                                               const UUID& client_id) {
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

    // NoF segment base is an NVMe namespace offset, so 0 is valid.
    if (size == 0) {
        LOG(ERROR) << "NoF segment mount: buffer=" << buffer
                   << " or size=" << size << " is invalid";
        return ErrorCode::INVALID_PARAMS;
    }

    if (nof_segment_manager_->memory_allocator_ ==
            BufferAllocatorType::CACHELIB &&
        (buffer % facebook::cachelib::Slab::kSize ||
         size % facebook::cachelib::Slab::kSize)) {
        LOG(ERROR) << "NoF segment mount: buffer=" << buffer
                   << " or size=" << size << " is not aligned to "
                   << facebook::cachelib::Slab::kSize
                   << " as required by Cachelib";
        return ErrorCode::INVALID_PARAMS;
    }

    auto exist_segment_it =
        nof_segment_manager_->mounted_segments_.find(segment.id);
    if (exist_segment_it != nof_segment_manager_->mounted_segments_.end()) {
        auto& exist_segment = exist_segment_it->second;
        if (exist_segment.status == SegmentStatus::OK) {
            LOG(WARNING) << "NoF segment mount: segment_name=" << segment.name
                         << ", warn=segment_already_exists_by_id";
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        }
        LOG(ERROR) << "NoF segment mount: segment_name=" << segment.name
                   << ", error=segment_already_exists_but_not_ok"
                   << ", status=" << exist_segment.status;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    // Treat the same transport endpoint as the same remote SSD namespace even
    // if a retry arrives with a different generated UUID.
    for (const auto& [existing_id, existing_segment] :
         nof_segment_manager_->mounted_segments_) {
        if (existing_segment.status == SegmentStatus::OK &&
            existing_segment.segment.te_endpoint == segment.te_endpoint) {
            LOG(WARNING) << "NoF segment mount: segment_name=" << segment.name
                         << ", endpoint=" << segment.te_endpoint
                         << ", warn=segment_already_exists_with_different_id";
            return ErrorCode::SEGMENT_ALREADY_EXISTS;
        }
    }

    auto created = CreateBufferAllocator(
        nof_segment_manager_->memory_allocator_, segment.name, buffer, size,
        segment.te_endpoint, ReplicaType::NOF_SSD);
    if (!created) {
        return created.error();
    }
    auto allocator = std::move(*created);

    allocator->AttachUsageTracker(nof_segment_manager_->usage_tracker_);
    auto registration = nof_segment_manager_->allocator_manager_.addAllocator(
        segment.name, allocator);
    nof_segment_manager_->client_segments_[client_id].push_back(segment.id);
    nof_segment_manager_->mounted_segments_[segment.id] = {
        segment, client_id, SegmentStatus::OK, std::move(allocator),
        registration};
    nof_segment_manager_->client_by_name_[segment.name] = client_id;
    MasterMetricManager::instance().inc_total_nof_capacity(segment.name, size);

    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentAccess::ReMountSegment(
    const std::vector<NoFSegment>& segments, const UUID& client_id) {
    for (const auto& segment : segments) {
        ErrorCode err = MountSegment(segment, client_id);
        if (err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
            err == ErrorCode::INTERNAL_ERROR) {
            LOG(ERROR) << "NoF segment remount: segment_name=" << segment.name
                       << ", error=fail_to_remount_segment";
            return err;
        }
        if (err == ErrorCode::INVALID_PARAMS) {
            LOG(WARNING) << "NoF segment remount: segment_name=" << segment.name
                         << ", warn=invalid_params";
        } else if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            LOG(WARNING) << "NoF segment remount: segment_name=" << segment.name
                         << ", warn=segment_already_exists";
        } else if (err != ErrorCode::OK) {
            LOG(ERROR) << "NoF segment remount: segment_name=" << segment.name
                       << ", error=unexpected_error (" << err << ")";
        }
    }

    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentAccess::PrepareUnmountSegment(
    const UUID& segment_id, size_t& metrics_dec_capacity) {
    auto it = nof_segment_manager_->mounted_segments_.find(segment_id);
    if (it == nof_segment_manager_->mounted_segments_.end()) {
        LOG(WARNING) << "NoF segment unmount: segment_id=" << segment_id
                     << ", warn=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    if (it->second.status == SegmentStatus::UNMOUNTING) {
        LOG(ERROR) << "NoF segment unmount: segment_id=" << segment_id
                   << ", error=segment_is_unmounting";
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    auto& mounted_segment = it->second;
    auto& segment = mounted_segment.segment;
    metrics_dec_capacity = segment.size;

    auto registration = mounted_segment.allocator_registration;
    if (HasAllocatorRegistration(nof_segment_manager_->allocator_manager_,
                                 segment.name, registration)) {
        nof_segment_manager_->allocator_manager_.removeAllocator(segment.name,
                                                                 registration);
    }

    registration->Invalidate();
    mounted_segment.allocator_registration.reset();
    mounted_segment.buf_allocator.reset();
    mounted_segment.status = SegmentStatus::UNMOUNTING;
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentAccess::CommitUnmountSegment(
    const UUID& segment_id, const UUID& client_id,
    const size_t& metrics_dec_capacity) {
    bool found_in_client_segments = false;
    auto client_it = nof_segment_manager_->client_segments_.find(client_id);
    if (client_it != nof_segment_manager_->client_segments_.end()) {
        auto& segments = client_it->second;
        auto segment_it =
            std::find(segments.begin(), segments.end(), segment_id);
        if (segment_it != segments.end()) {
            segments.erase(segment_it);
            found_in_client_segments = true;
        }
        if (segments.empty()) {
            nof_segment_manager_->client_segments_.erase(client_it);
        }
    }
    if (!found_in_client_segments) {
        LOG(ERROR) << "NoF segment unmount: segment_id=" << segment_id
                   << ", error=segment_not_found_in_client_segments";
    }

    std::string segment_name;
    auto segment_it = nof_segment_manager_->mounted_segments_.find(segment_id);
    if (segment_it != nof_segment_manager_->mounted_segments_.end()) {
        segment_name = segment_it->second.segment.name;
        nof_segment_manager_->client_by_name_.erase(segment_name);
    }

    nof_segment_manager_->mounted_segments_.erase(segment_id);
    MasterMetricManager::instance().dec_total_nof_capacity(
        segment_name, metrics_dec_capacity);
    MasterMetricManager::instance().remove_nof_segment_metrics(segment_name);

    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentAccess::GetClientSegments(
    const UUID& client_id, std::vector<NoFSegment>& segments) const {
    auto it = nof_segment_manager_->client_segments_.find(client_id);
    if (it == nof_segment_manager_->client_segments_.end()) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }
    segments.clear();
    for (auto& segment_id : it->second) {
        auto segment_it =
            nof_segment_manager_->mounted_segments_.find(segment_id);
        if (segment_it != nof_segment_manager_->mounted_segments_.end()) {
            segments.emplace_back(segment_it->second.segment);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentAccess::GetMountedSegments(
    std::vector<MountedNoFSegmentSnapshot>& segments) const {
    segments.clear();
    segments.reserve(nof_segment_manager_->mounted_segments_.size());
    for (const auto& it : nof_segment_manager_->mounted_segments_) {
        segments.push_back(MountedNoFSegmentSnapshot{
            .segment_id = it.first,
            .client_id = it.second.client_id,
            .segment = it.second.segment,
            .status = it.second.status,
        });
    }
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentAccess::GetAllSegments(
    std::vector<std::string>& all_segments) {
    all_segments.clear();
    for (auto& segment : nof_segment_manager_->mounted_segments_) {
        if (segment.second.status == SegmentStatus::OK) {
            all_segments.push_back(segment.second.segment.name);
        }
    }
    return ErrorCode::OK;
}

ErrorCode ScopedNoFSegmentAccess::QuerySegments(const std::string& segment,
                                                size_t& used,
                                                size_t& capacity) {
    size_t total_used = 0, total_capacity = 0;
    const auto& allocator_manager = nof_segment_manager_->allocator_manager_;
    const auto& allocators = allocator_manager.getAllocators(segment);
    if (allocators != nullptr) {
        for (const auto& registration : *allocators) {
            const auto allocator = registration->GetAllocator();
            total_used += allocator->size();
            total_capacity += allocator->capacity();
        }
    }

    if (total_capacity == 0) {
        VLOG(1) << "NoF segment query: segment=" << segment
                << ", error=segment_not_found";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    used = total_used;
    capacity = total_capacity;
    return ErrorCode::OK;
}

void NoFSegmentManager::GetMountedSegmentsSnapshot(
    std::vector<MountedNoFSegmentSnapshot>& segments) const {
    std::shared_lock<std::shared_mutex> lock(segment_mutex_);
    segments.clear();
    segments.reserve(mounted_segments_.size());
    for (const auto& [segment_id, mounted_segment] : mounted_segments_) {
        segments.push_back(MountedNoFSegmentSnapshot{
            segment_id, mounted_segment.client_id, mounted_segment.segment,
            mounted_segment.status});
    }
}

StorageUsageSnapshot NoFSegmentManager::GetUsageSnapshot() const {
    std::shared_lock<std::shared_mutex> lock(segment_mutex_);
    StorageUsageSnapshot snapshot;
    std::unordered_set<const BufferAllocatorBase*> counted_allocators;
    for (const auto& [segment_id, mounted_segment] : mounted_segments_) {
        (void)segment_id;
        AddAllocatorUsage(snapshot, mounted_segment.buf_allocator,
                          counted_allocators);
    }
    return snapshot;
}

void SegmentManager::releaseCapacityMetrics() {
    // Segments that are still mounted here never went through
    // CommitUnmountSegment, so their contribution to the capacity metrics
    // has not been released. MasterMetricManager outlives MasterService
    // instances (a new one is constructed per HA leadership term), so the
    // serving instance releases it at teardown to keep the gauges
    // consistent with the segments that are actually mounted.
    for (const auto& [segment_id, mounted_segment] : mounted_segments_) {
        const auto& segment = mounted_segment.segment;
        if (segment.protocol == "cxl") {
            // CXL mounts do not contribute to total_mem_capacity.
            continue;
        }
        MasterMetricManager::instance().dec_total_mem_capacity(segment.name,
                                                               segment.size);
        MasterMetricManager::instance().remove_segment_metrics(segment.name);
    }
}

ErrorCode SegmentManager::initializeCxlAllocator(const std::string& cxl_path,
                                                 size_t cxl_size) {
    LOG(INFO) << "Init CXL global allocator.";
    LOG(INFO) << "[CXL] create allocator with "
              << "path=" << cxl_path << " base=0x" << std::hex
              << DEFAULT_CXL_BASE << std::dec << " size=" << cxl_size << " ("
              << std::fixed << std::setprecision(2)
              << cxl_size / (1024.0 * 1024 * 1024) << " GB)";

    auto created =
        CreateBufferAllocator(BufferAllocatorType::CACHELIB, cxl_path,
                              DEFAULT_CXL_BASE, cxl_size, cxl_path);
    if (!created) {
        return created.error();
    }
    auto allocator = std::move(*created);
    allocator->AttachUsageTracker(usage_tracker_);
    {
        std::unique_lock<std::shared_mutex> lock(segment_mutex_);
        cxl_global_allocator_ = std::move(allocator);
    }
    MasterMetricManager::instance().inc_total_mem_capacity(cxl_path, cxl_size);
    return ErrorCode::OK;
}

bool SegmentManager::HasSegmentByEndpoint(const std::string& endpoint) const {
    std::shared_lock<std::shared_mutex> lock(segment_mutex_);
    for (const auto& [segment_id, mounted_segment] : mounted_segments_) {
        if (mounted_segment.segment.te_endpoint == endpoint) {
            return true;
        }
    }
    return false;
}

bool SegmentManager::GetSegmentBasicInfo(const UUID& segment_id,
                                         std::string& segment_name,
                                         std::string& te_endpoint) const {
    std::shared_lock<std::shared_mutex> lock(segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        return false;
    }
    const Segment& seg = it->second.segment;
    segment_name = seg.name;
    te_endpoint = seg.te_endpoint;
    return true;
}
}  // namespace mooncake
