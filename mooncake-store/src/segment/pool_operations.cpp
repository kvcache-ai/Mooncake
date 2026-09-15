#include "pool_operations_internal.h"

namespace mooncake {

ClientUnmountBatch SegmentPool::WriteAccess::BeginClientUnmount(
    const ClientSessionPtr& session) {
    if (!session) return {};
    const auto& client_id = session->client_id();
    std::vector<Segment> segments;
    (void)GetClientSegments(client_id, segments);
    ClientUnmountBatch batch;
    batch.prepared.reserve(segments.size());
    batch.pending.reserve(segments.size());
    for (const auto& segment : segments) {
        auto prepared = BeginUnmount(segment.id, session);
        if (prepared) {
            batch.prepared.push_back(std::move(*prepared));
        } else if (prepared.error() != ErrorCode::SEGMENT_NOT_FOUND) {
            batch.pending.push_back({segment, prepared.error()});
        }
    }
    return batch;
}

tl::expected<SegmentUnmountOperation, ErrorCode>
SegmentPool::WriteAccess::BeginUnmount(const UUID& segment_id,
                                       const ClientSessionPtr& session) {
    if (!session) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    const auto* mounted = catalog_.Find(segment_id);
    const auto* resource =
        mounted ? segment_pool_.GetResource(*mounted) : nullptr;
    // An old offboarding job must not touch a newer incarnation's resources.
    if (!resource || resource->candidate->client_session() != session)
        return tl::unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    return BeginUnmount(segment_id, session->client_id());
}

bool SegmentPool::WriteAccess::IsNameReserved(std::string_view name) const {
    return segment_pool_.reserved_names_.contains(std::string(name));
}

tl::expected<SegmentUnmountOperation, ErrorCode>
SegmentPool::WriteAccess::BeginUnmount(const UUID& segment_id,
                                       const UUID& client_id) {
    if (auto existing = segment_pool_.unmount_by_region_.find(segment_id);
        existing != segment_pool_.unmount_by_region_.end()) {
        const auto& state = *segment_pool_.unmounts_.at(existing->second);
        if (state.client_id != client_id)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        return SegmentUnmountOperation{existing->second, state.segment};
    }
    const auto* region = catalog_.Find(segment_id);
    if (!region) return tl::unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    if (region->client_id != client_id)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    // Allocate all bookkeeping before changing resource visibility.
    const UUID id = generate_uuid();
    auto state = std::make_unique<UnmountState>();
    state->segment = region->segment;
    state->client_id = client_id;
    const Segment descriptor = state->segment;
    segment_pool_.unmounts_.emplace(id, std::move(state));
    try {
        segment_pool_.unmount_by_region_.emplace(segment_id, id);
        ++segment_pool_.reserved_names_[descriptor.name];
    } catch (...) {
        segment_pool_.unmount_by_region_.erase(segment_id);
        segment_pool_.unmounts_.erase(id);
        throw;
    }
    auto prepared = PrepareUnmount(segment_id, client_id);
    if (!prepared) {
        if (--segment_pool_.reserved_names_.at(descriptor.name) == 0)
            segment_pool_.reserved_names_.erase(descriptor.name);
        segment_pool_.unmount_by_region_.erase(segment_id);
        segment_pool_.unmounts_.erase(id);
        return tl::unexpected(prepared.error());
    }
    segment_pool_.unmounts_.at(id)->transaction.emplace(std::move(*prepared));
    return SegmentUnmountOperation{id, descriptor};
}

ErrorCode SegmentPool::WriteAccess::ReleaseUnmountedResources(const UUID& id) {
    auto found = segment_pool_.unmounts_.find(id);
    if (found == segment_pool_.unmounts_.end())
        return ErrorCode::SEGMENT_NOT_FOUND;
    auto& state = *found->second;
    if (state.released) return ErrorCode::OK;
    const auto result = state.transaction->TryCommit(*this);
    if (result != ErrorCode::OK && result != ErrorCode::SEGMENT_NOT_FOUND)
        return result;
    state.transaction.reset();
    state.released = true;
    return ErrorCode::OK;
}

ErrorCode SegmentPool::WriteAccess::AcknowledgeUnmount(const UUID& id) {
    const auto found = segment_pool_.unmounts_.find(id);
    if (found == segment_pool_.unmounts_.end()) return ErrorCode::OK;
    const auto& state = *found->second;
    if (!state.released) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    if (--segment_pool_.reserved_names_.at(state.segment.name) == 0)
        segment_pool_.reserved_names_.erase(state.segment.name);
    segment_pool_.unmount_by_region_.erase(state.segment.id);
    segment_pool_.unmounts_.erase(found);
    return ErrorCode::OK;
}

ErrorCode SegmentPool::WriteAccess::StartGracefulUnmount(
    const UUID& segment_id, const UUID& client_id) {
    auto prepared = PrepareGracefulUnmount(segment_id, client_id);
    return prepared ? ErrorCode::OK : prepared.error();
}

ErrorCode SegmentPool::WriteAccess::StartDrain(
    std::span<const std::string> sources,
    std::span<const std::string> targets) {
    const auto valid = ValidateDrain(sources, targets);
    if (valid != ErrorCode::OK) return valid;
    size_t changed = 0;
    for (const auto& name : sources) {
        auto result = SetSegmentStatusByName(name, SegmentStatus::DRAINING);
        if (result != ErrorCode::OK) {
            for (size_t i = 0; i < changed; ++i)
                (void)SetSegmentStatusByName(sources[i], SegmentStatus::OK);
            return result;
        }
        ++changed;
    }
    return ErrorCode::OK;
}

void SegmentPool::WriteAccess::FinishDrain(std::string_view name) {
    (void)SetSegmentStatusByName(name, SegmentStatus::DRAINED);
}

void SegmentPool::WriteAccess::CancelDrain(std::span<const std::string> names) {
    for (const auto& name : names) {
        SegmentStatus status;
        if (GetSegmentStatus(name, status) == ErrorCode::OK &&
            status != SegmentStatus::UNMOUNTING)
            (void)SetSegmentStatusByName(name, SegmentStatus::OK);
    }
}

}  // namespace mooncake
