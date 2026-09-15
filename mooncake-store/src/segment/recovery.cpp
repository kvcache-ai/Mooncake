#include "pool_transaction_internal.h"

#include "segment/recovery.h"

#include <algorithm>
#include <limits>

#include "master_metric_manager.h"
#include "segment/pool_read_access.h"
#include "segment/pool_write_access.h"

namespace mooncake {

tl::expected<std::unique_ptr<SegmentRecovery>, ErrorCode>
SegmentRecovery::Create(std::vector<RecoverySegment> segments) {
    auto state = std::make_unique<SegmentRecovery>();
    state->segments_ = std::move(segments);
    state->ranges_.resize(state->segments_.size());
    for (size_t i = 0; i < state->segments_.size(); ++i) {
        const auto& segment = state->segments_[i];
        if (segment.name.empty() || segment.endpoint.empty() ||
            segment.capacity == 0 ||
            !state->aliases_.emplace(segment.endpoint, i).second ||
            (segment.name != segment.endpoint &&
             !state->aliases_.emplace(segment.name, i).second)) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto allocator = std::make_shared<DummyBufferAllocator>(
            segment.name, segment.endpoint);
        state->allocators_[segment.endpoint] = allocator;
        state->allocators_[segment.name] = std::move(allocator);
    }
    return state;
}

tl::expected<std::unique_ptr<AllocatedBuffer>, ErrorCode>
SegmentRecovery::Restore(const Replica::Descriptor& desc,
                         uint64_t object_size) {
    if (!desc.is_memory_replica())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    const auto& buffer = desc.get_memory_descriptor().buffer_descriptor;
    const auto found = aliases_.find(buffer.transport_endpoint_);
    if (found == aliases_.end() || buffer.size_ != object_size ||
        buffer.size_ == 0 ||
        buffer.buffer_address_ >
            std::numeric_limits<uintptr_t>::max() - buffer.size_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto index = found->second;
    const auto& segment = segments_[index];
    if (desc.status != ReplicaStatus::REMOVED &&
        desc.status != ReplicaStatus::FAILED) {
        auto& bytes = accounted_[segment.name];
        if (bytes > segment.capacity || buffer.size_ > segment.capacity - bytes)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        auto& ranges = ranges_[index];
        auto next = ranges.lower_bound(buffer.buffer_address_);
        const auto end = buffer.buffer_address_ + buffer.size_;
        if ((next != ranges.end() && end > next->first) ||
            (next != ranges.begin() &&
             std::prev(next)->first + std::prev(next)->second >
                 buffer.buffer_address_))
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        ranges.emplace(buffer.buffer_address_, buffer.size_);
        bytes += buffer.size_;
    }
    return std::make_unique<AllocatedBuffer>(
        allocators_.at(buffer.transport_endpoint_), buffer);
}

tl::expected<std::unique_ptr<AllocatedBuffer>, ErrorCode>
NoFBufferRecovery::Restore(const Replica::Descriptor& desc,
                           uint64_t object_size) {
    if (!desc.is_nof_replica())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    const auto& buffer = desc.get_nof_descriptor().buffer_descriptor;
    if (buffer.size_ == 0 || buffer.size_ != object_size)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    auto& allocator = allocators_[buffer.transport_endpoint_];
    if (!allocator)
        allocator = std::make_shared<DummyBufferAllocator>(
            buffer.transport_endpoint_, buffer.transport_endpoint_);
    return std::make_unique<AllocatedBuffer>(allocator, buffer);
}

bool SegmentPool::RestoreBufferBindings(
    const std::unordered_map<UUID, ClientSessionPtr, boost::hash<UUID>>&
        clients,
    std::span<AllocatedBuffer* const> buffers) {
    auto access = AcquireWriteAccess();
    if (!access.RestoreClientSessions(clients)) return false;
    bool valid = true;
    for (auto* buffer : buffers) {
        if (!buffer || !access.RebindBufferToOwningSegment(*buffer))
            valid = false;
    }
    return valid;
}

void SegmentPool::ClearRecovery() {
    if (!recovery_) return;
    for (const auto& [name, bytes] : recovery_->accounted_) {
        MasterMetricManager::instance().dec_allocated_mem_size(
            name, static_cast<int64_t>(bytes));
    }
    recovery_.reset();
}

std::unordered_set<std::string> SegmentPool::InstallRecovery(
    std::unique_ptr<SegmentRecovery> recovery) {
    auto access = AcquireWriteAccess();
    std::unordered_set<std::string> invalid;
    for (const auto& segment : recovery->segments_) {
        if (!access.HasEndpoint(segment.endpoint)) {
            invalid.insert(segment.endpoint);
            invalid.insert(segment.name);
        }
    }
    ClearRecovery();
    recovery_ = std::move(recovery);
    for (const auto& [name, bytes] : recovery_->accounted_) {
        MasterMetricManager::instance().inc_allocated_mem_size(
            name, static_cast<int64_t>(bytes));
    }
    return invalid;
}

ErrorCode SegmentPool::ValidateRemount(std::span<const Segment> segments,
                                       const UUID& client_id) const {
    std::unordered_set<UUID, boost::hash<UUID>> ids;
    for (const auto& segment : segments) {
        if (!ids.insert(segment.id).second ||
            reserved_names_.contains(segment.name))
            return ErrorCode::INVALID_PARAMS;
        if (recovery_) {
            const RecoverySegment* match = nullptr;
            for (const auto& entry : recovery_->segments_) {
                if (entry.endpoint != segment.te_endpoint &&
                    entry.name != segment.name)
                    continue;
                if (match) return ErrorCode::INVALID_PARAMS;
                match = &entry;
            }
            if (match && segment.protocol == "cxl")
                return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
            if (match && (match->name != segment.name ||
                          match->endpoint != segment.te_endpoint ||
                          match->capacity != segment.size))
                return ErrorCode::INVALID_PARAMS;
        }
        if (const auto* mounted = catalog_.Find(segment.id)) {
            if (mounted->client_id != client_id || mounted->segment != segment)
                return ErrorCode::INVALID_PARAMS;
            if (mounted->status == SegmentStatus::UNMOUNTING)
                return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }
    }
    return ErrorCode::OK;
}

tl::expected<RemountRequest, ErrorCode> SegmentPool::PlanRemount(
    std::span<const Segment> segments, const UUID& client_id) const {
    std::shared_lock lock(pool_mutex_);
    const auto result = ValidateRemount(segments, client_id);
    if (result != ErrorCode::OK) return tl::unexpected(result);
    RemountRequest request;
    request.client_id_ = client_id;
    for (const auto& segment : segments) {
        request.regions_.push_back({segment, {}, {}});
        if (recovery_ && recovery_->accounted_.contains(segment.name))
            request.needs_buffers_ = true;
    }
    return request;
}

tl::expected<bool, ErrorCode> RemountRequest::AddBuffer(
    size_t binding, AllocatedBuffer::Descriptor descriptor) {
    Region* match = nullptr;
    for (auto& region : regions_) {
        if (descriptor.transport_endpoint_ == region.segment.te_endpoint ||
            descriptor.transport_endpoint_ == region.segment.name) {
            if (match) return tl::unexpected(ErrorCode::INVALID_PARAMS);
            match = &region;
        }
    }
    if (!match) return false;
    if (!binding_ids_.insert(binding).second)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    descriptor.transport_endpoint_ = match->segment.te_endpoint;
    match->descriptors.push_back(std::move(descriptor));
    match->bindings.push_back(binding);
    return true;
}

struct PreparedRemount::State {
    SegmentPool& pool;
    SegmentPool::WriteAccess access;
    RemountRequest request;
    ClientSessionPtr liveness;
    std::vector<std::optional<RegionMountTxn>> transactions;
    std::unordered_map<std::string, uint64_t> imported;
    std::vector<RestoredBuffer> result;
    bool committed{false};
    State(SegmentPool& pool, RemountRequest request, ClientSessionPtr liveness)
        : pool(pool),
          access(pool.AcquireWriteAccess()),
          request(std::move(request)),
          liveness(std::move(liveness)) {}
};

PreparedRemount::PreparedRemount(std::unique_ptr<State> state)
    : state_(std::move(state)) {}
PreparedRemount::~PreparedRemount() = default;
PreparedRemount::PreparedRemount(PreparedRemount&&) noexcept = default;
PreparedRemount& PreparedRemount::operator=(PreparedRemount&&) noexcept =
    default;

tl::expected<PreparedRemount, ErrorCode> SegmentPool::PrepareRemount(
    RemountRequest request, ClientSessionPtr liveness) {
    if (!liveness || liveness->client_id() != request.client_id_)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    auto state = std::make_unique<PreparedRemount::State>(
        *this, std::move(request), std::move(liveness));
    if (!state->liveness->ShouldRetainResources())
        return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    std::vector<Segment> segments;
    for (const auto& region : state->request.regions_)
        segments.push_back(region.segment);
    auto validation = ValidateRemount(segments, state->request.client_id_);
    if (validation != ErrorCode::OK) return tl::unexpected(validation);
    size_t count = 0;
    for (const auto& region : state->request.regions_) {
        count += region.bindings.size();
        if (const auto* mounted = catalog_.Find(region.segment.id)) {
            const auto* resource = GetResource(*mounted);
            if (!resource) return tl::unexpected(ErrorCode::INTERNAL_ERROR);
            const auto owner = resource->candidate->client_session();
            if (owner && owner != state->liveness)
                return tl::unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        if (region.descriptors.empty() && catalog_.Find(region.segment.id)) {
            state->transactions.emplace_back();
            continue;
        }
        auto txn = region.descriptors.empty()
                       ? state->access.PrepareMount(region.segment,
                                                    state->request.client_id_)
                       : state->access.PrepareRestore(region.segment,
                                                      state->request.client_id_,
                                                      region.descriptors);
        if (!txn) return tl::unexpected(txn.error());
        txn->BindClientSession(state->liveness);
        if (txn->imported_buffers().size() != region.bindings.size() ||
            std::ranges::any_of(txn->imported_buffers(),
                                [](const auto& buffer) { return !buffer; }))
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        auto& bytes = state->imported[region.segment.name];
        if (txn->imported_requested_bytes() >
            std::numeric_limits<uint64_t>::max() - bytes)
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        bytes += txn->imported_requested_bytes();
        state->transactions.emplace_back(std::move(*txn));
    }
    for (const auto& [name, bytes] : state->imported) {
        if (bytes && (!recovery_ || !recovery_->accounted_.contains(name) ||
                      recovery_->accounted_.at(name) < bytes))
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    state->result.reserve(count);
    return PreparedRemount(std::move(state));
}

std::vector<RestoredBuffer> PreparedRemount::Commit() {
    CHECK(state_ && !state_->committed);
    auto& state = *state_;
    for (auto& transaction : state.transactions) {
        if (transaction) {
            CHECK(transaction->Commit(state.access) == ErrorCode::OK);
        }
    }

    for (size_t i = 0; i < state.transactions.size(); ++i) {
        const auto& region = state.request.regions_[i];
        auto& transaction = state.transactions[i];
        if (!transaction) {
            const auto* mounted = state.pool.catalog_.Find(region.segment.id);
            state.pool.GetResource(*mounted)->candidate->BindClientSession(
                state.liveness);
        } else {
            auto buffers = transaction->TakeImportedBuffers();
            for (size_t j = 0; j < buffers.size(); ++j) {
                CHECK(state.access.BindBufferToSegment(region.segment.id,
                                                       *buffers[j]));
                state.result.push_back(
                    {region.bindings[j], std::move(buffers[j])});
            }
        }
        if (state.pool.recovery_) {
            state.pool.recovery_->allocators_.erase(region.segment.name);
            state.pool.recovery_->allocators_.erase(region.segment.te_endpoint);
        }
    }

    // Replace placeholder accounting only after all live buffers are bound.
    for (const auto& [name, bytes] : state.imported) {
        if (!bytes) continue;
        MasterMetricManager::instance().dec_allocated_mem_size(
            name, static_cast<int64_t>(bytes));
        auto& accounted = state.pool.recovery_->accounted_.at(name);
        accounted -= bytes;
        if (!accounted) state.pool.recovery_->accounted_.erase(name);
    }

    // Publish the client host only after the entire batch has committed.
    // Preserve the first nonempty hint, including existing-region remounts.
    for (const auto& region : state.request.regions_) {
        if (!region.segment.host_id.empty()) {
            state.liveness->SetHostId(region.segment.host_id);
            break;
        }
    }
    state.committed = true;
    return std::move(state.result);
}

}  // namespace mooncake
