#include "master_service.h"

#include <functional>
#include <limits>

namespace mooncake {
namespace {

// Owns the precharge until a PiecePutRuntime takes it over. It also covers
// allocation failures before a record can be published.
class RoundCharge {
   public:
    RoundCharge(std::atomic<size_t>& rounds, size_t limit) : rounds_(rounds) {
        size_t count = rounds_.load(std::memory_order_relaxed);
        while (count < limit) {
            if (rounds_.compare_exchange_weak(count, count + 1,
                                              std::memory_order_relaxed)) {
                charged_ = true;
                break;
            }
        }
    }
    ~RoundCharge() {
        if (charged_) {
            rounds_.fetch_sub(1, std::memory_order_relaxed);
        }
    }
    explicit operator bool() const { return charged_; }
    void Transfer() { charged_ = false; }

   private:
    std::atomic<size_t>& rounds_;
    bool charged_{false};
};

}  // namespace

auto MasterService::GetPiecePutMasterInstance() const
    -> tl::expected<UUID, ErrorCode> {
    if (!enable_piece_put_ || enable_ha_ ||
        memory_allocator_type_ != BufferAllocatorType::OFFSET ||
        allocation_strategy_type_ == AllocationStrategyType::CXL) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return piece_put_master_instance_;
}

auto MasterService::GetPiecePutAdmission(const UUID& client_id,
                                         uint64_t previous_session_id,
                                         const std::string& host_id)
    -> tl::expected<PiecePutAdmission, ErrorCode> {
    auto instance = GetPiecePutMasterInstance();
    if (!instance) return tl::make_unexpected(instance.error());
    auto session = piece_put_admissions_.Get(
        client_id, previous_session_id, PiecePutState::Clock::now(), host_id);
    if (!session) return tl::make_unexpected(session.error());
    return PiecePutAdmission{*instance, *session,
                             PiecePutAdmissionRegistry::kCapacity};
}

void MasterService::SweepPiecePutAdmissions() {
    piece_put_admissions_.Sweep(PiecePutState::Clock::now());
}

auto MasterService::ValidatePiecePutMount(const Segment& segment) const
    -> tl::expected<void, ErrorCode> {
    if (!segment.tcp_instance || segment.tcp_instance->empty()) {
        return {};
    }
    const auto instance = GetPiecePutMasterInstance();
    if (!instance) {
        return tl::make_unexpected(instance.error());
    }
    // Validate before creating or reviving a client, or touching any allocator.
    // A lost old-Master mount reply may otherwise replay the same TCP address
    // into a fresh allocator before the target records its successful mount.
    if (!segment.piece_put_master_instance ||
        *segment.piece_put_master_instance != *instance) {
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }
    return {};
}

void MasterService::RefreshPiecePutRound(PiecePutRuntime& runtime) {
    const auto now = PiecePutState::Clock::now();
    runtime.state.Expire(now);
    const auto& registration = runtime.hold.registration();
    const auto liveness = registration->GetClientSession();
    if (!runtime.hold.CanContinue(now) || !liveness || !liveness->IsServing()) {
        runtime.state.Retire();
    }
}

bool MasterService::IsPublishedPiecePutRound(MetadataAccessorRW& accessor,
                                             const PiecePutToken& token) {
    if (token.master_instance != piece_put_master_instance_ ||
        !accessor.IsPublished() ||
        token.resolved_tenant != accessor.GetTenantId().value() ||
        token.generation != accessor.GetRef().generation ||
        accessor.GetState().piece_put || accessor.InProcessing()) {
        return false;
    }
    // Once published, fragment bookkeeping is released. This historical
    // response only reports that the same object generation completed; it
    // never grants another write or changes a producer's settlement state.
    const auto& metadata = accessor.Get();
    // Offload, replication and in-place updates may change the replica set.
    // This proves historical completion of this generation, not byte identity
    // after a later in-place update. Failed rounds erase their generation.
    return metadata.HasReplica(&Replica::fn_is_completed);
}

MasterService::PiecePutPlacement MasterService::MakePiecePutPlacement(
    const UUID& client_id) const {
    PiecePutPlacement placement;
    // client_mutex_ precedes the policy/master/key/segment locks.
    if (enable_piece_put_ &&
        allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST) {
        placement.writer_host_id = GetClientHostId(client_id);
    }
    return placement;
}

auto MasterService::PiecePutStart(const UUID& client_id,
                                  const PiecePutStartRequest& request,
                                  const TenantId& tenant_id)
    -> tl::expected<PiecePutStartResponse, ErrorCode> {
    auto placement = MakePiecePutPlacement(client_id);
    return PiecePutStartImpl(client_id, request, tenant_id, placement);
}

std::vector<tl::expected<PiecePutStartResponse, ErrorCode>>
MasterService::BatchPiecePutStart(
    const UUID& client_id, const std::vector<PiecePutStartRequest>& requests,
    const TenantId& tenant_id) {
    std::vector<tl::expected<PiecePutStartResponse, ErrorCode>> results;
    if (requests.size() > 64) return results;
    results.reserve(requests.size());
    auto placement = MakePiecePutPlacement(client_id);
    for (const auto& request : requests)
        results.emplace_back(
            PiecePutStartImpl(client_id, request, tenant_id, placement));
    return results;
}

auto MasterService::PiecePutStartImpl(const UUID& client_id,
                                      const PiecePutStartRequest& request,
                                      const TenantId& tenant_id,
                                      PiecePutPlacement& placement)
    -> tl::expected<PiecePutStartResponse, ErrorCode> {
    // No persistence, replica fanout, CXL or alternate transport in v1.
    // Ordinary operations remain available regardless of this experimental
    // switch.
    if (!enable_piece_put_ || enable_ha_ ||
        memory_allocator_type_ != BufferAllocatorType::OFFSET ||
        allocation_strategy_type_ == AllocationStrategyType::CXL) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (auto valid = PiecePutState::ValidateStartRequest(request); !valid) {
        return tl::make_unexpected(valid.error());
    }
    if (request.total_size > std::numeric_limits<size_t>::max()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (request.admission.session_id == 0 || request.operation_seq == 0 ||
        request.operation_seq > PiecePutAdmissionRegistry::kCapacity ||
        (request.token &&
         request.token->admission_session_id != request.admission.session_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }
    if (request.admission.master_instance != piece_put_master_instance_) {
        if (request.close_no_post &&
            request.admission.master_instance != UUID{}) {
            // This Master can never grant an operation from another epoch.
            // A lost Start reply remains a definitive no-post failure after
            // restart, rather than an unrecoverable SDK handle.
            PiecePutStartResponse closed;
            closed.admission_closed = true;
            return closed;
        }
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }
    const bool reserve = !request.lookup_only && !request.close_no_post;
    PiecePutAdmissionRegistry::Guard window;
    if (reserve || request.close_no_post) {
        auto acquired = piece_put_admissions_.Acquire(
            client_id, request.admission.session_id,
            PiecePutState::Clock::now());
        if (!acquired) return tl::make_unexpected(acquired.error());
        window = std::move(*acquired);
        if (reserve && !window) {
            PiecePutStartResponse closed;
            closed.admission_closed = true;
            return closed;
        }
    }
    if (reserve && window &&
        allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST &&
        !window.HostId().empty()) {
        placement.writer_host_id = window.HostId();
    }
    const auto object_id = MakeObjectIdentityForRequest(request.key, tenant_id);
    std::unique_ptr<AdmissionToken> admission;
    std::optional<ErrorCode> admission_error;
    if (reserve && policy_ != nullptr) {
        auto admitted = policy_->AdmitWrite(object_id.tenant_id);
        if (!admitted) {
            admission_error = admitted.error();
        } else {
            admission = std::move(*admitted);
        }
    }
    if (reserve && !admission_error &&
        !IsTenantRegistered(object_id.tenant_id)) {
        admission_error = ErrorCode::TENANT_NOT_REGISTERED;
    }

    // Runtime destruction, including its hold, follows release of the key and
    // master locks on both failure and successful reclamation paths.
    std::unique_ptr<PiecePutRuntime> released;
    std::shared_lock snapshot_lock(master_state_mutex_);
    auto start =
        [&](MetadataAccessorRW& accessor,
            bool fresh) -> tl::expected<PiecePutStartResponse, ErrorCode> {
        if (admission_error) return tl::make_unexpected(*admission_error);
        auto missing = [&] {
            PiecePutStartResponse response;
            response.admission_closed =
                request.close_no_post || (reserve && !fresh);
            return response;
        };
        if (request.token &&
            (request.token->master_instance != piece_put_master_instance_ ||
             !accessor.IsPublished() ||
             request.token->resolved_tenant != accessor.GetTenantId().value() ||
             request.token->generation != accessor.GetRef().generation)) {
            return missing();
        }
        uint64_t reclaimed_memory = 0;
        if (reserve && fresh && accessor.IsPublished()) {
            auto& owned = accessor.GetState().piece_put;
            if (owned) {
                RefreshPiecePutRound(*owned);
                if (owned->state.CanReclaim() ||
                    owned->hold.IsPhysicallyRetired()) {
                    // PiecePut v1 owns exactly one memory replica.
                    const auto& metadata = accessor.Get();
                    const auto old_memory =
                        metadata.HasReplica(&Replica::fn_is_memory_replica)
                            ? metadata.TotalLength()
                            : 0;
                    released = std::move(owned);
                    accessor.Erase();
                    // Erase returned the actual allocation. Keep its hold until
                    // key release, but let a one-round budget admit a
                    // replacement.
                    released->ReleaseCharge();
                    reclaimed_memory = old_memory;
                } else if (owned->state.round_state() ==
                           PiecePutRoundState::RETIRING) {
                    // Unknown old writes are still isolated, not bad geometry
                    // in this new request. No descriptor or token was issued.
                    return tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                }
            }
        }
        if (accessor.IsPublished()) {
            auto& owned = accessor.GetState().piece_put;
            if (!owned) {
                if (!reserve || !fresh) {
                    PiecePutStartResponse response = missing();
                    if (request.token &&
                        IsPublishedPiecePutRound(accessor, *request.token)) {
                        response.round_state =
                            PiecePutRoundState::ROUND_PUBLISHED;
                    }
                    return response;
                }
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            if (!reserve || !fresh) RefreshPiecePutRound(*owned);
            // An original token is only a lookup; its slot must still match
            // the caller's exact nonce and geometry, never a different slot.
            auto claim = owned->state.Claim(request, client_id,
                                            accessor.Get().TotalLength(),
                                            PiecePutState::Clock::now(), fresh);
            if (!claim) {
                if ((!reserve || !fresh) &&
                    claim.error() == ErrorCode::OBJECT_NOT_FOUND) {
                    return missing();
                }
                return tl::make_unexpected(claim.error());
            }
            if (request.token && request.token->slot != claim->slot) {
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            }
            PiecePutStartResponse response;
            response.admission_closed = request.close_no_post;
            response.piece_state = claim->piece_state;
            response.round_state = claim->round_state;
            response.token = PiecePutToken{
                piece_put_master_instance_, accessor.GetRef().generation,
                claim->slot, accessor.GetTenantId().value(),
                request.admission.session_id};
            if (request.close_no_post &&
                claim->piece_state == PiecePutPieceState::EXPOSED) {
                auto settled = owned->state.Finish(
                    claim->slot, client_id, request.operation_seq,
                    PiecePutFinishOutcome::NO_POST,
                    accessor.Get().TotalLength(), PiecePutState::Clock::now(),
                    request.admission.session_id);
                if (!settled) return tl::make_unexpected(settled.error());
                response.piece_state = settled->piece_state;
                response.round_state = settled->round_state;
                if (owned->state.CanReclaim() ||
                    owned->hold.IsPhysicallyRetired()) {
                    released = std::move(owned);
                    accessor.Erase();
                }
            }
            if (claim->allow_write) {
                // Prepare the possibly allocating descriptor before returning
                // a grant. The allocation stays pinned even if the reply is
                // lost.
                const auto* replica = accessor.Get().GetFirstReplica(
                    &Replica::fn_is_memory_replica);
                response.replica = replica->get_descriptor();
                response.tcp_instance =
                    owned->hold.registration()->TcpInstance();
            }
            return response;
        }
        if (!reserve || !fresh) {
            return missing();
        }
        // Under this key guard the old charge is settled only on release.
        // Reuse it only after its actual allocation was safely erased above.
        const auto growth = request.total_size > reclaimed_memory
                                ? request.total_size - reclaimed_memory
                                : 0;
        if (auto admitted = accessor.Grow(growth); !admitted) {
            return tl::make_unexpected(admitted.error());
        }
        RoundCharge charge(piece_put_rounds_, max_piece_put_rounds_);
        if (!charge) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        PiecePutState state(request.write_id,
                            PiecePutState::Clock::now() + piece_put_timeout_);
        auto claim = state.Claim(request, client_id, request.total_size,
                                 PiecePutState::Clock::now());
        if (!claim) {
            return tl::make_unexpected(claim.error());
        }

        std::vector<std::string> preferred;
        {
            auto access = segment_manager_.getAllocatorAccess();
            if (!placement.allocators) {
                placement.allocators.emplace(access.SnapshotAllocatorManager(
                    [](const SegmentAllocatorRegistration& registration) {
                        return !registration.TcpInstance().empty() &&
                               registration.IsAllocatable() &&
                               !registration.IsWriteClosed();
                    }));
            }
            if (!placement.writer_host_id.empty()) {
                preferred = access.GetHostOrderedSegments(
                    placement.writer_host_id, request.key);
            }
        }
        // One snapshot per batch, and no allocator work under segment_mutex_.
        auto allocated = allocation_strategy_->Allocate(
            *placement.allocators, request.total_size, 1, preferred,
            std::set<std::string>{}, ReplicaType::MEMORY);
        if (!allocated) {
            if (allocated.error() == ErrorCode::NO_AVAILABLE_HANDLE &&
                !placement.allocators->getServingNames().empty()) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                need_mem_eviction_.store(true, std::memory_order_relaxed);
            }
            return tl::make_unexpected(allocated.error());
        }
        auto replicas = std::move(*allocated);
        if (replicas.size() != 1 || !replicas.front().is_memory_replica())
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        const auto* buffer = replicas.front().memory_buffer();
        if (!buffer) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        SegmentWriteHold hold;
        {
            auto access = segment_manager_.getAllocatorAccess();
            auto acquired = access.AcquireWriteHold(*buffer);
            if (!acquired) return tl::make_unexpected(acquired.error());
            hold = std::move(*acquired);
        }
        replicas.front().bind_piece_put_identity(piece_put_master_instance_);
        // No group/session/soft-pin state is created. All throwing work,
        // including the first response, precedes Publish and pointer attach.
        PiecePutStartResponse response;
        response.piece_state = claim->piece_state;
        response.round_state = claim->round_state;
        response.replica = replicas.front().get_descriptor();
        response.tcp_instance = hold.registration()->TcpInstance();
        response.token = PiecePutToken{
            piece_put_master_instance_, 0, claim->slot,
            accessor.GetTenantId().value(), request.admission.session_id};
        auto runtime = std::make_unique<PiecePutRuntime>(
            std::move(state), std::move(hold), piece_put_rounds_);
        charge.Transfer();
        const auto generation = accessor.Publish(
            client_id, std::chrono::system_clock::now(), request.total_size,
            std::move(replicas), std::nullopt, false, ObjectDataType::UNKNOWN,
            std::string{}, object_id.tenant_id, request.key);
        accessor.GetState().piece_put = std::move(runtime);
        accessor.GetState().is_processing = true;
        response.token->generation = generation;
        return response;
    };
    if (request.lookup_only && !request.close_no_post) {
        MetadataAccessorRW accessor(this, object_id,
                                    MetadataAccessorRW::AsIs{});
        return start(accessor, false);
    }
    if (request.close_no_post || admission_error) {
        {
            // Retain the absent-key fence without creating a namespace for a
            // rejected write or a close that has no allocation to settle.
            MetadataAccessorRW accessor(this, object_id,
                                        MetadataAccessorRW::ExistingKey{});
            if (accessor.HasKeyGuard()) {
                return start(accessor,
                             window && window.Consume(request.operation_seq));
            }
        }
        if (piece_put_missing_namespace_hook_) {
            piece_put_missing_namespace_hook_();
        }
        const bool fresh = window && window.Consume(request.operation_seq);
        // A concurrent Start may have created the namespace after our miss.
        // If it consumed first, its namespace was already published and its
        // key guard fences this lookup until its claim is visible. If this
        // close consumed first, no later Start can grant this nonce.
        MetadataAccessorRW accessor(this, object_id,
                                    MetadataAccessorRW::ExistingKey{});
        return start(accessor, fresh);
    }
    // A granting Start consumes only under its key guard and keeps that guard
    // through publication, so a duplicate cannot see a spent bit first.
    MetadataAccessorRW accessor(
        this, object_id,
        MetadataAccessorRW::CreateKey{.cleanup_invalid = false});
    return start(accessor, window && window.Consume(request.operation_seq));
}

auto MasterService::PiecePutFinish(const UUID& client_id,
                                   const PiecePutFinishRequest& request,
                                   const TenantId& tenant_id)
    -> tl::expected<PiecePutFinishResponse, ErrorCode> {
    if (!enable_piece_put_ || enable_ha_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (request.key.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::unique_ptr<PiecePutRuntime> released;
    std::shared_lock snapshot_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(request.key, tenant_id);
    MetadataAccessorRW accessor(this, object_id, MetadataAccessorRW::AsIs{});
    if (request.token.master_instance != piece_put_master_instance_ ||
        !accessor.IsPublished() ||
        request.token.resolved_tenant != accessor.GetTenantId().value() ||
        request.token.generation != accessor.GetRef().generation) {
        return PiecePutFinishResponse{};
    }
    auto& owned = accessor.GetState().piece_put;
    if (!owned) {
        PiecePutFinishResponse response;
        if (IsPublishedPiecePutRound(accessor, request.token)) {
            response.round_state = PiecePutRoundState::ROUND_PUBLISHED;
        }
        return response;
    }
    auto& metadata = accessor.Get();
    // Settlement deliberately has no ServingGuard: old valid grants must be
    // settleable after graceful close or owner OFFLINE.
    RefreshPiecePutRound(*owned);
    auto finish = owned->state.Finish(
        request.token.slot, client_id, request.operation_seq, request.outcome,
        metadata.TotalLength(), PiecePutState::Clock::now(),
        request.token.admission_session_id);
    if (!finish) {
        return tl::make_unexpected(finish.error());
    }
    PiecePutFinishResponse response{finish->piece_state, finish->round_state};
    if (finish->ready_to_publish) {
        // Key -> segment shared lock keeps the exact registration stable. The
        // ACTIVE observation is the publication point; the following clock
        // read proves it preceded both fixed deadlines. A subsequent OFFLINE
        // is ordinary cache loss, while the hold still pins physical backing.
        bool can_publish = false;
        {
            auto access = segment_manager_.getAllocatorAccess();
            const auto liveness =
                owned->hold.registration()->GetClientSession();
            const bool serving = liveness && liveness->IsServing();
            const auto now = PiecePutState::Clock::now();
            can_publish = serving && owned->hold.CanContinue(now) &&
                          now < owned->state.round_deadline();
            if (can_publish) {
                metadata.VisitReplicas(&Replica::fn_is_memory_replica,
                                       [](Replica& r) { r.mark_complete(); });
                released = std::move(owned);
                accessor.EraseFromProcessing();
            }
        }
        if (can_publish) {
            response.round_state = PiecePutRoundState::ROUND_PUBLISHED;
            SyncCacheTotalAccounting(metadata);
            ScheduleEagerOffload(accessor);
            metadata.GrantReadLease(std::chrono::milliseconds::zero());
            PublishKvStored(request.key, metadata, object_id.tenant_id);
            return response;
        }
        owned->state.Retire();
        response.round_state = PiecePutRoundState::RETIRING;
    }
    if (owned->state.CanReclaim() || owned->hold.IsPhysicallyRetired()) {
        // Keep the hold alive while Erase returns the exact allocation. The
        // same key cannot be reused before all writes settle or target drain.
        released = std::move(owned);
        accessor.Erase();
    }
    return response;
}

void MasterService::SweepPiecePutRoundsLocked() {
    if (piece_put_rounds_.load(std::memory_order_relaxed) == 0) {
        return;
    }
    // The typed index excludes ordinary in-flight keys. Work follows live
    // PiecePut rounds plus 64 list heads per namespace, without a key cursor
    // whose progress could be delayed by ordinary Put/Copy/Promotion backlog.
    VisitNamespaces([&](const TenantId& tenant_id, metadata::Namespace& ns) {
        for (auto& key : ns.PiecePutInFlightKeys()) {
            std::unique_ptr<PiecePutRuntime> released;
            MetadataAccessorRW accessor(
                this, ObjectIdentity{tenant_id, std::move(key)},
                MetadataAccessorRW::AsIs{});
            if (!accessor.IsPublished() || !accessor.GetState().piece_put) {
                continue;
            }
            auto& owned = accessor.GetState().piece_put;
            RefreshPiecePutRound(*owned);
            if (owned->state.CanReclaim() ||
                owned->hold.IsPhysicallyRetired()) {
                released = std::move(owned);
                accessor.Erase();
            }
        }
    });
}

auto MasterService::RetirePiecePutEndpoint(const UUID& client_id,
                                           const std::string& tcp_instance)
    -> tl::expected<void, ErrorCode> {
    if (!enable_piece_put_ || tcp_instance.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    {
        std::shared_lock snapshot_lock(master_state_mutex_);
        auto access = segment_manager_.getSegmentAccess();
        const auto error = access.RetireTcpEndpoint(client_id, tcp_instance);
        if (error != ErrorCode::OK) return tl::make_unexpected(error);
    }
    replica_cleanup_worker_.Schedule();
    return {};
}

}  // namespace mooncake
