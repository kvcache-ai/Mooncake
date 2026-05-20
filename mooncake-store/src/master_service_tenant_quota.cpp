// Per-tenant storage quota for MasterService.
//
// All function definitions here are MasterService (or nested-class)
// member functions whose declarations live in master_service.h. They
// are split out of master_service.cpp so the bulk of the per-tenant
// quota implementation lives in one place rather than threading through
// the master service file alongside unrelated logic.
//
// Call sites that wire these helpers into the lifecycle of an object
// (Put / Upsert / Remove / BatchEvict / snapshot Reset and Restore /
// HTTP admin) stay in their original locations -- only the helper
// implementations themselves are extracted.
//
// Helpers shared with the global BatchEvict path (CanEvictReplicas,
// EvictReplicas, HasLocalDiskReplica, TryEvictOrOffload,
// TryEvictAndReconcile) intentionally stay in master_service.cpp
// because the global eviction loop also depends on them.

#include "master_service.h"

#include <glog/logging.h>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "master_metric_manager.h"
#include "serialize/serializer.hpp"
#include "tenant_quota.h"
#include "types.h"

namespace mooncake {

namespace {

// Mirror of the same constant in master_service.cpp. Per-cycle offload cap
// as a fraction of `offloading_queue_limit_`.
constexpr double kOffloadCapRatio = 0.5;

// Empty tenant_id is normalized to "default" so that callers who never
// set ReplicateConfig.tenant_id share a single bucket with everyone
// else who likewise leaves it unset.
inline const std::string& NormalizeTenantId(const std::string& tenant_id) {
    static const std::string kDefault = "default";
    return tenant_id.empty() ? kDefault : tenant_id;
}

}  // namespace

// ---- Per-tenant quota accounting helpers ----
//
// Every helper honors the enable_tenant_quota_ feature gate: when the
// gate is OFF every helper is a strict no-op so that single-tenant
// deployments observe identical behavior. When ON, they delegate to
// TenantQuotaTable and update the matching Prometheus gauge.

bool MasterService::ReserveTenantBytes(const std::string& tenant_id,
                                       uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return true;
    }
    const auto& normalized = NormalizeTenantId(tenant_id);
    auto result = tenant_quotas_.Reserve(normalized, bytes);
    if (result == TenantQuotaTable::ReserveResult::kOk) {
        MasterMetricManager::instance().inc_tenant_quota_reserved_bytes(
            normalized, static_cast<int64_t>(bytes));
    }
    return result == TenantQuotaTable::ReserveResult::kOk;
}

void MasterService::CommitTenantBytes(const std::string& tenant_id,
                                      uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto& normalized = NormalizeTenantId(tenant_id);
    tenant_quotas_.Commit(normalized, bytes);
    MasterMetricManager::instance().dec_tenant_quota_reserved_bytes(
        normalized, static_cast<int64_t>(bytes));
    MasterMetricManager::instance().inc_tenant_quota_used_bytes(
        normalized, static_cast<int64_t>(bytes));
}

void MasterService::AbortTenantBytes(const std::string& tenant_id,
                                     uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto& normalized = NormalizeTenantId(tenant_id);
    tenant_quotas_.Abort(normalized, bytes);
    MasterMetricManager::instance().dec_tenant_quota_reserved_bytes(
        normalized, static_cast<int64_t>(bytes));
}

void MasterService::ReleaseTenantBytes(const std::string& tenant_id,
                                       uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto& normalized = NormalizeTenantId(tenant_id);
    tenant_quotas_.Release(normalized, bytes);
    MasterMetricManager::instance().dec_tenant_quota_used_bytes(
        normalized, static_cast<int64_t>(bytes));
}

void MasterService::ReleaseTenantBytesPartial(const std::string& tenant_id,
                                              uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto& normalized = NormalizeTenantId(tenant_id);
    tenant_quotas_.ReleasePartial(normalized, bytes);
    MasterMetricManager::instance().dec_tenant_quota_used_bytes(
        normalized, static_cast<int64_t>(bytes));
}

uint64_t MasterService::ComputeTenantEvictTarget(
    const std::string& tenant_id, uint64_t incoming_bytes) const {
    if (!enable_tenant_quota_ || incoming_bytes == 0) {
        return 0;
    }
    return tenant_quotas_.ComputeEvictTarget(NormalizeTenantId(tenant_id),
                                             incoming_bytes);
}

uint64_t MasterService::TenantBytesForMetadata(
    const ObjectMetadata& metadata) const {
    // Counts memory replicas only (matches the global AccountLiveBytes
    // accounting in MasterMetricManager). Disk / local-disk replicas
    // are not billed against the tenant quota.
    const size_t memory_replica_count =
        metadata.CountReplicas(&Replica::fn_is_memory_replica);
    return static_cast<uint64_t>(metadata.size) *
           static_cast<uint64_t>(memory_replica_count);
}

void MasterService::RefreshTenantQuotaMetrics(
    const std::unordered_set<std::string>& known_before_tenant_ids) {
    if (!enable_tenant_quota_) {
        return;
    }
    auto& metrics = MasterMetricManager::instance();
    auto current_snapshots = tenant_quotas_.ListAll();

    // Compute which tenants existed before this refresh but no longer
    // do and clear their gauges. ylt's dynamic_gauge does not support
    // label removal, so a 0 publish is the cheapest way to keep stale
    // labels from masquerading as live state.
    std::unordered_set<std::string> current_ids;
    current_ids.reserve(current_snapshots.size());
    for (const auto& snap : current_snapshots) {
        current_ids.insert(snap.tenant_id);
    }
    for (const auto& old_id : known_before_tenant_ids) {
        if (!current_ids.count(old_id)) {
            metrics.set_tenant_quota_max_bytes(old_id, 0);
            metrics.set_tenant_quota_used_bytes(old_id, 0);
            metrics.set_tenant_quota_reserved_bytes(old_id, 0);
        }
    }

    // Republish the current ledger.
    metrics.set_tenant_quota_max_bytes(
        "_default",
        static_cast<int64_t>(tenant_quotas_.GetDefaultPolicy().max_bytes));
    for (const auto& snap : current_snapshots) {
        metrics.set_tenant_quota_max_bytes(
            snap.tenant_id, static_cast<int64_t>(snap.policy.max_bytes));
        metrics.set_tenant_quota_used_bytes(
            snap.tenant_id, static_cast<int64_t>(snap.state.used_bytes));
        metrics.set_tenant_quota_reserved_bytes(
            snap.tenant_id, static_cast<int64_t>(snap.state.reserved_bytes));
    }
}

// ============================================================================
// Per-tenant frontier index helpers
// ============================================================================
//
// IndexMetadata / UnindexMetadata / ReindexMetadataAfterPinChange are
// the single entry/exit gate for an object's participation in the
// per-tenant frontier. They run while the owning MetadataShard's
// mutex is held in WRITE mode (so the snapshot they capture from the
// ObjectMetadata is consistent), and forward the actual frontier
// mutation to TenantQuotaTable, which acquires its own mu_ briefly.
// Lock order is preserved: shard mutex (#2) -> tenant_quotas_.mu_
// (#5).
//
// Every insert into shard.metadata MUST be matched by exactly one
// IndexMetadata; every erase MUST be preceded by exactly one
// UnindexMetadata. Mismatched calls leak frontier entries and
// silently break BatchEvictInTenant.

// Returns the frontier bucket this object should currently live in
// given its hard-pin / soft-pin state. kNone is reserved for objects
// that must NEVER appear as eviction candidates (today: only
// hard-pinned ones).
//
// We INTENTIONALLY index objects whose lease has not yet expired:
// - The frontier is ordered by lease_timeout ASC; future-lease
//   entries sit at the tail and never bubble up to the K-best
//   candidates that BatchEvictInTenant pops.
// - Lease renewals (GrantLease / GetReplicaList hot paths) update
//   metadata.lease_timeout but do NOT touch the frontier; evict-time
//   consumers reconcile the discrepancy by comparing the snapshot
//   stored on the ObjectMetadata against the live lease_timeout
//   ("lazy maintenance" -- the lazy frontier maintenance contract).
FrontierBucket MasterService::ChooseFrontierBucket(
    const MasterService::ObjectMetadata& metadata,
    const std::chrono::system_clock::time_point& now) {
    if (metadata.IsHardPinned()) {
        return FrontierBucket::kNone;
    }
    if (metadata.IsSoftPinned(now)) {
        return FrontierBucket::kSoftPin;
    }
    return FrontierBucket::kNoPin;
}

std::chrono::system_clock::time_point MasterService::ReadLeaseSnapshot(
    const MasterService::ObjectMetadata& metadata) {
    SpinLocker locker(&metadata.lock);
    return metadata.lease_timeout;
}

void MasterService::IndexMetadata(MetadataShard& /*shard*/,
                                  const std::string& key,
                                  ObjectMetadata& metadata) {
    // Feature gate: when tenant quota is OFF, the frontier is never
    // consulted (BatchEvictInTenant is gated by enable_tenant_quota_
    // upstream), so skip the IndexFrontier call entirely. This
    // preserves the "quota OFF == no-op on tenant_quotas_" contract
    // single-tenant deployments rely on (TenantQuotaDisabledIsNoOp).
    if (!enable_tenant_quota_) {
        metadata.frontier_bucket = FrontierBucket::kNone;
        return;
    }

    const auto bucket =
        ChooseFrontierBucket(metadata, std::chrono::system_clock::now());
    if (bucket == FrontierBucket::kNone) {
        metadata.frontier_bucket = FrontierBucket::kNone;
        return;
    }

    const auto lease_snapshot = ReadLeaseSnapshot(metadata);
    metadata.frontier_bucket = bucket;
    metadata.frontier_lease_snapshot = lease_snapshot;
    tenant_quotas_.IndexFrontier(metadata.tenant_id, key, lease_snapshot,
                                 metadata.size, bucket);
}

void MasterService::UnindexMetadata(MetadataShard& /*shard*/,
                                    const std::string& key,
                                    ObjectMetadata& metadata) {
    // See IndexMetadata for the feature-gate rationale; symmetric
    // skip keeps the gate-OFF path strictly no-op on tenant_quotas_.
    if (!enable_tenant_quota_) {
        return;
    }
    if (metadata.frontier_bucket == FrontierBucket::kNone) {
        return;
    }
    tenant_quotas_.UnindexFrontier(metadata.tenant_id, key,
                                   metadata.frontier_lease_snapshot,
                                   metadata.size, metadata.frontier_bucket);
    metadata.frontier_bucket = FrontierBucket::kNone;
}

void MasterService::ReindexMetadataAfterPinChange(MetadataShard& /*shard*/,
                                                  const std::string& key,
                                                  ObjectMetadata& metadata) {
    // See IndexMetadata for the feature-gate rationale; quota OFF
    // means no entries were ever inserted, so there is nothing to
    // migrate.
    if (!enable_tenant_quota_) {
        return;
    }
    // Only meaningful when the object is currently in a frontier; pre-
    // expiry objects are not yet indexed and will be picked up by the
    // sweep with their then-current pin state.
    if (metadata.frontier_bucket == FrontierBucket::kNone) {
        return;
    }

    auto soft_now = std::chrono::system_clock::now();
    const auto desired_bucket = metadata.IsSoftPinned(soft_now)
                                    ? FrontierBucket::kSoftPin
                                    : FrontierBucket::kNoPin;
    if (desired_bucket == metadata.frontier_bucket) {
        return;
    }

    tenant_quotas_.MoveFrontier(metadata.tenant_id, key,
                                metadata.frontier_lease_snapshot, metadata.size,
                                metadata.frontier_bucket, desired_bucket);
    metadata.frontier_bucket = desired_bucket;
    // frontier_lease_snapshot stays the same: pin transition does not
    // change the lease, and the lazy-maintenance contract intentionally
    // keeps the snapshot frozen until the next full re-index.
}

std::deque<TenantFrontierEntry> MasterService::RefillTenantFrontier(
    const std::string& tenant_id, FrontierBucket bucket,
    size_t batch_size) const {
    // Bump unconditionally so the perf-baseline test can assert
    // "refills are amortized across many NextVictim() calls". Each
    // refill is a single tenant_quotas_.mu_ acquisition + an O(K)
    // std::set walk.
    refill_tenant_frontier_count_.fetch_add(1, std::memory_order_relaxed);

    if (batch_size == 0) {
        return {};
    }
    return tenant_quotas_.SnapshotTopK(tenant_id, bucket, batch_size);
}

// ---- TenantEvictionContext ----

MasterService::TenantEvictionContext::TenantEvictionContext(
    const MasterService* master_service, std::string tenant_id)
    : master_(master_service), tenant_id_(std::move(tenant_id)) {}

bool MasterService::TenantEvictionContext::Refill(FrontierBucket bucket) {
    auto& cache = (bucket == FrontierBucket::kSoftPin) ? cached_soft_pin_
                                                       : cached_no_pin_;
    auto& counter = (bucket == FrontierBucket::kSoftPin) ? refills_soft_pin_
                                                         : refills_no_pin_;

    // Hard cap on per-pass refills bounds the worst-case work for a
    // single failing allocation (each refill is one
    // tenant_quotas_.mu_ acquisition + O(K) set walk). Once exhausted
    // we surface "no candidate" so AllocateAndInsertMetadata can fail
    // fast with NO_AVAILABLE_HANDLE rather than spinning.
    if (counter >= kMaxRefills) {
        return false;
    }
    ++counter;

    auto fresh_batch =
        master_->RefillTenantFrontier(tenant_id_, bucket, kBatchSize);
    if (fresh_batch.empty()) {
        // The tenant has nothing left in this frontier -- terminal
        // for this pass.
        return false;
    }

    // Splice the fresh batch onto the cache. We expect the cache to
    // be empty at refill time (NextVictim refills only on empty), but
    // append-rather-than-overwrite keeps the helper safe if a future
    // caller pre-populates the cache.
    for (auto& entry : fresh_batch) {
        cache.push_back(std::move(entry));
    }
    return true;
}

std::optional<TenantFrontierEntry>
MasterService::TenantEvictionContext::NextVictim(FrontierBucket bucket) {
    auto& cache = (bucket == FrontierBucket::kSoftPin) ? cached_soft_pin_
                                                       : cached_no_pin_;

    if (cache.empty()) {
        if (!Refill(bucket)) {
            return std::nullopt;
        }
        // Refill returned true but the cache could still be empty if
        // RefillTenantFrontier returned an empty deque (Refill returns
        // false in that case, so reaching here means cache is non-empty).
        DCHECK(!cache.empty())
            << "Refill reported success but cache is empty; tenant_id="
            << tenant_id_ << ", bucket=" << static_cast<int>(bucket);
    }

    TenantFrontierEntry victim = std::move(cache.front());
    cache.pop_front();
    return victim;
}

uint64_t MasterService::BatchEvictInTenant(const std::string& tenant_id,
                                           uint64_t target_bytes,
                                           size_t excluded_shard_index) {
    if (target_bytes == 0) {
        return 0;
    }

    // One eviction cycle, one EvictCycleContext on the stack -- mirrors
    // BatchEvict. Counters scope to this call only so per-tenant evict
    // pressure does not pollute the global cycle's offload accounting.
    auto now = std::chrono::system_clock::now();
    EvictCycleContext ctx{
        /*offload_on_evict=*/offload_on_evict_,
        /*offload_force_evict=*/offload_force_evict_,
        /*offload_cap=*/
        offload_on_evict_
            ? static_cast<long>(offloading_queue_limit_ * kOffloadCapRatio)
            : 0,
        /*now=*/now,
    };

    TenantEvictionContext eviction_ctx(this, tenant_id);
    uint64_t freed_total = 0;

    auto try_one_candidate = [&](const TenantFrontierEntry& entry,
                                 FrontierBucket bucket) -> bool {
        // Returns true if the candidate was successfully evicted (or
        // offload-deferred), false if it must be skipped. Skipped
        // candidates are simply discarded -- see lazy-maintenance
        // rationale in the function-level comment.
        const size_t shard_index = getShardIndex(entry.key);
        // Skip same-shard candidates: AllocateAndInsertMetadata is
        // the typical caller and already holds this shard's writer
        // lock; std::shared_mutex is not recursive.
        if (shard_index == excluded_shard_index) {
            return false;
        }
        MetadataShardAccessorRW shard(this, shard_index, std::try_to_lock);
        if (!shard.owns_lock()) {
            return false;  // Shard lock contended; skip to avoid ABBA
        }

        auto it = shard->metadata.find(entry.key);
        if (it == shard->metadata.end()) {
            // Key already erased; clean the orphan frontier entry.
            tenant_quotas_.DropStaleFrontier(tenant_id, entry.key,
                                             entry.lease_timeout,
                                             -entry.neg_size, bucket);
            return false;
        }
        ObjectMetadata& metadata = it->second;

        // Tenant mismatch: the metadata map is keyed by raw key, so
        // a cross-tenant same-key replacement can leave a stale
        // frontier entry pointing at a key now owned by another
        // tenant. Drop the orphan and skip.
        if (metadata.tenant_id != tenant_id) {
            tenant_quotas_.DropStaleFrontier(tenant_id, entry.key,
                                             entry.lease_timeout,
                                             -entry.neg_size, bucket);
            return false;
        }

        // Hard-pinned objects must never be evicted. Drop stale entry.
        if (metadata.IsHardPinned()) {
            tenant_quotas_.DropStaleFrontier(tenant_id, entry.key,
                                             entry.lease_timeout,
                                             -entry.neg_size, bucket);
            return false;
        }

        // Soft-pin bucket mismatch: reconcile against live pin state.
        // On mismatch, migrate the entry and skip this candidate.
        {
            auto desired_bucket = ChooseFrontierBucket(metadata, ctx.now);
            if (desired_bucket != FrontierBucket::kNone &&
                desired_bucket != bucket) {
                tenant_quotas_.MoveFrontier(metadata.tenant_id, entry.key,
                                            metadata.frontier_lease_snapshot,
                                            metadata.size, bucket,
                                            desired_bucket);
                metadata.frontier_bucket = desired_bucket;
                return false;
            }
        }

        // Stale snapshot: lease was bumped since indexing. Drop and
        // let the object be re-indexed when its renewed lease expires.
        if (entry.lease_timeout != metadata.frontier_lease_snapshot ||
            metadata.frontier_bucket != bucket) {
            tenant_quotas_.DropStaleFrontier(tenant_id, entry.key,
                                             entry.lease_timeout,
                                             -entry.neg_size, bucket);
            return false;
        }

        // Active lease — not yet evictable. Refresh the frontier
        // entry with the live lease_timeout so it sorts correctly.
        //
        // Lock invariant: this branch holds `shard`'s shared_mutex in
        // EXCLUSIVE mode (MetadataShardAccessorRW). GrantLease — the
        // only writer of metadata.lease_timeout — runs from
        // GetReplicaList paths under SHARED shard lock, so it cannot
        // observe or interleave with our reads/writes here.
        // ReadLeaseSnapshot's SpinLock is therefore redundant but kept
        // for symmetry with IndexMetadata (which has the same lock
        // ordering); the write to frontier_lease_snapshot is safe under
        // the same exclusive shard lock that protects every other
        // mutator of this field.
        if (!metadata.IsLeaseExpired(ctx.now)) {
            tenant_quotas_.DropStaleFrontier(tenant_id, entry.key,
                                             entry.lease_timeout,
                                             -entry.neg_size, bucket);
            auto live_lease = ReadLeaseSnapshot(metadata);
            metadata.frontier_lease_snapshot = live_lease;
            tenant_quotas_.IndexFrontier(metadata.tenant_id, entry.key,
                                         live_lease, metadata.size, bucket);
            return false;
        }

        // Must have at least one evictable memory replica.
        if (!CanEvictReplicas(metadata)) {
            return false;
        }

        EvictReconcileResult reconcile;
        TryEvictAndReconcile(ctx, shard, it, reconcile);
        freed_total += reconcile.freed_bytes;
        return reconcile.freed_bytes > 0;
    };

    // ---- Pass 1: no-pin candidates ----
    while (freed_total < target_bytes) {
        auto victim = eviction_ctx.NextVictim(FrontierBucket::kNoPin);
        if (!victim.has_value()) {
            break;
        }
        try_one_candidate(*victim, FrontierBucket::kNoPin);
    }

    // ---- Pass 2: soft-pin candidates ----
    // Only enter when the service-level switch allows evicting
    // soft-pinned objects under pressure (matches BatchEvict's pass-2
    // guard).
    if (freed_total < target_bytes && allow_evict_soft_pinned_objects_) {
        while (freed_total < target_bytes) {
            auto victim = eviction_ctx.NextVictim(FrontierBucket::kSoftPin);
            if (!victim.has_value()) {
                break;
            }
            try_one_candidate(*victim, FrontierBucket::kSoftPin);
        }
    }

    if (freed_total > 0) {
        MasterMetricManager::instance().inc_tenant_evict_bytes_total(
            tenant_id, static_cast<int64_t>(freed_total));
    }

    return freed_total;
}

// ---- Snapshot serialization for tenant quota policies ----
//
// Quota *usage* (used / reserved / committed_count) is intentionally
// excluded -- it is rebuilt from restored object metadata via
// AccumulateUsage during DeserializeShard. Only operator-set policy
// (default + explicit overrides) crosses snapshot boundaries.

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeTenantQuotas(
    MsgpackPacker& packer) const {
    // Layout (map for forward compatibility -- new fields can be appended
    // without bumping the snapshot version):
    //   {
    //     "default_policy": { "max_bytes": <uint64> },
    //     "policies":       [ { "tenant_id": <str>,
    //                           "max_bytes": <uint64> }, ... ]
    //   }
    //
    // Only explicit (operator-set) policies are persisted. Tenants that
    // resolve to the default policy carry no operator intent worth saving;
    // they reappear lazily when traffic next references them.
    const auto default_policy = service_->tenant_quotas_.GetDefaultPolicy();
    auto explicit_policies = service_->tenant_quotas_.ListExplicitPolicies();

    // Sort by tenant_id so the serialized payload is byte-stable across
    // process runs. The snapshot child-process tests in this directory
    // rely on the snapshot binary roundtripping byte-for-byte after a
    // restore, and ListExplicitPolicies() returns iteration order from an
    // unordered_map, which is not stable.
    std::sort(explicit_policies.begin(), explicit_policies.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    packer.pack_map(2);

    packer.pack("default_policy");
    packer.pack_map(1);
    packer.pack("max_bytes");
    packer.pack(default_policy.max_bytes);

    packer.pack("policies");
    packer.pack_array(explicit_policies.size());
    for (const auto& [tenant_id, policy] : explicit_policies) {
        packer.pack_map(2);
        packer.pack("tenant_id");
        packer.pack(tenant_id);
        packer.pack("max_bytes");
        packer.pack(policy.max_bytes);
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeTenantQuotas(
    const msgpack::object& obj) {
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "tenant_quotas: expected map"));
    }

    auto find_field = [&](std::string_view name) -> const msgpack::object* {
        for (uint32_t i = 0; i < obj.via.map.size; ++i) {
            const auto& key = obj.via.map.ptr[i].key;
            if (key.type != msgpack::type::STR) continue;
            std::string_view candidate(key.via.str.ptr, key.via.str.size);
            if (candidate == name) {
                return &obj.via.map.ptr[i].val;
            }
        }
        return nullptr;
    };

    // Strict validation: a tenant_quotas map without default_policy is
    // treated as a corrupted payload rather than silently restoring as
    // "default unlimited", which would inadvertently disable quota
    // enforcement. The tenant_quotas key being entirely absent is a
    // separate (legacy) case handled by the caller.
    const auto* default_obj = find_field("default_policy");
    if (default_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "tenant_quotas: missing required field 'default_policy'"));
    }
    if (default_obj->type != msgpack::type::MAP) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "tenant_quotas.default_policy: expected map"));
    }

    TenantQuotaPolicy default_policy;
    bool default_max_bytes_seen = false;
    for (uint32_t i = 0; i < default_obj->via.map.size; ++i) {
        const auto& key = default_obj->via.map.ptr[i].key;
        if (key.type != msgpack::type::STR) continue;
        std::string_view candidate(key.via.str.ptr, key.via.str.size);
        if (candidate == "max_bytes") {
            try {
                default_policy.max_bytes =
                    default_obj->via.map.ptr[i].val.as<uint64_t>();
                default_max_bytes_seen = true;
            } catch (const std::exception& e) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    std::string("tenant_quotas.default_policy.max_bytes: ") +
                        e.what()));
            }
        }
    }
    if (!default_max_bytes_seen) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "tenant_quotas.default_policy: missing 'max_bytes'"));
    }

    // policies is required when tenant_quotas is present. The serializer
    // always emits it (possibly as an empty array for the default-only
    // case), so a missing key signals a truncated or otherwise corrupted
    // payload rather than an intentional "no overrides" record.
    const auto* policies_obj = find_field("policies");
    if (policies_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "tenant_quotas: missing required field 'policies'"));
    }
    if (policies_obj->type != msgpack::type::ARRAY) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "tenant_quotas.policies: expected array"));
    }

    std::vector<std::pair<std::string, TenantQuotaPolicy>> explicit_policies;
    explicit_policies.reserve(policies_obj->via.array.size);
    for (uint32_t i = 0; i < policies_obj->via.array.size; ++i) {
        const auto& item = policies_obj->via.array.ptr[i];
        if (item.type != msgpack::type::MAP) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("tenant_quotas.policies[{}]: expected map", i)));
        }
        std::string tenant_id;
        TenantQuotaPolicy policy;
        bool max_bytes_seen = false;
        for (uint32_t j = 0; j < item.via.map.size; ++j) {
            const auto& key = item.via.map.ptr[j].key;
            if (key.type != msgpack::type::STR) continue;
            std::string_view candidate(key.via.str.ptr, key.via.str.size);
            try {
                if (candidate == "tenant_id") {
                    tenant_id = item.via.map.ptr[j].val.as<std::string>();
                } else if (candidate == "max_bytes") {
                    policy.max_bytes = item.via.map.ptr[j].val.as<uint64_t>();
                    max_bytes_seen = true;
                }
            } catch (const std::exception& e) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("tenant_quotas.policies[{}].{}: {}", i,
                                std::string(candidate), e.what())));
            }
        }
        if (tenant_id.empty()) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("tenant_quotas.policies[{}]: missing tenant_id",
                            i)));
        }
        // Same rationale as the default policy: a policy entry without an
        // explicit max_bytes must not silently restore the named tenant
        // as unlimited.
        if (!max_bytes_seen) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "tenant_quotas.policies[{}]: missing 'max_bytes' for "
                    "tenant '{}'",
                    i, tenant_id)));
        }
        explicit_policies.emplace_back(std::move(tenant_id), policy);
    }

    service_->tenant_quotas_.RestorePolicies(default_policy, explicit_policies);
    LOG(INFO) << "[Restore] Loaded tenant quota policies: default_max_bytes="
              << default_policy.max_bytes
              << ", explicit_count=" << explicit_policies.size();
    // Gauge republish is deferred to RefreshTenantQuotaMetrics() at the
    // tail of TryRestoreStateFromSnapshot so that the post-cleanup ledger
    // (rather than the pre-cleanup one observable here) drives /metrics.

    return {};
}

}  // namespace mooncake
