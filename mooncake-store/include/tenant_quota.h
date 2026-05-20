#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace mooncake {

struct TenantQuotaPolicy {
    uint64_t max_bytes = 0;  // 0 means unlimited
};

struct TenantQuotaState {
    uint64_t used_bytes = 0;       // committed live bytes
    uint64_t reserved_bytes = 0;   // reserved but not yet committed/aborted
    uint64_t committed_count = 0;  // current alive object count (diagnostics)
};

struct TenantQuotaSnapshot {
    std::string tenant_id;
    TenantQuotaPolicy policy;
    TenantQuotaState state;
    bool has_explicit_policy = false;
};

// Frontier candidate entry. Held by TenantQuotaTable::Entry's per-tenant
// std::set<TenantFrontierEntry>; ordered by (lease_timeout ASC, neg_size ASC,
// key ASC) so that begin() yields the global top candidate.
//
// neg_size = -static_cast<int64_t>(size) so the natural ascending set order
// places larger objects first within the same lease_timeout (size DESC).
//
// lease_timeout is a SNAPSHOT of metadata.lease_timeout taken at insert time.
// Lease renewal hot paths (GrantLease / GetReplicaList) do NOT mutate the
// frontier; evict consumers re-check the live lease_timeout against now() and
// drop stale popped entries via DropStaleFrontier (lazy maintenance).
struct TenantFrontierEntry {
    std::chrono::system_clock::time_point lease_timeout;
    int64_t neg_size;
    std::string key;

    bool operator<(const TenantFrontierEntry& other) const {
        if (lease_timeout != other.lease_timeout)
            return lease_timeout < other.lease_timeout;
        if (neg_size != other.neg_size) return neg_size < other.neg_size;
        return key < other.key;
    }
};

// Soft-pin location of a tracked object. Hard-pinned objects are NEVER
// indexed; they pass kNone here too (callers must not call IndexFrontier
// for hard-pinned objects).
enum class FrontierBucket { kNone, kNoPin, kSoftPin };

class TenantQuotaTable {
   public:
    enum class ReserveResult { kOk, kQuotaExceeded };

    explicit TenantQuotaTable(
        const TenantQuotaPolicy& default_policy = TenantQuotaPolicy{});

    // Policy CRUD (called from HTTP admin endpoints)
    void UpsertPolicy(const std::string& tenant_id,
                      const TenantQuotaPolicy& policy);
    bool ErasePolicy(const std::string& tenant_id);
    std::optional<TenantQuotaPolicy> GetPolicy(
        const std::string& tenant_id) const;
    std::vector<TenantQuotaSnapshot> ListAll() const;
    std::optional<TenantQuotaSnapshot> GetSnapshot(
        const std::string& tenant_id) const;

    // Two-phase accounting
    ReserveResult Reserve(const std::string& tenant_id, uint64_t bytes);
    void Commit(const std::string& tenant_id, uint64_t bytes);
    void Abort(const std::string& tenant_id, uint64_t bytes);

    // Direct release on full object teardown: decrements used_bytes by
    // `bytes` and committed_count by one. Use when the entire object is
    // gone (Remove / full Evict / PutRevoke / Discard).
    void Release(const std::string& tenant_id, uint64_t bytes);

    // Partial release: decrements used_bytes only. Use when only some
    // of an object's memory replicas were freed but the object itself
    // is still alive (BatchEvict survival branch, BatchReplicaClear
    // partial-segment clear). committed_count is intentionally NOT
    // decremented here -- it tracks alive objects, and the matching
    // decrement happens at the eventual full Release.
    void ReleasePartial(const std::string& tenant_id, uint64_t bytes);

    // Compute how many bytes need to be evicted to fit incoming_bytes
    uint64_t ComputeEvictTarget(const std::string& tenant_id,
                                uint64_t incoming_bytes) const;

    // Restore path: reset all usage then re-accumulate from metadata
    void ResetUsage();
    void AccumulateUsage(const std::string& tenant_id, uint64_t bytes);

    // Default policy accessors
    void SetDefaultPolicy(const TenantQuotaPolicy& policy);
    TenantQuotaPolicy GetDefaultPolicy() const;

    // Snapshot/restore support: enumerate every tenant_id whose policy was
    // set explicitly (via UpsertPolicy / HTTP PUT). Default-derived tenants
    // are excluded -- those carry no operator intent worth persisting.
    std::vector<std::pair<std::string, TenantQuotaPolicy>>
    ListExplicitPolicies() const;

    // Replace the operator-facing policy state in one atomic step.
    //
    // Effect:
    //   - default_policy_ is overwritten with `default_policy`.
    //   - Every existing entry's has_explicit_policy is cleared and its
    //     `policy` re-pointed at the new default.
    //   - Each (tenant_id, policy) in `explicit_policies` is then written
    //     back as an explicit override, creating the entry if absent.
    //   - Entries that end up with no explicit policy AND zero state AND
    //     empty frontier sets are dropped, preventing accumulation of
    //     orphan entries across repeated restore attempts.
    //
    // Usage state (used_bytes / reserved_bytes / committed_count) and
    // frontier sets are NOT touched -- the caller is responsible for
    // wiping them via ResetUsage() before AccumulateUsage() rebuilds them
    // from the deserialized object metadata.
    void RestorePolicies(
        const TenantQuotaPolicy& default_policy,
        const std::vector<std::pair<std::string, TenantQuotaPolicy>>&
            explicit_policies);

    // ---- Per-tenant frontier ----
    //
    // Frontier indexes live alongside policy/state inside Entry and share
    // the SAME mu_ as quota accounting. This collapses the pull stage into
    // a single set::begin() walk under one mutex, since per-tenant write
    // traffic is already serialised by Reserve/Commit on the same mu_.

    // Insert an object into its tenant's frontier set selected by `bucket`.
    // No-op if bucket==kNone or bytes==0. The entry stores the supplied
    // lease_timeout snapshot; callers must pass the same snapshot to
    // UnindexFrontier when removing the entry later.
    void IndexFrontier(const std::string& tenant_id, const std::string& key,
                       std::chrono::system_clock::time_point lease_timeout,
                       uint64_t size, FrontierBucket bucket);

    // Remove an object's frontier entry. The (lease_timeout, size, key)
    // triple must match the snapshot used at IndexFrontier time so the
    // std::set lookup is exact. No-op if bucket==kNone.
    void UnindexFrontier(const std::string& tenant_id, const std::string& key,
                         std::chrono::system_clock::time_point lease_timeout,
                         uint64_t size, FrontierBucket bucket);

    // Migrate a frontier entry between buckets when the soft-pin state
    // flips. Equivalent to UnindexFrontier(old_bucket) +
    // IndexFrontier(new_bucket) under one lock; same key/lease/size on
    // both sides.
    void MoveFrontier(const std::string& tenant_id, const std::string& key,
                      std::chrono::system_clock::time_point lease_timeout,
                      uint64_t size, FrontierBucket old_bucket,
                      FrontierBucket new_bucket);

    // Lazy-maintenance cleanup: erase a stale frontier entry whose
    // lease_timeout snapshot is now obsolete (typically discovered when
    // an evict consumer pops the entry and finds the live metadata's
    // lease has been renewed). Identical signature to UnindexFrontier on
    // purpose -- caller already has the snapshot triple in hand.
    void DropStaleFrontier(const std::string& tenant_id, const std::string& key,
                           std::chrono::system_clock::time_point lease_timeout,
                           uint64_t size, FrontierBucket bucket);

    // Pull the top `batch_size` candidates of `tenant_id`'s frontier in
    // sorted order (oldest lease, largest size first). Returns an empty
    // deque if the tenant has no entries in the requested bucket.
    //
    // `bucket` selects which frontier to pull from: kNoPin or kSoftPin.
    std::deque<TenantFrontierEntry> SnapshotTopK(const std::string& tenant_id,
                                                 FrontierBucket bucket,
                                                 size_t batch_size) const;

    // Test/diagnostic: total frontier size for a tenant in the given
    // bucket. Returns 0 if the tenant has no entry.
    size_t FrontierSize(const std::string& tenant_id,
                        FrontierBucket bucket) const;

   private:
    struct Entry {
        TenantQuotaPolicy policy;
        TenantQuotaState state;
        bool has_explicit_policy = false;

        // Per-tenant frontier sets, guarded by TenantQuotaTable::mu_.
        std::set<TenantFrontierEntry> no_pin_frontier;
        std::set<TenantFrontierEntry> soft_pin_frontier;
    };

    // Returns effective max_bytes for a tenant (0 = unlimited)
    uint64_t GetEffectiveMaxBytes(const Entry& entry) const;

    // Gets or creates entry (with default policy)
    Entry& GetOrCreateEntry(const std::string& tenant_id);

    // Selects the per-tenant frontier set inside an Entry by bucket.
    // Returns nullptr for kNone (caller treats as no-op). Static because it
    // operates only on the supplied Entry; defined as a member of
    // TenantQuotaTable so it can access the private nested Entry type.
    static std::set<TenantFrontierEntry>* SelectFrontier(Entry& entry,
                                                         FrontierBucket bucket);
    static const std::set<TenantFrontierEntry>* SelectFrontier(
        const Entry& entry, FrontierBucket bucket);

    // std::mutex (not shared_mutex) -- write traffic dominates, the
    // shared_mutex reader-counter overhead would not be amortised.
    mutable std::mutex mu_;
    std::unordered_map<std::string, Entry> entries_;
    TenantQuotaPolicy default_policy_;
};

}  // namespace mooncake
