#include "tenant_quota.h"

#include <glog/logging.h>

#include <algorithm>
#include <limits>

namespace mooncake {

TenantQuotaTable::TenantQuotaTable(const TenantQuotaPolicy& default_policy)
    : default_policy_(default_policy) {}

void TenantQuotaTable::UpsertPolicy(const std::string& tenant_id,
                                    const TenantQuotaPolicy& policy) {
    std::lock_guard<std::mutex> lock(mu_);
    auto& entry = GetOrCreateEntry(tenant_id);
    entry.policy = policy;
    entry.has_explicit_policy = true;
}

bool TenantQuotaTable::ErasePolicy(const std::string& tenant_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end() || !it->second.has_explicit_policy) {
        return false;
    }
    it->second.has_explicit_policy = false;
    it->second.policy = default_policy_;
    // If state is all zeros and no explicit policy, we can remove the entry.
    // Frontier sets are not consulted: an entry with active frontier members
    // necessarily has nonzero used_bytes or reserved_bytes, so the same guard
    // protects it.
    const auto& state = it->second.state;
    if (state.used_bytes == 0 && state.reserved_bytes == 0 &&
        state.committed_count == 0) {
        DCHECK(it->second.no_pin_frontier.empty() &&
               it->second.soft_pin_frontier.empty())
            << "ErasePolicy: frontier sets should be empty when state is "
               "all zeros; tenant_id="
            << tenant_id;
        entries_.erase(it);
    }
    return true;
}

std::optional<TenantQuotaPolicy> TenantQuotaTable::GetPolicy(
    const std::string& tenant_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    if (it->second.has_explicit_policy) {
        return it->second.policy;
    }
    return std::nullopt;
}

std::vector<TenantQuotaSnapshot> TenantQuotaTable::ListAll() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<TenantQuotaSnapshot> result;
    result.reserve(entries_.size());
    for (const auto& [tenant_id, entry] : entries_) {
        TenantQuotaSnapshot snapshot;
        snapshot.tenant_id = tenant_id;
        snapshot.policy =
            entry.has_explicit_policy ? entry.policy : default_policy_;
        snapshot.state = entry.state;
        snapshot.has_explicit_policy = entry.has_explicit_policy;
        result.push_back(std::move(snapshot));
    }
    return result;
}

std::optional<TenantQuotaSnapshot> TenantQuotaTable::GetSnapshot(
    const std::string& tenant_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    TenantQuotaSnapshot snapshot;
    snapshot.tenant_id = tenant_id;
    snapshot.policy =
        it->second.has_explicit_policy ? it->second.policy : default_policy_;
    snapshot.state = it->second.state;
    snapshot.has_explicit_policy = it->second.has_explicit_policy;
    return snapshot;
}

TenantQuotaTable::ReserveResult TenantQuotaTable::Reserve(
    const std::string& tenant_id, uint64_t bytes) {
    if (bytes == 0) {
        return ReserveResult::kOk;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto& entry = GetOrCreateEntry(tenant_id);
    const uint64_t effective_max = GetEffectiveMaxBytes(entry);
    const uint64_t used = entry.state.used_bytes;
    const uint64_t reserved = entry.state.reserved_bytes;

    // Compute headroom subtractively to avoid overflow when callers pass
    // adversarial sizes that would wrap used+reserved+bytes around zero
    // and silently bypass the quota check.
    constexpr uint64_t kMaxU64 = std::numeric_limits<uint64_t>::max();
    if (effective_max == 0) {
        // Unlimited quota; still guard reserved_bytes from wrapping.
        if (bytes > kMaxU64 - reserved) {
            return ReserveResult::kQuotaExceeded;
        }
        entry.state.reserved_bytes = reserved + bytes;
        return ReserveResult::kOk;
    }
    if (used > effective_max || reserved > effective_max - used) {
        // Ledger already at or beyond cap (defensive against drift).
        return ReserveResult::kQuotaExceeded;
    }
    const uint64_t headroom = effective_max - used - reserved;
    if (bytes > headroom) {
        return ReserveResult::kQuotaExceeded;
    }
    entry.state.reserved_bytes = reserved + bytes;
    return ReserveResult::kOk;
}

void TenantQuotaTable::Commit(const std::string& tenant_id, uint64_t bytes) {
    if (bytes == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        LOG(ERROR) << "TenantQuotaTable::Commit called for unknown tenant '"
                   << tenant_id << "' (no prior Reserve). Ignoring.";
        return;
    }
    auto& state = it->second.state;
    if (state.reserved_bytes < bytes) {
        // Reservation drained out from under us (a previous ResetUsage()
        // wiped reserved_bytes between Reserve and Commit, or the caller
        // is committing more than it reserved). Credit only what we
        // actually have on the books to preserve the
        // used + reserved <= max_bytes invariant; over-crediting would
        // let later Reserve calls silently wrap or underflow.
        LOG(ERROR) << "TenantQuotaTable::Commit: bytes=" << bytes
                   << " exceeds reserved_bytes=" << state.reserved_bytes
                   << " for tenant '" << tenant_id
                   << "'. Crediting only the available reserved amount "
                      "to used_bytes; the missing reservation indicates "
                      "a Reserve/Commit mismatch and warrants "
                      "investigation.";
        state.used_bytes += state.reserved_bytes;
        state.reserved_bytes = 0;
    } else {
        state.reserved_bytes -= bytes;
        state.used_bytes += bytes;
    }
    state.committed_count++;
}

void TenantQuotaTable::Abort(const std::string& tenant_id, uint64_t bytes) {
    if (bytes == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        LOG(ERROR) << "TenantQuotaTable::Abort called for unknown tenant '"
                   << tenant_id << "' (no prior Reserve). Ignoring.";
        return;
    }
    auto& state = it->second.state;
    if (state.reserved_bytes < bytes) {
        LOG(ERROR) << "TenantQuotaTable::Abort: bytes=" << bytes
                   << " exceeds reserved_bytes=" << state.reserved_bytes
                   << " for tenant '" << tenant_id
                   << "'. Clamping to zero to avoid underflow.";
        state.reserved_bytes = 0;
    } else {
        state.reserved_bytes -= bytes;
    }
}

void TenantQuotaTable::Release(const std::string& tenant_id, uint64_t bytes) {
    if (bytes == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return;
    }
    auto& state = it->second.state;
    if (state.used_bytes < bytes) {
        LOG(ERROR) << "TenantQuotaTable::Release: bytes=" << bytes
                   << " exceeds used_bytes=" << state.used_bytes
                   << " for tenant '" << tenant_id
                   << "'. Clamping to zero to avoid underflow.";
        state.used_bytes = 0;
    } else {
        state.used_bytes -= bytes;
    }
    if (state.committed_count > 0) {
        state.committed_count--;
    }
}

void TenantQuotaTable::ReleasePartial(const std::string& tenant_id,
                                      uint64_t bytes) {
    if (bytes == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return;
    }
    auto& state = it->second.state;
    if (state.used_bytes < bytes) {
        LOG(ERROR) << "TenantQuotaTable::ReleasePartial: bytes=" << bytes
                   << " exceeds used_bytes=" << state.used_bytes
                   << " for tenant '" << tenant_id
                   << "'. Clamping to zero to avoid underflow.";
        state.used_bytes = 0;
    } else {
        state.used_bytes -= bytes;
    }
    // Intentionally NOT decrementing committed_count: the object is still
    // alive after a partial replica release.
}

uint64_t TenantQuotaTable::ComputeEvictTarget(const std::string& tenant_id,
                                              uint64_t incoming_bytes) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return 0;
    }
    uint64_t effective_max = GetEffectiveMaxBytes(it->second);
    if (effective_max == 0) {
        // Unlimited
        return 0;
    }
    uint64_t current_usage =
        it->second.state.used_bytes + it->second.state.reserved_bytes;
    uint64_t needed = current_usage + incoming_bytes;
    if (needed <= effective_max) {
        return 0;
    }
    return needed - effective_max;
}

void TenantQuotaTable::ResetUsage() {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [tenant_id, entry] : entries_) {
        entry.state.used_bytes = 0;
        // Intentionally do NOT clear reserved_bytes here. Reserved bytes
        // represent in-flight PutStart requests whose Commit/Abort has
        // not yet been decided; they are not derivable from the metadata
        // that AccumulateUsage walks. Clearing them would also lose the
        // pending allocation: a subsequent Commit would observe
        // reserved_bytes==0, take the under-reserved branch, and credit
        // nothing to used_bytes. Leaving reserved_bytes alone lets the
        // in-flight caller drain its own reservation through the normal
        // Commit / Abort path.
        entry.state.committed_count = 0;
        // Clear frontier sets so snapshot restore can rebuild them from
        // scratch via IndexMetadata without orphan/duplicate entries.
        entry.no_pin_frontier.clear();
        entry.soft_pin_frontier.clear();
    }
}

void TenantQuotaTable::AccumulateUsage(const std::string& tenant_id,
                                       uint64_t bytes) {
    if (bytes == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto& entry = GetOrCreateEntry(tenant_id);
    entry.state.used_bytes += bytes;
    entry.state.committed_count++;
}

void TenantQuotaTable::SetDefaultPolicy(const TenantQuotaPolicy& policy) {
    std::lock_guard<std::mutex> lock(mu_);
    default_policy_ = policy;
    // Update entries that don't have explicit policies
    for (auto& [tenant_id, entry] : entries_) {
        if (!entry.has_explicit_policy) {
            entry.policy = default_policy_;
        }
    }
}

TenantQuotaPolicy TenantQuotaTable::GetDefaultPolicy() const {
    std::lock_guard<std::mutex> lock(mu_);
    return default_policy_;
}

std::vector<std::pair<std::string, TenantQuotaPolicy>>
TenantQuotaTable::ListExplicitPolicies() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::pair<std::string, TenantQuotaPolicy>> result;
    result.reserve(entries_.size());
    for (const auto& [tenant_id, entry] : entries_) {
        if (entry.has_explicit_policy) {
            result.emplace_back(tenant_id, entry.policy);
        }
    }
    return result;
}

void TenantQuotaTable::RestorePolicies(
    const TenantQuotaPolicy& default_policy,
    const std::vector<std::pair<std::string, TenantQuotaPolicy>>&
        explicit_policies) {
    std::lock_guard<std::mutex> lock(mu_);
    default_policy_ = default_policy;

    for (auto& [tenant_id, entry] : entries_) {
        entry.has_explicit_policy = false;
        entry.policy = default_policy_;
    }

    for (const auto& [tenant_id, policy] : explicit_policies) {
        auto it = entries_.find(tenant_id);
        if (it == entries_.end()) {
            auto [new_it, inserted] = entries_.emplace(tenant_id, Entry{});
            new_it->second.policy = policy;
            new_it->second.has_explicit_policy = true;
        } else {
            it->second.policy = policy;
            it->second.has_explicit_policy = true;
        }
    }

    for (auto it = entries_.begin(); it != entries_.end();) {
        const auto& entry = it->second;
        const auto& state = entry.state;
        if (!entry.has_explicit_policy && state.used_bytes == 0 &&
            state.reserved_bytes == 0 && state.committed_count == 0 &&
            entry.no_pin_frontier.empty() && entry.soft_pin_frontier.empty()) {
            it = entries_.erase(it);
        } else {
            ++it;
        }
    }
}

uint64_t TenantQuotaTable::GetEffectiveMaxBytes(const Entry& entry) const {
    if (entry.has_explicit_policy) {
        return entry.policy.max_bytes;
    }
    return default_policy_.max_bytes;
}

TenantQuotaTable::Entry& TenantQuotaTable::GetOrCreateEntry(
    const std::string& tenant_id) {
    auto it = entries_.find(tenant_id);
    if (it != entries_.end()) {
        return it->second;
    }
    auto [new_it, inserted] = entries_.emplace(tenant_id, Entry{});
    new_it->second.policy = default_policy_;
    new_it->second.has_explicit_policy = false;
    return new_it->second;
}

// ---- Frontier API ----

std::set<TenantFrontierEntry>* TenantQuotaTable::SelectFrontier(
    Entry& entry, FrontierBucket bucket) {
    switch (bucket) {
        case FrontierBucket::kNoPin:
            return &entry.no_pin_frontier;
        case FrontierBucket::kSoftPin:
            return &entry.soft_pin_frontier;
        case FrontierBucket::kNone:
        default:
            return nullptr;
    }
}

const std::set<TenantFrontierEntry>* TenantQuotaTable::SelectFrontier(
    const Entry& entry, FrontierBucket bucket) {
    switch (bucket) {
        case FrontierBucket::kNoPin:
            return &entry.no_pin_frontier;
        case FrontierBucket::kSoftPin:
            return &entry.soft_pin_frontier;
        case FrontierBucket::kNone:
        default:
            return nullptr;
    }
}

void TenantQuotaTable::IndexFrontier(
    const std::string& tenant_id, const std::string& key,
    std::chrono::system_clock::time_point lease_timeout, uint64_t size,
    FrontierBucket bucket) {
    if (bucket == FrontierBucket::kNone || size == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto& entry = GetOrCreateEntry(tenant_id);
    auto* target = SelectFrontier(entry, bucket);
    if (target == nullptr) {
        return;
    }
    DCHECK(size <= static_cast<uint64_t>(INT64_MAX))
        << "Object size exceeds int64_t range; neg_size would overflow";
    target->insert(TenantFrontierEntry{
        lease_timeout,
        -static_cast<int64_t>(size),
        key,
    });
}

void TenantQuotaTable::UnindexFrontier(
    const std::string& tenant_id, const std::string& key,
    std::chrono::system_clock::time_point lease_timeout, uint64_t size,
    FrontierBucket bucket) {
    if (bucket == FrontierBucket::kNone) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return;
    }
    auto* target = SelectFrontier(it->second, bucket);
    if (target == nullptr) {
        return;
    }
    target->erase(TenantFrontierEntry{
        lease_timeout,
        -static_cast<int64_t>(size),
        key,
    });
}

void TenantQuotaTable::MoveFrontier(
    const std::string& tenant_id, const std::string& key,
    std::chrono::system_clock::time_point lease_timeout, uint64_t size,
    FrontierBucket old_bucket, FrontierBucket new_bucket) {
    if (old_bucket == new_bucket) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    // Use GetOrCreateEntry so a kNone -> kNoPin/kSoftPin transition still
    // creates the entry (matches IndexFrontier semantics).
    auto& entry = GetOrCreateEntry(tenant_id);
    if (auto* old_set = SelectFrontier(entry, old_bucket)) {
        old_set->erase(TenantFrontierEntry{
            lease_timeout,
            -static_cast<int64_t>(size),
            key,
        });
    }
    if (auto* new_set = SelectFrontier(entry, new_bucket);
        new_set != nullptr && size > 0) {
        new_set->insert(TenantFrontierEntry{
            lease_timeout,
            -static_cast<int64_t>(size),
            key,
        });
    }
}

void TenantQuotaTable::DropStaleFrontier(
    const std::string& tenant_id, const std::string& key,
    std::chrono::system_clock::time_point lease_timeout, uint64_t size,
    FrontierBucket bucket) {
    // Identical mechanics to UnindexFrontier; the distinct name keeps the
    // call sites self-documenting (lazy-maintenance vs. explicit removal).
    UnindexFrontier(tenant_id, key, lease_timeout, size, bucket);
}

std::deque<TenantFrontierEntry> TenantQuotaTable::SnapshotTopK(
    const std::string& tenant_id, FrontierBucket bucket,
    size_t batch_size) const {
    std::deque<TenantFrontierEntry> result;
    if (batch_size == 0) {
        return result;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return result;
    }
    const auto* target = SelectFrontier(it->second, bucket);
    if (target == nullptr) {
        return result;
    }
    size_t taken = 0;
    for (auto iter = target->begin();
         iter != target->end() && taken < batch_size; ++iter, ++taken) {
        result.push_back(*iter);
    }
    return result;
}

size_t TenantQuotaTable::FrontierSize(const std::string& tenant_id,
                                      FrontierBucket bucket) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = entries_.find(tenant_id);
    if (it == entries_.end()) {
        return 0;
    }
    const auto* target = SelectFrontier(it->second, bucket);
    return target != nullptr ? target->size() : 0;
}

}  // namespace mooncake
