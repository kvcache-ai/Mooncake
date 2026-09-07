#include "p2p/client/v2/lease_manager.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {
namespace {
constexpr uint32_t kDefaultLeaseDurationMs = 5000;
constexpr uint32_t kDefaultLeaseScanIntervalMs = 1000;
}  // namespace

namespace {
// Kept in lockstep with `records`, so the count is exactly the number of live
// leases for that key and never a stale positive.
void DecrementKeyCount(PinnedLeaseShard& shard, const std::string& key) {
    auto it = shard.leases_per_key.find(key);
    if (it == shard.leases_per_key.end()) return;
    if (--it->second == 0) shard.leases_per_key.erase(it);
}
}  // namespace

size_t PinnedLeaseIdHash::operator()(const PinnedLeaseId& id) const noexcept {
    size_t seed = StringHash{}(id.key);
    boost::hash_combine(seed, id.registration_id.registry_shard);
    boost::hash_combine(seed, id.registration_id.shard_sequence);
    boost::hash_combine(seed, id.block_id.local_id);
    boost::hash_combine(seed, id.block_id.generation);
    return seed;
}

LeaseManager::LeaseManager(const KeyLeaseConfig& config,
                           std::shared_ptr<Clock> clock, size_t shard_count)
    : lease_duration_(config.duration_ms > 0 ? config.duration_ms
                                             : kDefaultLeaseDurationMs),
      scan_interval_(std::max<uint32_t>(1, config.scan_interval_ms > 0
                                               ? config.scan_interval_ms
                                               : kDefaultLeaseScanIntervalMs)),
      clock_(std::move(clock)) {
    CHECK(clock_ != nullptr) << "LeaseManager requires a Clock";
    const size_t shards = std::max<size_t>(1, shard_count);
    pending_writes_.reserve(shards);
    pinned_reads_.reserve(shards);
    for (size_t i = 0; i < shards; ++i) {
        pending_writes_.push_back(std::make_unique<PendingWriteShard>());
        pinned_reads_.push_back(std::make_unique<PinnedLeaseShard>());
    }
    scanner_ = std::thread(&LeaseManager::ScannerMain, this);
}

LeaseManager::~LeaseManager() { StopAndDrain(); }

size_t LeaseManager::ShardFor(std::string_view key) const {
    return StringHash{}(key) % pending_writes_.size();
}

// ---------------------------------------------------------------------------
// Pending writes
// ---------------------------------------------------------------------------

tl::expected<UUID, ErrorCode> LeaseManager::ReservePendingWrite(
    std::string_view key, Clock::time_point deadline) {
    if (key.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto& shard = *pending_writes_[ShardFor(key)];
    const auto now = clock_->Now();
    const UUID token = generate_uuid();

    // The record is destroyed after the lock is released: it may own a
    // MutableBlock, whose destructor frees a physical block.
    PendingWriteLease evicted;
    bool has_evicted = false;
    {
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.records.find(key);
        if (it != shard.records.end()) {
            if (it->second.deadline > now) {
                // Live claim. The caller has allocated nothing yet, which is
                // the point of claiming before allocating.
                return tl::make_unexpected(ErrorCode::REPLICA_IS_PROCESSING);
            }
            evicted = std::move(it->second);
            has_evicted = true;
            shard.records.erase(it);
        }

        PendingWriteLease lease;
        lease.key.assign(key.data(), key.size());
        lease.write_token = token;
        lease.deadline = deadline;
        shard.records.emplace(lease.key, std::move(lease));
        shard.deadlines.push(
            LeaseDeadline<std::string>{deadline, std::string(key), token});
    }
    if (has_evicted && evicted.transaction.has_value()) {
        VLOG(1) << "Reclaimed an expired pending write while reserving, key="
                << evicted.key;
    }
    return token;
}

tl::expected<void, ErrorCode> LeaseManager::AttachPendingWriteTransaction(
    std::string_view key, const UUID& token, const UUID& tiler_id,
    std::variant<MutableBlock, CompletedBlock> transaction) {
    if (key.empty() || IsZeroUUID(token)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& shard = *pending_writes_[ShardFor(key)];

    // Held so the rejected transaction is destroyed outside the shard lock.
    std::optional<std::variant<MutableBlock, CompletedBlock>> rejected;
    {
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.records.find(key);
        if (it == shard.records.end()) {
            rejected = std::move(transaction);
        } else if (it->second.write_token != token) {
            rejected = std::move(transaction);
        } else {
            it->second.tiler_id = tiler_id;
            it->second.transaction = std::move(transaction);
            return {};
        }
    }
    if (rejected.has_value()) {
        LOG(ERROR) << "AttachPendingWriteTransaction: no matching claim, key="
                   << key;
    }
    return tl::make_unexpected(ErrorCode::INVALID_WRITE);
}

tl::expected<LeaseManager::DetachedPendingWrite, ErrorCode>
LeaseManager::DetachPendingWriteTransaction(std::string_view key,
                                            const UUID& token) {
    if (key.empty() || IsZeroUUID(token)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& shard = *pending_writes_[ShardFor(key)];
    const auto now = clock_->Now();

    std::optional<DetachedPendingWrite> detached;
    ErrorCode error = ErrorCode::OK;
    {
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.records.find(key);
        if (it == shard.records.end()) {
            error = ErrorCode::OBJECT_NOT_FOUND;
        } else if (it->second.deadline <= now) {
            error = ErrorCode::LEASE_EXPIRED;
        } else if (it->second.write_token != token) {
            error = ErrorCode::INVALID_WRITE;
        } else if (!it->second.transaction.has_value()) {
            error = ErrorCode::INVALID_WRITE;
        } else {
            // The claim itself stays: it is what keeps a concurrent writer of
            // this key from allocating.
            detached = DetachedPendingWrite{std::move(*it->second.transaction),
                                            it->second.tiler_id};
            it->second.transaction.reset();
        }
    }
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return std::move(*detached);
}

tl::expected<PendingWriteLease, ErrorCode> LeaseManager::TakePendingWrite(
    std::string_view key, const UUID& token) {
    if (key.empty() || IsZeroUUID(token)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& shard = *pending_writes_[ShardFor(key)];
    const auto now = clock_->Now();

    // Same ordering rule: take it out under the lock, let the caller (or this
    // function's error path) destroy it outside.
    PendingWriteLease taken;
    ErrorCode error = ErrorCode::OK;
    bool have_taken = false;
    {
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.records.find(key);
        if (it == shard.records.end()) {
            error = ErrorCode::OBJECT_NOT_FOUND;
        } else if (it->second.deadline <= now) {
            taken = std::move(it->second);
            have_taken = true;
            shard.records.erase(it);
            error = ErrorCode::LEASE_EXPIRED;
        } else if (it->second.write_token != token) {
            error = ErrorCode::INVALID_WRITE;
        } else {
            taken = std::move(it->second);
            have_taken = true;
            shard.records.erase(it);
        }
    }
    if (error != ErrorCode::OK) {
        // `taken` (an expired lease, if any) is destroyed here, unlocked.
        (void)have_taken;
        return tl::make_unexpected(error);
    }
    return taken;
}

bool LeaseManager::HasPendingWrite(std::string_view key) const {
    if (key.empty()) return false;
    auto& shard = *pending_writes_[ShardFor(key)];
    const auto now = clock_->Now();
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.records.find(key);
    return it != shard.records.end() && it->second.deadline > now;
}

// ---------------------------------------------------------------------------
// Pinned reads
// ---------------------------------------------------------------------------

tl::expected<PinLeaseResult, ErrorCode> LeaseManager::Pin(
    ImmutableBlock block, Clock::time_point deadline) {
    if (!block) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    PinnedLeaseId id;
    id.key = block.Key();
    id.registration_id = block.Registration();
    id.block_id = block.Id();

    auto& shard = *pinned_reads_[ShardFor(id.key)];
    const auto now = clock_->Now();

    // Any lease we drop is destroyed after the lock: it owns an ImmutableBlock.
    PinnedReadLease dropped;
    PinLeaseResult result;
    {
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.records.find(id);
        if (it != shard.records.end()) {
            if (it->second.deadline > now) {
                // Exact same key + registration + block: reuse the token and
                // renew. Anything less exact would let a lease outlive the
                // block it names.
                it->second.ref_count++;
                it->second.deadline = deadline;
                shard.deadlines.push(LeaseDeadline<PinnedLeaseId>{
                    deadline, id, it->second.read_token});
                result.read_token = it->second.read_token;
                result.ref_count = it->second.ref_count;
                result.deadline = deadline;
                return result;
            }
            dropped = std::move(it->second);
            shard.by_token.erase(dropped.read_token);
            shard.records.erase(it);
            DecrementKeyCount(shard, dropped.id.key);
        }

        PinnedReadLease lease;
        lease.id = id;
        lease.block = std::move(block);
        lease.read_token = generate_uuid();
        lease.ref_count = 1;
        lease.deadline = deadline;

        result.read_token = lease.read_token;
        result.ref_count = 1;
        result.deadline = deadline;

        shard.by_token.emplace(lease.read_token, id);
        shard.deadlines.push(
            LeaseDeadline<PinnedLeaseId>{deadline, id, lease.read_token});
        ++shard.leases_per_key[id.key];
        shard.records.emplace(id, std::move(lease));
    }
    return result;
}

tl::expected<void, ErrorCode> LeaseManager::Unpin(std::string_view key,
                                                  const UUID& token) {
    if (key.empty() || IsZeroUUID(token)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto& shard = *pinned_reads_[ShardFor(key)];
    const auto now = clock_->Now();

    PinnedReadLease released;
    ErrorCode error = ErrorCode::OK;
    {
        std::lock_guard<std::mutex> lock(shard.mu);
        auto token_it = shard.by_token.find(token);
        if (token_it == shard.by_token.end()) {
            // V1 resolves by key first, so the answer depends on whether the
            // key is pinned at all: unpinning an unpinned key is idempotent
            // success, but presenting a foreign token for a key that *is*
            // pinned must be refused rather than silently accepted.
            if (shard.leases_per_key.find(key) != shard.leases_per_key.end()) {
                error = ErrorCode::INVALID_READ;
            }
        } else if (token_it->second.key != key) {
            // The token exists but names another key; refuse rather than
            // release someone else's lease.
            error = ErrorCode::INVALID_READ;
        } else {
            auto it = shard.records.find(token_it->second);
            if (it == shard.records.end()) {
                // by_token and records are maintained together, so this means
                // the tables disagree.
                LOG(ERROR) << "Pinned lease token index is out of sync, key="
                           << key;
                shard.by_token.erase(token_it);
                error = ErrorCode::INTERNAL_ERROR;
            } else if (it->second.deadline <= now) {
                released = std::move(it->second);
                shard.records.erase(it);
                shard.by_token.erase(token_it);
                DecrementKeyCount(shard, released.id.key);
                error = ErrorCode::LEASE_EXPIRED;
            } else if (it->second.ref_count > 1) {
                it->second.ref_count--;
            } else {
                released = std::move(it->second);
                shard.records.erase(it);
                shard.by_token.erase(token_it);
                DecrementKeyCount(shard, released.id.key);
            }
        }
    }
    // `released` dies here, unlocked, which is where the ImmutableBlock (and
    // possibly the physical block) is finally let go.
    if (error != ErrorCode::OK) return tl::make_unexpected(error);
    return {};
}

// ---------------------------------------------------------------------------
// Expiry
// ---------------------------------------------------------------------------

size_t LeaseManager::ScanPendingWrites(
    Clock::time_point now, std::vector<PendingWriteLease>& detached) {
    size_t removed = 0;
    for (auto& shard : pending_writes_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        while (!shard->deadlines.empty()) {
            const auto& top = shard->deadlines.top();
            if (top.deadline > now) break;
            // The heap can hold stale entries (renewal pushes instead of
            // updating), so the record is always re-checked before removal.
            auto it = shard->records.find(top.key);
            if (it != shard->records.end() &&
                it->second.write_token == top.token &&
                it->second.deadline <= now) {
                detached.push_back(std::move(it->second));
                shard->records.erase(it);
                ++removed;
            }
            shard->deadlines.pop();
        }
    }
    return removed;
}

size_t LeaseManager::ScanPinnedReads(Clock::time_point now,
                                     std::vector<PinnedReadLease>& detached) {
    size_t removed = 0;
    for (auto& shard : pinned_reads_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        while (!shard->deadlines.empty()) {
            const auto& top = shard->deadlines.top();
            if (top.deadline > now) break;
            auto it = shard->records.find(top.key);
            if (it != shard->records.end() &&
                it->second.read_token == top.token &&
                it->second.deadline <= now) {
                shard->by_token.erase(it->second.read_token);
                detached.push_back(std::move(it->second));
                shard->records.erase(it);
                DecrementKeyCount(*shard, detached.back().id.key);
                ++removed;
            }
            shard->deadlines.pop();
        }
    }
    return removed;
}

size_t LeaseManager::ScanExpiredNow() {
    const auto now = clock_->Now();
    // Detached under the shard locks, destroyed here: a MutableBlock or an
    // ImmutableBlock destructor must never run inside a lease shard lock,
    // because it can call BlockPool::Free.
    std::vector<PendingWriteLease> writes;
    std::vector<PinnedReadLease> reads;
    const size_t removed =
        ScanPendingWrites(now, writes) + ScanPinnedReads(now, reads);
    writes.clear();
    reads.clear();
    return removed;
}

void LeaseManager::ScannerMain() {
    for (;;) {
        {
            std::unique_lock<std::mutex> lock(scanner_mu_);
            scanner_cv_.wait_for(lock, scan_interval_,
                                 [this] { return scanner_stop_; });
            if (scanner_stop_) return;
        }
        const size_t removed = ScanExpiredNow();
        if (removed > 0) {
            VLOG(1) << "LeaseManager expired " << removed << " leases";
        }
    }
}

void LeaseManager::StopAndDrain() {
    {
        std::lock_guard<std::mutex> lock(scanner_mu_);
        if (!scanner_stop_) {
            scanner_stop_ = true;
            scanner_cv_.notify_all();
        }
    }
    if (scanner_.joinable()) scanner_.join();

    std::vector<PendingWriteLease> writes;
    std::vector<PinnedReadLease> reads;
    for (auto& shard : pending_writes_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        for (auto& [key, record] : shard->records) {
            writes.push_back(std::move(record));
        }
        shard->records.clear();
        while (!shard->deadlines.empty()) shard->deadlines.pop();
    }
    for (auto& shard : pinned_reads_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        for (auto& [id, record] : shard->records) {
            reads.push_back(std::move(record));
        }
        shard->records.clear();
        shard->by_token.clear();
        shard->leases_per_key.clear();
        while (!shard->deadlines.empty()) shard->deadlines.pop();
    }
    // Destroyed here, outside every shard lock.
    writes.clear();
    reads.clear();
}

size_t LeaseManager::PendingWriteCount() const {
    size_t total = 0;
    for (const auto& shard : pending_writes_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        total += shard->records.size();
    }
    return total;
}

size_t LeaseManager::PinnedLeaseCount() const {
    size_t total = 0;
    for (const auto& shard : pinned_reads_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        total += shard->records.size();
    }
    return total;
}

size_t LeaseManager::PinnedTokenIndexSize() const {
    size_t total = 0;
    for (const auto& shard : pinned_reads_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        total += shard->by_token.size();
    }
    return total;
}

}  // namespace mooncake::v2
