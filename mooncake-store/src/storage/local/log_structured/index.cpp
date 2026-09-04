#include "storage/local/log_structured/index.h"

#include <functional>
#include <mutex>

namespace mooncake::logstructured {
namespace {

size_t HashCombine(size_t seed, size_t value) {
    return seed ^ (value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2));
}

}  // namespace

size_t VersionIndex::IdentityHash::operator()(
    const RecordIdentity& identity) const {
    size_t hash = std::hash<std::string>{}(identity.tenant_id);
    hash = HashCombine(hash, std::hash<std::string>{}(identity.object_key));
    hash = HashCombine(hash, std::hash<uint64_t>{}(identity.incarnation.high));
    return HashCombine(hash, std::hash<uint64_t>{}(identity.incarnation.low));
}

size_t VersionIndex::LogicalKeyHash::operator()(const LogicalKey& key) const {
    return HashCombine(std::hash<std::string>{}(key.tenant_id),
                       std::hash<std::string>{}(key.object_key));
}

VersionIndex::LogicalKey VersionIndex::ToLogicalKey(
    const RecordIdentity& identity) {
    return LogicalKey{.tenant_id = identity.tenant_id,
                      .object_key = identity.object_key};
}

tl::expected<void, IndexError> VersionIndex::Prepare(
    const RecordIdentity& identity, const PhysicalRecord& physical,
    uint64_t sequence) {
    std::unique_lock lock(mutex_);
    auto [it, inserted] = versions_.try_emplace(
        identity, VersionEntry{.physical = physical,
                               .state = VersionState::kPrepared,
                               .sequence = sequence,
                               .mutation_epoch = 1});
    if (inserted) {
        return {};
    }
    if (it->second.state == VersionState::kPrepared &&
        it->second.sequence == sequence && it->second.physical == physical) {
        return {};
    }
    if (sequence < it->second.sequence) {
        return tl::unexpected(IndexError::kStaleSequence);
    }
    return tl::unexpected(IndexError::kInvalidTransition);
}

tl::expected<void, IndexError> VersionIndex::Commit(
    const RecordIdentity& identity, uint64_t sequence) {
    std::unique_lock lock(mutex_);
    auto it = versions_.find(identity);
    if (it == versions_.end()) {
        return tl::unexpected(IndexError::kNotFound);
    }
    if (sequence < it->second.sequence) {
        return tl::unexpected(IndexError::kStaleSequence);
    }
    if (it->second.state == VersionState::kCommitted &&
        it->second.sequence == sequence) {
        return {};
    }
    if (it->second.state != VersionState::kPrepared ||
        it->second.sequence != sequence) {
        return tl::unexpected(IndexError::kInvalidTransition);
    }

    const auto logical_key = ToLogicalKey(identity);
    auto current = current_.find(logical_key);
    if (current != current_.end() && current->second != identity) {
        auto old = versions_.find(current->second);
        if (old != versions_.end()) {
            if (old->second.sequence >= sequence) {
                return tl::unexpected(IndexError::kStaleSequence);
            }
            old->second.state = VersionState::kObsolete;
            ++old->second.mutation_epoch;
        }
    }

    it->second.state = VersionState::kCommitted;
    ++it->second.mutation_epoch;
    current_.insert_or_assign(logical_key, identity);
    return {};
}

tl::expected<void, IndexError> VersionIndex::Abort(
    const RecordIdentity& identity, uint64_t sequence) {
    std::unique_lock lock(mutex_);
    auto it = versions_.find(identity);
    if (it == versions_.end()) {
        return tl::unexpected(IndexError::kNotFound);
    }
    if (sequence < it->second.sequence) {
        return tl::unexpected(IndexError::kStaleSequence);
    }
    if (it->second.state == VersionState::kAborted &&
        it->second.sequence == sequence) {
        return {};
    }
    if (it->second.state != VersionState::kPrepared ||
        it->second.sequence != sequence) {
        return tl::unexpected(IndexError::kInvalidTransition);
    }
    it->second.state = VersionState::kAborted;
    ++it->second.mutation_epoch;
    return {};
}

tl::expected<void, IndexError> VersionIndex::ApplyTombstone(
    const RecordIdentity& identity, uint64_t delete_sequence) {
    std::unique_lock lock(mutex_);
    auto it = versions_.find(identity);
    if (it == versions_.end()) {
        versions_.emplace(identity,
                          VersionEntry{.physical = {},
                                       .state = VersionState::kTombstoned,
                                       .sequence = delete_sequence,
                                       .mutation_epoch = 1});
        return {};
    }
    if (delete_sequence < it->second.sequence) {
        return tl::unexpected(IndexError::kStaleSequence);
    }
    if (it->second.state == VersionState::kTombstoned &&
        it->second.sequence == delete_sequence) {
        return {};
    }
    if (it->second.state == VersionState::kReclaimable) {
        return tl::unexpected(IndexError::kInvalidTransition);
    }

    const auto logical_key = ToLogicalKey(identity);
    auto current = current_.find(logical_key);
    if (current != current_.end() && current->second == identity) {
        current_.erase(current);
    }
    it->second.physical = {};
    it->second.state = VersionState::kTombstoned;
    it->second.sequence = delete_sequence;
    ++it->second.mutation_epoch;
    return {};
}

tl::expected<void, IndexError> VersionIndex::MarkReclaimable(
    const RecordIdentity& identity, uint64_t sequence) {
    std::unique_lock lock(mutex_);
    auto it = versions_.find(identity);
    if (it == versions_.end()) {
        return tl::unexpected(IndexError::kNotFound);
    }
    if (sequence < it->second.sequence) {
        return tl::unexpected(IndexError::kStaleSequence);
    }
    if (it->second.state == VersionState::kReclaimable) {
        return {};
    }
    if (it->second.state != VersionState::kAborted &&
        it->second.state != VersionState::kObsolete &&
        it->second.state != VersionState::kTombstoned) {
        return tl::unexpected(IndexError::kInvalidTransition);
    }
    it->second.state = VersionState::kReclaimable;
    it->second.sequence = sequence;
    ++it->second.mutation_epoch;
    return {};
}

tl::expected<void, IndexError> VersionIndex::InstallCompactionCopy(
    const RecordIdentity& identity, const PhysicalRecord& expected_source,
    uint64_t expected_epoch, const PhysicalRecord& target) {
    std::unique_lock lock(mutex_);
    auto it = versions_.find(identity);
    if (it == versions_.end()) {
        return tl::unexpected(IndexError::kNotFound);
    }
    if (it->second.state != VersionState::kCommitted) {
        return tl::unexpected(IndexError::kInvalidTransition);
    }
    if (it->second.mutation_epoch != expected_epoch) {
        return tl::unexpected(IndexError::kStaleSequence);
    }
    if (it->second.physical != expected_source) {
        return tl::unexpected(IndexError::kPhysicalMismatch);
    }
    it->second.physical = target;
    ++it->second.mutation_epoch;
    return {};
}

std::optional<VersionEntry> VersionIndex::LookupCommitted(
    const RecordIdentity& identity) const {
    std::shared_lock lock(mutex_);
    auto it = versions_.find(identity);
    if (it == versions_.end() || it->second.state != VersionState::kCommitted) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<VersionEntry> VersionIndex::Lookup(
    const RecordIdentity& identity) const {
    std::shared_lock lock(mutex_);
    auto it = versions_.find(identity);
    if (it == versions_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<IndexSnapshotEntry> VersionIndex::Snapshot() const {
    std::shared_lock lock(mutex_);
    std::vector<IndexSnapshotEntry> snapshot;
    snapshot.reserve(versions_.size());
    for (const auto& [identity, version] : versions_) {
        snapshot.push_back(
            IndexSnapshotEntry{.identity = identity, .version = version});
    }
    return snapshot;
}

size_t VersionIndex::size() const {
    std::shared_lock lock(mutex_);
    return versions_.size();
}

}  // namespace mooncake::logstructured
