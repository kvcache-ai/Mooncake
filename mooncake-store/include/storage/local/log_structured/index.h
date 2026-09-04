#pragma once

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/local/log_structured/record_format.h"
#include "storage/local/log_structured/segment.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake::logstructured {

enum class VersionState : uint8_t {
    kPrepared,
    kCommitted,
    kAborted,
    kObsolete,
    kTombstoned,
    kReclaimable,
};

enum class IndexError {
    kNotFound,
    kInvalidTransition,
    kStaleSequence,
    kPhysicalMismatch,
};

struct VersionEntry {
    PhysicalRecord physical;
    VersionState state{VersionState::kPrepared};
    uint64_t sequence{0};
    uint64_t mutation_epoch{0};

    bool operator==(const VersionEntry&) const = default;
};

struct IndexSnapshotEntry {
    RecordIdentity identity;
    VersionEntry version;
};

class VersionIndex {
   public:
    tl::expected<void, IndexError> Prepare(const RecordIdentity& identity,
                                           const PhysicalRecord& physical,
                                           uint64_t sequence);
    tl::expected<void, IndexError> Commit(const RecordIdentity& identity,
                                          uint64_t sequence);
    tl::expected<void, IndexError> Abort(const RecordIdentity& identity,
                                         uint64_t sequence);
    tl::expected<void, IndexError> ApplyTombstone(
        const RecordIdentity& identity, uint64_t delete_sequence);
    tl::expected<void, IndexError> MarkReclaimable(
        const RecordIdentity& identity, uint64_t sequence);

    tl::expected<void, IndexError> InstallCompactionCopy(
        const RecordIdentity& identity, const PhysicalRecord& expected_source,
        uint64_t expected_epoch, const PhysicalRecord& target);

    std::optional<VersionEntry> LookupCommitted(
        const RecordIdentity& identity) const;
    std::optional<IndexSnapshotEntry> LookupCurrent(
        std::string_view tenant_id, std::string_view object_key) const;
    std::optional<VersionEntry> Lookup(const RecordIdentity& identity) const;
    std::vector<IndexSnapshotEntry> Snapshot() const;
    std::vector<IndexSnapshotEntry> CurrentSnapshot() const;
    tl::expected<void, IndexError> Restore(
        const std::vector<IndexSnapshotEntry>& snapshot);
    size_t size() const;

   private:
    struct IdentityHash {
        size_t operator()(const RecordIdentity& identity) const;
    };

    struct LogicalKey {
        std::string tenant_id;
        std::string object_key;

        bool operator==(const LogicalKey&) const = default;
    };

    struct LogicalKeyHash {
        size_t operator()(const LogicalKey& key) const;
    };

    static LogicalKey ToLogicalKey(const RecordIdentity& identity);

    mutable std::shared_mutex mutex_;
    std::unordered_map<RecordIdentity, VersionEntry, IdentityHash> versions_;
    std::unordered_map<LogicalKey, RecordIdentity, LogicalKeyHash> current_;
};

}  // namespace mooncake::logstructured
