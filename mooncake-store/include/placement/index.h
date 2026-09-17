#pragma once

#include <array>
#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>
#include <cassert>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "placement/candidate.h"
#include "types.h"
#include "common/transparent_string_hash.h"

namespace mooncake {

class PlacementIndex final {
   public:
    // One (segment_name, kind) entry. Candidates are interchangeable for one
    // allocation; multi-replica placement uses each entry at most once.
    struct Entry final {
        std::string segment_name;
        const AllocationCandidateKind kind;
        boost::container::flat_set<const AllocationCandidate*> candidates;
    };

    PlacementIndex() = default;
    PlacementIndex(PlacementIndex&&) = default;
    PlacementIndex& operator=(PlacementIndex&&) = default;
    PlacementIndex(const PlacementIndex&) = delete;
    PlacementIndex& operator=(const PlacementIndex&) = delete;

    // A candidate keeps the same segment name and host identity until removed.
    bool AddCandidate(std::string_view segment_name,
                      const AllocationCandidate& candidate,
                      std::string_view host_id);
    bool RemoveCandidate(std::string_view segment_name,
                         const AllocationCandidate& candidate,
                         std::string_view host_id);
    bool ReplaceCandidate(std::string_view segment_name,
                          const AllocationCandidate& expected,
                          const AllocationCandidate& replacement,
                          std::string_view host_id);
    void Clear();

    // Visit unique names in host/key order under the read lock. Returning
    // true stops traversal. Borrowed names remain valid only under this lock.
    void VisitHostOrderedSegmentNames(
        std::string_view writer_host_id, std::string_view key,
        const std::function<bool(std::string_view)>& visit) const;
    const Entry* Find(std::string_view segment_name,
                      AllocationCandidateKind kind) const;
    bool Contains(std::string_view segment_name,
                  AllocationCandidateKind kind) const {
        return Find(segment_name, kind) != nullptr;
    }
    void GetActiveSegmentNames(AllocationCandidateKind kind,
                               std::vector<std::string>& names) const;
    std::span<const Entry* const> active_entries(
        AllocationCandidateKind kind) const {
        return entries_by_kind_[KindIndex(kind)].ActiveEntries();
    }

   private:
    static constexpr size_t KindIndex(AllocationCandidateKind kind) noexcept {
        const auto index = static_cast<size_t>(kind);
        assert(index < kAllocationCandidateKindCount);
        return index;
    }

    void AddHostMember(std::string_view host_id, std::string_view segment_name);
    void RemoveHostMember(std::string_view host_id,
                          std::string_view segment_name);

    // Counts span kinds and regions. A name remains until its last active
    // candidate on this host is removed. flat_map provides indexed name access.
    using HostSegments =
        boost::container::flat_map<std::string, size_t, std::less<>>;
    std::map<std::string, HostSegments, std::less<>> segments_by_host_;

    // Both views contain the same active entries; inactive regions live in
    // RegionCatalog. EntryMap owns their lifetime and keeps the views in sync.
    class EntryMap final {
       public:
        Entry* Find(std::string_view name);
        const Entry* Find(std::string_view name) const;
        void Insert(std::unique_ptr<Entry> entry) noexcept;
        void Erase(const Entry& entry);
        void Clear();
        std::span<const Entry* const> ActiveEntries() const { return active_; }

       private:
        std::unordered_map<std::string, std::unique_ptr<Entry>,
                           TransparentStringHash, std::equal_to<>>
            by_name_;
        std::vector<const Entry*> active_;
    };
    std::array<EntryMap, kAllocationCandidateKindCount> entries_by_kind_;
};

class RegionCatalog;

// Borrows the indexes and holds their shared lock. The owner of the indexes
// and mutex must outlive this access object.
class ScopedPlacementReadAccess final {
   public:
    ScopedPlacementReadAccess(const PlacementIndex& placement,
                              const RegionCatalog& catalog,
                              std::shared_mutex& mutex)
        : placement_(placement), catalog_(catalog), lock_(mutex) {}

    // The view and entries borrowed from it must only be used while this
    // access object retains its lock.
    const PlacementIndex& GetView() const { return placement_; }

    std::optional<UUID> GetOwnerClientId(std::string_view segment_name) const;

   private:
    const PlacementIndex& placement_;
    const RegionCatalog& catalog_;
    std::shared_lock<std::shared_mutex> lock_;
};

}  // namespace mooncake
