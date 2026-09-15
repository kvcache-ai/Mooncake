#pragma once

#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "types.h"
#include "segment/status.h"
#include "placement/candidate.h"

namespace mooncake {

class RegionCatalog;
class PlacementIndex;

// Owned resource description: no allocator, candidate, or catalog lifetime.
struct SegmentInfo {
    Segment segment;
    UUID client_id;
    SegmentStatus status;
};

// Semantic queries used only while the enclosing Pool access retains its lock.
// The same surface on read/write access preserves multi-query consistency.
class SegmentQueries {
   public:
    std::vector<SegmentInfo> Segments() const;
    std::optional<SegmentInfo> FindSegment(const UUID& id) const;
    bool ContainsSegment(std::string_view name) const;
    bool HasEndpoint(std::string_view endpoint) const;
    void GetSegmentNames(std::vector<std::string>& names) const;
    ErrorCode GetClientSegments(const UUID& client,
                                std::vector<Segment>& segments) const;
    ErrorCode GetSegmentStatus(std::string_view name,
                               SegmentStatus& status) const;
    ErrorCode GetSegmentStatus(const UUID& id, SegmentStatus& status) const;
    std::optional<UUID> FindOwnerClientId(std::string_view name) const;
    ErrorCode GetSegmentOwner(std::string_view name, UUID& owner) const;
    bool HasServingCandidate(std::string_view name) const;
    void GetActiveSegmentNames(std::vector<std::string>& names) const;
    ErrorCode ValidateTargets(std::span<const std::string> targets) const;
    ErrorCode ValidateDrain(std::span<const std::string> sources,
                            std::span<const std::string> targets) const;
    std::optional<std::string> SelectDrainTarget(
        std::string_view source, std::span<const std::string> existing,
        std::span<const std::string> requested) const;
    std::optional<std::string> SelectReplicationTarget(
        std::string_view key, size_t size,
        std::span<const std::string> existing,
        const std::optional<std::string>& preferred,
        double high_watermark) const;
    ErrorCode QueryCapacity(std::string_view name, size_t& used,
                            size_t& capacity) const;

   protected:
    SegmentQueries(const RegionCatalog& catalog,
                   const PlacementIndex& placement,
                   AllocationCandidateKind kind)
        : query_catalog_(catalog),
          query_placement_(placement),
          query_kind_(kind) {}

   private:
    const RegionCatalog& query_catalog_;
    const PlacementIndex& query_placement_;
    AllocationCandidateKind query_kind_;
};

}  // namespace mooncake
