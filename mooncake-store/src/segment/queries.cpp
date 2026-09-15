#include "segment/queries.h"

#include <algorithm>
#include <limits>
#include <unordered_set>

#include "placement/index.h"
#include "placement/replica_placement.h"
#include <tuple>
#include "segment/catalog.h"

namespace mooncake {

std::vector<SegmentInfo> SegmentQueries::Segments() const {
    std::vector<SegmentInfo> result;
    for (const auto& region : query_catalog_.Regions()) {
        result.push_back({region.segment, region.client_id, region.status});
    }
    return result;
}

std::optional<SegmentInfo> SegmentQueries::FindSegment(const UUID& id) const {
    const auto* region = query_catalog_.Find(id);
    if (!region) return std::nullopt;
    return SegmentInfo{region->segment, region->client_id, region->status};
}

bool SegmentQueries::ContainsSegment(std::string_view name) const {
    return !query_catalog_.RegionIds(name).empty();
}

bool SegmentQueries::HasEndpoint(std::string_view endpoint) const {
    return std::ranges::any_of(
        query_catalog_.Regions(), [&](const auto& region) {
            return region.segment.te_endpoint == endpoint;
        });
}

void SegmentQueries::GetSegmentNames(std::vector<std::string>& names) const {
    names.clear();
    std::unordered_set<std::string> seen;
    for (const auto& region : query_catalog_.Regions()) {
        if (seen.insert(region.segment.name).second)
            names.push_back(region.segment.name);
    }
}

ErrorCode SegmentQueries::GetClientSegments(
    const UUID& client, std::vector<Segment>& segments) const {
    segments.clear();
    for (const auto& region : query_catalog_.Regions()) {
        if (region.client_id == client) segments.push_back(region.segment);
    }
    return segments.empty() ? ErrorCode::SEGMENT_NOT_FOUND : ErrorCode::OK;
}

ErrorCode SegmentQueries::GetSegmentStatus(std::string_view name,
                                           SegmentStatus& status) const {
    const auto ids = query_catalog_.RegionIds(name);
    if (ids.empty()) return ErrorCode::SEGMENT_NOT_FOUND;
    status = SegmentStatus::UNDEFINED;
    for (const auto& id : ids) {
        const auto candidate = query_catalog_.Find(id)->status;
        if (SegmentStatusAvailabilityRank(candidate) <
            SegmentStatusAvailabilityRank(status)) {
            status = candidate;
        }
    }
    return ErrorCode::OK;
}

ErrorCode SegmentQueries::GetSegmentStatus(const UUID& id,
                                           SegmentStatus& status) const {
    const auto* region = query_catalog_.Find(id);
    if (!region) return ErrorCode::SEGMENT_NOT_FOUND;
    status = region->status;
    return ErrorCode::OK;
}

std::optional<UUID> SegmentQueries::FindOwnerClientId(
    std::string_view name) const {
    return query_catalog_.FindOwnerClientId(name);
}

ErrorCode SegmentQueries::GetSegmentOwner(std::string_view name,
                                          UUID& owner) const {
    const auto found = FindOwnerClientId(name);
    if (!found) return ErrorCode::SEGMENT_NOT_FOUND;
    owner = *found;
    return ErrorCode::OK;
}

bool SegmentQueries::HasServingCandidate(std::string_view name) const {
    const auto* entry = query_placement_.Find(name, query_kind_);
    return entry &&
           std::ranges::any_of(entry->candidates, [](const auto* candidate) {
               return candidate->IsServing();
           });
}

void SegmentQueries::GetActiveSegmentNames(
    std::vector<std::string>& names) const {
    query_placement_.GetActiveSegmentNames(query_kind_, names);
}

ErrorCode SegmentQueries::QueryCapacity(std::string_view name, size_t& used,
                                        size_t& capacity) const {
    const auto* entry = query_placement_.Find(name, query_kind_);
    if (!entry) return ErrorCode::SEGMENT_NOT_FOUND;
    used = capacity = 0;
    for (const auto* candidate : entry->candidates) {
        used += candidate->Used();
        capacity += candidate->Capacity();
    }
    return capacity ? ErrorCode::OK : ErrorCode::SEGMENT_NOT_FOUND;
}

ErrorCode SegmentQueries::ValidateTargets(
    std::span<const std::string> targets) const {
    for (const auto& name : targets) {
        if (!ContainsSegment(name)) return ErrorCode::SEGMENT_NOT_FOUND;
        if (!HasServingCandidate(name))
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    return ErrorCode::OK;
}

ErrorCode SegmentQueries::ValidateDrain(
    std::span<const std::string> sources,
    std::span<const std::string> targets) const {
    if (sources.empty()) return ErrorCode::INVALID_PARAMS;
    std::unordered_set<std::string> unique(sources.begin(), sources.end());
    if (unique.size() != sources.size()) return ErrorCode::INVALID_PARAMS;
    for (const auto& name : sources) {
        SegmentStatus status;
        auto result = GetSegmentStatus(name, status);
        if (result != ErrorCode::OK) return result;
        if (status != SegmentStatus::OK)
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    for (const auto& name : targets) {
        if (unique.contains(name)) return ErrorCode::INVALID_PARAMS;
        SegmentStatus status;
        auto result = GetSegmentStatus(name, status);
        if (result != ErrorCode::OK) return result;
        if (status != SegmentStatus::OK || !HasServingCandidate(name))
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    return ErrorCode::OK;
}

std::optional<std::string> SegmentQueries::SelectDrainTarget(
    std::string_view source, std::span<const std::string> existing,
    std::span<const std::string> requested) const {
    std::vector<std::string> active;
    if (requested.empty()) {
        GetActiveSegmentNames(active);
        requested = active;
    }
    double best_util = std::numeric_limits<double>::max();
    std::optional<std::string> best;
    for (const auto& candidate : requested) {
        if (candidate == source ||
            std::find(existing.begin(), existing.end(), candidate) !=
                existing.end() ||
            !HasServingCandidate(candidate))
            continue;
        size_t used, capacity;
        if (QueryCapacity(candidate, used, capacity) != ErrorCode::OK ||
            !capacity)
            continue;
        const double util = static_cast<double>(used) / capacity;
        if (util < best_util) {
            best_util = util;
            best = candidate;
        }
    }
    return best;
}

std::optional<std::string> SegmentQueries::SelectReplicationTarget(
    std::string_view key, size_t size, std::span<const std::string> existing,
    const std::optional<std::string>& preferred, double high_watermark) const {
    std::unordered_set<std::string> existing_hosts;
    // Match the first resource of each name, just as the previous registry
    // projection did. Same-name resources may have independent owners.
    for (const auto& name : existing) {
        for (const auto& mounted : query_catalog_.Regions()) {
            if (mounted.segment.name == name) {
                if (!mounted.segment.host_id.empty())
                    existing_hosts.insert(mounted.segment.host_id);
                break;
            }
        }
    }
    using Score = std::tuple<bool, double, uint64_t>;
    const auto score = [&](const Segment& segment) -> std::optional<Score> {
        if (std::find(existing.begin(), existing.end(), segment.name) !=
                existing.end() ||
            !HasServingCandidate(segment.name))
            return std::nullopt;
        size_t used, capacity;
        if (QueryCapacity(segment.name, used, capacity) != ErrorCode::OK ||
            !capacity || used >= capacity || capacity - used < size ||
            static_cast<double>(used + size) / capacity >= high_watermark)
            return std::nullopt;
        return Score{segment.host_id.empty() || existing_hosts.empty() ||
                         !existing_hosts.contains(segment.host_id),
                     static_cast<double>(used) / capacity,
                     StableSegmentScore(key, segment.name)};
    };
    if (preferred) {
        for (const auto& mounted : query_catalog_.Regions()) {
            if (mounted.segment.name == *preferred) {
                if (score(mounted.segment)) return mounted.segment.name;
                break;
            }
        }
    }
    std::optional<Score> best_score;
    std::optional<std::string> best;
    for (const auto& mounted : query_catalog_.Regions()) {
        const auto candidate = score(mounted.segment);
        if (!candidate) continue;
        if (!best_score || std::get<0>(*candidate) > std::get<0>(*best_score) ||
            (std::get<0>(*candidate) == std::get<0>(*best_score) &&
             std::tie(std::get<1>(*candidate), std::get<2>(*candidate)) <
                 std::tie(std::get<1>(*best_score),
                          std::get<2>(*best_score)))) {
            best_score = candidate;
            best = mounted.segment.name;
        }
    }
    return best;
}

}  // namespace mooncake
