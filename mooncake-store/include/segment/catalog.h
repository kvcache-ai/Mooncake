#pragma once

#include <boost/functional/hash.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <optional>
#include <ranges>
#include <string_view>

#include "segment/mounted_region.h"
#include "common/transparent_string_hash.h"

namespace mooncake {

class RegionCatalog final {
    struct SegmentId {
        using result_type = const UUID&;
        result_type operator()(const MountedRegion& r) const {
            return r.segment.id;
        }
    };
    struct SegmentName {
        using result_type = const std::string&;
        result_type operator()(const MountedRegion& r) const {
            return r.segment.name;
        }
    };
    using Records = boost::multi_index_container<
        MountedRegion,
        boost::multi_index::indexed_by<
            boost::multi_index::hashed_unique<SegmentId, boost::hash<UUID>>,
            boost::multi_index::hashed_non_unique<
                SegmentName, TransparentStringHash, std::equal_to<>>>>;

   public:
    // Borrowed records and ranges must not outlive the owner's Pool lock.
    const MountedRegion* Find(const UUID& segment_id) const;
    auto Regions() const { return std::views::all(records_); }
    auto RegionIds(std::string_view name) const {
        auto [first, last] = records_.get<1>().equal_range(name);
        return std::ranges::subrange(first, last, std::distance(first, last)) |
               std::views::transform([](const MountedRegion& r) -> const UUID& {
                   return r.segment.id;
               });
    }
    ErrorCode Register(const MountedRegion& mounted);
    bool Erase(const UUID& segment_id);
    bool SetStatus(const UUID& segment_id, SegmentStatus status);
    void Clear();
    std::optional<UUID> FindOwnerClientId(std::string_view name) const;

   private:
    Records records_;
    // Never reset by Clear: outstanding transactions may outlive the records.
    uint64_t next_generation_{0};
};

}  // namespace mooncake
