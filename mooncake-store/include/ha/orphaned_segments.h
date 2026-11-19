#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "allocator.h"
#include "metadata/object_route.h"
#include "metadata_store.h"
#include "replica.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

// A replica an HA restore put on an orphaned segment, named by the object that
// holds it so a remount finds it without walking the store. The replica can be
// gone by the time the segment is re-adopted; the lookup notices.
struct OrphanedReplica {
    TenantId tenant_id;
    route::ObjectRef object;
    ReplicaID replica_id = 0;
};

// A memory segment an HA restore brought back without its client. The replicas
// restored onto it point at `keepalive`, and `charged_bytes` of them are
// counted as allocated on the segment until a client re-adopts it.
struct OrphanedSegment {
    StandbySegmentInfo info;
    std::shared_ptr<BufferAllocatorBase> keepalive;
    uint64_t charged_bytes = 0;
    // Whether the segment was mounted when the restore ran. Replicas on an
    // unmounted one stay unreadable until its client remounts it.
    bool readable = false;
    // Every live memory replica restored onto the segment.
    std::vector<OrphanedReplica> replicas;
};

// The segments an HA restore left orphaned, from the restore that installs them
// to the remount that ends each one.
//
// Callers hold the master's state lock: shared to read, exclusive to change.
class OrphanedSegments {
   public:
    // Bytes counted as allocated on a segment on an orphan's behalf.
    struct Charge {
        std::string segment_name;
        uint64_t bytes = 0;
    };

    // Replaces every orphan with `orphans`, whose names and endpoints are
    // unique, and returns the charges of the ones it discards.
    std::vector<Charge> Reset(std::vector<OrphanedSegment> orphans) {
        std::vector<Charge> discarded = ReleaseAll();
        for (auto& orphan : orphans) {
            const std::string name = orphan.info.segment_name;
            name_by_endpoint_[orphan.info.transport_endpoint] = name;
            by_name_[name] = std::move(orphan);
        }
        return discarded;
    }

    bool empty() const { return by_name_.empty(); }

    // The orphan at exactly `endpoint`, or nullptr. A later restore onto the
    // same endpoint takes over its allocator and its replicas, so the buffers
    // restored before stay alive and are still re-adopted.
    const OrphanedSegment* FindAtEndpoint(const std::string& endpoint) const {
        return FindByEndpoint(endpoint);
    }

    // Validates that a client presenting `segment` does not contradict an
    // orphan it names, or name two different ones.
    ErrorCode Validate(const Segment& segment) const {
        const OrphanedSegment* by_endpoint =
            FindByEndpoint(segment.te_endpoint);
        const OrphanedSegment* by_name = FindByName(segment.name);
        if (by_endpoint != nullptr && by_name != nullptr &&
            by_endpoint != by_name) {
            return ErrorCode::INVALID_PARAMS;
        }
        const OrphanedSegment* orphan =
            by_endpoint != nullptr ? by_endpoint : by_name;
        if (orphan == nullptr) {
            return ErrorCode::OK;
        }
        if (segment.protocol == "cxl") {
            return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
        }
        if (orphan->info.segment_name != segment.name ||
            orphan->info.transport_endpoint != segment.te_endpoint ||
            orphan->info.capacity != segment.size) {
            return ErrorCode::INVALID_PARAMS;
        }
        return ErrorCode::OK;
    }

    // The orphan matching `segment`, or nullptr when none matches.
    const OrphanedSegment* Find(const Segment& segment) const {
        const OrphanedSegment* orphan = FindByName(segment.name);
        if (orphan == nullptr || segment.protocol == "cxl" ||
            orphan->info.transport_endpoint != segment.te_endpoint ||
            orphan->info.capacity != segment.size) {
            return nullptr;
        }
        return orphan;
    }

    // Whether replicas at `endpoint`, a segment's transport endpoint or its
    // name, belong to an orphan nobody has re-adopted yet.
    bool IsUnreadable(const std::string& endpoint) const {
        const OrphanedSegment* orphan = FindByEndpoint(endpoint);
        if (orphan == nullptr) {
            orphan = FindByName(endpoint);
        }
        return orphan != nullptr && !orphan->readable;
    }

    // Ends the orphan `segment` re-adopts: its replicas become readable, its
    // allocator is released, and whatever was charged to it is returned, since
    // the re-adopted allocator counts the replicas that survived on its own.
    std::optional<Charge> Adopt(const Segment& segment) {
        if (Find(segment) == nullptr) {
            return std::nullopt;
        }
        OrphanedSegment orphan = std::move(by_name_.at(segment.name));
        name_by_endpoint_.erase(orphan.info.transport_endpoint);
        by_name_.erase(segment.name);
        if (orphan.charged_bytes == 0) {
            return std::nullopt;
        }
        return Charge{std::move(orphan.info.segment_name),
                      orphan.charged_bytes};
    }

    // Drops every orphan and returns what was charged to them.
    std::vector<Charge> ReleaseAll() {
        std::vector<Charge> charges;
        for (auto& [name, orphan] : by_name_) {
            if (orphan.charged_bytes > 0) {
                charges.push_back({name, orphan.charged_bytes});
            }
        }
        by_name_.clear();
        name_by_endpoint_.clear();
        return charges;
    }

   private:
    const OrphanedSegment* FindByName(const std::string& name) const {
        auto it = by_name_.find(name);
        return it == by_name_.end() ? nullptr : &it->second;
    }

    const OrphanedSegment* FindByEndpoint(const std::string& endpoint) const {
        auto it = name_by_endpoint_.find(endpoint);
        return it == name_by_endpoint_.end() ? nullptr : FindByName(it->second);
    }

    std::unordered_map<std::string, OrphanedSegment> by_name_;
    std::unordered_map<std::string, std::string> name_by_endpoint_;
};

}  // namespace mooncake
