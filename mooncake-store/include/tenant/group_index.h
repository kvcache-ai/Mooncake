#pragma once

// GroupIndex: group_id -> {shared Lease, member keys}. A group is not a
// container of objects: it is a thin membership table plus a single shared
// Lease. The lease is the all-or-none unit consulted by eviction (the
// per-member ObjectMetadata entry points at it); the read path extends that
// shared lease on a member hit without touching this index.

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lease.h"

namespace mooncake {
namespace tenant {

// GroupIndex: group_id -> {shared Lease, member keys}. A group is not a
// container of objects: it is a thin membership table plus a single shared
// Lease. The lease is the all-or-none unit consulted by eviction (the
// per-member ObjectMetadata entry points at it); the read path extends that
// shared lease on a member hit without touching this index.
class GroupIndex {
   public:
    // Return (creating on demand) the single shared Lease for a group_id.
    std::shared_ptr<Lease> LeaseFor(const std::string& group_id) {
        std::unique_lock<std::shared_mutex> lock(lock_);
        auto [it, inserted] = groups_.try_emplace(group_id);
        if (inserted) {
            it->second.lease = std::make_shared<Lease>();
        }
        return it->second.lease;
    }

    bool AddMember(const std::string& group_id, const std::string& member_key) {
        std::unique_lock<std::shared_mutex> lock(lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return false;  // group must be materialized via LeaseFor first
        }
        return it->second.member_keys.insert(member_key).second;
    }

    bool RemoveMember(const std::string& group_id,
                      const std::string& member_key) {
        std::unique_lock<std::shared_mutex> lock(lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return false;
        }
        const bool erased = it->second.member_keys.erase(member_key) > 0;
        if (erased && it->second.Empty()) {
            groups_.erase(it);
        }
        return erased;
    }

    std::vector<std::string> Members(const std::string& group_id) const {
        std::shared_lock<std::shared_mutex> lock(lock_);
        auto it = groups_.find(group_id);
        if (it == groups_.end()) {
            return {};
        }
        return {it->second.member_keys.begin(), it->second.member_keys.end()};
    }

    bool Empty() const {
        std::shared_lock<std::shared_mutex> lock(lock_);
        return groups_.empty();
    }

   private:
    struct GroupState {
        std::unordered_set<std::string> member_keys;
        std::shared_ptr<Lease> lease;

        bool Empty() const { return member_keys.empty(); }
    };

    mutable std::shared_mutex lock_;
    std::unordered_map<std::string, GroupState> groups_;
};

}  // namespace tenant
}  // namespace mooncake
