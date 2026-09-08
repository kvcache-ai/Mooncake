#pragma once

// TenantCatalog: the per-tenant aggregate inside the MetadataCatalog —
// the tenant's ObjectIndex plus quota and eviction-census bookkeeping.

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "tenant/group_index.h"
#include "tenant/object_index.h"
#include "tenant_quota.h"

namespace mooncake {
namespace tenant {

class TenantCatalog {
   public:
    TenantQuotaHandle quota_account{nullptr};

    // Primary per-tenant object index (key -> strong ObjectEntry handle) with
    // a per-object mutation boundary (ObjectEntry::mutex).
    ObjectIndex object_index;

    // GroupIndex: group_id -> shared Lease + member keys.
    GroupIndex group_index;

    // Publication transaction: wire the group lease and register membership
    // before publishing, rolling membership back if the publish fails — a
    // concurrently-pinned entry never observes a half-wired grouped member.
    bool InsertObject(std::string key, std::shared_ptr<ObjectEntry> entry) {
        if (key != entry->key()) {
            return false;
        }
        const std::string group_id = entry->group_id();
        if (!group_id.empty()) {
            entry->metadata().SetLease(group_index.LeaseFor(group_id));
            if (!group_index.AddMember(group_id, entry->key())) {
                return false;
            }
        }
        if (!object_index.Insert(entry->key(), entry)) {
            if (!group_id.empty()) {
                group_index.RemoveMember(group_id, entry->key());
            }
            return false;
        }
        return true;
    }

    // Empty of objects, group membership and (via ObjectIndex) in-flight
    // dynamic-replication leases.
    bool Empty() const {
        return group_index.Empty() && object_index.Empty();
    }

    // Count of objects with >=1 completed LOCAL_DISK replica; the eviction
    // base is ObjectCount() - disk_object_count.
    std::atomic<long> disk_object_count{0};

    std::shared_ptr<ObjectEntry> Pin(const std::string& key) const {
        return object_index.Pin(key);
    }
    // Insert a NEW ObjectEntry; returns false if `key` already present.
    // Only erase when the route still resolves to `expected`; see
    // ObjectIndex::EraseIf.
    bool EraseObjectIf(const std::string& key,
                       const ObjectEntry* expected) {
        return object_index.EraseIf(key, expected);
    }
    bool ContainsObject(const std::string& key) const {
        return object_index.Contains(key);
    }
    size_t ObjectCount() const { return object_index.ObjectCount(); }
    std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects() const {
        return object_index.SnapshotObjects();
    }
    // True when this tenant holds no object route, no group membership, and
    // no in-flight dynamic-replication lease.
};

}  // namespace tenant
}  // namespace mooncake
