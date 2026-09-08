#pragma once

// TenantCatalog: the per-tenant aggregate inside the MetadataCatalog. It owns
// the ObjectIndex for this tenant plus tenant-scoped bookkeeping (quota
// handle, LOCAL_DISK replica census). Business policy stays in MasterService;
// this type is pure state + thin pass-through views over the ObjectIndex.

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "tenant/object_index.h"
#include "tenant_quota.h"

namespace mooncake {
namespace tenant {

class TenantCatalog {
   public:
    TenantQuotaHandle quota_account{nullptr};

    // Primary per-tenant object index (key -> strong ObjectEntry handle) with
    // a per-object mutation boundary (ObjectEntry::mutex) and the thin flat
    // group membership (one shared Lease + member keys).
    ObjectIndex object_index;

    // Count of objects with >=1 completed LOCAL_DISK replica; the eviction
    // base is ObjectCount() - disk_object_count.
    std::atomic<long> disk_object_count{0};

    std::shared_ptr<ObjectEntry> Pin(const std::string& key) const {
        return object_index.Pin(key);
    }
    // Insert a NEW ObjectEntry; returns false if `key` already present.
    bool InsertObject(std::string key,
                      std::shared_ptr<ObjectEntry> entry) {
        return object_index.InsertObject(std::move(key), std::move(entry));
    }
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
    bool Empty() const { return object_index.Empty(); }
};

}  // namespace tenant
}  // namespace mooncake
