#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "ha/kv/ha_kv_backend.h"
#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

// Complete storage target identity; credentials and resource limits are not
// part of it. Changing any target field requires a different catalog/namespace.
struct ObjectStorageNamespace {
    std::string provider;
    std::string endpoint;
    std::string bucket;
    std::string region;
    std::string prefix;
    bool path_style{false};
    YLT_REFL(ObjectStorageNamespace, provider, endpoint, bucket, region, prefix,
             path_style);
};

struct ObjectStorageCatalogConfig {
    ObjectStorageNamespace storage_namespace;
    uint64_t max_object_bytes{256ULL * 1024 * 1024};
    uint64_t max_pending_bytes{1024ULL * 1024 * 1024};
    uint64_t max_pending_objects{10000};
    uint64_t maintenance_batch_size{32};
};

enum class ObjectStoragePhase : uint8_t {
    PREPARED,
    UPLOADING,
    COMPLETE,
    RETIRED,
    DELETED
};
enum class ObjectUploadOutcome : uint8_t { COMPLETE, NOT_STARTED, UNKNOWN };

struct ObjectStorageRecord {
    std::string tenant;
    std::string key;
    std::string object_key;
    std::string namespace_id;
    uint64_t object_size{0};
    uint64_t generation{0};
    uint64_t revision{0};
    ObjectStoragePhase phase{ObjectStoragePhase::PREPARED};
    bool foreground_complete{false};
    bool upload_started{false};
    bool upload_finished{false};
    bool upload_succeeded{false};
    bool reclaim_started{false};
    uint64_t retire_after_ms{0};
    std::optional<uint64_t> checksum;
    YLT_REFL(ObjectStorageRecord, tenant, key, object_key, namespace_id,
             object_size, generation, revision, phase, foreground_complete,
             upload_started, upload_finished, upload_succeeded, reclaim_started,
             retire_after_ms, checksum);
};

// Independent lifecycle component using the existing transactional HA KV API.
// Production persistence currently requires EtcdHaKvBackend. An injected test
// backend must support this catalog's arbitrary string keys and transactions;
// the existing oplog-only RedisHaKvBackend does not support its keyspace.
// Open must complete before use. epoch=0 acquires the next catalog epoch;
// explicit epochs must strictly increase. Unresolved transaction outcomes fail
// closed until Open succeeds again. Calls never retry a physical object PUT.
// The caller enforces read leases/source ownership and stops new leases before
// Retire, supplying the latest outstanding lease deadline in milliseconds.
class ObjectStorageCatalog {
   public:
    ObjectStorageCatalog(ObjectStorageCatalogConfig config,
                         std::shared_ptr<HaKvBackend> backend,
                         std::string cluster_id);
    ErrorCode Open(uint64_t epoch = 0);
    tl::expected<ObjectStorageRecord, ErrorCode> Prepare(
        const std::string& tenant, const std::string& key,
        uint64_t object_size);
    // Exactly one successful claim authorizes one PUT. A repeated claim fails,
    // even if the caller has not yet used the first permission.
    ErrorCode Claim(const std::string& object_key);
    ErrorCode Finish(const std::string& object_key,
                     ObjectUploadOutcome outcome);
    // Foreground commit is separate from upload completion (also usable by
    // future background-mode integration). Retired attempts cannot be
    // committed.
    ErrorCode Commit(const std::string& object_key,
                     std::optional<uint64_t> checksum = std::nullopt);
    ErrorCode Retire(const std::string& object_key, uint64_t not_before_ms);
    tl::expected<ObjectStorageRecord, ErrorCode> Find(
        const std::string& object_key) const;
    tl::expected<ObjectStorageRecord, ErrorCode> Head(
        const std::string& tenant, const std::string& key) const;
    tl::expected<std::vector<ObjectStorageRecord>, ErrorCode> Records() const;
    bool CanReserve(uint64_t bytes) const;
    // Bounds inspected records as well as DELETEs, rotates for fairness, and
    // performs provider DELETE outside the catalog mutex. An uncertain PUT
    // retains its tombstone even after DELETE succeeds. Once reclamation is
    // admitted, extending that attempt's lease deadline is rejected.
    ErrorCode Reclaim(ObjectStorageAdapter& adapter, uint64_t now_ms);

   private:
    using Identity = std::pair<std::string, std::string>;
    bool Valid(const ObjectStorageRecord& record) const;
    bool ApplyLocked(const ObjectStorageRecord& record);
    void EraseLocked(const std::string& object_key);
    bool CanReserveLocked(uint64_t bytes,
                          const ObjectStorageRecord* credit) const;
    ErrorCode PersistLocked(ObjectStorageRecord record,
                            std::optional<ObjectStorageRecord> replaced = {});
    ErrorCode LoadLocked();
    std::string RecordKey(const std::string& object_key) const;
    std::string HeadKey(const Identity& identity) const;
    std::string ObjectKey(const Identity& identity, uint64_t generation) const;

    const ObjectStorageCatalogConfig config_;
    const std::shared_ptr<HaKvBackend> backend_;
    const std::string cluster_id_;
    const std::string prefix_;
    const std::string namespace_id_;
    mutable std::mutex mutex_;
    std::string fence_;
    bool ready_{false};
    std::map<std::string, ObjectStorageRecord> records_;
    // Heads survive physical GC, preventing generation reuse and resurrection
    // from an older snapshot. Distinct historical keys are not compacted yet.
    std::map<Identity, ObjectStorageRecord> heads_;
    std::set<std::string> reclaim_keys_;
    std::string reclaim_cursor_;
    uint64_t pending_bytes_{0};
    uint64_t pending_count_{0};
};

}  // namespace mooncake
