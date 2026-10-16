#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <map>
#include <set>
#include <vector>

#include "config/object_storage_config.h"
#include "ha/kv/ha_kv_backend.h"
#include "replica.h"
#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

enum class ObjectStoragePhase : uint8_t {
    PREPARED,
    DEFERRED,
    READY,
    UPLOADING,
    COMPLETE,
    RETIRED,
    DELETED,
};

// One durable intent per immutable backend name. Records with an uncertain PUT
// outcome remain tombstones even after a successful DELETE: a timed-out PUT
// may still materialize later. Admission bounds those unresolved records.
struct ObjectStorageRecord {
    std::string tenant_id;
    std::string key;
    Replica::Descriptor target;
    Replica::Descriptor source;
    UUID writer{};
    UUID uploader{};
    ObjectStoragePhase phase{ObjectStoragePhase::PREPARED};
    bool upload_started{false};
    bool upload_finished{false};
    bool foreground_complete{false};
    bool source_released{false};
    bool object_deleted{false};
    uint64_t upload_started_ms{0};
    uint64_t revision{0};
    uint64_t retire_after_ms{0};
    std::optional<uint64_t> checksum;
    std::string group_id;
    bool hard_pinned{false};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};

    const ObjectStorageDescriptor& descriptor() const {
        return target.get_object_storage_descriptor();
    }
    YLT_REFL(ObjectStorageRecord, tenant_id, key, target, source, writer,
             uploader, phase, upload_started, upload_finished,
             foreground_complete, source_released, object_deleted,
             upload_started_ms, revision, retire_after_ms, checksum, group_id,
             hard_pinned, data_type);
};

// The existing HA transactional KV backend owns durability and atomic fencing.
// No object-store network request executes under this catalog's mutex or a
// Master metadata lock. Callers serialize per-key visibility transitions.
class ObjectStorageCatalog {
   public:
    ObjectStorageCatalog(ObjectStorageConfig config,
                         std::shared_ptr<HaKvBackend> backend,
                         std::string cluster_id);

    ErrorCode Open(uint64_t leader_epoch);
    ErrorCode Save(const ObjectStorageRecord& record);
    ErrorCode Retire(const std::string& object_key, uint64_t not_before_ms);
    std::optional<ObjectStorageRecord> Find(
        const std::string& object_key) const;
    std::vector<ObjectStorageRecord> Records() const;
    std::vector<ObjectStorageRecord> Heads() const;
    std::optional<ObjectStorageRecord> Head(const std::string& tenant,
                                            const std::string& key) const;
    std::vector<ObjectStorageRecord> NextRecords(std::string& cursor) const;
    std::vector<ObjectStorageRecord> TasksFor(const UUID& client) const;
    std::vector<ObjectStorageRecord> ForObject(const std::string& tenant,
                                               const std::string& key) const;
    bool CanReserve(uint64_t bytes) const;

    // Called by bounded background maintenance. Uncertain uploads are retried
    // on subsequent rounds; permission/timeout failures never become success.
    ErrorCode Reclaim(ObjectStorageAdapter& adapter, uint64_t now_ms);
    const ObjectStorageConfig& config() const { return config_; }

   private:
    void ApplyLocked(const ObjectStorageRecord& record);
    void Index(const ObjectStorageRecord& record, bool add);
    ErrorCode SaveLocked(const ObjectStorageRecord& record);
    bool CanReserveLocked(uint64_t bytes) const;
    std::string RecordKey(const std::string& object_key) const;

    ObjectStorageConfig config_;
    std::shared_ptr<HaKvBackend> backend_;
    std::string prefix_;
    std::string fence_;
    mutable std::mutex mutex_;
    std::map<std::string, ObjectStorageRecord> records_;
    // One durable logical version per key, retained after physical object GC.
    std::map<std::pair<std::string, std::string>, ObjectStorageRecord> heads_;
    std::set<std::string> maintenance_keys_;
    std::set<std::string> retired_keys_;
    std::map<UUID, std::set<std::string>> ready_keys_;
    std::map<std::pair<std::string, std::string>, std::set<std::string>>
        object_keys_;
    uint64_t pending_bytes_{0};
    uint64_t pending_count_{0};
    std::string reclaim_cursor_;
};

}  // namespace mooncake
