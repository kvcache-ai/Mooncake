#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "replica.h"
#include "config/object_storage_config.h"

namespace mooncake {

// Object addressing only; this review unit does not add a Store replica type.
struct ObjectStorageLocation {
    std::string object_key;
    uint64_t object_size{0};
    YLT_REFL(ObjectStorageLocation, object_key, object_size);
};

enum class ObjectUploadPhase : uint8_t {
    AUTHORIZED = 0,
    UPLOADED = 1,
    RETIRED = 2,
};

// No logical head or commit flag: visibility is owned by Master metadata.
// A retired, uncertain upload remains tracked even after a successful DELETE,
// because its single authorized PUT can still materialize later.
struct ObjectUploadRecord {
    ObjectReplicaTarget target;
    std::string tenant_id;
    std::string key;
    UUID writer{};
    ReplicaID replica_id{0};
    ObjectStorageLocation object;
    ObjectUploadPhase phase{ObjectUploadPhase::AUTHORIZED};
    uint64_t not_before_ms{0};
    bool upload_finished{false};
    YLT_REFL(ObjectUploadRecord, target, tenant_id, key, writer, replica_id,
             object, phase, not_before_ms, upload_finished);
};

// One ordered-log entry changes visibility and its upload/retirement
// obligations together. metadata_payload is the existing MetadataPayload codec.
struct ObjectReplicaMutation {
    ObjectReplicaTarget target;
    std::vector<ObjectUploadRecord> records;
    std::vector<std::string> forget;
    std::string metadata_payload;
    bool remove_value{false};
    YLT_REFL(ObjectReplicaMutation, target, records, forget, metadata_payload,
             remove_value);
};

struct ObjectReplicaSnapshot {
    std::optional<ObjectReplicaTarget> target;
    std::vector<ObjectUploadRecord> records;
    YLT_REFL(ObjectReplicaSnapshot, target, records);
};

}  // namespace mooncake
