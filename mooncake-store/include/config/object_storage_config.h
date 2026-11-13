#pragma once

#include <cstdint>
#include <string>
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

enum class ObjectStorageWriteMode : uint8_t { SYNC };

// Non-secret cluster configuration. Credentials remain process-local.
struct ObjectReplicaTarget {
    std::string provider;
    std::string endpoint;
    std::string bucket;
    std::string region;
    std::string prefix;
    bool path_style{false};
    YLT_REFL(ObjectReplicaTarget, provider, endpoint, bucket, region, prefix,
             path_style);
    bool operator==(const ObjectReplicaTarget&) const = default;
};

struct ObjectStorageConfig {
    bool enabled{false};
    ObjectReplicaTarget target;
    uint64_t max_object_bytes{256ULL * 1024 * 1024};
    uint64_t max_staging_bytes{1024ULL * 1024 * 1024};
    uint64_t max_records{10000};
    uint64_t maintenance_batch_size{32};
    uint64_t cleanup_interval_ms{10000};
    bool Validate() const;
    std::string NamespaceId() const;
    YLT_REFL(ObjectStorageConfig, enabled, target, max_object_bytes,
             max_staging_bytes, max_records, maintenance_batch_size,
             cleanup_interval_ms);
};

}  // namespace mooncake
