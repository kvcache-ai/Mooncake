#pragma once

#include <cstdint>
#include <string>
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

enum class ObjectStorageWriteMode : uint8_t { SYNC, ASYNC, OFFLOAD_ON_EVICT };

// Public cluster configuration. Credentials are deliberately process-local.
struct ObjectStorageConfig {
    bool enabled{false};
    std::string provider{"oss"};
    std::string endpoint;
    std::string bucket;
    std::string region;
    std::string prefix;
    bool path_style{false};
    bool anonymous{false};
    std::string write_mode{"sync"};
    uint64_t max_object_bytes{256ULL * 1024 * 1024};
    uint64_t max_pending_bytes{1024ULL * 1024 * 1024};
    uint64_t max_pending_objects{10000};
    uint64_t maintenance_batch_size{32};
    uint64_t request_timeout_ms{30000};

    bool Validate() const;
    ObjectStorageWriteMode WriteMode() const;
    std::string NamespaceId() const;
    YLT_REFL(ObjectStorageConfig, enabled, provider, endpoint, bucket, region,
             prefix, path_style, anonymous, write_mode, max_object_bytes,
             max_pending_bytes, max_pending_objects, maintenance_batch_size,
             request_timeout_ms);
};

}  // namespace mooncake
