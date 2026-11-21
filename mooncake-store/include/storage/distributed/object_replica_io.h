#pragma once
#include "config/object_storage_config.h"
#include "storage/distributed/object_storage_backend.h"
namespace mooncake {
tl::expected<std::shared_ptr<ObjectStorageAdapter>, ErrorCode>
CreateObjectReplicaAdapter(const ObjectStorageConfig& config);
}
