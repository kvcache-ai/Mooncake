#include "storage/distributed/object_replica_io.h"
#ifdef HAVE_S3_ADAPTER
#include "storage/distributed/s3_adapter.h"
#endif
#ifdef HAVE_OSS_ADAPTER
#include "storage/distributed/oss_adapter.h"
#endif
namespace mooncake {
tl::expected<std::shared_ptr<ObjectStorageAdapter>, ErrorCode>
CreateObjectReplicaAdapter(const ObjectStorageConfig& config) {
    if (!config.enabled || !config.Validate())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    std::shared_ptr<ObjectStorageAdapter> adapter;
#ifdef HAVE_S3_ADAPTER
    if (config.target.provider == "s3")
        adapter = std::make_shared<S3ObjectStorageAdapter>(config.target);
#endif
#ifdef HAVE_OSS_ADAPTER
    if (config.target.provider == "oss")
        adapter = std::make_shared<OssObjectStorageAdapter>(config.target);
#endif
    if (!adapter) return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    auto initialized = adapter->Init();
    if (!initialized) return tl::make_unexpected(initialized.error());
    if (!adapter->SupportsSinglePutVAttempt())
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    return adapter;
}
}  // namespace mooncake
