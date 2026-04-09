#include "ha/snapshot/object/snapshot_object_store.h"

#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#ifdef HAVE_AWS_SDK
#include "ha/snapshot/object/backends/s3/s3_snapshot_object_store.h"
#endif
#ifdef STORE_USE_ETCD
#include "ha/snapshot/object/backends/etcd/etcd_snapshot_object_store.h"
#endif

namespace mooncake {

std::unique_ptr<SnapshotObjectStore> SnapshotObjectStore::Create(
    SnapshotObjectStoreType type, const std::string& etcd_endpoints) {
    switch (type) {
#ifdef STORE_USE_ETCD
        case SnapshotObjectStoreType::ETCD:
            return std::make_unique<EtcdSnapshotObjectStore>(etcd_endpoints);
#else
        case SnapshotObjectStoreType::ETCD:
            throw std::runtime_error(
                "ETCD snapshot object store requested but STORE_USE_ETCD "
                "is not enabled. Please rebuild with STORE_USE_ETCD or "
                "use the 'local' object store.");
#endif
#ifdef HAVE_AWS_SDK
        case SnapshotObjectStoreType::S3:
            return std::make_unique<S3SnapshotObjectStore>();
#else
        case SnapshotObjectStoreType::S3:
            throw std::runtime_error(
                "S3 snapshot object store requested but AWS SDK is not "
                "available. Please rebuild with HAVE_AWS_SDK or use the "
                "'local' object store.");
#endif
        case SnapshotObjectStoreType::LOCAL_FILE:
            return std::make_unique<LocalFileSnapshotObjectStore>();
        default:
            throw std::invalid_argument("Unknown snapshot object store type");
    }
}

}  // namespace mooncake
