#include "ha/oplog/oplog_store_factory.h"

#include <glog/logging.h>

#ifdef STORE_USE_ETCD
#include "ha/oplog/etcd_oplog_store.h"
#endif

#include "ha/oplog/localfs_oplog_store.h"

namespace mooncake {

std::unique_ptr<OpLogStore> OpLogStoreFactory::Create(
    OpLogStoreType type, const std::string& cluster_id, OpLogStoreRole role,
    const std::string& oplog_root_dir, int poll_interval_ms) {
    switch (type) {
        case OpLogStoreType::ETCD: {
#ifdef STORE_USE_ETCD
            bool batch_update = (role == OpLogStoreRole::WRITER);
            bool batch_write = (role == OpLogStoreRole::WRITER);
            auto store = std::make_unique<EtcdOpLogStore>(
                cluster_id, batch_update, batch_write);
            if (store->Init() != ErrorCode::OK) {
                LOG(ERROR) << "OpLogStoreFactory: failed to init EtcdOpLogStore"
                           << ", cluster_id=" << cluster_id;
                return nullptr;
            }
            return store;
#else
            LOG(ERROR) << "OpLogStoreFactory: ETCD support not compiled in";
            return nullptr;
#endif
        }
        case OpLogStoreType::LOCAL_FS: {
            bool enable_batch_write = (role == OpLogStoreRole::WRITER);
            auto store = std::make_unique<LocalFsOpLogStore>(
                cluster_id, oplog_root_dir, enable_batch_write,
                poll_interval_ms);
            if (store->Init() != ErrorCode::OK) {
                LOG(ERROR)
                    << "OpLogStoreFactory: failed to init LocalFsOpLogStore"
                    << ", cluster_id=" << cluster_id
                    << ", root_dir=" << oplog_root_dir;
                return nullptr;
            }
            return store;
        }
        default:
            LOG(ERROR) << "OpLogStoreFactory: unknown store type";
            return nullptr;
    }
}

}  // namespace mooncake
