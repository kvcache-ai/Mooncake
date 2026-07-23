#include "ha/oplog/oplog_store_factory.h"

#include <glog/logging.h>

#include "ha/oplog/localfs_oplog_store.h"
#ifdef STORE_USE_REDIS
#include "ha/oplog/redis_oplog_store.h"
#endif

namespace mooncake {

std::unique_ptr<OpLogStore> OpLogStoreFactory::Create(
    OpLogStoreType type, const std::string& cluster_id, OpLogStoreRole role,
    const std::string& oplog_root_dir, int poll_interval_ms,
    const std::string& password, const std::string& username, int db_index,
    size_t async_queue_max_entries,
    const std::string& async_queue_overflow_mode,
    size_t best_effort_max_retries) {
    switch (type) {
        case OpLogStoreType::ETCD: {
            // EtcdOpLogStore is not yet ported to this branch.
            // Use LocalFs instead or wait for the full port.
            LOG(ERROR)
                << "OpLogStoreFactory: ETCD store not supported in this build";
            return nullptr;
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
        case OpLogStoreType::REDIS: {
#ifdef STORE_USE_REDIS
            if (async_queue_max_entries == 0 || best_effort_max_retries == 0 ||
                (async_queue_overflow_mode != "reject" &&
                 async_queue_overflow_mode != "bypass")) {
                LOG(ERROR) << "OpLogStoreFactory: invalid Redis async OpLog "
                              "configuration"
                           << ", queue_max_entries=" << async_queue_max_entries
                           << ", overflow_mode=" << async_queue_overflow_mode
                           << ", best_effort_max_retries="
                           << best_effort_max_retries;
                return nullptr;
            }
            bool enable_write = (role == OpLogStoreRole::WRITER);
            const auto overflow_mode =
                async_queue_overflow_mode == "bypass"
                    ? OpLogAsyncQueueOverflowMode::BYPASS
                    : OpLogAsyncQueueOverflowMode::REJECT;
            auto store = std::make_unique<RedisOpLogStore>(
                cluster_id, oplog_root_dir, enable_write, poll_interval_ms,
                password, username, db_index, async_queue_max_entries,
                overflow_mode, best_effort_max_retries);
            if (store->Init() != ErrorCode::OK) {
                LOG(ERROR)
                    << "OpLogStoreFactory: failed to init RedisOpLogStore"
                    << ", cluster_id=" << cluster_id
                    << ", endpoint=" << oplog_root_dir;
                return nullptr;
            }
            return store;
#else
            LOG(ERROR) << "OpLogStoreFactory: REDIS store requested but "
                          "STORE_USE_REDIS is not enabled";
            return nullptr;
#endif
        }
        default:
            LOG(ERROR) << "OpLogStoreFactory: unknown store type";
            return nullptr;
    }
}

}  // namespace mooncake
