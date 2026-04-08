#include "ha/oplog/oplog_store_factory.h"

#include <memory>

#include <glog/logging.h>

#include "ha/common/etcd/etcd_helper.h"
#include "ha/oplog/backends/etcd/etcd_oplog_store.h"
#include "ha/oplog/backends/redis/redis_oplog_store.h"

namespace mooncake {
namespace ha {

tl::expected<std::shared_ptr<OpLogStore>, ErrorCode> CreateOpLogStore(
    const HABackendSpec& spec, const OpLogStoreFactoryOptions& options) {
    switch (spec.type) {
        case HABackendType::ETCD: {
#ifndef STORE_USE_ETCD
            return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
            if (spec.connstring.empty() || spec.cluster_namespace.empty()) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto err =
                EtcdHelper::ConnectToEtcdStoreClient(spec.connstring.c_str());
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }

            auto store = std::make_shared<EtcdOpLogStore>(
                spec.cluster_namespace, options.enable_latest_seq_batch_update,
                options.enable_batch_write);
            err = store->Init();
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }
            return std::static_pointer_cast<OpLogStore>(store);
#endif
        }
        case HABackendType::REDIS: {
#ifndef STORE_USE_REDIS
            return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
            if (spec.connstring.empty() || spec.cluster_namespace.empty()) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto store = std::make_shared<backends::redis::RedisOpLogStore>(
                spec.connstring, spec.cluster_namespace);
            auto latest = store->GetLatestSequence();
            if (!latest) {
                return tl::make_unexpected(latest.error());
            }
            return std::static_pointer_cast<OpLogStore>(store);
#endif
        }
        case HABackendType::K8S:
        case HABackendType::UNKNOWN:
            break;
    }
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

bool SupportsOpLogFollowing(HABackendType backend_type) {
    return backend_type == HABackendType::ETCD ||
           backend_type == HABackendType::REDIS;
}

}  // namespace ha
}  // namespace mooncake
