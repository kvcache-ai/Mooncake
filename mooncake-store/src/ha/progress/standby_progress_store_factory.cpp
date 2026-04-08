#include "ha/progress/standby_progress_store_factory.h"

#include <memory>

#include "ha/common/etcd/etcd_helper.h"
#include "ha/progress/backends/etcd/etcd_standby_progress_store.h"
#include "ha/progress/backends/redis/redis_standby_progress_store.h"

namespace mooncake {
namespace ha {

tl::expected<std::shared_ptr<StandbyProgressStore>, ErrorCode>
CreateStandbyProgressStore(const HABackendSpec& spec) {
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
            return std::static_pointer_cast<StandbyProgressStore>(
                std::make_shared<backends::etcd::EtcdStandbyProgressStore>(
                    spec.cluster_namespace));
#endif
        }
        case HABackendType::REDIS: {
#ifndef STORE_USE_REDIS
            return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
            if (spec.connstring.empty() || spec.cluster_namespace.empty()) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return std::static_pointer_cast<StandbyProgressStore>(
                std::make_shared<backends::redis::RedisStandbyProgressStore>(
                    spec.connstring, spec.cluster_namespace));
#endif
        }
        case HABackendType::K8S:
        case HABackendType::UNKNOWN:
            break;
    }

    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

}  // namespace ha
}  // namespace mooncake
