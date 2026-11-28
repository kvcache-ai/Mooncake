#include "ha/kv/ha_kv_backend_factory.h"

#include "config/redis_connection_config.h"
#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "ha/kv/redis_ha_kv_backend.h"

namespace mooncake {

tl::expected<std::shared_ptr<HaKvBackend>, ErrorCode> CreateHaKvBackend(
    const ha::HABackendSpec& spec) {
    switch (spec.type) {
        case ha::HABackendType::ETCD: {
#ifdef STORE_USE_ETCD
            if (spec.connstring.empty()) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            const ErrorCode err =
                EtcdHelper::ConnectToEtcdStoreClient(spec.connstring);
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }
            return std::make_shared<EtcdHaKvBackend>();
#else
            return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#endif
        }
        case ha::HABackendType::REDIS: {
#ifdef STORE_USE_REDIS
            if (spec.connstring.empty()) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            const auto config = RedisConnectionConfig::FromEnvironment();
            if (!config) return tl::make_unexpected(config.error());
            const bool persistent = config->durability_profile ==
                                    RedisDurabilityProfile::kPersistent;
            auto backend =
                std::make_shared<RedisHaKvBackend>(KvWriteDurabilityPolicy{
                    .replicas_required =
                        spec.ha_redis_expected_replicas_per_shard,
                    .replica_ack_timeout = std::chrono::milliseconds(
                        config->replication_ack_timeout_ms),
                    .require_local_aof = persistent,
                    .replica_aof_required =
                        persistent ? spec.ha_redis_expected_replicas_per_shard
                                   : 0,
                    .replica_aof_ack_timeout =
                        std::chrono::milliseconds(config->aof_ack_timeout_ms)});
            const ErrorCode err = backend->Connect(spec.connstring);
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }
            return backend;
#else
            return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#endif
        }
        case ha::HABackendType::UNKNOWN:
        case ha::HABackendType::K8S:
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake
