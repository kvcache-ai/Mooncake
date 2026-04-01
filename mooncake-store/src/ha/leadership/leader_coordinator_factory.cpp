#include "ha/leadership/leader_coordinator_factory.h"

#include "ha/leadership/backends/etcd/etcd_leader_coordinator.h"
#include "ha/leadership/backends/redis/redis_leader_coordinator.h"

namespace mooncake {
namespace ha {

tl::expected<std::unique_ptr<LeaderCoordinator>, ErrorCode>
CreateLeaderCoordinator(const HABackendSpec& spec) {
    switch (spec.type) {
        case HABackendType::UNKNOWN:
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        case HABackendType::ETCD: {
            auto coordinator =
                std::make_unique<backends::etcd::EtcdLeaderCoordinator>(spec);
            auto err = coordinator->Connect();
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }
            return std::unique_ptr<LeaderCoordinator>(std::move(coordinator));
        }
        case HABackendType::REDIS: {
            auto coordinator =
                std::make_unique<backends::redis::RedisLeaderCoordinator>(spec);
            auto err = coordinator->Connect();
            if (err != ErrorCode::OK) {
                return tl::make_unexpected(err);
            }
            return std::unique_ptr<LeaderCoordinator>(std::move(coordinator));
        }
        case HABackendType::K8S:
            return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

}  // namespace ha
}  // namespace mooncake
