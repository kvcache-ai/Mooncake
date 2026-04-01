#pragma once

#include <string>
#include <string_view>

#include <hiredis/hiredis.h>
#include <ylt/util/tl/expected.hpp>

#include "ha/common/redis/redis_connection.h"
#include "ha/ha_types.h"
#include "types.h"

namespace mooncake {
namespace testing {

using RedisContextPtr = ha::common::redis::RedisContextPtr;
using RedisReplyPtr = ha::common::redis::RedisReplyPtr;

inline tl::expected<RedisContextPtr, ErrorCode> ConnectRedisForTest(
    std::string_view endpoint) {
    return ha::common::redis::ConnectRedis(endpoint);
}

inline std::string BuildRedisScopedKey(
    const ha::ClusterNamespace& cluster_namespace, std::string_view suffix) {
    const auto hash_tag = ha::common::redis::SanitizeHashTagComponent(
        std::string(cluster_namespace));
    return "mooncake-store/{" + hash_tag + "}/" + std::string(suffix);
}

inline std::string BuildRedisMasterViewKey(
    const ha::ClusterNamespace& cluster_namespace) {
    return BuildRedisScopedKey(cluster_namespace, "master_view");
}

inline std::string BuildRedisSnapshotLatestKey(
    const ha::ClusterNamespace& cluster_namespace) {
    return BuildRedisScopedKey(cluster_namespace, "snapshot/latest");
}

inline std::string BuildRedisSnapshotIndexKey(
    const ha::ClusterNamespace& cluster_namespace) {
    return BuildRedisScopedKey(cluster_namespace, "snapshot/index");
}

}  // namespace testing
}  // namespace mooncake
