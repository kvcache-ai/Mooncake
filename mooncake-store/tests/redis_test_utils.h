#pragma once

#include <string>
#include <string_view>

#include <hiredis/hiredis.h>
#include <ylt/util/tl/expected.hpp>

#include "ha/backends/redis/redis_client_helper.h"
#include "ha/ha_types.h"
#include "types.h"

namespace mooncake {
namespace testing {

using RedisContextPtr = ha::backends::redis::RedisContextPtr;
using RedisReplyPtr = ha::backends::redis::RedisReplyPtr;

inline tl::expected<RedisContextPtr, ErrorCode> ConnectRedisForTest(
    std::string_view endpoint) {
    return ha::backends::redis::ConnectRedis(endpoint);
}

inline std::string BuildRedisScopedKey(
    const ha::ClusterNamespace& cluster_namespace, std::string_view suffix) {
    const auto hash_tag = ha::backends::redis::SanitizeHashTagComponent(
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
