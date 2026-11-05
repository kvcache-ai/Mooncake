#ifdef STORE_USE_REDIS

#include "p2p/ha/oplog/redis_oplog_trim_worker.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace mooncake {
namespace {

RedisMasterRegistryEntry Standby(uint64_t applied_sequence_id,
                                 bool snapshot_ready = true) {
    RedisMasterRegistryEntry entry;
    entry.instance_id = "standby-" + std::to_string(applied_sequence_id);
    entry.snapshot_endpoint = "127.0.0.1:52051";
    entry.role = "standby";
    entry.snapshot_ready = snapshot_ready;
    entry.applied_sequence_id = applied_sequence_id;
    return entry;
}

RedisMasterRegistryEntry StandbyWithId(const std::string& instance_id,
                                       uint64_t applied_sequence_id) {
    auto entry = Standby(applied_sequence_id);
    entry.instance_id = instance_id;
    return entry;
}

TEST(RedisOpLogTrimWorkerTest, KeepsRetentionWhenSnapshotIsCaughtUp) {
    EXPECT_EQ(900000u, ComputeRedisOpLogTrimTarget(1000000, 100, 100000, 1,
                                                   {Standby(1000000)}));
}

TEST(RedisOpLogTrimWorkerTest, CapsAtSnapshotWatermark) {
    EXPECT_EQ(850000u, ComputeRedisOpLogTrimTarget(1000000, 100, 100000, 1,
                                                   {Standby(850000)}));
}

TEST(RedisOpLogTrimWorkerTest, RequiresEnoughSnapshotReadyStandbys) {
    EXPECT_EQ(100u, ComputeRedisOpLogTrimTarget(
                        1000000, 100, 100000, 2,
                        {Standby(950000), Standby(990000, false)}));
}

TEST(RedisOpLogTrimWorkerTest, UsesNthSnapshotReadyStandby) {
    EXPECT_EQ(850000u,
              ComputeRedisOpLogTrimTarget(1000000, 100, 100000, 2,
                                          {Standby(950000), Standby(850000)}));
}

TEST(RedisOpLogTrimWorkerTest, IgnoresSelfInstance) {
    EXPECT_EQ(100u, ComputeRedisOpLogTrimTarget(
                        1000000, 100, 100000, 1,
                        {StandbyWithId("self", 1000000)}, "self"));
}

}  // namespace
}  // namespace mooncake

#endif  // STORE_USE_REDIS
