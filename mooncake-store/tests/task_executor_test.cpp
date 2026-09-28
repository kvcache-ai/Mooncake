#include "task_manager.h"

#include <gtest/gtest.h>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <string>

namespace mooncake {

TEST(TaskPayloadSerializationTest, ReplicaCopyPayloadRoundTrip) {
    ReplicaCopyPayload payload;
    payload.tenant_id = "tenant-a";
    payload.key = "copy-key";
    payload.source = "source-segment";
    payload.targets = {"target-segment-1", "target-segment-2"};
    payload.dynamic_replication_lease_id_high = 123;
    payload.dynamic_replication_lease_id_low = 456;
    payload.dynamic_replication_version_epoch = 789;

    std::string json;
    struct_json::to_json(payload, json);

    ReplicaCopyPayload round_tripped;
    struct_json::from_json(round_tripped, json);

    EXPECT_EQ(round_tripped.tenant_id, payload.tenant_id);
    EXPECT_EQ(round_tripped.key, payload.key);
    EXPECT_EQ(round_tripped.source, payload.source);
    EXPECT_EQ(round_tripped.targets, payload.targets);
    EXPECT_EQ(round_tripped.dynamic_replication_lease_id_high,
              payload.dynamic_replication_lease_id_high);
    EXPECT_EQ(round_tripped.dynamic_replication_lease_id_low,
              payload.dynamic_replication_lease_id_low);
    EXPECT_EQ(round_tripped.dynamic_replication_version_epoch,
              payload.dynamic_replication_version_epoch);
}

TEST(TaskPayloadSerializationTest, ReplicaMovePayloadRoundTrip) {
    ReplicaMovePayload payload;
    payload.tenant_id = "tenant-b";
    payload.key = "move-key";
    payload.source = "source-segment";
    payload.target = "target-segment";

    std::string json;
    struct_json::to_json(payload, json);

    ReplicaMovePayload round_tripped;
    struct_json::from_json(round_tripped, json);

    EXPECT_EQ(round_tripped.tenant_id, payload.tenant_id);
    EXPECT_EQ(round_tripped.key, payload.key);
    EXPECT_EQ(round_tripped.source, payload.source);
    EXPECT_EQ(round_tripped.target, payload.target);
}

}  // namespace mooncake
