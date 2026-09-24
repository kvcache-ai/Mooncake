#include "ha/oplog/oplog_types.h"

#include <gtest/gtest.h>

#include "weight_management.h"

namespace mooncake::test {

TEST(OpLogTypesTest, ChecksumRoundTrips) {
    OpLogEntry entry;
    entry.payload = "payload";
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    EXPECT_TRUE(VerifyOpLogChecksum(entry));
}

TEST(OpLogTypesTest, RejectsOversizedEntry) {
    OpLogEntry entry;
    entry.object_key.assign(kMaxOpLogObjectKeySize + 1, 'x');
    EXPECT_FALSE(ValidateOpLogEntrySize(entry));
}

TEST(OpLogTypesTest, NormalizesTrailingClusterSlashes) {
    std::string cluster_id = "cluster///";
    EXPECT_TRUE(NormalizeAndValidateClusterId(cluster_id));
    EXPECT_EQ("cluster", cluster_id);
}

TEST(OpLogTypesTest, WeightOperationNumbersAppendWithoutRenumbering) {
    EXPECT_EQ(8, static_cast<int>(OpType::WEIGHT_METADATA_UPSERT));
    EXPECT_EQ(9, static_cast<int>(OpType::WEIGHT_METADATA_DELETE));
    EXPECT_EQ(10, static_cast<int>(OpType::WEIGHT_LEASE_UPSERT));
    EXPECT_EQ(11, static_cast<int>(OpType::WEIGHT_LEASE_DELETE));
}

TEST(OpLogTypesTest, WeightDeleteTombstonesRoundTrip) {
    WeightRevisionIdentity identity{
        .tenant_id = "tenant-a",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
    WeightMetadataDeleteOp metadata_delete{
        .identity = identity,
        .metadata_generation = 9,
    };
    const auto metadata_bytes = struct_pack::serialize(metadata_delete);
    WeightMetadataDeleteOp decoded_metadata;
    ASSERT_EQ(struct_pack::errc::ok,
              struct_pack::deserialize_to(decoded_metadata, metadata_bytes));
    EXPECT_EQ(metadata_delete, decoded_metadata);

    WeightLeaseDeleteOp lease_delete{
        .lease_id = 42,
        .identity = identity,
        .fenced_metadata_generation = 9,
    };
    const auto lease_bytes = struct_pack::serialize(lease_delete);
    WeightLeaseDeleteOp decoded_lease;
    ASSERT_EQ(struct_pack::errc::ok,
              struct_pack::deserialize_to(decoded_lease, lease_bytes));
    EXPECT_EQ(lease_delete, decoded_lease);
}

}  // namespace mooncake::test
