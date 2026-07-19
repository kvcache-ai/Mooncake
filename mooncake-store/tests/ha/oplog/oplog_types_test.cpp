#include "ha/oplog/oplog_types.h"

#include <gtest/gtest.h>

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

}  // namespace mooncake::test
