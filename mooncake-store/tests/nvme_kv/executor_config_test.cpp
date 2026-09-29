#include "../../src/nvme_kv/config/executor_config.h"

#include <gtest/gtest.h>

#include "environ.h"

namespace mooncake::test {
namespace {

class NvmeKvExecutorConfigTest : public ::testing::Test {
   protected:
    NvmeKvExecutorConfig Load() const {
        return NvmeKvExecutorConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(NvmeKvExecutorConfigTest, UsesDefaultsWhenUnset) {
    const auto config = Load();
    EXPECT_EQ(config.transfer_alignment_bytes, 4096u);
    EXPECT_EQ(config.value_block_unit_bytes, 512u);
    EXPECT_EQ(config.protocol_max_value_size, 512u * 1024u);
    EXPECT_EQ(config.read_plan_batch_size, 8u);
}

TEST_F(NvmeKvExecutorConfigTest, PreservesNvmeUnsignedSyntax) {
    source_.Set("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "0x2000");
    source_.Set("MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES", "+1024");
    source_.Set("MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE", " 65536");
    source_.Set("MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE", "0x20");

    const auto config = Load();
    EXPECT_EQ(config.transfer_alignment_bytes, 8192u);
    EXPECT_EQ(config.value_block_unit_bytes, 1024u);
    EXPECT_EQ(config.protocol_max_value_size, 65536u);
    EXPECT_EQ(config.read_plan_batch_size, 32u);
}

TEST_F(NvmeKvExecutorConfigTest, InvalidValuesUseDefaultsSilently) {
    source_.Set("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "17 ");
    source_.Set("MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES", "-1");
    source_.Set("MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE", "4294967296");
    source_.Set("MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE", "0");

    const auto config = Load();
    EXPECT_EQ(config.transfer_alignment_bytes, 4096u);
    EXPECT_EQ(config.value_block_unit_bytes, 512u);
    EXPECT_EQ(config.protocol_max_value_size, 512u * 1024u);
    EXPECT_EQ(config.read_plan_batch_size, 8u);
}

TEST_F(NvmeKvExecutorConfigTest, CapsReadPlanBatchSize) {
    source_.Set("MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE", "2048");
    EXPECT_EQ(NvmeKvExecutorConfig::ReadPlanBatchSizeFromEnvironment(
                  Environ(source_)),
              1024u);
}

TEST_F(NvmeKvExecutorConfigTest, ReadsAtExistingCallBoundaries) {
    const Environ env(source_);
    source_.Set("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "8192");
    EXPECT_EQ(
        NvmeKvExecutorConfig::ReadTransferAlignmentBytesFromEnvironment(env),
        8192u);
    source_.Set("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "16384");
    EXPECT_EQ(
        NvmeKvExecutorConfig::ReadTransferAlignmentBytesFromEnvironment(env),
        16384u);
}

}  // namespace
}  // namespace mooncake::test
