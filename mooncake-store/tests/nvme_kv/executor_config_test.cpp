#include "../../src/nvme_kv/config/executor_config.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

class NvmeKvExecutorConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        for (size_t i = 0; i < variables_.size(); ++i) {
            if (const char* value = std::getenv(variables_[i])) {
                original_[i] = value;
            }
            ASSERT_EQ(unsetenv(variables_[i]), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < variables_.size(); ++i) {
            if (original_[i].has_value()) {
                EXPECT_EQ(setenv(variables_[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(variables_[i]), 0);
            }
        }
    }

    inline static constexpr std::array<const char*, 4> variables_ = {
        "MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES",
        "MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES",
        "MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE",
        "MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE"};
    std::array<std::optional<std::string>, variables_.size()> original_;
};

TEST_F(NvmeKvExecutorConfigTest, UsesDefaultsWhenUnset) {
    const auto config = NvmeKvExecutorConfig::FromEnvironment();
    EXPECT_EQ(config.transfer_alignment_bytes, 4096u);
    EXPECT_EQ(config.value_block_unit_bytes, 512u);
    EXPECT_EQ(config.protocol_max_value_size, 512u * 1024u);
    EXPECT_EQ(config.read_plan_batch_size, 8u);
}

TEST_F(NvmeKvExecutorConfigTest, PreservesNvmeUnsignedSyntax) {
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "0x2000", 1),
              0);
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES", "+1024", 1), 0);
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE", " 65536", 1),
              0);
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE", "0x20", 1), 0);

    const auto config = NvmeKvExecutorConfig::FromEnvironment();
    EXPECT_EQ(config.transfer_alignment_bytes, 8192u);
    EXPECT_EQ(config.value_block_unit_bytes, 1024u);
    EXPECT_EQ(config.protocol_max_value_size, 65536u);
    EXPECT_EQ(config.read_plan_batch_size, 32u);
}

TEST_F(NvmeKvExecutorConfigTest, InvalidValuesUseDefaultsSilently) {
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "17 ", 1), 0);
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_VALUE_BLOCK_UNIT_BYTES", "-1", 1), 0);
    ASSERT_EQ(
        setenv("MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE", "4294967296", 1), 0);
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE", "0", 1), 0);

    const auto config = NvmeKvExecutorConfig::FromEnvironment();
    EXPECT_EQ(config.transfer_alignment_bytes, 4096u);
    EXPECT_EQ(config.value_block_unit_bytes, 512u);
    EXPECT_EQ(config.protocol_max_value_size, 512u * 1024u);
    EXPECT_EQ(config.read_plan_batch_size, 8u);
}

TEST_F(NvmeKvExecutorConfigTest, CapsReadPlanBatchSize) {
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_READ_PLAN_BATCH_SIZE", "2048", 1), 0);
    EXPECT_EQ(NvmeKvExecutorConfig::ReadPlanBatchSizeFromEnvironment(), 1024u);
}

TEST_F(NvmeKvExecutorConfigTest, ReadsAtExistingCallBoundaries) {
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "8192", 1),
              0);
    EXPECT_EQ(NvmeKvExecutorConfig::ReadTransferAlignmentBytesFromEnvironment(),
              8192u);
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_TRANSFER_ALIGNMENT_BYTES", "16384", 1),
              0);
    EXPECT_EQ(NvmeKvExecutorConfig::ReadTransferAlignmentBytesFromEnvironment(),
              16384u);
}

}  // namespace
}  // namespace mooncake::test
