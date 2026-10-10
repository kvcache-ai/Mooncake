#include "../../src/nvme_kv/config/io_concurrency_config.h"

#include <gtest/gtest.h>

#include <array>
#include <string>

#include "environ.h"

namespace mooncake::test {
namespace {

class NvmeKvIoConcurrencyConfigTest : public ::testing::Test {
   protected:
    NvmeKvIoConcurrencyConfig Load(std::size_t device_queue_depth) const {
        return NvmeKvIoConcurrencyConfig::FromEnvironment(Environ(source_),
                                                          device_queue_depth);
    }

    void ExpectConfig(const NvmeKvIoConcurrencyConfig& config,
                      std::size_t maximum, std::size_t io, std::size_t batch,
                      std::size_t root, std::size_t prepare) {
        EXPECT_EQ(config.max_io_concurrency, maximum);
        EXPECT_EQ(config.io_concurrency, io);
        EXPECT_EQ(config.batch_submit_concurrency, batch);
        EXPECT_EQ(config.root_submit_concurrency, root);
        EXPECT_EQ(config.prepare_concurrency, prepare);
    }

    inline static constexpr std::array<const char*, 5> kVariables = {
        "MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY",
        "MOONCAKE_NVME_KV_IO_CONCURRENCY",
        "MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY",
        "MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY",
        "MOONCAKE_NVME_KV_PREPARE_CONCURRENCY"};

    MapEnvironSource source_;
};

TEST_F(NvmeKvIoConcurrencyConfigTest, DefaultsFollowDeviceQueueDepth) {
    ExpectConfig(Load(0), 256, 1, 1, 1, 1);
    ExpectConfig(Load(8), 256, 8, 6, 1, 2);
    ExpectConfig(Load(64), 256, 18, 6, 1, 12);
}

TEST_F(NvmeKvIoConcurrencyConfigTest, PreservesLegacyUnsignedSyntax) {
    struct Case {
        const char* value;
        std::size_t expected;
    };
    const Case cases[] = {{"+17", 17}, {" 17", 17},
                          {"010", 8},  {"0x10", 16},
                          {"-0", 256}, {"4294967295", 4294967295ULL}};

    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        source_.Set("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY", entry.value);
        const auto config = Load(64);
        EXPECT_EQ(config.max_io_concurrency, entry.expected);
    }
}

TEST_F(NvmeKvIoConcurrencyConfigTest,
       InvalidAndZeroValuesKeepDefaultsSilently) {
    const char* values[] = {"",   "0",  "17 ",        "17x",
                            "-1", "0x", "4294967296", "abc"};
    for (const char* variable : kVariables) {
        for (const char* value : values) {
            SCOPED_TRACE(std::string(variable) + "=" + value);
            source_.Set(variable, value);

            testing::internal::CaptureStderr();
            const auto config = Load(64);
            const std::string diagnostics =
                testing::internal::GetCapturedStderr();

            ExpectConfig(config, 256, 18, 6, 1, 12);
            EXPECT_TRUE(diagnostics.empty()) << diagnostics;
            source_.Unset(variable);
        }
    }
}

TEST_F(NvmeKvIoConcurrencyConfigTest, AppliesDependentCapsInOrder) {
    source_.Set("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY", "8");
    source_.Set("MOONCAKE_NVME_KV_IO_CONCURRENCY", "16");
    source_.Set("MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY", "99");
    source_.Set("MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY", "99");
    source_.Set("MOONCAKE_NVME_KV_PREPARE_CONCURRENCY", "99");

    ExpectConfig(Load(64), 8, 8, 7, 7, 1);
}

TEST_F(NvmeKvIoConcurrencyConfigTest, SettingsRemainIndependent) {
    source_.Set("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY", "10");
    ExpectConfig(Load(64), 10, 10, 6, 1, 4);
    source_.Unset("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY");

    source_.Set("MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY", "4");
    ExpectConfig(Load(64), 256, 18, 4, 1, 12);
    source_.Unset("MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY");

    source_.Set("MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY", "9");
    ExpectConfig(Load(64), 256, 18, 6, 9, 12);
    source_.Unset("MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY");

    source_.Set("MOONCAKE_NVME_KV_PREPARE_CONCURRENCY", "99");
    ExpectConfig(Load(64), 256, 18, 6, 1, 12);
}

TEST_F(NvmeKvIoConcurrencyConfigTest, NewConfigsReadCurrentEnvironment) {
    source_.Set("MOONCAKE_NVME_KV_IO_CONCURRENCY", "4");
    const auto first = Load(64);
    source_.Set("MOONCAKE_NVME_KV_IO_CONCURRENCY", "7");
    const auto second = Load(64);

    EXPECT_EQ(first.io_concurrency, 4);
    EXPECT_EQ(second.io_concurrency, 7);
}

}  // namespace
}  // namespace mooncake::test
