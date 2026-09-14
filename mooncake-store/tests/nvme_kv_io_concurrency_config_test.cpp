#include "../src/config/nvme_kv_io_concurrency_config.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

class NvmeKvIoConcurrencyConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        environment_lock_ = std::unique_lock<std::mutex>(environment_mutex_);
        for (std::size_t i = 0; i < kVariables.size(); ++i) {
            if (const char* value = std::getenv(kVariables[i])) {
                original_values_[i] = value;
            }
            ASSERT_EQ(unsetenv(kVariables[i]), 0);
        }
    }

    void TearDown() override {
        for (std::size_t i = 0; i < kVariables.size(); ++i) {
            if (original_values_[i].has_value()) {
                EXPECT_EQ(
                    setenv(kVariables[i], original_values_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(kVariables[i]), 0);
            }
        }
    }

    void SetEnvironment(const char* name, const char* value) {
        ASSERT_EQ(setenv(name, value, 1), 0);
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

   private:
    inline static std::mutex environment_mutex_;
    std::unique_lock<std::mutex> environment_lock_;
    std::array<std::optional<std::string>, kVariables.size()> original_values_;
};

TEST_F(NvmeKvIoConcurrencyConfigTest, DefaultsFollowDeviceQueueDepth) {
    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(0), 256, 1, 1, 1,
                 1);
    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(8), 256, 8, 6, 1,
                 2);
    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(64), 256, 18, 6, 1,
                 12);
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
        SetEnvironment("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY", entry.value);
        const auto config = NvmeKvIoConcurrencyConfig::FromEnvironment(64);
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
            SetEnvironment(variable, value);

            testing::internal::CaptureStderr();
            const auto config = NvmeKvIoConcurrencyConfig::FromEnvironment(64);
            const std::string diagnostics =
                testing::internal::GetCapturedStderr();

            ExpectConfig(config, 256, 18, 6, 1, 12);
            EXPECT_TRUE(diagnostics.empty()) << diagnostics;
            ASSERT_EQ(unsetenv(variable), 0);
        }
    }
}

TEST_F(NvmeKvIoConcurrencyConfigTest, AppliesDependentCapsInOrder) {
    SetEnvironment("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY", "8");
    SetEnvironment("MOONCAKE_NVME_KV_IO_CONCURRENCY", "16");
    SetEnvironment("MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY", "99");
    SetEnvironment("MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY", "99");
    SetEnvironment("MOONCAKE_NVME_KV_PREPARE_CONCURRENCY", "99");

    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(64), 8, 8, 7, 7, 1);
}

TEST_F(NvmeKvIoConcurrencyConfigTest, SettingsRemainIndependent) {
    SetEnvironment("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY", "10");
    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(64), 10, 10, 6, 1,
                 4);
    ASSERT_EQ(unsetenv("MOONCAKE_NVME_KV_MAX_IO_CONCURRENCY"), 0);

    SetEnvironment("MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY", "4");
    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(64), 256, 18, 4, 1,
                 12);
    ASSERT_EQ(unsetenv("MOONCAKE_NVME_KV_BATCH_SUBMIT_CONCURRENCY"), 0);

    SetEnvironment("MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY", "9");
    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(64), 256, 18, 6, 9,
                 12);
    ASSERT_EQ(unsetenv("MOONCAKE_NVME_KV_ROOT_SUBMIT_CONCURRENCY"), 0);

    SetEnvironment("MOONCAKE_NVME_KV_PREPARE_CONCURRENCY", "99");
    ExpectConfig(NvmeKvIoConcurrencyConfig::FromEnvironment(64), 256, 18, 6, 1,
                 12);
}

TEST_F(NvmeKvIoConcurrencyConfigTest, NewConfigsReadCurrentEnvironment) {
    SetEnvironment("MOONCAKE_NVME_KV_IO_CONCURRENCY", "4");
    const auto first = NvmeKvIoConcurrencyConfig::FromEnvironment(64);
    SetEnvironment("MOONCAKE_NVME_KV_IO_CONCURRENCY", "7");
    const auto second = NvmeKvIoConcurrencyConfig::FromEnvironment(64);

    EXPECT_EQ(first.io_concurrency, 4);
    EXPECT_EQ(second.io_concurrency, 7);
}

}  // namespace
}  // namespace mooncake::test
