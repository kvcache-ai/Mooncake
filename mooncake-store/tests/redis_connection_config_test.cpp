#include "ha/common/redis/redis_connection.h"

#include "../src/config/redis_connection_config.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

std::mutex environment_mutex;

class RedisConnectionConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        environment_lock_ = std::unique_lock<std::mutex>(environment_mutex);
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (const char* value = std::getenv(kVariables[i])) {
                original_[i] = value;
            }
            ASSERT_EQ(unsetenv(kVariables[i]), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (original_[i].has_value()) {
                EXPECT_EQ(setenv(kVariables[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(kVariables[i]), 0);
            }
        }
    }

    void SetDbIndex(const char* value) {
        ASSERT_EQ(setenv("MC_REDIS_DB_INDEX", value, 1), 0);
    }

   private:
    inline static constexpr std::array<const char*, 3> kVariables = {
        "MC_REDIS_DB_INDEX", "MC_REDIS_USERNAME", "MC_REDIS_PASSWORD"};
    std::array<std::optional<std::string>, kVariables.size()> original_;
    std::unique_lock<std::mutex> environment_lock_;
};

TEST_F(RedisConnectionConfigTest, UnsetAndEmptyDbIndexUseZero) {
    auto result = ha::common::redis::ResolveRedisDbIndex();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0);

    SetDbIndex("");
    result = ha::common::redis::ResolveRedisDbIndex();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0);
}

TEST_F(RedisConnectionConfigTest, AcceptsSupportedDbIndexSyntax) {
    struct Case {
        const char* value;
        int expected;
    };
    const Case cases[] = {{"0", 0},    {"255", 255}, {" \t42\r\n", 42},
                          {"+17", 17}, {"010", 10},  {"-0", 0}};
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        SetDbIndex(entry.value);
        const auto result = ha::common::redis::ResolveRedisDbIndex();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, entry.expected);
    }
}

TEST_F(RedisConnectionConfigTest, RejectsInvalidDbIndexSilently) {
    const char* values[] = {"-1", "256", "999999999999999999999999", "abc"};
    for (const char* value : values) {
        SCOPED_TRACE(value);
        SetDbIndex(value);
        testing::internal::CaptureStderr();
        const auto result = ha::common::redis::ResolveRedisDbIndex();
        const auto diagnostics = testing::internal::GetCapturedStderr();
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_TRUE(diagnostics.empty()) << diagnostics;
    }
}

TEST_F(RedisConnectionConfigTest, RejectsDbIndexWithNonIntegerSuffix) {
    for (const char* value : {"1junk", "1e2", "0x1"}) {
        SCOPED_TRACE(value);
        SetDbIndex(value);
        const auto result = ha::common::redis::ResolveRedisDbIndex();
        EXPECT_FALSE(result.has_value());
        if (result.has_value()) {
            continue;
        }
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_F(RedisConnectionConfigTest, UnsetValuesUseConnectionDefaults) {
    const auto config = RedisConnectionConfig::FromEnvironment();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->db_index, 0);
    EXPECT_TRUE(config->username.empty());
    EXPECT_TRUE(config->password.empty());
}

TEST_F(RedisConnectionConfigTest, EmptyCredentialsRemainEmpty) {
    ASSERT_EQ(setenv("MC_REDIS_USERNAME", "", 1), 0);
    ASSERT_EQ(setenv("MC_REDIS_PASSWORD", "", 1), 0);

    const auto config = RedisConnectionConfig::FromEnvironment();

    ASSERT_TRUE(config.has_value());
    EXPECT_TRUE(config->username.empty());
    EXPECT_TRUE(config->password.empty());
}

TEST_F(RedisConnectionConfigTest, UsernameWithoutPasswordRemainsValid) {
    ASSERT_EQ(setenv("MC_REDIS_USERNAME", "unused-user", 1), 0);

    const auto config = RedisConnectionConfig::FromEnvironment();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->username, "unused-user");
    EXPECT_TRUE(config->password.empty());
}

TEST_F(RedisConnectionConfigTest, PasswordWithoutUsernameRemainsValid) {
    ASSERT_EQ(setenv("MC_REDIS_PASSWORD", "secret", 1), 0);

    const auto config = RedisConnectionConfig::FromEnvironment();

    ASSERT_TRUE(config.has_value());
    EXPECT_TRUE(config->username.empty());
    EXPECT_EQ(config->password, "secret");
}

TEST_F(RedisConnectionConfigTest, PreservesCredentialTextExactly) {
    ASSERT_EQ(setenv("MC_REDIS_USERNAME", "user name", 1), 0);
    ASSERT_EQ(setenv("MC_REDIS_PASSWORD", "p@ss word", 1), 0);

    const auto config = RedisConnectionConfig::FromEnvironment();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->username, "user name");
    EXPECT_EQ(config->password, "p@ss word");
}

TEST_F(RedisConnectionConfigTest, LoadsIndependentConnectionSettings) {
    SetDbIndex("7");
    ASSERT_EQ(setenv("MC_REDIS_USERNAME", "alice", 1), 0);
    ASSERT_EQ(setenv("MC_REDIS_PASSWORD", "secret", 1), 0);

    const auto config = RedisConnectionConfig::FromEnvironment();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->db_index, 7);
    EXPECT_EQ(config->username, "alice");
    EXPECT_EQ(config->password, "secret");
}

TEST_F(RedisConnectionConfigTest, NewConfigsReadCurrentEnvironment) {
    SetDbIndex("1");
    const auto first = RedisConnectionConfig::FromEnvironment();
    SetDbIndex("2");
    const auto second = RedisConnectionConfig::FromEnvironment();

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->db_index, 1);
    EXPECT_EQ(second->db_index, 2);
}

TEST_F(RedisConnectionConfigTest, PublicDbResolverMatchesOwnerConfig) {
    const char* values[] = {"",   "0",  "255", " \t42\r\n", "+17",   "010",
                            "-0", "-1", "256", "abc",       "1junk", "0x1"};
    for (const char* value : values) {
        SCOPED_TRACE(value);
        SetDbIndex(value);
        const auto config = RedisConnectionConfig::FromEnvironment();
        const auto resolved = ha::common::redis::ResolveRedisDbIndex();
        EXPECT_EQ(config.has_value(), resolved.has_value());
        if (config.has_value() && resolved.has_value()) {
            EXPECT_EQ(config->db_index, *resolved);
        } else if (!config.has_value() && !resolved.has_value()) {
            EXPECT_EQ(config.error(), resolved.error());
        }
    }
}

#ifdef STORE_USE_REDIS
TEST_F(RedisConnectionConfigTest,
       ConnectRedisRejectsMalformedDbBeforeOpeningConnection) {
    SetDbIndex("1junk");
    const auto result = ha::common::redis::ConnectRedis(
        "redis://127.0.0.1:1", ErrorCode::PERSISTENT_FAIL);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}
#endif

}  // namespace
}  // namespace mooncake::test
