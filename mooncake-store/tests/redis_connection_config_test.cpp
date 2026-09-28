#include "ha/common/redis/redis_connection.h"

#include "../src/config/redis_connection_config.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>

#include "environ.h"
#include "environ_test_peer.h"

namespace mooncake::test {
namespace {

std::mutex environment_mutex;

class RedisConnectionConfigTest : public ::testing::Test {
   protected:
    // ResolveRedisDbIndex() and ConnectRedis() read the process environment,
    // so MC_REDIS_DB_INDEX is saved, cleared, and restored around each test.
    void SetUp() override {
        environment_lock_ = std::unique_lock<std::mutex>(environment_mutex);
        if (const char* value = std::getenv(kDbIndexVariable)) {
            original_db_index_ = value;
        }
        ASSERT_EQ(mooncake::test::EnvironTestPeer::UnsetEnv(kDbIndexVariable),
                  0);
    }

    void TearDown() override {
        if (original_db_index_.has_value()) {
            EXPECT_EQ(mooncake::test::EnvironTestPeer::SetEnv(
                          kDbIndexVariable, original_db_index_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(
                mooncake::test::EnvironTestPeer::UnsetEnv(kDbIndexVariable), 0);
        }
    }

    void SetProcessDbIndex(const char* value) {
        ASSERT_EQ(
            mooncake::test::EnvironTestPeer::SetEnv(kDbIndexVariable, value, 1),
            0);
    }

    tl::expected<RedisConnectionConfig, ErrorCode> Load() const {
        return RedisConnectionConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;

   private:
    inline static constexpr const char* kDbIndexVariable = "MC_REDIS_DB_INDEX";
    std::optional<std::string> original_db_index_;
    std::unique_lock<std::mutex> environment_lock_;
};

TEST_F(RedisConnectionConfigTest, UnsetAndEmptyDbIndexUseZero) {
    auto result = ha::common::redis::ResolveRedisDbIndex();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0);

    SetProcessDbIndex("");
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
        SetProcessDbIndex(entry.value);
        const auto result = ha::common::redis::ResolveRedisDbIndex();
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, entry.expected);
    }
}

TEST_F(RedisConnectionConfigTest, RejectsInvalidDbIndexSilently) {
    const char* values[] = {"-1", "256", "999999999999999999999999", "abc"};
    for (const char* value : values) {
        SCOPED_TRACE(value);
        SetProcessDbIndex(value);
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
        SetProcessDbIndex(value);
        const auto result = ha::common::redis::ResolveRedisDbIndex();
        EXPECT_FALSE(result.has_value());
        if (result.has_value()) {
            continue;
        }
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST_F(RedisConnectionConfigTest, UnsetValuesUseConnectionDefaults) {
    const auto config = Load();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->db_index, 0);
    EXPECT_TRUE(config->username.empty());
    EXPECT_TRUE(config->password.empty());
}

TEST_F(RedisConnectionConfigTest, EmptyCredentialsRemainEmpty) {
    source_.Set("MC_REDIS_USERNAME", "");
    source_.Set("MC_REDIS_PASSWORD", "");

    const auto config = Load();

    ASSERT_TRUE(config.has_value());
    EXPECT_TRUE(config->username.empty());
    EXPECT_TRUE(config->password.empty());
}

TEST_F(RedisConnectionConfigTest, UsernameWithoutPasswordRemainsValid) {
    source_.Set("MC_REDIS_USERNAME", "unused-user");

    const auto config = Load();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->username, "unused-user");
    EXPECT_TRUE(config->password.empty());
}

TEST_F(RedisConnectionConfigTest, PasswordWithoutUsernameRemainsValid) {
    source_.Set("MC_REDIS_PASSWORD", "secret");

    const auto config = Load();

    ASSERT_TRUE(config.has_value());
    EXPECT_TRUE(config->username.empty());
    EXPECT_EQ(config->password, "secret");
}

TEST_F(RedisConnectionConfigTest, PreservesCredentialTextExactly) {
    source_.Set("MC_REDIS_USERNAME", "user name");
    source_.Set("MC_REDIS_PASSWORD", "p@ss word");

    const auto config = Load();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->username, "user name");
    EXPECT_EQ(config->password, "p@ss word");
}

TEST_F(RedisConnectionConfigTest, LoadsIndependentConnectionSettings) {
    source_.Set("MC_REDIS_DB_INDEX", "7");
    source_.Set("MC_REDIS_USERNAME", "alice");
    source_.Set("MC_REDIS_PASSWORD", "secret");

    const auto config = Load();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->db_index, 7);
    EXPECT_EQ(config->username, "alice");
    EXPECT_EQ(config->password, "secret");
}

TEST_F(RedisConnectionConfigTest, NewConfigsReadCurrentEnvironment) {
    source_.Set("MC_REDIS_DB_INDEX", "1");
    const auto first = Load();
    source_.Set("MC_REDIS_DB_INDEX", "2");
    const auto second = Load();

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
        SetProcessDbIndex(value);
        const auto config =
            RedisConnectionConfig::FromEnvironment(Environ::Process());
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
    SetProcessDbIndex("1junk");
    const auto result = ha::common::redis::ConnectRedis(
        "redis://127.0.0.1:1", ErrorCode::PERSISTENT_FAIL);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}
#endif

}  // namespace
}  // namespace mooncake::test
