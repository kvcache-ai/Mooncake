#include "default_config.h"
#include "duration_utils.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace mooncake {

class DefaultConfigTest : public ::testing::Test {
   public:
    struct ConfigData {
        int test_integer;
        float test_float;
        std::string test_string;
        bool test_boolean;
        int test_nested_integer;
        std::string test_nested_string;
        int64_t default_value;
    };

   protected:
    void SetUp() override {
        // Initialize default configuration
        path_ = std::filesystem::current_path().parent_path().string();
    }
    ConfigData config_data_;
    std::string path_;
};

TEST_F(DefaultConfigTest, LoadJsonSuccess) {
    DefaultConfig config;
    config.SetPath(path_ + "/../../mooncake-common/tests/test.json");
    config.Load();

    // Validate some default values
    config.GetInt32("testInteger", &config_data_.test_integer, 0);
    config.GetFloat("testFloat", &config_data_.test_float, 0.0f);
    config.GetString("testString", &config_data_.test_string, "");
    config.GetBool("testBoolean", &config_data_.test_boolean, false);
    config.GetInt32("testObject.nestedInteger",
                    &config_data_.test_nested_integer, 0);
    config.GetString("testObject.nestedString",
                     &config_data_.test_nested_string, "");
    config.GetInt64("defaultValue", &config_data_.default_value, 10000);

    ASSERT_EQ(config_data_.test_integer, 43);
    ASSERT_FLOAT_EQ(config_data_.test_float, 4.15f);
    ASSERT_EQ(config_data_.test_string, "Hello, World");
    ASSERT_FALSE(config_data_.test_boolean);
    ASSERT_EQ(config_data_.test_nested_integer, 1000);
    ASSERT_EQ(config_data_.test_nested_string, "Nested Hello, World");
    ASSERT_EQ(config_data_.default_value, 10000);
};

TEST_F(DefaultConfigTest, LoadYamlSuccess) {
    DefaultConfig config;
    config.SetPath(path_ + "/../../mooncake-common/tests/test.yaml");
    config.Load();

    // Validate some default values
    config.GetInt32("testInteger", &config_data_.test_integer, 0);
    config.GetFloat("testFloat", &config_data_.test_float, 0.0f);
    config.GetString("testString", &config_data_.test_string, "");
    config.GetBool("testBoolean", &config_data_.test_boolean, false);
    config.GetInt32("testObject.nestedInteger",
                    &config_data_.test_nested_integer, 0);
    config.GetString("testObject.nestedString",
                     &config_data_.test_nested_string, "");
    config.GetInt64("defaultValue", &config_data_.default_value, 10000);

    ASSERT_EQ(config_data_.test_integer, 42);
    ASSERT_FLOAT_EQ(config_data_.test_float, 3.14f);
    ASSERT_EQ(config_data_.test_string, "Hello, World!");
    ASSERT_TRUE(config_data_.test_boolean);
    ASSERT_EQ(config_data_.test_nested_integer, 100);
    ASSERT_EQ(config_data_.test_nested_string, "Nested Hello");
    ASSERT_EQ(config_data_.default_value, 10000);
};

TEST(DurationUtilsTest, ParseDurationMsSupportsLegacyMillisecondsAndUnits) {
    uint64_t value = 0;

    ASSERT_TRUE(ParseDurationMs("5000", &value));
    ASSERT_EQ(value, 5000);

    ASSERT_TRUE(ParseDurationMs("5000ms", &value));
    ASSERT_EQ(value, 5000);

    ASSERT_TRUE(ParseDurationMs("5s", &value));
    ASSERT_EQ(value, 5000);

    ASSERT_TRUE(ParseDurationMs("30m", &value));
    ASSERT_EQ(value, 30 * 60 * 1000);

    ASSERT_TRUE(ParseDurationMs("1H", &value));
    ASSERT_EQ(value, 60 * 60 * 1000);

    ASSERT_TRUE(ParseDurationMs(" 7 m ", &value));
    ASSERT_EQ(value, 7 * 60 * 1000);
}

TEST(DurationUtilsTest, ParseDurationMsRejectsInvalidInput) {
    uint64_t value = 0;

    ASSERT_FALSE(ParseDurationMs("", &value));
    ASSERT_FALSE(ParseDurationMs("abc", &value));
    ASSERT_FALSE(ParseDurationMs("-1", &value));
    ASSERT_FALSE(ParseDurationMs("1d", &value));
    ASSERT_FALSE(ParseDurationMs("18446744073709551616", &value));
    ASSERT_FALSE(ParseDurationMs("18446744073709552h", &value));
}

TEST_F(DefaultConfigTest, GetDurationMsFromJsonSupportsNumbersAndStrings) {
    DefaultConfig config;
    config.SetPath(path_ + "/../../mooncake-common/tests/test.json");
    config.Load();

    uint64_t legacy_ms = 0;
    uint64_t seconds = 0;
    uint64_t minutes = 0;
    uint64_t hours = 0;
    uint64_t whitespace = 0;
    uint64_t missing_default = 0;

    config.GetDurationMs("legacyDurationMs", &legacy_ms, 0);
    config.GetDurationMs("durationSeconds", &seconds, 0);
    config.GetDurationMs("durationMinutes", &minutes, 0);
    config.GetDurationMs("durationHours", &hours, 0);
    config.GetDurationMs("durationWhitespace", &whitespace, 0);
    config.GetDurationMs("missingDuration", &missing_default, 1234);

    ASSERT_EQ(legacy_ms, 5000);
    ASSERT_EQ(seconds, 5000);
    ASSERT_EQ(minutes, 30 * 60 * 1000);
    ASSERT_EQ(hours, 60 * 60 * 1000);
    ASSERT_EQ(whitespace, 7 * 60 * 1000);
    ASSERT_EQ(missing_default, 1234);
}

TEST_F(DefaultConfigTest, GetDurationMsFromYamlSupportsNumbersAndStrings) {
    DefaultConfig config;
    config.SetPath(path_ + "/../../mooncake-common/tests/test.yaml");
    config.Load();

    uint64_t legacy_ms = 0;
    uint64_t seconds = 0;
    uint64_t minutes = 0;
    uint64_t hours = 0;
    uint64_t whitespace = 0;
    uint64_t missing_default = 0;

    config.GetDurationMs("legacyDurationMs", &legacy_ms, 0);
    config.GetDurationMs("durationSeconds", &seconds, 0);
    config.GetDurationMs("durationMinutes", &minutes, 0);
    config.GetDurationMs("durationHours", &hours, 0);
    config.GetDurationMs("durationWhitespace", &whitespace, 0);
    config.GetDurationMs("missingDuration", &missing_default, 4321);

    ASSERT_EQ(legacy_ms, 6000);
    ASSERT_EQ(seconds, 6000);
    ASSERT_EQ(minutes, 7 * 60 * 1000);
    ASSERT_EQ(hours, 60 * 60 * 1000);
    ASSERT_EQ(whitespace, 8 * 60 * 60 * 1000);
    ASSERT_EQ(missing_default, 4321);
}

TEST_F(DefaultConfigTest, LoadInvalidFile) {
    DefaultConfig config;
    config.SetPath(path_ + "/invalid_file.txt");
    EXPECT_THROW(config.Load(), std::runtime_error);
}
}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
