#include "default_config.h"

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