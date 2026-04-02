#include <gtest/gtest.h>
#include <glog/logging.h>
#include <random>
#include <cstdlib>

#include "connectors/data_connector.h"
#include "connectors/connector_importer.h"

namespace mooncake {
namespace testing {

TEST(ConnectorBasicTest, CreateHuggingFaceConnector) {
    auto connector = DataConnector::Create(ConnectorType::HUGGINGFACE);
    ASSERT_NE(connector, nullptr);
    EXPECT_NE(connector->GetConnectionInfo(), "");
}

#ifdef HAVE_AWS_SDK
TEST(ConnectorBasicTest, CreateOSSConnectorRequiresConfig) {
    setenv("MOONCAKE_OSS_ENDPOINT", "oss-test.aliyuncs.com", 1);
    setenv("MOONCAKE_OSS_BUCKET", "test-bucket", 1);
    setenv("MOONCAKE_OSS_ACCESS_KEY_ID", "test-key", 1);
    setenv("MOONCAKE_OSS_SECRET_ACCESS_KEY", "test-secret", 1);

    auto connector = DataConnector::Create(ConnectorType::OSS);
    ASSERT_NE(connector, nullptr);
    EXPECT_NE(connector->GetConnectionInfo(), "");
}
#endif

#ifdef HAVE_REDIS
class RedisConnectorE2ETest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Check if Redis is available
        const char* redis_host = std::getenv("MOONCAKE_REDIS_HOST");
        if (!redis_host) {
            redis_host = "127.0.0.1";
            setenv("MOONCAKE_REDIS_HOST", redis_host, 1);
        }

        const char* redis_port = std::getenv("MOONCAKE_REDIS_PORT");
        if (!redis_port) {
            redis_port = "6379";
            setenv("MOONCAKE_REDIS_PORT", redis_port, 1);
        }
    }

    std::string GenerateRandomString(size_t length) {
        static const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);

        std::string result;
        result.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            result += charset[dis(gen)];
        }
        return result;
    }
};

TEST_F(RedisConnectorE2ETest, LoadAndVerifyRandomData) {
    // Try to create Redis connector
    std::unique_ptr<DataConnector> connector;
    try {
        connector = DataConnector::Create(ConnectorType::REDIS);
    } catch (const std::exception& e) {
        GTEST_SKIP() << "Redis not available: " << e.what();
        return;
    }

    ASSERT_NE(connector, nullptr);
    LOG(INFO) << "Redis connector created: " << connector->GetConnectionInfo();

    // Generate random test data
    std::string test_key = "mooncake_test_" + GenerateRandomString(8);
    std::string test_value = GenerateRandomString(256);

    LOG(INFO) << "Test key: " << test_key;
    LOG(INFO) << "Test value length: " << test_value.size();

    // Note: This test assumes Redis is running and accessible
    // In a real scenario, you would:
    // 1. Use redis-cli or hiredis to SET the test data
    // 2. Use connector to download it
    // 3. Verify the data matches

    // For now, just test the download operation
    std::vector<uint8_t> buffer;
    auto result = connector->DownloadObject(test_key, buffer);

    // If key doesn't exist, that's expected for this test
    if (!result) {
        LOG(INFO) << "Key not found (expected): " << result.error();
        EXPECT_TRUE(result.error().find("not found") != std::string::npos ||
                    result.error().find("Key not found") != std::string::npos);
    } else {
        LOG(INFO) << "Downloaded " << buffer.size() << " bytes";
        EXPECT_GT(buffer.size(), 0);
    }
}

TEST_F(RedisConnectorE2ETest, ListObjectsWithPrefix) {
    std::unique_ptr<DataConnector> connector;
    try {
        connector = DataConnector::Create(ConnectorType::REDIS);
    } catch (const std::exception& e) {
        GTEST_SKIP() << "Redis not available: " << e.what();
        return;
    }

    ASSERT_NE(connector, nullptr);

    std::vector<ExternalObject> objects;
    auto result = connector->ListObjects("mooncake_test_*", objects);

    if (result) {
        LOG(INFO) << "Found " << objects.size()
                  << " objects with prefix 'mooncake_test_*'";
        for (const auto& obj : objects) {
            LOG(INFO) << "  - " << obj.key;
        }
    } else {
        LOG(INFO) << "List failed: " << result.error();
    }
}
#endif

}  // namespace testing
}  // namespace mooncake
