#include <gtest/gtest.h>
#include "connectors/data_connector.h"
#include "connectors/huggingface_connector.h"

namespace mooncake {

TEST(DataConnectorTest, CreateHuggingFaceConnector) {
    auto connector = DataConnector::Create(ConnectorType::HUGGINGFACE);
    ASSERT_NE(connector, nullptr);
    EXPECT_NE(connector->GetConnectionInfo(), "");
}

#ifdef HAVE_AWS_SDK
TEST(DataConnectorTest, CreateOSSConnector) {
    setenv("MOONCAKE_OSS_ENDPOINT", "oss-test.aliyuncs.com", 1);
    setenv("MOONCAKE_OSS_BUCKET", "test-bucket", 1);
    setenv("MOONCAKE_OSS_ACCESS_KEY_ID", "test-key", 1);
    setenv("MOONCAKE_OSS_SECRET_ACCESS_KEY", "test-secret", 1);

    auto connector = DataConnector::Create(ConnectorType::OSS);
    ASSERT_NE(connector, nullptr);
}
#endif

#ifdef HAVE_REDIS
TEST(DataConnectorTest, CreateRedisConnector) {
    setenv("MOONCAKE_REDIS_HOST", "127.0.0.1", 1);
    setenv("MOONCAKE_REDIS_PORT", "6379", 1);

    EXPECT_THROW(DataConnector::Create(ConnectorType::REDIS),
                 std::runtime_error);
}
#endif

}  // namespace mooncake
