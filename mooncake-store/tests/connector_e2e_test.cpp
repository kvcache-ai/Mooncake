#include <gtest/gtest.h>
#include <glog/logging.h>

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

}  // namespace testing
}  // namespace mooncake
