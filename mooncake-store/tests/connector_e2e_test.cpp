#include <gtest/gtest.h>
#include <glog/logging.h>
#include <fstream>
#include <filesystem>

#include "connectors/data_connector.h"
#include "connectors/connector_importer.h"
#include "client_service.h"
#include "test_server_helpers.h"
#include "default_config.h"

namespace mooncake {
namespace testing {

class ConnectorE2ETest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        test_server_ = std::make_unique<TestServerHelper>();
        test_server_->StartMaster();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    static void TearDownTestSuite() {
        if (test_server_) {
            test_server_->StopMaster();
            test_server_.reset();
        }
    }

    void SetUp() override {
        auto client_opt = Client::Create(
            "localhost", "P2PHANDSHAKE", "tcp", std::nullopt,
            test_server_->GetMasterAddress());
        ASSERT_TRUE(client_opt.has_value());
        client_ = client_opt.value();
    }

    void TearDown() override {
        if (client_) {
            client_->tearDownAll();
        }
    }

    static std::unique_ptr<TestServerHelper> test_server_;
    std::shared_ptr<Client> client_;
};

std::unique_ptr<TestServerHelper> ConnectorE2ETest::test_server_;

TEST_F(ConnectorE2ETest, HuggingFaceConnectorBasic) {
    auto connector = DataConnector::Create(ConnectorType::HUGGINGFACE);
    ASSERT_NE(connector, nullptr);
    EXPECT_NE(connector->GetConnectionInfo(), "");
}

TEST_F(ConnectorE2ETest, ConnectorImporterWithMockData) {
    std::string test_key = "test_object";
    std::string test_data = "Hello from connector test!";

    int result = client_->Put(test_key,
        std::span<const char>(test_data.data(), test_data.size()));
    ASSERT_EQ(result, 0);

    std::vector<char> buffer(test_data.size());
    int64_t read_size = client_->get_into(test_key, buffer.data(), buffer.size());
    ASSERT_EQ(read_size, static_cast<int64_t>(test_data.size()));
    EXPECT_EQ(std::string(buffer.data(), buffer.size()), test_data);
}

}  // namespace testing
}  // namespace mooncake
