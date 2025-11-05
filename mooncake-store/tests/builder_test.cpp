#include <glog/logging.h>
#include <gtest/gtest.h>

#include "client.h"

namespace mooncake::test {

class MooncakeStoreBuilderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MooncakeStoreBuilderTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(MooncakeStoreBuilderTest, MissingAllRequiredFields) {
    MooncakeStoreBuilder builder;

    auto result = builder.Build();

    ASSERT_FALSE(result.has_value());
    const std::string& error = result.error();
    EXPECT_NE(error.find("local_hostname"), std::string::npos);
    EXPECT_NE(error.find("metadata_connstring"), std::string::npos);
}

TEST_F(MooncakeStoreBuilderTest, MissingLocalHostnameOnly) {
    MooncakeStoreBuilder builder;
    builder.WithMetadataConnectionString("metastore:1234");

    auto result = builder.Build();

    ASSERT_FALSE(result.has_value());
    const std::string& error = result.error();
    EXPECT_NE(error.find("local_hostname"), std::string::npos);
    EXPECT_EQ(error.find("metadata_connstring"), std::string::npos);
}

TEST_F(MooncakeStoreBuilderTest, MissingMetadataConnectionStringOnly) {
    MooncakeStoreBuilder builder;
    builder.WithLocalHostname("localhost:1234");

    auto result = builder.Build();

    ASSERT_FALSE(result.has_value());
    const std::string& error = result.error();
    EXPECT_NE(error.find("metadata_connstring"), std::string::npos);
    EXPECT_EQ(error.find("local_hostname"), std::string::npos);
}

TEST_F(MooncakeStoreBuilderTest, ProvidedAllRequiredFields) {
    MooncakeStoreBuilder builder;
    builder.WithLocalHostname("localhost:1234");
    builder.WithMetadataConnectionString("metadata:5678");

    auto result = builder.Build();

    if (result.has_value()) {
        SUCCEED();
    } else {
        const std::string& error = result.error();
        EXPECT_EQ(error.find("missing required fields"), std::string::npos);
        EXPECT_EQ(error.find("local_hostname"), std::string::npos);
        EXPECT_EQ(error.find("metadata_connstring"), std::string::npos);
    }
}

}  // namespace mooncake::test
