#include "../client/config/client_host_identity_config.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace mooncake {
namespace {

std::mutex environment_mutex;

class ClientHostIdentityConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        environment_lock_ = std::unique_lock<std::mutex>(environment_mutex);
        if (const char* value = std::getenv(kVariable)) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv(kVariable), 0);
    }

    void TearDown() override {
        if (original_) {
            EXPECT_EQ(setenv(kVariable, original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(kVariable), 0);
        }
    }

    void Set(const char* value) { ASSERT_EQ(setenv(kVariable, value, 1), 0); }

   private:
    inline static constexpr char kVariable[] = "MOONCAKE_HOST_ID";
    std::optional<std::string> original_;
    std::unique_lock<std::mutex> environment_lock_;
};

TEST_F(ClientHostIdentityConfigTest, UsesLocalHostnameAndRejectsLoopback) {
    EXPECT_EQ(ClientHostIdentityConfig::FromEnvironment("hostB:5000").host_id,
              "hostB");
    EXPECT_EQ(ClientHostIdentityConfig::FromEnvironment("hostB:5001").host_id,
              "hostB");
    EXPECT_EQ(
        ClientHostIdentityConfig::FromEnvironment("[2001:db8::1]:5000").host_id,
        "2001:db8::1");
    EXPECT_TRUE(ClientHostIdentityConfig::FromEnvironment("localhost:5000")
                    .host_id.empty());
    EXPECT_TRUE(ClientHostIdentityConfig::FromEnvironment("127.0.0.1:5000")
                    .host_id.empty());
    EXPECT_TRUE(ClientHostIdentityConfig::FromEnvironment("0.0.0.0:5000")
                    .host_id.empty());
    EXPECT_TRUE(
        ClientHostIdentityConfig::FromEnvironment("::1").host_id.empty());
    EXPECT_TRUE(ClientHostIdentityConfig::FromEnvironment("[::1]:5000")
                    .host_id.empty());
    EXPECT_TRUE(
        ClientHostIdentityConfig::FromEnvironment("::").host_id.empty());
    EXPECT_TRUE(
        ClientHostIdentityConfig::FromEnvironment("[::]").host_id.empty());
    EXPECT_TRUE(
        ClientHostIdentityConfig::FromEnvironment("[::]:5000").host_id.empty());
}

TEST_F(ClientHostIdentityConfigTest, PrefersDeploymentOverride) {
    Set("  kubernetes-node-a  ");

    EXPECT_EQ(
        ClientHostIdentityConfig::FromEnvironment("10.244.1.17:5000").host_id,
        "kubernetes-node-a");
}

TEST_F(ClientHostIdentityConfigTest, NormalizesEndpointOverride) {
    Set("  kubernetes-node-a:5000  ");

    EXPECT_EQ(
        ClientHostIdentityConfig::FromEnvironment("10.244.1.17:5000").host_id,
        "kubernetes-node-a");
}

TEST_F(ClientHostIdentityConfigTest, FallsBackForEmptyOverride) {
    Set("");
    EXPECT_EQ(ClientHostIdentityConfig::FromEnvironment("hostB:5000").host_id,
              "hostB");

    Set(" \t ");
    EXPECT_EQ(ClientHostIdentityConfig::FromEnvironment("hostB:5000").host_id,
              "hostB");
}

TEST_F(ClientHostIdentityConfigTest, RejectsInvalidOverrideWithoutFallback) {
    const std::vector<const char*> invalid_host_ids = {
        "localhost", "localhost:5000", "LOCALHOST",  "LoCaLhOsT:5000",
        "127.0.0.1", "127.0.0.1:5000", "0.0.0.0",    "0.0.0.0:5000",
        "::1",       "[::1]",          "[::1]:5000", "::",
        "[::]",      "[::]:5000"};
    for (const char* invalid_host_id : invalid_host_ids) {
        SCOPED_TRACE(invalid_host_id);
        Set(invalid_host_id);
        EXPECT_TRUE(ClientHostIdentityConfig::FromEnvironment("hostB:5000")
                        .host_id.empty());
    }
}

TEST_F(ClientHostIdentityConfigTest, NewConfigsReadCurrentEnvironment) {
    Set("host-a");
    const auto first =
        ClientHostIdentityConfig::FromEnvironment("fallback:5000");
    Set("host-b");
    const auto second =
        ClientHostIdentityConfig::FromEnvironment("fallback:5000");

    EXPECT_EQ(first.host_id, "host-a");
    EXPECT_EQ(second.host_id, "host-b");
}

}  // namespace
}  // namespace mooncake
