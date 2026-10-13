#include "../src/config/client_host_identity_config.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "environ.h"

namespace mooncake {
namespace {

class ClientHostIdentityConfigTest : public ::testing::Test {
   protected:
    ClientHostIdentityConfig Load(const std::string& local_hostname) const {
        return ClientHostIdentityConfig::FromEnvironment(Environ(source_),
                                                         local_hostname);
    }

    void Set(const char* value) { source_.Set(kVariable, value); }

   private:
    inline static constexpr char kVariable[] = "MOONCAKE_HOST_ID";
    MapEnvironSource source_;
};

TEST_F(ClientHostIdentityConfigTest, UsesLocalHostnameAndRejectsLoopback) {
    EXPECT_EQ(Load("hostB:5000").host_id, "hostB");
    EXPECT_EQ(Load("hostB:5001").host_id, "hostB");
    EXPECT_EQ(Load("[2001:db8::1]:5000").host_id, "2001:db8::1");
    EXPECT_TRUE(Load("localhost:5000").host_id.empty());
    EXPECT_TRUE(Load("127.0.0.1:5000").host_id.empty());
    EXPECT_TRUE(Load("0.0.0.0:5000").host_id.empty());
    EXPECT_TRUE(Load("::1").host_id.empty());
    EXPECT_TRUE(Load("[::1]:5000").host_id.empty());
    EXPECT_TRUE(Load("::").host_id.empty());
    EXPECT_TRUE(Load("[::]").host_id.empty());
    EXPECT_TRUE(Load("[::]:5000").host_id.empty());
}

TEST_F(ClientHostIdentityConfigTest, PrefersDeploymentOverride) {
    Set("  kubernetes-node-a  ");

    EXPECT_EQ(Load("10.244.1.17:5000").host_id, "kubernetes-node-a");
}

TEST_F(ClientHostIdentityConfigTest, NormalizesEndpointOverride) {
    Set("  kubernetes-node-a:5000  ");

    EXPECT_EQ(Load("10.244.1.17:5000").host_id, "kubernetes-node-a");
}

TEST_F(ClientHostIdentityConfigTest, FallsBackForEmptyOverride) {
    Set("");
    EXPECT_EQ(Load("hostB:5000").host_id, "hostB");

    Set(" \t ");
    EXPECT_EQ(Load("hostB:5000").host_id, "hostB");
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
        EXPECT_TRUE(Load("hostB:5000").host_id.empty());
    }
}

TEST_F(ClientHostIdentityConfigTest, NewConfigsReadCurrentEnvironment) {
    Set("host-a");
    const auto first = Load("fallback:5000");
    Set("host-b");
    const auto second = Load("fallback:5000");

    EXPECT_EQ(first.host_id, "host-a");
    EXPECT_EQ(second.host_id, "host-b");
}

}  // namespace
}  // namespace mooncake
