#include "../src/config/client_host_identity_config.h"

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

class PhysicalEnvGuard {
   public:
    PhysicalEnvGuard() {
        for (const auto* name : {"MOONCAKE_FABRIC_DOMAIN", "MC_RACK", "MC_TRAY",
                                 "MC_PHYSICAL_GPU", "CUDA_VISIBLE_DEVICES"}) {
            const auto* value = std::getenv(name);
            saved_.emplace_back(
                name, value ? std::optional<std::string>(value) : std::nullopt);
            unsetenv(name);
        }
    }
    ~PhysicalEnvGuard() {
        for (const auto& [name, value] : saved_) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
    }

   private:
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_;
};

TEST_F(ClientHostIdentityConfigTest, PhysicalDomainIsNotInferredFromRack) {
    PhysicalEnvGuard env;
    ASSERT_EQ(setenv("MC_RACK", "rack1", 1), 0);
    ASSERT_EQ(setenv("MC_PHYSICAL_GPU", "3", 1), 0);
    EXPECT_FALSE(
        ClientHostIdentityConfig::PhysicalFromEnvironment("host:5000"));
    ASSERT_EQ(setenv("MOONCAKE_FABRIC_DOMAIN", " fabric1 ", 1), 0);
    auto placement =
        ClientHostIdentityConfig::PhysicalFromEnvironment("host:5000");
    ASSERT_TRUE(placement);
    EXPECT_TRUE(placement->Valid());
    EXPECT_EQ(placement->fabric_domain, "fabric1");
    EXPECT_EQ(placement->tray, "host");
    EXPECT_EQ(placement->physical_gpu, 3);
}

TEST_F(ClientHostIdentityConfigTest, PhysicalGpuDoesNotUseLogicalCudaZero) {
    PhysicalEnvGuard env;
    ASSERT_EQ(setenv("MOONCAKE_FABRIC_DOMAIN", "fabric1", 1), 0);
    ASSERT_EQ(setenv("MC_RACK", "rack1", 1), 0);
    ASSERT_EQ(setenv("CUDA_VISIBLE_DEVICES", "7", 1), 0);
    Set("host-override:5000");
    auto placement =
        ClientHostIdentityConfig::PhysicalFromEnvironment("fallback");
    ASSERT_TRUE(placement);
    EXPECT_TRUE(placement->Valid());
    EXPECT_EQ(placement->physical_gpu, 7);
    EXPECT_EQ(placement->tray, "host-override");
    ASSERT_EQ(setenv("MC_PHYSICAL_GPU", " 3 ", 1), 0);
    ASSERT_EQ(setenv("MC_TRAY", " tray1 ", 1), 0);
    placement = ClientHostIdentityConfig::PhysicalFromEnvironment("fallback");
    ASSERT_TRUE(placement);
    EXPECT_EQ(placement->physical_gpu, 3);
    EXPECT_EQ(placement->tray, "tray1");
}

TEST_F(ClientHostIdentityConfigTest, AmbiguousGpuRequiresExplicitPhysicalId) {
    PhysicalEnvGuard env;
    ASSERT_EQ(setenv("MOONCAKE_FABRIC_DOMAIN", "fabric1", 1), 0);
    ASSERT_EQ(setenv("MC_RACK", "rack1", 1), 0);
    for (const auto* gpu : {"", "0,1", "GPU-uuid", "-1", "3x", "2147483648"}) {
        ASSERT_EQ(setenv("CUDA_VISIBLE_DEVICES", gpu, 1), 0);
        auto placement =
            ClientHostIdentityConfig::PhysicalFromEnvironment("host");
        ASSERT_TRUE(placement);
        EXPECT_FALSE(placement->Valid()) << gpu;
    }
    ASSERT_EQ(setenv("MC_PHYSICAL_GPU", "3", 1), 0);
    auto placement = ClientHostIdentityConfig::PhysicalFromEnvironment("host");
    ASSERT_TRUE(placement);
    EXPECT_TRUE(placement->Valid());
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
