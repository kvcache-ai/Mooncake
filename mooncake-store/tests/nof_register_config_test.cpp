#include "../src/config/nof_register_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

namespace mooncake::test {
namespace {

class NoFRegisterConfigTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("NoFRegisterConfigTest");
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
        if (const char* value = std::getenv("MC_NOF_TRTYPE")) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv("MC_NOF_TRTYPE"), 0);
    }

    void TearDown() override {
        if (original_.has_value()) {
            EXPECT_EQ(setenv("MC_NOF_TRTYPE", original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv("MC_NOF_TRTYPE"), 0);
        }
        FLAGS_logtostderr = original_logtostderr_;
    }

   private:
    std::optional<std::string> original_;
    bool original_logtostderr_ = false;
};

TEST_F(NoFRegisterConfigTest, UnsetTransportDefaultsToRdmaSilently) {
    testing::internal::CaptureStderr();
    const auto config = NoFRegisterConfig::FromEnvironment();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_EQ(config.transport_type, "RDMA");
    EXPECT_TRUE(diagnostics.empty()) << diagnostics;
}

TEST_F(NoFRegisterConfigTest, ValidTransportIsCaseInsensitive) {
    struct Case {
        const char* value;
        const char* expected;
    };
    const Case cases[] = {{"RDMA", "RDMA"}, {"rdma", "RDMA"}, {"RdMa", "RDMA"},
                          {"TCP", "TCP"},   {"tcp", "TCP"},   {"TcP", "TCP"}};
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        ASSERT_EQ(setenv("MC_NOF_TRTYPE", entry.value, 1), 0);

        testing::internal::CaptureStderr();
        const auto config = NoFRegisterConfig::FromEnvironment();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.transport_type, entry.expected);
        EXPECT_TRUE(diagnostics.empty()) << diagnostics;
    }
}

TEST_F(NoFRegisterConfigTest, EmptyAndInvalidValuesWarnAndFallbackToRdma) {
    struct Case {
        const char* value;
        const char* normalized;
    };
    const Case cases[] = {{"", ""},
                          {"udp", "UDP"},
                          {" tcp", " TCP"},
                          {"tcp ", "TCP "},
                          {"1", "1"}};
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        ASSERT_EQ(setenv("MC_NOF_TRTYPE", entry.value, 1), 0);

        testing::internal::CaptureStderr();
        const auto config = NoFRegisterConfig::FromEnvironment();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.transport_type, "RDMA");
        EXPECT_NE(diagnostics.find(std::string("Invalid MC_NOF_TRTYPE=") +
                                   entry.normalized + ", fallback to RDMA"),
                  std::string::npos);
    }
}

TEST_F(NoFRegisterConfigTest, NewConfigsReadCurrentEnvironment) {
    ASSERT_EQ(setenv("MC_NOF_TRTYPE", "tcp", 1), 0);
    const auto first = NoFRegisterConfig::FromEnvironment();
    ASSERT_EQ(setenv("MC_NOF_TRTYPE", "rdma", 1), 0);
    const auto second = NoFRegisterConfig::FromEnvironment();
    ASSERT_EQ(unsetenv("MC_NOF_TRTYPE"), 0);
    const auto third = NoFRegisterConfig::FromEnvironment();

    EXPECT_EQ(first.transport_type, "TCP");
    EXPECT_EQ(second.transport_type, "RDMA");
    EXPECT_EQ(third.transport_type, "RDMA");
}

}  // namespace
}  // namespace mooncake::test
