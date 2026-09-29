#include "../src/config/nof_register_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "environ.h"

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
    }

    void TearDown() override { FLAGS_logtostderr = original_logtostderr_; }

    NoFRegisterConfig Load() const {
        return NoFRegisterConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;

   private:
    bool original_logtostderr_ = false;
};

TEST_F(NoFRegisterConfigTest, UnsetTransportDefaultsToRdmaSilently) {
    testing::internal::CaptureStderr();
    const auto config = Load();
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
        source_.Set("MC_NOF_TRTYPE", entry.value);

        testing::internal::CaptureStderr();
        const auto config = Load();
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
        source_.Set("MC_NOF_TRTYPE", entry.value);

        testing::internal::CaptureStderr();
        const auto config = Load();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.transport_type, "RDMA");
        EXPECT_NE(diagnostics.find(std::string("Invalid MC_NOF_TRTYPE=") +
                                   entry.normalized + ", fallback to RDMA"),
                  std::string::npos);
    }
}

TEST_F(NoFRegisterConfigTest, NewConfigsReadCurrentEnvironment) {
    source_.Set("MC_NOF_TRTYPE", "tcp");
    const auto first = Load();
    source_.Set("MC_NOF_TRTYPE", "rdma");
    const auto second = Load();
    source_.Unset("MC_NOF_TRTYPE");
    const auto third = Load();

    EXPECT_EQ(first.transport_type, "TCP");
    EXPECT_EQ(second.transport_type, "RDMA");
    EXPECT_EQ(third.transport_type, "RDMA");
}

}  // namespace
}  // namespace mooncake::test
