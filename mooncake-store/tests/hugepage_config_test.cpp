#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "../src/config/hugepage_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class HugepageConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = google::WARNING;
    }

    bool IsEnabled() const {
        return HugepageConfig::IsEnabledFromEnvironment(Environ(source_));
    }

    HugepageConfig Load() const {
        return HugepageConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(HugepageConfigTest, UnsetDisablesHugepagesAndSkipsSizeValidation) {
    source_.Set("MC_STORE_HUGEPAGE_SIZE", "invalid");
    ::testing::internal::CaptureStderr();

    const HugepageConfig config = Load();

    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.page_size, 0);
    const std::string logs = ::testing::internal::GetCapturedStderr();
    EXPECT_EQ(logs.find("Invalid MC_STORE_HUGEPAGE_SIZE"), std::string::npos);
}

TEST_F(HugepageConfigTest, AnyPresentEnableValueRequestsHugepages) {
    for (const char* value : {"", "0"}) {
        SCOPED_TRACE(value);
        source_.Set("MC_STORE_USE_HUGEPAGE", value);

        EXPECT_TRUE(IsEnabled());
        const HugepageConfig config = Load();
        EXPECT_TRUE(config.enabled);
        EXPECT_EQ(config.page_size, 2ULL * 1024 * 1024);
    }
}

TEST_F(HugepageConfigTest, UnsetSizeDefaultsTo2Mb) {
    source_.Set("MC_STORE_USE_HUGEPAGE", "1");
    source_.Unset("MC_STORE_HUGEPAGE_SIZE");

    const HugepageConfig config = Load();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.page_size, 2ULL * 1024 * 1024);
}

TEST_F(HugepageConfigTest, AcceptsLegacyByteSizeSyntaxForSupportedSizes) {
    struct Case {
        const char* value;
        size_t expected;
    };
    const Case cases[] = {
        {"2MB", 2ULL * 1024 * 1024},     {"512MB", 512ULL * 1024 * 1024},
        {"1GB", 1024ULL * 1024 * 1024},  {" 2 mb ", 2ULL * 1024 * 1024},
        {"+512M", 512ULL * 1024 * 1024}, {"0.5GB", 512ULL * 1024 * 1024},
        {"2048K", 2ULL * 1024 * 1024},
    };

    source_.Set("MC_STORE_USE_HUGEPAGE", "1");
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        source_.Set("MC_STORE_HUGEPAGE_SIZE", entry.value);

        const HugepageConfig config = Load();

        EXPECT_TRUE(config.enabled);
        EXPECT_EQ(config.page_size, entry.expected);
    }
}

TEST_F(HugepageConfigTest, InvalidSizesWarnAndFallBackTo2Mb) {
    source_.Set("MC_STORE_USE_HUGEPAGE", "1");
    for (const char* value :
         {"", " ", "2MiB", "2MBjunk", "1e999", "infinite", "256MB", "1.5GB"}) {
        SCOPED_TRACE(value);
        source_.Set("MC_STORE_HUGEPAGE_SIZE", value);
        ::testing::internal::CaptureStderr();

        const HugepageConfig config = Load();
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_TRUE(config.enabled);
        EXPECT_EQ(config.page_size, 2ULL * 1024 * 1024);
        EXPECT_NE(logs.find("Invalid MC_STORE_HUGEPAGE_SIZE='"),
                  std::string::npos);
        EXPECT_NE(logs.find("Supported: 2MB, 512MB, 1GB. Fallback to 2MB."),
                  std::string::npos);
    }
}

TEST_F(HugepageConfigTest, EachCallReadsCurrentEnvironment) {
    source_.Set("MC_STORE_USE_HUGEPAGE", "1");
    source_.Set("MC_STORE_HUGEPAGE_SIZE", "2MB");
    EXPECT_EQ(Load().page_size, 2ULL * 1024 * 1024);

    source_.Set("MC_STORE_HUGEPAGE_SIZE", "1GB");
    EXPECT_EQ(Load().page_size, 1024ULL * 1024 * 1024);

    source_.Unset("MC_STORE_USE_HUGEPAGE");
    EXPECT_FALSE(IsEnabled());
    EXPECT_EQ(Load().page_size, 0);
}

}  // namespace
}  // namespace mooncake
