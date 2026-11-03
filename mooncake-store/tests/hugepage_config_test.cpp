#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "../src/config/hugepage_config.h"

namespace mooncake {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
            original_ = value;
        }
        EXPECT_EQ(unsetenv(name), 0);
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            EXPECT_EQ(setenv(name_.c_str(), original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(name_.c_str()), 0);
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

    void Set(const char* value) {
        ASSERT_EQ(setenv(name_.c_str(), value, 1), 0);
    }

    void Unset() { ASSERT_EQ(unsetenv(name_.c_str()), 0); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

class HugepageConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = google::WARNING;
    }

    ScopedEnvVar use_hugepage{"MC_STORE_USE_HUGEPAGE"};
    ScopedEnvVar hugepage_size{"MC_STORE_HUGEPAGE_SIZE"};
};

TEST_F(HugepageConfigTest, UnsetDisablesHugepagesAndSkipsSizeValidation) {
    hugepage_size.Set("invalid");
    ::testing::internal::CaptureStderr();

    const HugepageConfig config = HugepageConfig::FromEnvironment();

    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.page_size, 0);
    const std::string logs = ::testing::internal::GetCapturedStderr();
    EXPECT_EQ(logs.find("Invalid MC_STORE_HUGEPAGE_SIZE"), std::string::npos);
}

TEST_F(HugepageConfigTest, AnyPresentEnableValueRequestsHugepages) {
    for (const char* value : {"", "0"}) {
        SCOPED_TRACE(value);
        use_hugepage.Set(value);

        EXPECT_TRUE(HugepageConfig::IsEnabledFromEnvironment());
        const HugepageConfig config = HugepageConfig::FromEnvironment();
        EXPECT_TRUE(config.enabled);
        EXPECT_EQ(config.page_size, 2ULL * 1024 * 1024);
    }
}

TEST_F(HugepageConfigTest, UnsetSizeDefaultsTo2Mb) {
    use_hugepage.Set("1");
    hugepage_size.Unset();

    const HugepageConfig config = HugepageConfig::FromEnvironment();

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

    use_hugepage.Set("1");
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        hugepage_size.Set(entry.value);

        const HugepageConfig config = HugepageConfig::FromEnvironment();

        EXPECT_TRUE(config.enabled);
        EXPECT_EQ(config.page_size, entry.expected);
    }
}

TEST_F(HugepageConfigTest, InvalidSizesWarnAndFallBackTo2Mb) {
    use_hugepage.Set("1");
    for (const char* value :
         {"", " ", "2MiB", "2MBjunk", "1e999", "infinite", "256MB", "1.5GB"}) {
        SCOPED_TRACE(value);
        hugepage_size.Set(value);
        ::testing::internal::CaptureStderr();

        const HugepageConfig config = HugepageConfig::FromEnvironment();
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
    use_hugepage.Set("1");
    hugepage_size.Set("2MB");
    EXPECT_EQ(HugepageConfig::FromEnvironment().page_size, 2ULL * 1024 * 1024);

    hugepage_size.Set("1GB");
    EXPECT_EQ(HugepageConfig::FromEnvironment().page_size,
              1024ULL * 1024 * 1024);

    use_hugepage.Unset();
    EXPECT_FALSE(HugepageConfig::IsEnabledFromEnvironment());
    EXPECT_EQ(HugepageConfig::FromEnvironment().page_size, 0);
}

}  // namespace
}  // namespace mooncake
