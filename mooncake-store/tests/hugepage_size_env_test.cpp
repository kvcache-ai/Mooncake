#include "common/client_buffer_allocation.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

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

class HugepageSizeEnvTest : public ::testing::Test {
   protected:
    void SetUp() override {
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = google::WARNING;
    }

    ScopedEnvVar use_hugepage{"MC_STORE_USE_HUGEPAGE"};
    ScopedEnvVar hugepage_size{"MC_STORE_HUGEPAGE_SIZE"};
};

TEST_F(HugepageSizeEnvTest, AppliesMmapAndMemfdFlagsForEachSupportedSize) {
    struct Case {
        const char* value;
        unsigned int mmap_size_flag;
        unsigned int memfd_size_flag;
    };
    const Case cases[] = {
        {"2MB", MAP_HUGE_2MB, MFD_HUGE_2MB},
        {"512MB", MAP_HUGE_512MB, MFD_HUGE_512MB},
        {"1GB", MAP_HUGE_1GB, MFD_HUGE_1GB},
    };

    use_hugepage.Set("1");
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        hugepage_size.Set(entry.value);

        unsigned int mmap_flags = MAP_PRIVATE;
        EXPECT_NE(get_hugepage_size_from_env(&mmap_flags), 0);
        EXPECT_EQ(mmap_flags, MAP_PRIVATE | MAP_HUGETLB | entry.mmap_size_flag);

        unsigned int memfd_flags = MFD_CLOEXEC;
        EXPECT_NE(get_hugepage_size_from_env(&memfd_flags, true), 0);
        EXPECT_EQ(memfd_flags,
                  MFD_CLOEXEC | MFD_HUGETLB | entry.memfd_size_flag);
    }
}

TEST_F(HugepageSizeEnvTest, LogsSelectedSizeOnlyWhenFlagsAreRequested) {
    use_hugepage.Set("1");
    hugepage_size.Set("512MB");

    ::testing::internal::CaptureStderr();
    EXPECT_EQ(get_hugepage_size_from_env(), 512ULL * 1024 * 1024);
    std::string logs = ::testing::internal::GetCapturedStderr();
    EXPECT_EQ(logs.find("Using hugepage size"), std::string::npos);

    FLAGS_minloglevel = google::INFO;
    ::testing::internal::CaptureStderr();
    unsigned int flags = 0;
    EXPECT_EQ(get_hugepage_size_from_env(&flags), 512ULL * 1024 * 1024);
    logs = ::testing::internal::GetCapturedStderr();
    EXPECT_NE(logs.find("Using hugepage size: 512MB"), std::string::npos);
}

}  // namespace
}  // namespace mooncake
