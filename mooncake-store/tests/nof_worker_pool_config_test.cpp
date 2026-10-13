#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <optional>
#include <string>
#include <utility>

#include "../src/config/nof_worker_pool_config.h"
#include "environ.h"
#ifdef USE_NOF
#include "transfer_task.h"
#endif

namespace mooncake {
namespace {

// Only for tests that exercise AtFirstUse() or pool construction, which read
// the process environment.
class ScopedEnvironment {
   public:
    explicit ScopedEnvironment(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) original_ = value;
        EXPECT_EQ(unsetenv(name), 0);
    }
    ~ScopedEnvironment() {
        if (original_) {
            EXPECT_EQ(setenv(name_.c_str(), original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(name_.c_str()), 0);
        }
    }
    void Set(const char* value) {
        ASSERT_EQ(setenv(name_.c_str(), value, 1), 0);
    }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

class NoFWorkerPoolConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NoFWorkerPoolConfigTest");
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
    }

    void TearDown() override {
        FLAGS_logtostderr = original_logtostderr_;
        google::ShutdownGoogleLogging();
    }

    NoFWorkerPoolConfig Load() const {
        return NoFWorkerPoolConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;

   private:
    bool original_logtostderr_ = false;
};

TEST_F(NoFWorkerPoolConfigTest, UnsetAndEmptyKeepFourWorkersSilently) {
    testing::internal::CaptureStderr();
    EXPECT_EQ(Load().worker_count, 4);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    source_.Set("MC_NOF_WORKERS", "");
    testing::internal::CaptureStderr();
    EXPECT_EQ(Load().worker_count, 4);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(NoFWorkerPoolConfigTest, AcceptsPositiveIntegersAndTrailingWhitespace) {
    for (const auto& [value, expected] :
         {std::pair{"2", 2}, std::pair{"+3", 3}, std::pair{" 4 ", 4},
          std::pair{"2147483647", 2147483647}}) {
        SCOPED_TRACE(value);
        source_.Set("MC_NOF_WORKERS", value);
        testing::internal::CaptureStderr();
        EXPECT_EQ(Load().worker_count, expected);
        EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
    }
}

TEST_F(NoFWorkerPoolConfigTest, InvalidValuesWarnWithUnchangedRawValue) {
    for (const char* value : {"0", "-1", "oops", "2x", "2147483648"}) {
        SCOPED_TRACE(value);
        source_.Set("MC_NOF_WORKERS", value);
        testing::internal::CaptureStderr();
        EXPECT_EQ(Load().worker_count, 4);
        const auto log = testing::internal::GetCapturedStderr();
        EXPECT_NE(log.find(std::string("Invalid value for MC_NOF_WORKERS: ") +
                           value + ", using default 4"),
                  std::string::npos);
    }
}

TEST_F(NoFWorkerPoolConfigTest, FreshReadsSeeChangesButFirstUseIsCached) {
    ScopedEnvironment workers("MC_NOF_WORKERS");
    workers.Set("2");
    EXPECT_EQ(
        NoFWorkerPoolConfig::FromEnvironment(Environ::Process()).worker_count,
        2);
    EXPECT_EQ(NoFWorkerPoolConfig::AtFirstUse().worker_count, 2);
    workers.Set("3");
    EXPECT_EQ(
        NoFWorkerPoolConfig::FromEnvironment(Environ::Process()).worker_count,
        3);
    EXPECT_EQ(NoFWorkerPoolConfig::AtFirstUse().worker_count, 2);
}

#ifdef USE_NOF
TEST_F(NoFWorkerPoolConfigTest, PoolConstructionUsesCachedWorkerCount) {
    ScopedEnvironment workers("MC_NOF_WORKERS");
    workers.Set("2");
    const auto count_threads = [] {
        return std::distance(
            std::filesystem::directory_iterator("/proc/self/task"),
            std::filesystem::directory_iterator());
    };
    const auto before = count_threads();
    SpdkNofWorkerPool first;
    EXPECT_EQ(count_threads() - before, 2);
    workers.Set("3");
    SpdkNofWorkerPool second;
    EXPECT_EQ(count_threads() - before, 4);
    EXPECT_EQ(NoFWorkerPoolConfig::AtFirstUse().worker_count, 2);
}
#endif

}  // namespace
}  // namespace mooncake
