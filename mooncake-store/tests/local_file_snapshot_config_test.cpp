#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>

#include "../src/config/local_file_snapshot_config.h"

namespace mooncake {
namespace {

class LocalFileSnapshotConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MOONCAKE_SNAPSHOT_LOCAL_PATH")) {
            original_path_ = value;
        }
        ASSERT_EQ(unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH"), 0);

        std::string pattern = (std::filesystem::temp_directory_path() /
                               "local_file_snapshot_config_test_XXXXXX")
                                  .string();
        char* dir = mkdtemp(pattern.data());
        ASSERT_NE(dir, nullptr);
        tmp_dir_ = dir;
    }

    void TearDown() override {
        if (original_path_) {
            EXPECT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH",
                             original_path_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH"), 0);
        }
        if (!tmp_dir_.empty()) {
            std::filesystem::remove_all(tmp_dir_);
        }
    }

    std::filesystem::path tmp_dir_;

   private:
    std::optional<std::string> original_path_;
};

TEST_F(LocalFileSnapshotConfigTest, RejectsMissingAndEmptyPath) {
    EXPECT_THROW(LocalFileSnapshotConfig::FromEnvironment(),
                 std::runtime_error);

    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", "", 1), 0);
    EXPECT_THROW(LocalFileSnapshotConfig::FromEnvironment(),
                 std::runtime_error);
}

TEST_F(LocalFileSnapshotConfigTest, PreservesPathLiterally) {
    for (const char* value :
         {"relative/snapshots", "/tmp/snapshots", " ", " snapshots/../data "}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", value, 1), 0);
        EXPECT_EQ(LocalFileSnapshotConfig::FromEnvironment().base_path, value);
    }
}

TEST_F(LocalFileSnapshotConfigTest, ReadsEnvironmentForEachConfig) {
    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", "first", 1), 0);
    const auto first = LocalFileSnapshotConfig::FromEnvironment();

    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", "second", 1), 0);
    EXPECT_EQ(LocalFileSnapshotConfig::FromEnvironment().base_path, "second");
    EXPECT_EQ(first.base_path, "first");

    ASSERT_EQ(unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH"), 0);
    EXPECT_THROW(LocalFileSnapshotConfig::FromEnvironment(),
                 std::runtime_error);
    EXPECT_EQ(first.base_path, "first");
}

TEST_F(LocalFileSnapshotConfigTest, DoesNotCreateDirectory) {
    const auto path = tmp_dir_ / "not-created" / "snapshots";
    ASSERT_FALSE(std::filesystem::exists(path));
    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", path.c_str(), 1), 0);

    EXPECT_EQ(LocalFileSnapshotConfig::FromEnvironment().base_path,
              path.string());
    EXPECT_FALSE(std::filesystem::exists(path));
}

TEST_F(LocalFileSnapshotConfigTest, LeavesFilesystemValidationToStore) {
    const auto path = tmp_dir_ / "regular-file";
    {
        std::ofstream file(path);
        ASSERT_TRUE(file.is_open());
    }
    ASSERT_TRUE(std::filesystem::is_regular_file(path));
    ASSERT_EQ(setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", path.c_str(), 1), 0);

    EXPECT_EQ(LocalFileSnapshotConfig::FromEnvironment().base_path,
              path.string());
    EXPECT_TRUE(std::filesystem::is_regular_file(path));
}

}  // namespace
}  // namespace mooncake
