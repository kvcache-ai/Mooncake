#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#include "../src/config/local_file_snapshot_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class LocalFileSnapshotConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() /
                               "local_file_snapshot_config_test_XXXXXX")
                                  .string();
        char* dir = mkdtemp(pattern.data());
        ASSERT_NE(dir, nullptr);
        tmp_dir_ = dir;
    }

    void TearDown() override {
        if (!tmp_dir_.empty()) {
            std::filesystem::remove_all(tmp_dir_);
        }
    }

    LocalFileSnapshotConfig Load() const {
        return LocalFileSnapshotConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
    std::filesystem::path tmp_dir_;
};

TEST_F(LocalFileSnapshotConfigTest, RejectsMissingAndEmptyPath) {
    EXPECT_THROW(Load(), std::runtime_error);

    source_.Set("MOONCAKE_SNAPSHOT_LOCAL_PATH", "");
    EXPECT_THROW(Load(), std::runtime_error);
}

TEST_F(LocalFileSnapshotConfigTest, PreservesPathLiterally) {
    for (const char* value :
         {"relative/snapshots", "/tmp/snapshots", " ", " snapshots/../data "}) {
        SCOPED_TRACE(value);
        source_.Set("MOONCAKE_SNAPSHOT_LOCAL_PATH", value);
        EXPECT_EQ(Load().base_path, value);
    }
}

TEST_F(LocalFileSnapshotConfigTest, ReadsEnvironmentForEachConfig) {
    source_.Set("MOONCAKE_SNAPSHOT_LOCAL_PATH", "first");
    const auto first = Load();

    source_.Set("MOONCAKE_SNAPSHOT_LOCAL_PATH", "second");
    EXPECT_EQ(Load().base_path, "second");
    EXPECT_EQ(first.base_path, "first");

    source_.Unset("MOONCAKE_SNAPSHOT_LOCAL_PATH");
    EXPECT_THROW(Load(), std::runtime_error);
    EXPECT_EQ(first.base_path, "first");
}

TEST_F(LocalFileSnapshotConfigTest, DoesNotCreateDirectory) {
    const auto path = tmp_dir_ / "not-created" / "snapshots";
    ASSERT_FALSE(std::filesystem::exists(path));
    source_.Set("MOONCAKE_SNAPSHOT_LOCAL_PATH", path.string());

    EXPECT_EQ(Load().base_path, path.string());
    EXPECT_FALSE(std::filesystem::exists(path));
}

TEST_F(LocalFileSnapshotConfigTest, LeavesFilesystemValidationToStore) {
    const auto path = tmp_dir_ / "regular-file";
    {
        std::ofstream file(path);
        ASSERT_TRUE(file.is_open());
    }
    ASSERT_TRUE(std::filesystem::is_regular_file(path));
    source_.Set("MOONCAKE_SNAPSHOT_LOCAL_PATH", path.string());

    EXPECT_EQ(Load().base_path, path.string());
    EXPECT_TRUE(std::filesystem::is_regular_file(path));
}

}  // namespace
}  // namespace mooncake
