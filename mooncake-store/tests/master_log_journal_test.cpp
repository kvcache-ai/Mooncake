// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "master_log_journal.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace mooncake {
namespace {

namespace fs = std::filesystem;

std::string ReadFile(const fs::path& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

// Returns the merged journal file: the single non-symlink regular file in the
// journal directory.
fs::path FindMergedFile(const fs::path& dir) {
    fs::path result;
    int count = 0;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (fs::is_symlink(entry.path())) {
            continue;
        }
        if (fs::is_regular_file(entry.path())) {
            result = entry.path();
            ++count;
        }
    }
    EXPECT_EQ(count, 1) << "expected exactly one merged journal file in "
                        << dir;
    return result;
}

// The journal and glog are process-global, so the full lifecycle is exercised
// in a single test to keep behavior deterministic.
TEST(MasterLogJournalTest, MergesAllSeveritiesIntoSingleDirectory) {
    const fs::path journal_dir =
        fs::temp_directory_path() /
        ("mooncake_journal_test_" + std::to_string(::getpid()));
    fs::remove_all(journal_dir);

    MasterLogJournalConfig config;
    config.enable = true;
    config.program_name = "mooncake_master";
    config.journal_dir = journal_dir.string();
    config.merge_severities = true;
    config.also_log_to_stderr = false;

    auto& journal = MasterLogJournal::Instance();
    ASSERT_TRUE(journal.Init(config));
    ASSERT_TRUE(journal.active());
    EXPECT_EQ(journal.journal_dir(), journal_dir.string());

    const std::string info_marker = "journal_info_marker_12345";
    const std::string warn_marker = "journal_warning_marker_23456";
    const std::string error_marker = "journal_error_marker_34567";
    LOG(INFO) << info_marker;
    LOG(WARNING) << warn_marker;
    LOG(ERROR) << error_marker;
    journal.Flush();

    ASSERT_TRUE(fs::exists(journal_dir));

    // The merge must produce exactly one journal file (no per-severity files).
    const fs::path merged = FindMergedFile(journal_dir);
    ASSERT_FALSE(merged.empty());

    // No separate WARNING/ERROR/FATAL symlinks or files should be created.
    EXPECT_FALSE(fs::exists(journal_dir / "mooncake_master.WARNING"));
    EXPECT_FALSE(fs::exists(journal_dir / "mooncake_master.ERROR"));
    EXPECT_FALSE(fs::exists(journal_dir / "mooncake_master.FATAL"));

    // The stable symlink must point at the merged file.
    const fs::path symlink = journal_dir / "mooncake_master.INFO";
    ASSERT_TRUE(fs::is_symlink(symlink));
    EXPECT_EQ(journal.journal_symlink(), symlink.string());

    // All three severities must land in the one merged file.
    const std::string contents = ReadFile(merged);
    EXPECT_NE(contents.find(info_marker), std::string::npos);
    EXPECT_NE(contents.find(warn_marker), std::string::npos);
    EXPECT_NE(contents.find(error_marker), std::string::npos);

    // Re-init is a no-op and must remain active.
    EXPECT_TRUE(journal.Init(config));
    EXPECT_TRUE(journal.active());

    journal.Flush();
    fs::remove_all(journal_dir);
}

TEST(MasterLogJournalTest, DisabledJournalIsNoOp) {
    // A separate disabled config must not throw and must report success even
    // though the global journal was already activated by the test above.
    MasterLogJournalConfig config;
    config.enable = false;
    EXPECT_TRUE(MasterLogJournal::Instance().Init(config));
}

}  // namespace
}  // namespace mooncake
