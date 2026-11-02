#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>

#include "common/file_util.h"

namespace mooncake::test {

namespace fs = std::filesystem;

class FileUtilTest : public ::testing::Test {
   protected:
    const std::string& tmp_dir() const { return tmp_dir_; }

    void SetUp() override {
        google::InitGoogleLogging("FileUtilTest");
        FLAGS_logtostderr = true;

        std::string tmpl =
            (fs::temp_directory_path() / "file_util_test_XXXXXX").string();
        char* dir = mkdtemp(tmpl.data());
        ASSERT_NE(dir, nullptr) << "Failed to create temp directory";
        tmp_dir_ = dir;
    }

    void TearDown() override {
        if (!tmp_dir().empty() && fs::exists(tmp_dir())) {
            fs::remove_all(tmp_dir());
        }
        google::ShutdownGoogleLogging();
    }

    // Helper: read file content back as string
    std::string ReadFileContent(const std::string& path) {
        std::ifstream f(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(f),
                std::istreambuf_iterator<char>()};
    }

   private:
    std::string tmp_dir_;
};

TEST_F(FileUtilTest, SortByCreationTime_EmptyAndSingle) {
    std::vector<fs::directory_entry> files;
    FileUtil::SortByCreationTime(files);
    EXPECT_TRUE(files.empty());

    const auto path = fs::path(tmp_dir()) / "one";
    std::ofstream(path) << "data";
    files.emplace_back(path);
    FileUtil::SortByCreationTime(files);
    ASSERT_EQ(files.size(), 1);
    EXPECT_EQ(files.front().path(), path);
}

TEST_F(FileUtilTest, SortByCreationTime_MissingFileKeepsKnownFilesOrdered) {
    const auto early = fs::path(tmp_dir()) / "z_early";
    const auto late = fs::path(tmp_dir()) / "a_late";
    const auto missing = fs::path(tmp_dir()) / "removed_after_scan";
    std::ofstream(early) << "early";
    std::ofstream(missing) << "temporary";
    const fs::directory_entry missing_entry(missing);
    ASSERT_TRUE(fs::remove(missing));
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    std::ofstream(late) << "late";

#if defined(__linux__)
    struct statx early_stat{}, late_stat{};
    ASSERT_EQ(statx(AT_FDCWD, early.c_str(), 0, STATX_BTIME, &early_stat), 0);
    ASSERT_EQ(statx(AT_FDCWD, late.c_str(), 0, STATX_BTIME, &late_stat), 0);
    if (!(early_stat.stx_mask & STATX_BTIME) ||
        !(late_stat.stx_mask & STATX_BTIME) ||
        std::tie(early_stat.stx_btime.tv_sec, early_stat.stx_btime.tv_nsec) >=
            std::tie(late_stat.stx_btime.tv_sec, late_stat.stx_btime.tv_nsec)) {
        GTEST_SKIP() << "Filesystem did not provide distinct birth times";
    }
#elif defined(__APPLE__)
    struct stat early_stat{}, late_stat{};
    ASSERT_EQ(stat(early.c_str(), &early_stat), 0);
    ASSERT_EQ(stat(late.c_str(), &late_stat), 0);
    if (std::tie(early_stat.st_birthtimespec.tv_sec,
                 early_stat.st_birthtimespec.tv_nsec) >=
        std::tie(late_stat.st_birthtimespec.tv_sec,
                 late_stat.st_birthtimespec.tv_nsec)) {
        GTEST_SKIP() << "Filesystem did not provide distinct birth times";
    }
#else
    GTEST_SKIP() << "Birth-time queries are not supported on this platform";
#endif

    const std::vector<fs::directory_entry> entries{
        fs::directory_entry(early), fs::directory_entry(late), missing_entry};
    std::vector<int> order{0, 1, 2};
    do {
        std::vector<fs::directory_entry> files;
        for (int index : order) files.push_back(entries[index]);
        FileUtil::SortByCreationTime(files);
        ASSERT_EQ(files.size(), 3);
        EXPECT_EQ(files[0].path(), early);
        EXPECT_EQ(files[1].path(), late);
        EXPECT_EQ(files[2].path(), missing);
    } while (std::next_permutation(order.begin(), order.end()));
    EXPECT_TRUE(fs::exists(early));
    EXPECT_TRUE(fs::exists(late));
}

TEST_F(FileUtilTest, SortByCreationTime_RetainsAndOrdersFailedQueries) {
    const auto a = fs::path(tmp_dir()) / "a";
    const auto z = fs::path(tmp_dir()) / "z";
    std::ofstream(a) << "a";
    std::ofstream(z) << "z";
    std::vector<fs::directory_entry> files{fs::directory_entry(z),
                                           fs::directory_entry(a)};
    ASSERT_TRUE(fs::remove(a));
    ASSERT_TRUE(fs::remove(z));
    FileUtil::SortByCreationTime(files);
    ASSERT_EQ(files.size(), 2);
    EXPECT_EQ(files[0].path(), a);
    EXPECT_EQ(files[1].path(), z);
}

TEST_F(FileUtilTest, SortByCreationTime_EqualBirthTimesUsePath) {
    const auto a = fs::path(tmp_dir()) / "a";
    const auto z = fs::path(tmp_dir()) / "z";
    std::ofstream(z) << "data";
    fs::create_hard_link(z, a);
    std::vector<fs::directory_entry> files{fs::directory_entry(z),
                                           fs::directory_entry(a)};
    FileUtil::SortByCreationTime(files);
    ASSERT_EQ(files.size(), 2);
    EXPECT_EQ(files[0].path(), a);
    EXPECT_EQ(files[1].path(), z);
    EXPECT_EQ(ReadFileContent(a.string()), "data");
    EXPECT_EQ(ReadFileContent(z.string()), "data");
}

#if defined(__linux__)
TEST_F(FileUtilTest, SortByCreationTime_RetainsReadableFileWithoutBirthTime) {
    const fs::path without_birth_time = "/proc/self/status";
    struct statx info{};
    ASSERT_EQ(
        statx(AT_FDCWD, without_birth_time.c_str(), 0, STATX_BTIME, &info), 0);
    if (info.stx_mask & STATX_BTIME) {
        GTEST_SKIP() << "procfs provides birth times on this system";
    }
    const auto known = fs::path(tmp_dir()) / "known";
    std::ofstream(known) << "data";
    ASSERT_EQ(statx(AT_FDCWD, known.c_str(), 0, STATX_BTIME, &info), 0);
    if (!(info.stx_mask & STATX_BTIME)) {
        GTEST_SKIP() << "Temporary filesystem does not provide birth times";
    }
    std::vector<fs::directory_entry> files{
        fs::directory_entry(without_birth_time), fs::directory_entry(known)};
    FileUtil::SortByCreationTime(files);
    ASSERT_EQ(files.size(), 2);
    EXPECT_EQ(files[0].path(), known);
    EXPECT_EQ(files[1].path(), without_birth_time);
    EXPECT_FALSE(ReadFileContent(without_birth_time.string()).empty());
}
#endif

// ========== SaveStringToFile ==========

TEST_F(FileUtilTest, SaveStringToFile_Basic) {
    std::string content = "hello mooncake";
    std::string path = tmp_dir() + "/test.txt";
    auto result = FileUtil::SaveStringToFile(content, path);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(ReadFileContent(path), content);
}

TEST_F(FileUtilTest, SaveStringToFile_CreatesSubdirectories) {
    std::string content = "nested content";
    std::string path = tmp_dir() + "/a/b/c/nested.txt";
    auto result = FileUtil::SaveStringToFile(content, path);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(ReadFileContent(path), content);
}

TEST_F(FileUtilTest, SaveStringToFile_EmptyContent) {
    std::string path = tmp_dir() + "/empty.txt";
    auto result = FileUtil::SaveStringToFile("", path);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(ReadFileContent(path), "");
}

// ========== SaveBinaryToFile ==========

TEST_F(FileUtilTest, SaveBinaryToFile_Basic) {
    std::vector<uint8_t> data = {0, 1, 2, 128, 254, 255};
    std::string path = tmp_dir() + "/bin.dat";
    auto result = FileUtil::SaveBinaryToFile(data, path);
    ASSERT_TRUE(result.has_value()) << result.error();

    std::string raw = ReadFileContent(path);
    std::vector<uint8_t> read_back(raw.begin(), raw.end());
    EXPECT_EQ(read_back, data);
}

TEST_F(FileUtilTest, SaveBinaryToFile_CreatesSubdirectories) {
    std::vector<uint8_t> data = {42};
    std::string path = tmp_dir() + "/x/y/z/deep.dat";
    auto result = FileUtil::SaveBinaryToFile(data, path);
    ASSERT_TRUE(result.has_value()) << result.error();

    std::string raw = ReadFileContent(path);
    std::vector<uint8_t> read_back(raw.begin(), raw.end());
    EXPECT_EQ(read_back, data);
}

// ========== EnsureDirExists ==========

TEST_F(FileUtilTest, EnsureDirExists_CreateNew) {
    std::string dir = tmp_dir() + "/new_dir";
    EXPECT_FALSE(fs::exists(dir));
    auto result = FileUtil::EnsureDirExists(dir);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_TRUE(fs::is_directory(dir));
}

TEST_F(FileUtilTest, EnsureDirExists_AlreadyExists) {
    // tmp_dir() already exists
    auto result = FileUtil::EnsureDirExists(tmp_dir());
    EXPECT_TRUE(result.has_value());
}

TEST_F(FileUtilTest, EnsureDirExists_PathIsFile_ReturnsError) {
    // Create a regular file, then try EnsureDirExists on it
    std::string file_path = tmp_dir() + "/a_file";
    std::ofstream(file_path) << "data";
    ASSERT_TRUE(fs::is_regular_file(file_path));

    auto result = FileUtil::EnsureDirExists(file_path);
    EXPECT_FALSE(result.has_value());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
