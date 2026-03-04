#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "utils/file_util.h"

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
