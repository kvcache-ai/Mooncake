#include "cold_write_journal.h"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

#include <ylt/struct_pack.hpp>

namespace mooncake::test {
namespace {
namespace fs = std::filesystem;

class ColdWriteJournalTest : public ::testing::Test {
   protected:
    fs::path root;
    void SetUp() override {
        root = fs::current_path() /
               ("cold-journal-test-" + std::to_string(getpid()));
        fs::remove_all(root);
        fs::create_directories(root);
    }
    void TearDown() override { fs::remove_all(root); }
    static ColdWriteRequest Request() {
        ColdWriteRequest request;
        request.writer = {1, 2};
        request.owner = {3, 4};
        request.reservation = 17;
        request.tenant = "tenant";
        request.key = "weight/key";
        request.size = 512 * 1024 * 1024;
        request.deadline_ms = 1234567;
        request.checksum = 9981;
        return request;
    }
    fs::path TokenPath() {
        for (const auto& entry :
             fs::directory_iterator(root / "cold_write_journal")) {
            if (entry.path().extension() == ".token") return entry.path();
        }
        return {};
    }
};

TEST_F(ColdWriteJournalTest, ExactTokenRoundtripIdempotenceAndRemoval) {
    auto request = Request();
    {
        ColdWriteJournal journal(root.string());
        ASSERT_TRUE(journal.Init());
        ASSERT_TRUE(journal.List());
        EXPECT_TRUE(journal.List()->empty());
        ASSERT_TRUE(journal.Put("tenant::weight/key", request));
        ASSERT_TRUE(journal.Put("tenant::weight/key", request));
        auto changed = request;
        ++changed.deadline_ms;
        auto conflict = journal.Put("tenant::weight/key", changed);
        ASSERT_FALSE(conflict);
        EXPECT_EQ(conflict.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
        auto records = journal.List();
        ASSERT_TRUE(records);
        ASSERT_EQ(records->size(), 1);
        EXPECT_EQ(struct_pack::serialize<std::string>(
                      records->at("tenant::weight/key")),
                  struct_pack::serialize<std::string>(request));
    }
    {
        ColdWriteJournal journal(root.string());
        ASSERT_TRUE(journal.Init());
        auto records = journal.List();
        ASSERT_TRUE(records);
        ASSERT_EQ(records->size(), 1);
        EXPECT_EQ(records->at("tenant::weight/key").owner, request.owner);
        ASSERT_TRUE(journal.Remove("tenant::weight/key"));
        ASSERT_TRUE(journal.Remove("tenant::weight/key"));
        ++request.reservation;
        ASSERT_TRUE(journal.Put("tenant::weight/key", request));
        ASSERT_TRUE(journal.Put("tenant::different/key", request));
        ASSERT_EQ(journal.List()->size(), 2);
        ASSERT_TRUE(journal.Remove("tenant::weight/key"));
    }
    ColdWriteJournal journal(root.string());
    ASSERT_TRUE(journal.Init());
    ASSERT_EQ(journal.List()->size(), 1);
    EXPECT_EQ(journal.List()->count("tenant::weight/key"), 0);
}

TEST_F(ColdWriteJournalTest, CrashAfterDurablePutAndIncompleteTemporaryFile) {
    auto pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        ColdWriteJournal journal(root.string());
        if (!journal.Init() || !journal.Put("committed", Request())) _exit(2);
        // Abrupt process exit bypasses journal destruction and lock cleanup.
        _exit(0);
    }
    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    {
        std::ofstream partial(root / "cold_write_journal" /
                              "interrupted.token.tmp");
        partial << "partial record before rename";
    }
    ColdWriteJournal journal(root.string());
    ASSERT_TRUE(journal.Init());
    auto records = journal.List();
    ASSERT_TRUE(records);
    ASSERT_EQ(records->size(), 1);
    EXPECT_EQ(records->at("committed").reservation, Request().reservation);
}

TEST_F(ColdWriteJournalTest, CorruptFinalTokenFailsClosedOnReloadAndPut) {
    {
        ColdWriteJournal journal(root.string());
        ASSERT_TRUE(journal.Init());
        ASSERT_TRUE(journal.Put("managed", Request()));
        const auto path = TokenPath();
        ASSERT_FALSE(path.empty());
        std::fstream file(path,
                          std::ios::binary | std::ios::in | std::ios::out);
        char byte = 0;
        file.read(&byte, 1);
        byte ^= 1;
        file.seekp(0);
        file.write(&byte, 1);
        file.close();
        EXPECT_FALSE(journal.List());
        EXPECT_FALSE(journal.Put("managed", Request()));
    }
    ColdWriteJournal restarted(root.string());
    auto init = restarted.Init();
    ASSERT_FALSE(init);
    EXPECT_EQ(init.error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_FALSE(restarted.List());
}

TEST_F(ColdWriteJournalTest, TruncatedOversizedAndMisnamedTokensFailClosed) {
    fs::path path;
    std::string bytes;
    {
        ColdWriteJournal journal(root.string());
        ASSERT_TRUE(journal.Init());
        ASSERT_TRUE(journal.Put("managed", Request()));
        path = TokenPath();
        std::ifstream file(path, std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char>(file), {});
    }
    fs::resize_file(path, bytes.size() / 2);
    {
        ColdWriteJournal journal(root.string());
        EXPECT_FALSE(journal.Init());
    }
    fs::resize_file(path, 2 * 1024 * 1024);
    {
        ColdWriteJournal journal(root.string());
        EXPECT_FALSE(journal.Init());
    }
    {
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        file << bytes;
    }
    fs::rename(path, path.parent_path() / "wrong-name.token");
    ColdWriteJournal journal(root.string());
    EXPECT_FALSE(journal.Init());
}

TEST_F(ColdWriteJournalTest, LiveWriterExclusionAndBoundedRecord) {
    ColdWriteJournal first(root.string());
    ASSERT_TRUE(first.Init());
    ColdWriteJournal second(root.string());
    auto init = second.Init();
    ASSERT_FALSE(init);
    EXPECT_EQ(init.error(), ErrorCode::FILE_LOCK_FAIL);
    EXPECT_FALSE(first.Put("", Request()));
    auto request = Request();
    request.key.assign(2 * 1024 * 1024, 'x');
    EXPECT_FALSE(first.Put("too-large", request));
    EXPECT_TRUE(first.List()->empty());
}

}  // namespace
}  // namespace mooncake::test
