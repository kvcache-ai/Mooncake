#include "storage/local/log_structured/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::logstructured {
namespace {

class WalTempDirectory {
   public:
    WalTempDirectory() {
        const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake-log-wal-test-" + std::to_string(getpid()) + "-" +
                 std::to_string(id));
        std::filesystem::create_directories(path_);
    }

    ~WalTempDirectory() { std::filesystem::remove_all(path_); }

    std::filesystem::path File(std::string_view name) const {
        return path_ / name;
    }

   private:
    inline static std::atomic<uint64_t> next_id_{0};
    std::filesystem::path path_;
};

RecordIdentity WalIdentity(std::string key, uint64_t incarnation) {
    return RecordIdentity{
        .tenant_id = "tenant-a",
        .object_key = std::move(key),
        .incarnation = ObjectIncarnation{.high = 4, .low = incarnation},
    };
}

PhysicalRecord WalPhysical(uint64_t segment, uint64_t offset) {
    return PhysicalRecord{.segment_id = segment,
                          .record_offset = offset,
                          .value_offset = offset + 80,
                          .value_length = 64,
                          .total_length = 160};
}

WalRecord Transition(WalRecordType type, const RecordIdentity& identity,
                     uint64_t sequence, PhysicalRecord physical = {}) {
    return WalRecord{.type = type,
                     .sequence = sequence,
                     .identity = identity,
                     .physical = physical};
}

TEST(LogStructuredWalTest, AppendsScansAndReplaysTransitions) {
    WalTempDirectory temp;
    const auto path = temp.File("WAL-000001");
    auto writer = WalWriter::Create(path.string());
    ASSERT_TRUE(writer.has_value());

    const auto identity = WalIdentity("key", 1);
    ASSERT_TRUE((*writer)
                    ->Append(Transition(WalRecordType::kPrepareValue, identity,
                                        10, WalPhysical(1, 0)),
                             false)
                    .has_value());
    ASSERT_TRUE(
        (*writer)
            ->Append(Transition(WalRecordType::kCommitValue, identity, 10),
                     true)
            .has_value());
    writer.value().reset();

    auto scan = ScanWal(path.string());
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, WalScanTermination::kCleanEof);
    ASSERT_EQ(scan->records.size(), size_t{2});

    VersionIndex index;
    ASSERT_TRUE(ReplayWal(scan->records, index).has_value());
    auto committed = index.LookupCommitted(identity);
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ(committed->physical, WalPhysical(1, 0));
}

TEST(LogStructuredWalTest, RejectsNonzeroReservedHeaderField) {
    WalTempDirectory temp;
    const auto path = temp.File("WAL-reserved");
    auto writer = WalWriter::Create(path.string());
    ASSERT_TRUE(writer.has_value());
    ASSERT_TRUE(
        (*writer)
            ->Append(Transition(WalRecordType::kPrepareValue,
                                WalIdentity("key", 1), 1, WalPhysical(1, 0)),
                     true)
            .has_value());
    writer.value().reset();

    const int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    const char reserved = 1;
    ASSERT_EQ(pwrite(fd, &reserved, 1, 92), 1);
    ASSERT_EQ(close(fd), 0);

    auto scan = ScanWal(path.string());
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, WalScanTermination::kCorruptRecord);
    EXPECT_EQ(scan->valid_bytes, uint64_t{0});
}

TEST(LogStructuredWalTest, IncompleteTailIsTruncatedBeforeAppend) {
    WalTempDirectory temp;
    const auto path = temp.File("WAL-000002");
    auto writer = WalWriter::Create(path.string());
    ASSERT_TRUE(writer.has_value());
    const auto first_identity = WalIdentity("first", 1);
    ASSERT_TRUE((*writer)
                    ->Append(Transition(WalRecordType::kPrepareValue,
                                        first_identity, 1, WalPhysical(1, 0)),
                             true)
                    .has_value());
    const uint64_t valid_bytes = (*writer)->tail();
    writer.value().reset();

    {
        std::ofstream output(path, std::ios::binary | std::ios::app);
        ASSERT_TRUE(output.good());
        output << "torn-wal-record";
    }
    auto torn = ScanWal(path.string());
    ASSERT_TRUE(torn.has_value());
    EXPECT_EQ(torn->termination, WalScanTermination::kIncompleteTail);
    EXPECT_EQ(torn->valid_bytes, valid_bytes);

    auto reopened = WalWriter::OpenForAppend(path.string(), torn->valid_bytes);
    ASSERT_TRUE(reopened.has_value());
    ASSERT_TRUE(
        (*reopened)
            ->Append(Transition(WalRecordType::kAbortValue, first_identity, 1),
                     true)
            .has_value());
    reopened.value().reset();

    auto repaired = ScanWal(path.string());
    ASSERT_TRUE(repaired.has_value());
    EXPECT_EQ(repaired->termination, WalScanTermination::kCleanEof);
    ASSERT_EQ(repaired->records.size(), size_t{2});
    VersionIndex index;
    ASSERT_TRUE(ReplayWal(repaired->records, index).has_value());
    EXPECT_EQ(index.Lookup(first_identity)->state, VersionState::kAborted);
}

TEST(LogStructuredWalTest, CorruptionStopsAtLastValidTransition) {
    WalTempDirectory temp;
    const auto path = temp.File("WAL-000003");
    auto writer = WalWriter::Create(path.string());
    ASSERT_TRUE(writer.has_value());
    const auto identity = WalIdentity("key", 1);
    ASSERT_TRUE((*writer)
                    ->Append(Transition(WalRecordType::kPrepareValue, identity,
                                        1, WalPhysical(1, 0)),
                             false)
                    .has_value());
    const uint64_t second_offset = (*writer)->tail();
    ASSERT_TRUE(
        (*writer)
            ->Append(Transition(WalRecordType::kCommitValue, identity, 1), true)
            .has_value());
    writer.value().reset();

    const int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    char byte = 0;
    ASSERT_EQ(pread(fd, &byte, 1, second_offset), 1);
    byte ^= 0x01;
    ASSERT_EQ(pwrite(fd, &byte, 1, second_offset), 1);
    ASSERT_EQ(close(fd), 0);

    auto scan = ScanWal(path.string());
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, WalScanTermination::kCorruptRecord);
    EXPECT_EQ(scan->valid_bytes, second_offset);
    ASSERT_EQ(scan->records.size(), size_t{1});
}

TEST(LogStructuredWalTest, ReplayedAbortPreservesPreviousIncarnation) {
    const auto old_identity = WalIdentity("key", 1);
    const auto new_identity = WalIdentity("key", 2);
    std::vector<WalRecord> records = {
        Transition(WalRecordType::kPrepareValue, old_identity, 1,
                   WalPhysical(1, 0)),
        Transition(WalRecordType::kCommitValue, old_identity, 1),
        Transition(WalRecordType::kPrepareValue, new_identity, 2,
                   WalPhysical(1, 160)),
        Transition(WalRecordType::kAbortValue, new_identity, 2),
    };

    VersionIndex index;
    ASSERT_TRUE(ReplayWal(records, index).has_value());
    EXPECT_TRUE(index.LookupCommitted(old_identity).has_value());
    EXPECT_FALSE(index.LookupCommitted(new_identity).has_value());
}

TEST(LogStructuredWalTest, AppendsBatchWithSingleDurabilityBoundary) {
    WalTempDirectory temp;
    const auto path = temp.File("WAL-batch");
    auto writer = WalWriter::Create(path.string());
    ASSERT_TRUE(writer.has_value());

    const auto first = WalIdentity("first", 1);
    const auto second = WalIdentity("second", 2);
    std::vector<WalRecord> records = {
        Transition(WalRecordType::kPrepareValue, first, 1, WalPhysical(1, 0)),
        Transition(WalRecordType::kPrepareValue, second, 2,
                   WalPhysical(1, 160)),
    };
    ASSERT_TRUE((*writer)->AppendBatch(records, true).has_value());
    writer.value().reset();

    auto scan = ScanWal(path.string());
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, WalScanTermination::kCleanEof);
    EXPECT_EQ(scan->records, records);
}

TEST(LogStructuredWalTest, GroupsConcurrentDurableAppends) {
    WalTempDirectory temp;
    const auto path = temp.File("WAL-group-commit");
    auto writer = WalWriter::Create(path.string());
    ASSERT_TRUE(writer.has_value());

    std::mutex hook_mutex;
    std::condition_variable hook_cv;
    bool first_group_blocked = false;
    bool release_first_group = false;
    std::atomic<size_t> hook_calls{0};
    WalWriter::SetBeforeWriteHookForTest([&] {
        if (hook_calls.fetch_add(1, std::memory_order_relaxed) != 0) return;
        std::unique_lock lock(hook_mutex);
        first_group_blocked = true;
        hook_cv.notify_all();
        hook_cv.wait(lock, [&] { return release_first_group; });
    });

    auto first = std::async(std::launch::async, [&] {
        const auto identity = WalIdentity("first", 1);
        return (*writer)->Append(Transition(WalRecordType::kPrepareValue,
                                            identity, 1, WalPhysical(1, 0)),
                                 true);
    });
    bool hook_entered = false;
    {
        std::unique_lock lock(hook_mutex);
        hook_entered = hook_cv.wait_for(lock, std::chrono::seconds(2),
                                        [&] { return first_group_blocked; });
    }
    EXPECT_TRUE(hook_entered);

    constexpr size_t kFollowers = 16;
    std::vector<std::future<tl::expected<void, WalError>>> followers;
    followers.reserve(kFollowers);
    for (size_t index = 0; index < kFollowers; ++index) {
        followers.push_back(std::async(std::launch::async, [&, index] {
            const auto identity =
                WalIdentity("follower-" + std::to_string(index), index + 2);
            return (*writer)->Append(
                Transition(WalRecordType::kPrepareValue, identity, index + 2,
                           WalPhysical(1, (index + 1) * 160)),
                true);
        }));
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((*writer)->SnapshotStats().append_requests < kFollowers + 1 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    const bool all_queued =
        (*writer)->SnapshotStats().append_requests == kFollowers + 1;
    {
        std::lock_guard lock(hook_mutex);
        release_first_group = true;
    }
    hook_cv.notify_all();

    EXPECT_TRUE(first.get().has_value());
    for (auto& follower : followers) {
        EXPECT_TRUE(follower.get().has_value());
    }
    WalWriter::SetBeforeWriteHookForTest({});

    ASSERT_TRUE(all_queued);
    const auto stats = (*writer)->SnapshotStats();
    EXPECT_EQ(stats.append_requests, kFollowers + 1);
    EXPECT_EQ(stats.write_groups, uint64_t{2});
    EXPECT_EQ(stats.sync_groups, uint64_t{2});
    EXPECT_EQ(stats.max_group_requests, kFollowers);
    writer.value().reset();

    auto scan = ScanWal(path.string());
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->termination, WalScanTermination::kCleanEof);
    EXPECT_EQ(scan->records.size(), kFollowers + 1);
}

}  // namespace
}  // namespace mooncake::logstructured
