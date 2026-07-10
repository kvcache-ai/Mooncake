#include <gtest/gtest.h>
#include <glog/logging.h>
#include <filesystem>
#include <fstream>

#include "ha/oplog/localfs_oplog_store.h"
#include "ha/oplog/oplog_store_factory.h"

namespace fs = std::filesystem;

namespace mooncake {
namespace {

class LocalFsOpLogStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = "/tmp/localfs_oplog_test_" + std::to_string(::getpid()) +
                    "_" + std::to_string(test_counter_++);
        fs::create_directories(test_dir_);
        cluster_id_ = "test_cluster";
    }

    void TearDown() override { fs::remove_all(test_dir_); }

    std::unique_ptr<LocalFsOpLogStore> CreateWriter() {
        auto store = std::make_unique<LocalFsOpLogStore>(
            cluster_id_, test_dir_, /*enable_batch_write=*/true);
        EXPECT_EQ(ErrorCode::OK, store->Init());
        return store;
    }

    std::unique_ptr<LocalFsOpLogStore> CreateReader() {
        auto store = std::make_unique<LocalFsOpLogStore>(
            cluster_id_, test_dir_, /*enable_batch_write=*/false);
        EXPECT_EQ(ErrorCode::OK, store->Init());
        return store;
    }

    OpLogEntry MakeEntry(uint64_t seq_id, const std::string& key = "test_key") {
        OpLogEntry entry;
        entry.sequence_id = seq_id;
        entry.op_type = OpType::PUT_END;
        entry.object_key = key;
        entry.payload = "payload_" + std::to_string(seq_id);
        return entry;
    }

    std::string test_dir_;
    std::string cluster_id_;
    static inline int test_counter_ = 0;
};

// --- Init tests ---

TEST_F(LocalFsOpLogStoreTest, InitCreatesDirectoryStructure) {
    auto store = CreateWriter();
    EXPECT_TRUE(fs::exists(test_dir_ + "/" + cluster_id_ + "/segments"));
    // snapshots dir is created lazily on first RecordSnapshotSequenceId
    EXPECT_FALSE(fs::exists(test_dir_ + "/" + cluster_id_ + "/snapshots"));
    EXPECT_TRUE(fs::exists(test_dir_ + "/" + cluster_id_ + "/latest"));
}

TEST_F(LocalFsOpLogStoreTest, InitWriterCreatesLatestFile) {
    auto store = CreateWriter();
    std::string latest_path = test_dir_ + "/" + cluster_id_ + "/latest";
    std::ifstream f(latest_path);
    std::string content;
    f >> content;
    EXPECT_EQ("0", content);
}

TEST_F(LocalFsOpLogStoreTest, InitReaderDoesNotCreateLatestFile) {
    // Remove the test dir so Init starts fresh
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    auto store = std::make_unique<LocalFsOpLogStore>(
        cluster_id_, test_dir_, /*enable_batch_write=*/false);
    EXPECT_EQ(ErrorCode::OK, store->Init());
    // Reader should create dirs but NOT create latest file
    EXPECT_TRUE(fs::exists(test_dir_ + "/" + cluster_id_ + "/segments"));
    EXPECT_FALSE(fs::exists(test_dir_ + "/" + cluster_id_ + "/latest"));
}

TEST_F(LocalFsOpLogStoreTest, InitCleansTmpFiles) {
    // Create directory structure manually
    std::string seg_dir = test_dir_ + "/" + cluster_id_ + "/segments";
    fs::create_directories(seg_dir);
    // Create a stale .tmp file
    std::ofstream(seg_dir +
                  "/seg_00000000000000000001_00000000000000000010.tmp")
        << "stale data";
    EXPECT_TRUE(fs::exists(
        seg_dir + "/seg_00000000000000000001_00000000000000000010.tmp"));

    auto store = CreateWriter();
    EXPECT_FALSE(fs::exists(
        seg_dir + "/seg_00000000000000000001_00000000000000000010.tmp"));
}

// --- Write tests ---

TEST_F(LocalFsOpLogStoreTest, WriteSyncEntry) {
    auto store = CreateWriter();
    auto entry = MakeEntry(1);
    EXPECT_EQ(ErrorCode::OK, store->WriteOpLog(entry, /*sync=*/true));

    // Verify it was flushed to a segment file
    auto segments_path = test_dir_ + "/" + cluster_id_ + "/segments";
    int file_count = 0;
    for (auto& p : fs::directory_iterator(segments_path)) {
        if (p.path().extension() != ".tmp") file_count++;
    }
    EXPECT_GE(file_count, 1);
}

TEST_F(LocalFsOpLogStoreTest, WriteAsyncBatchFlush) {
    auto store = CreateWriter();
    // Write several async entries
    for (uint64_t i = 1; i <= 5; i++) {
        EXPECT_EQ(ErrorCode::OK,
                  store->WriteOpLog(MakeEntry(i), /*sync=*/false));
    }
    // Force flush by writing a sync entry
    EXPECT_EQ(ErrorCode::OK, store->WriteOpLog(MakeEntry(6), /*sync=*/true));

    // All entries should be readable
    OpLogEntry read_entry;
    for (uint64_t i = 1; i <= 6; i++) {
        EXPECT_EQ(ErrorCode::OK, store->ReadOpLog(i, read_entry));
        EXPECT_EQ(i, read_entry.sequence_id);
    }
}

TEST_F(LocalFsOpLogStoreTest, WriteReaderReturnsError) {
    auto store = CreateReader();
    auto entry = MakeEntry(1);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, store->WriteOpLog(entry, true));
}

TEST_F(LocalFsOpLogStoreTest, WriteUpdatesLatestFile) {
    auto store = CreateWriter();
    for (uint64_t i = 1; i <= 3; i++) {
        store->WriteOpLog(MakeEntry(i), /*sync=*/false);
    }
    store->WriteOpLog(MakeEntry(4), /*sync=*/true);

    uint64_t latest = 0;
    EXPECT_EQ(ErrorCode::OK, store->GetLatestSequenceId(latest));
    EXPECT_EQ(4, latest);
}

TEST_F(LocalFsOpLogStoreTest, WriteOutOfOrderBatchStillReadableBySequenceId) {
    auto store = CreateWriter();

    EXPECT_EQ(ErrorCode::OK, store->WriteOpLog(MakeEntry(2), /*sync=*/false));
    EXPECT_EQ(ErrorCode::OK, store->WriteOpLog(MakeEntry(1), /*sync=*/true));

    OpLogEntry entry1;
    OpLogEntry entry2;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLog(1, entry1));
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLog(2, entry2));
    EXPECT_EQ(1u, entry1.sequence_id);
    EXPECT_EQ(2u, entry2.sequence_id);

    std::vector<OpLogEntry> entries;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLogSince(0, 10, entries));
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ(1u, entries[0].sequence_id);
    EXPECT_EQ(2u, entries[1].sequence_id);
}

TEST_F(LocalFsOpLogStoreTest,
       RewritePersistedSequenceWithDifferentPayloadFails) {
    auto store = CreateWriter();
    auto entry = MakeEntry(1);
    ASSERT_EQ(ErrorCode::OK, store->WriteOpLog(entry, /*sync=*/true));

    auto conflicting = entry;
    conflicting.payload = "different_payload";
    EXPECT_NE(ErrorCode::OK, store->WriteOpLog(conflicting, /*sync=*/true));

    OpLogEntry persisted;
    ASSERT_EQ(ErrorCode::OK, store->ReadOpLog(1, persisted));
    EXPECT_EQ(entry.payload, persisted.payload);
}

TEST_F(LocalFsOpLogStoreTest,
       RewritePersistedSequenceAfterRestartWithDifferentPayloadFails) {
    {
        auto store = CreateWriter();
        auto entry = MakeEntry(1);
        ASSERT_EQ(ErrorCode::OK, store->WriteOpLog(entry, /*sync=*/true));
    }

    auto reopened = CreateWriter();
    auto conflicting = MakeEntry(1);
    conflicting.payload = "payload_after_restart_conflict";
    EXPECT_NE(ErrorCode::OK, reopened->WriteOpLog(conflicting, /*sync=*/true));

    OpLogEntry persisted;
    ASSERT_EQ(ErrorCode::OK, reopened->ReadOpLog(1, persisted));
    EXPECT_EQ("payload_1", persisted.payload);
}

// --- Read tests ---

TEST_F(LocalFsOpLogStoreTest, ReadOpLogSingleEntry) {
    auto store = CreateWriter();
    store->WriteOpLog(MakeEntry(1), true);

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLog(1, entry));
    EXPECT_EQ(1, entry.sequence_id);
    EXPECT_EQ("test_key", entry.object_key);
}

TEST_F(LocalFsOpLogStoreTest, ReadOpLogNotFound) {
    auto store = CreateWriter();
    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND, store->ReadOpLog(999, entry));
}

TEST_F(LocalFsOpLogStoreTest, ReadOpLogSinceBasic) {
    auto store = CreateWriter();
    for (uint64_t i = 1; i <= 10; i++) {
        store->WriteOpLog(MakeEntry(i), /*sync=*/(i == 10));
    }

    std::vector<OpLogEntry> entries;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLogSince(0, 100, entries));
    EXPECT_EQ(10, entries.size());
    EXPECT_EQ(1, entries.front().sequence_id);
    EXPECT_EQ(10, entries.back().sequence_id);
}

TEST_F(LocalFsOpLogStoreTest, ReadOpLogSinceWithOffset) {
    auto store = CreateWriter();
    for (uint64_t i = 1; i <= 10; i++) {
        store->WriteOpLog(MakeEntry(i), /*sync=*/(i == 10));
    }

    std::vector<OpLogEntry> entries;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLogSince(5, 100, entries));
    EXPECT_EQ(5, entries.size());
    EXPECT_EQ(6, entries.front().sequence_id);
}

TEST_F(LocalFsOpLogStoreTest, ReadOpLogSinceWithLimit) {
    auto store = CreateWriter();
    for (uint64_t i = 1; i <= 10; i++) {
        store->WriteOpLog(MakeEntry(i), /*sync=*/(i == 10));
    }

    std::vector<OpLogEntry> entries;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLogSince(0, 3, entries));
    EXPECT_EQ(3, entries.size());
}

TEST_F(LocalFsOpLogStoreTest, ReadOpLogSinceAcrossSegments) {
    auto store = CreateWriter();
    // Write entries with sync=true to force separate segments
    for (uint64_t i = 1; i <= 3; i++) {
        store->WriteOpLog(MakeEntry(i), /*sync=*/true);
    }

    std::vector<OpLogEntry> entries;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLogSince(0, 100, entries));
    EXPECT_EQ(3, entries.size());
}

TEST_F(LocalFsOpLogStoreTest, GetLatestSequenceIdFirstBoot) {
    auto store = CreateWriter();
    uint64_t seq = 999;
    EXPECT_EQ(ErrorCode::OK, store->GetLatestSequenceId(seq));
    EXPECT_EQ(0, seq);
}

TEST_F(LocalFsOpLogStoreTest, GetMaxSequenceIdEmpty) {
    auto store = CreateWriter();
    uint64_t seq = 0;
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND, store->GetMaxSequenceId(seq));
}

TEST_F(LocalFsOpLogStoreTest, GetMaxSequenceIdAfterWrite) {
    auto store = CreateWriter();
    for (uint64_t i = 1; i <= 5; i++) {
        store->WriteOpLog(MakeEntry(i), /*sync=*/(i == 5));
    }
    uint64_t seq = 0;
    EXPECT_EQ(ErrorCode::OK, store->GetMaxSequenceId(seq));
    EXPECT_EQ(5, seq);
}

// --- Corrupted segment file tests ---

TEST_F(LocalFsOpLogStoreTest, ReadCorruptedSegmentBadMagic) {
    auto store = CreateWriter();
    store->WriteOpLog(MakeEntry(1), /*sync=*/true);

    // Corrupt the segment file by overwriting magic bytes
    auto seg_dir = test_dir_ + "/" + cluster_id_ + "/segments";
    for (auto& p : fs::directory_iterator(seg_dir)) {
        if (p.path().extension() != ".tmp") {
            std::fstream f(p.path(),
                           std::ios::in | std::ios::out | std::ios::binary);
            f.write("XXXX", 4);  // corrupt magic
            f.close();
            break;
        }
    }

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, store->ReadOpLog(1, entry));
}

TEST_F(LocalFsOpLogStoreTest, ReadCorruptedSegmentWrongVersion) {
    auto store = CreateWriter();
    store->WriteOpLog(MakeEntry(1), /*sync=*/true);

    auto seg_dir = test_dir_ + "/" + cluster_id_ + "/segments";
    for (auto& p : fs::directory_iterator(seg_dir)) {
        if (p.path().extension() != ".tmp") {
            std::fstream f(p.path(),
                           std::ios::in | std::ios::out | std::ios::binary);
            f.seekp(4);  // skip magic, write bad version
            uint32_t bad_version = 99;
            f.write(reinterpret_cast<const char*>(&bad_version),
                    sizeof(bad_version));
            f.close();
            break;
        }
    }

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, store->ReadOpLog(1, entry));
}

TEST_F(LocalFsOpLogStoreTest, ReadCorruptedSegmentTruncated) {
    auto store = CreateWriter();
    store->WriteOpLog(MakeEntry(1), /*sync=*/true);

    auto seg_dir = test_dir_ + "/" + cluster_id_ + "/segments";
    for (auto& p : fs::directory_iterator(seg_dir)) {
        if (p.path().extension() != ".tmp") {
            // Truncate to just 16 bytes (less than header size)
            fs::resize_file(p.path(), 16);
            break;
        }
    }

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, store->ReadOpLog(1, entry));
}

// --- Snapshot tests ---

TEST_F(LocalFsOpLogStoreTest, SnapshotRecordAndGet) {
    auto store = CreateWriter();
    EXPECT_EQ(ErrorCode::OK, store->RecordSnapshotSequenceId("snap1", 42));

    uint64_t seq = 0;
    EXPECT_EQ(ErrorCode::OK, store->GetSnapshotSequenceId("snap1", seq));
    EXPECT_EQ(42, seq);
}

TEST_F(LocalFsOpLogStoreTest, SnapshotNotFound) {
    auto store = CreateWriter();
    uint64_t seq = 0;
    EXPECT_NE(ErrorCode::OK, store->GetSnapshotSequenceId("nonexistent", seq));
}

TEST_F(LocalFsOpLogStoreTest, SnapshotIdValidation) {
    auto store = CreateWriter();
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              store->RecordSnapshotSequenceId("../escape", 1));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              store->RecordSnapshotSequenceId("path/slash", 1));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              store->RecordSnapshotSequenceId(std::string("null\0byte", 9), 1));
}

// --- Cleanup tests ---

TEST_F(LocalFsOpLogStoreTest, CleanupRemovesOldSegments) {
    auto store = CreateWriter();
    // Write entries in separate segments (sync=true each time)
    for (uint64_t i = 1; i <= 5; i++) {
        store->WriteOpLog(MakeEntry(i), /*sync=*/true);
    }

    // Count segments before cleanup
    auto seg_dir = test_dir_ + "/" + cluster_id_ + "/segments";
    int before_count = 0;
    for (auto& p : fs::directory_iterator(seg_dir)) {
        (void)p;
        before_count++;
    }
    EXPECT_EQ(5, before_count);

    // Cleanup entries before seq 4 (should remove segments with max_seq < 4)
    EXPECT_EQ(ErrorCode::OK, store->CleanupOpLogBefore(4));

    // Entries 4 and 5 should still be readable
    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLog(4, entry));
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLog(5, entry));
}

TEST_F(LocalFsOpLogStoreTest, CleanupEmptyDir) {
    auto store = CreateWriter();
    EXPECT_EQ(ErrorCode::OK, store->CleanupOpLogBefore(100));
}

// --- ChangeNotifier tests ---

TEST_F(LocalFsOpLogStoreTest, ChangeNotifierBasic) {
    auto writer = CreateWriter();
    auto reader = CreateReader();

    auto notifier = reader->CreateChangeNotifier(cluster_id_);
    ASSERT_NE(nullptr, notifier);

    std::vector<OpLogEntry> received;
    std::mutex received_mutex;
    std::condition_variable received_cv;

    auto on_entry = [&](const OpLogEntry& entry) {
        std::lock_guard<std::mutex> lock(received_mutex);
        received.push_back(entry);
        received_cv.notify_one();
    };
    auto on_error = [](ErrorCode) {};

    EXPECT_EQ(ErrorCode::OK, notifier->Start(0, on_entry, on_error));
    EXPECT_TRUE(notifier->IsHealthy());

    // Write an entry from writer
    writer->WriteOpLog(MakeEntry(1), /*sync=*/true);

    // Wait for notifier to deliver it (with timeout)
    {
        std::unique_lock<std::mutex> lock(received_mutex);
        EXPECT_TRUE(received_cv.wait_for(lock, std::chrono::seconds(5),
                                         [&] { return received.size() >= 1; }));
    }
    EXPECT_EQ(1, received.size());
    EXPECT_EQ(1, received[0].sequence_id);

    notifier->Stop();
}

TEST_F(LocalFsOpLogStoreTest, ChangeNotifierCatchUp) {
    auto writer = CreateWriter();
    // Write entries BEFORE starting notifier
    for (uint64_t i = 1; i <= 5; i++) {
        writer->WriteOpLog(MakeEntry(i), /*sync=*/(i == 5));
    }

    auto reader = CreateReader();
    auto notifier = reader->CreateChangeNotifier(cluster_id_);
    ASSERT_NE(nullptr, notifier);

    std::vector<OpLogEntry> received;
    std::mutex received_mutex;
    std::condition_variable received_cv;

    auto on_entry = [&](const OpLogEntry& entry) {
        std::lock_guard<std::mutex> lock(received_mutex);
        received.push_back(entry);
        received_cv.notify_one();
    };
    auto on_error = [](ErrorCode) {};

    // Start from 0 should catch up with existing entries
    EXPECT_EQ(ErrorCode::OK, notifier->Start(0, on_entry, on_error));

    {
        std::unique_lock<std::mutex> lock(received_mutex);
        EXPECT_TRUE(received_cv.wait_for(lock, std::chrono::seconds(5),
                                         [&] { return received.size() >= 5; }));
    }
    EXPECT_EQ(5, received.size());
    EXPECT_EQ(1, received[0].sequence_id);
    EXPECT_EQ(5, received[4].sequence_id);

    notifier->Stop();
}

TEST_F(LocalFsOpLogStoreTest, ChangeNotifierStopNoMoreCallbacks) {
    auto writer = CreateWriter();
    auto reader = CreateReader();

    auto notifier = reader->CreateChangeNotifier(cluster_id_);
    ASSERT_NE(nullptr, notifier);

    std::atomic<int> callback_count{0};
    auto on_entry = [&](const OpLogEntry&) { callback_count++; };
    auto on_error = [](ErrorCode) {};

    EXPECT_EQ(ErrorCode::OK, notifier->Start(0, on_entry, on_error));
    notifier->Stop();

    // Write after stop - should not trigger callback
    writer->WriteOpLog(MakeEntry(1), /*sync=*/true);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(0, callback_count.load());
}

// --- Factory tests ---

TEST_F(LocalFsOpLogStoreTest, FactoryCreateWriter) {
    auto store =
        OpLogStoreFactory::Create(OpLogStoreType::LOCAL_FS, cluster_id_,
                                  OpLogStoreRole::WRITER, test_dir_, 100);
    ASSERT_NE(nullptr, store);

    // Should be able to write
    OpLogEntry entry;
    entry.sequence_id = 1;
    entry.op_type = OpType::PUT_END;
    entry.object_key = "factory_test";
    entry.payload = "data";
    EXPECT_EQ(ErrorCode::OK, store->WriteOpLog(entry, /*sync=*/true));

    // Should be able to read back
    OpLogEntry read_entry;
    EXPECT_EQ(ErrorCode::OK, store->ReadOpLog(1, read_entry));
    EXPECT_EQ(1, read_entry.sequence_id);
}

TEST_F(LocalFsOpLogStoreTest, FactoryCreateReader) {
    // First create some data with a writer
    {
        auto writer =
            OpLogStoreFactory::Create(OpLogStoreType::LOCAL_FS, cluster_id_,
                                      OpLogStoreRole::WRITER, test_dir_, 100);
        ASSERT_NE(nullptr, writer);
        OpLogEntry entry;
        entry.sequence_id = 1;
        entry.op_type = OpType::PUT_END;
        entry.object_key = "key";
        entry.payload = "data";
        writer->WriteOpLog(entry, /*sync=*/true);
    }

    // Reader should be able to read but not write
    auto reader =
        OpLogStoreFactory::Create(OpLogStoreType::LOCAL_FS, cluster_id_,
                                  OpLogStoreRole::READER, test_dir_, 100);
    ASSERT_NE(nullptr, reader);

    OpLogEntry read_entry;
    EXPECT_EQ(ErrorCode::OK, reader->ReadOpLog(1, read_entry));
    EXPECT_EQ(1, read_entry.sequence_id);

    // Writer should fail on reader
    OpLogEntry new_entry;
    new_entry.sequence_id = 2;
    new_entry.op_type = OpType::PUT_END;
    new_entry.object_key = "key2";
    new_entry.payload = "data2";
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              reader->WriteOpLog(new_entry, /*sync=*/true));
}

}  // namespace
}  // namespace mooncake
