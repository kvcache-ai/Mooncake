#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ha/oplog/oplog_codec.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store_factory.h"
#include "types.h"

namespace fs = std::filesystem;

namespace mooncake::test {
namespace {

std::string MakeTempDir() {
    return "/tmp/localfs-oplog-store-test-" + UuidToString(generate_uuid());
}

std::string MakeClusterNamespace() {
    return "localfs-oplog-cluster-" + UuidToString(generate_uuid());
}

size_t CountSegmentFiles(const std::string& root_dir,
                         const std::string& cluster_namespace) {
    const auto segments_dir =
        fs::path(root_dir) / cluster_namespace / "segments";
    if (!fs::exists(segments_dir)) {
        return 0;
    }

    size_t count = 0;
    for (const auto& entry : fs::directory_iterator(segments_dir)) {
        if (entry.is_regular_file()) {
            ++count;
        }
    }
    return count;
}

OpLogEntry MakeEntry(OpLogManager& manager, OpType op_type,
                     const std::string& key, const std::string& payload) {
    return manager.AllocateEntry(op_type, key, payload);
}

}  // namespace

class LocalFsOpLogStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        temp_dir_ = MakeTempDir();
        cluster_namespace_ = MakeClusterNamespace();
        fs::create_directories(temp_dir_);
    }

    void TearDown() override {
        std::error_code error;
        fs::remove_all(temp_dir_, error);
    }

    std::shared_ptr<ha::OpLogStore> CreateWriter() const {
        auto store = ha::CreateOpLogStore(
            ha::HABackendSpec{
                .type = ha::HABackendType::LOCALFS,
                .connstring = temp_dir_,
                .cluster_namespace = cluster_namespace_,
            },
            ha::OpLogStoreFactoryOptions{
                .enable_batch_write = true,
            });
        EXPECT_TRUE(store.has_value()) << toString(store.error());
        return store ? store.value() : nullptr;
    }

    std::shared_ptr<ha::OpLogStore> CreateReader() const {
        auto store = ha::CreateOpLogStore(ha::HABackendSpec{
            .type = ha::HABackendType::LOCALFS,
            .connstring = temp_dir_,
            .cluster_namespace = cluster_namespace_,
        });
        EXPECT_TRUE(store.has_value()) << toString(store.error());
        return store ? store.value() : nullptr;
    }

    std::string temp_dir_;
    std::string cluster_namespace_;
};

TEST_F(LocalFsOpLogStoreTest, AppendPollAndResolveLatest) {
    auto writer = CreateWriter();
    auto reader = CreateReader();
    ASSERT_NE(writer, nullptr);
    ASSERT_NE(reader, nullptr);

    OpLogManager manager;
    const auto first = MakeEntry(manager, OpType::PUT_END, "key-1", "value-1");
    const auto second = MakeEntry(manager, OpType::REMOVE, "key-2", "");

    auto append_first = writer->Append(ha::oplog::BuildAppendRequest(first));
    ASSERT_TRUE(append_first.has_value());
    auto append_second = writer->Append(ha::oplog::BuildAppendRequest(second));
    ASSERT_TRUE(append_second.has_value());

    auto latest = reader->GetLatestSequence();
    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(second.sequence_id, latest.value());

    auto poll = reader->PollFrom(0, 10, std::chrono::milliseconds(0));
    ASSERT_TRUE(poll.has_value());
    ASSERT_EQ(2u, poll->records.size());
    EXPECT_EQ(first.sequence_id, poll->records[0].seq);
    EXPECT_EQ(second.sequence_id, poll->records[1].seq);
    EXPECT_FALSE(poll->timed_out);

    auto decoded = ha::oplog::DeserializeEntryPayload(poll->records[0].payload);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(first.object_key, decoded->object_key);
    EXPECT_EQ(first.payload, decoded->payload);
}

TEST_F(LocalFsOpLogStoreTest, AppendIsIdempotentForSamePayload) {
    auto writer = CreateWriter();
    ASSERT_NE(writer, nullptr);

    OpLogManager manager;
    const auto entry =
        MakeEntry(manager, OpType::PUT_END, "idempotent-key", "payload-1");
    const auto request = ha::oplog::BuildAppendRequest(entry);

    auto first = writer->Append(request);
    ASSERT_TRUE(first.has_value());
    auto second = writer->Append(request);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first.value(), second.value());

    auto latest = writer->GetLatestSequence();
    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(entry.sequence_id, latest.value());
}

TEST_F(LocalFsOpLogStoreTest, AppendRejectsConflictingRetry) {
    auto writer = CreateWriter();
    ASSERT_NE(writer, nullptr);

    OpLogManager manager;
    const auto entry =
        MakeEntry(manager, OpType::PUT_END, "conflict-key", "payload-1");
    auto request = ha::oplog::BuildAppendRequest(entry);
    ASSERT_TRUE(writer->Append(request).has_value());

    OpLogEntry conflicting = entry;
    conflicting.payload = "payload-2";
    request.payload = ha::oplog::SerializeEntryPayload(conflicting);

    auto result = writer->Append(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(ErrorCode::PERSISTENT_FAIL, result.error());
}

TEST_F(LocalFsOpLogStoreTest, CleanupBeforeDeletesSafePrefixOnly) {
    auto writer = CreateWriter();
    auto reader = CreateReader();
    ASSERT_NE(writer, nullptr);
    ASSERT_NE(reader, nullptr);

    OpLogManager manager;
    for (int index = 0; index < 5; ++index) {
        auto entry =
            MakeEntry(manager, OpType::PUT_END, "key-" + std::to_string(index),
                      "payload-" + std::to_string(index));
        ASSERT_TRUE(
            writer->Append(ha::oplog::BuildAppendRequest(entry)).has_value());
    }

    EXPECT_EQ(ErrorCode::OK, writer->CleanupBefore(4));

    auto poll = reader->PollFrom(0, 10, std::chrono::milliseconds(0));
    ASSERT_TRUE(poll.has_value());
    ASSERT_EQ(2u, poll->records.size());
    EXPECT_EQ(4u, poll->records[0].seq);
    EXPECT_EQ(5u, poll->records[1].seq);
}

TEST_F(LocalFsOpLogStoreTest, PollFromWaitsForLaterRecords) {
    auto writer = CreateWriter();
    auto reader = CreateReader();
    ASSERT_NE(writer, nullptr);
    ASSERT_NE(reader, nullptr);

    OpLogManager manager;
    const auto entry =
        MakeEntry(manager, OpType::PUT_END, "wait-key", "wait-payload");
    bool append_ok = false;

    std::thread writer_thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        auto result = writer->Append(ha::oplog::BuildAppendRequest(entry));
        append_ok = result.has_value();
    });

    auto poll = reader->PollFrom(0, 10, std::chrono::milliseconds(500));
    writer_thread.join();

    ASSERT_TRUE(append_ok);
    ASSERT_TRUE(poll.has_value());
    ASSERT_EQ(1u, poll->records.size());
    EXPECT_EQ(entry.sequence_id, poll->records.front().seq);
    EXPECT_FALSE(poll->timed_out);
}

TEST_F(LocalFsOpLogStoreTest, BurstAppendCoalescesSegments) {
    auto writer = CreateWriter();
    auto reader = CreateReader();
    ASSERT_NE(writer, nullptr);
    ASSERT_NE(reader, nullptr);

    constexpr size_t kThreadCount = 8;
    constexpr size_t kEntryCount = 128;

    OpLogManager manager;
    std::vector<OpLogEntry> entries;
    entries.reserve(kEntryCount);
    for (size_t index = 0; index < kEntryCount; ++index) {
        entries.push_back(MakeEntry(manager, OpType::PUT_END,
                                    "burst-key-" + std::to_string(index),
                                    "burst-payload-" + std::to_string(index)));
    }

    std::atomic<size_t> next_index{0};
    std::atomic<size_t> success_count{0};
    std::atomic<size_t> failure_count{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(kThreadCount);
    for (size_t worker_id = 0; worker_id < kThreadCount; ++worker_id) {
        workers.emplace_back([&]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (true) {
                const size_t index =
                    next_index.fetch_add(1, std::memory_order_acq_rel);
                if (index >= entries.size()) {
                    return;
                }
                auto result = writer->Append(
                    ha::oplog::BuildAppendRequest(entries[index]));
                if (!result.has_value()) {
                    failure_count.fetch_add(1, std::memory_order_acq_rel);
                    return;
                }
                success_count.fetch_add(1, std::memory_order_acq_rel);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }

    EXPECT_EQ(kEntryCount, success_count.load(std::memory_order_acquire));
    EXPECT_EQ(0u, failure_count.load(std::memory_order_acquire));

    auto latest = reader->GetLatestSequence();
    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(entries.back().sequence_id, latest.value());

    auto poll =
        reader->PollFrom(0, kEntryCount + 1, std::chrono::milliseconds(0));
    ASSERT_TRUE(poll.has_value());
    EXPECT_EQ(kEntryCount, poll->records.size());

    const auto segment_file_count =
        CountSegmentFiles(temp_dir_, cluster_namespace_);
    EXPECT_LT(segment_file_count, kEntryCount);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return result;
}
