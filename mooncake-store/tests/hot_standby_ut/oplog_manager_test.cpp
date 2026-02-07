#include "oplog_manager.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::test {

class OpLogManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogManagerTest");
        FLAGS_logtostderr = 1;
        manager_ = std::make_unique<OpLogManager>();
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    OpLogManager& M() { return *manager_; }

    std::unique_ptr<OpLogManager> manager_;
};

// ========== 2.1.1 Basic functionality tests ==========

TEST_F(OpLogManagerTest, TestAppendEntry) {
    uint64_t id = M().Append(OpType::PUT_END, "key1", "value1");

    EXPECT_LT(0u, id);
    EXPECT_EQ(id, M().GetLastSequenceId());
    EXPECT_EQ(1u, M().GetEntryCount());
}

TEST_F(OpLogManagerTest, TestSequenceIdIncrement) {
    uint64_t id1 = M().Append(OpType::PUT_END, "key1", "value1");
    uint64_t id2 = M().Append(OpType::PUT_END, "key2", "value2");
    uint64_t id3 = M().Append(OpType::REMOVE, "key3", "");

    EXPECT_LT(0u, id1);
    EXPECT_EQ(id1 + 1, id2);
    EXPECT_EQ(id2 + 1, id3);
    EXPECT_EQ(id3, M().GetLastSequenceId());
    EXPECT_EQ(3u, M().GetEntryCount());
}

TEST_F(OpLogManagerTest, TestAllocateEntry) {
    OpLogEntry e1 = M().AllocateEntry(OpType::PUT_END, "key1", "value1");
    OpLogEntry e2 = M().AllocateEntry(OpType::PUT_END, "key2", "value2");

    EXPECT_LT(0u, e1.sequence_id);
    EXPECT_EQ(e1.sequence_id + 1, e2.sequence_id);
    EXPECT_EQ(e2.sequence_id, M().GetLastSequenceId());
    EXPECT_EQ(2u, M().GetEntryCount());

    // Basic field validation
    EXPECT_EQ(OpType::PUT_END, e1.op_type);
    EXPECT_EQ("key1", e1.object_key);
    EXPECT_EQ("value1", e1.payload);
    EXPECT_NE(0u, e1.timestamp_ms);
    EXPECT_NE(0u, e1.checksum);
    EXPECT_NE(0u, e1.prefix_hash);
}

TEST_F(OpLogManagerTest, TestPersistEntryToEtcd) {
    // Without an EtcdOpLogStore configured, PersistEntryToEtcd should return an
    // error
    OpLogEntry entry =
        M().AllocateEntry(OpType::PUT_END, "key", "payload-data");

    ErrorCode err = M().PersistEntryToEtcd(entry);
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, err);
}

TEST_F(OpLogManagerTest, TestAppendAndPersist) {
    // Without an EtcdOpLogStore configured, AppendAndPersist should return an
    // error
    auto res = M().AppendAndPersist(OpType::REMOVE, "key", "");
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, res.error());
}

// ========== Initial Sequence Id Tests ==========

TEST_F(OpLogManagerTest, SetInitialSequenceIdOnEmptyManager) {
    EXPECT_EQ(0u, M().GetLastSequenceId());
    EXPECT_EQ(0u, M().GetEntryCount());

    M().SetInitialSequenceId(100);
    EXPECT_EQ(100u, M().GetLastSequenceId());

    // The first appended entry should have sequence_id 101
    uint64_t id = M().Append(OpType::PUT_END, "key", "value");
    EXPECT_EQ(101u, id);
}

TEST_F(OpLogManagerTest, SetInitialSequenceIdIgnoredWhenNotEmpty) {
    uint64_t id1 = M().Append(OpType::PUT_END, "key1", "value1");
    EXPECT_EQ(id1, M().GetLastSequenceId());

    // Setting initial sequence id on a non-empty manager should be ignored
    M().SetInitialSequenceId(500);
    EXPECT_EQ(id1, M().GetLastSequenceId());
}

// ========== 2.1.2 Checksum tests ==========

TEST_F(OpLogManagerTest, TestChecksumComputation) {
    // Same payload => same checksum; different payload => different checksum
    OpLogEntry e1 = M().AllocateEntry(OpType::PUT_END, "k1", "payload-X");
    OpLogEntry e2 = M().AllocateEntry(OpType::PUT_END, "k2", "payload-X");
    OpLogEntry e3 = M().AllocateEntry(OpType::PUT_END, "k3", "payload-Y");

    EXPECT_EQ(e1.checksum, e2.checksum);
    EXPECT_NE(e1.checksum, e3.checksum);
}

TEST_F(OpLogManagerTest, TestPrefixHashComputation) {
    // Same key => same prefix_hash; different key => (with high probability)
    // different prefix_hash
    OpLogEntry e1 = M().AllocateEntry(OpType::PUT_END, "same-key", "v1");
    OpLogEntry e2 = M().AllocateEntry(OpType::PUT_END, "same-key", "v2");
    OpLogEntry e3 = M().AllocateEntry(OpType::PUT_END, "other-key", "v3");

    EXPECT_EQ(e1.prefix_hash, e2.prefix_hash);
    EXPECT_NE(e1.prefix_hash, e3.prefix_hash);
}

TEST_F(OpLogManagerTest, TestVerifyChecksum) {
    OpLogEntry entry =
        M().AllocateEntry(OpType::PUT_END, "key", "payload-data");
    EXPECT_TRUE(OpLogManager::VerifyChecksum(entry));

    // Verification should fail after tampering with the payload
    entry.payload = "tampered";
    EXPECT_FALSE(OpLogManager::VerifyChecksum(entry));
}

// ========== 2.1.3 Size validation tests ==========

TEST_F(OpLogManagerTest, TestValidateEntrySize_Valid) {
    OpLogEntry entry;
    entry.object_key = "normal-key";
    entry.payload = "small-payload";

    std::string reason;
    EXPECT_TRUE(OpLogManager::ValidateEntrySize(entry, &reason));
    EXPECT_TRUE(reason.empty());
}

TEST_F(OpLogManagerTest, TestValidateEntrySize_KeyTooLarge) {
    OpLogEntry entry;
    entry.object_key.assign(OpLogManager::kMaxObjectKeySize + 1, 'k');
    entry.payload = "payload";

    std::string reason;
    EXPECT_FALSE(OpLogManager::ValidateEntrySize(entry, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST_F(OpLogManagerTest, TestValidateEntrySize_PayloadTooLarge) {
    OpLogEntry entry;
    entry.object_key = "key";
    entry.payload.assign(OpLogManager::kMaxPayloadSize + 1, 'p');

    std::string reason;
    EXPECT_FALSE(OpLogManager::ValidateEntrySize(entry, &reason));
    EXPECT_FALSE(reason.empty());
}

TEST_F(OpLogManagerTest, TestValidateEntrySize_EmptyKey) {
    // Current implementation only enforces upper bounds; empty keys are
    // accepted
    OpLogEntry entry;
    entry.object_key = "";
    entry.payload = "payload";

    std::string reason;
    EXPECT_TRUE(OpLogManager::ValidateEntrySize(entry, &reason));
}

// ========== 2.1.4 Etcd integration tests (placeholder) ==========

TEST_F(OpLogManagerTest, TestWriteToEtcd_Success) {
#if defined(STORE_USE_ETCD)
    GTEST_SKIP()
        << "TODO: requires real EtcdOpLogStore and running etcd cluster.";
#else
    GTEST_SKIP() << "STORE_USE_ETCD is disabled.";
#endif
}

TEST_F(OpLogManagerTest, TestWriteToEtcd_Failure) {
    OpLogEntry entry =
        M().AllocateEntry(OpType::PUT_END, "key", "payload-data");
    ErrorCode err = M().PersistEntryToEtcd(entry);
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, err);
}

TEST_F(OpLogManagerTest, TestWriteToEtcd_Retry) {
    GTEST_SKIP() << "TODO: retry / idempotent semantics are tested at "
                    "EtcdOpLogStore level.";
}

TEST_F(OpLogManagerTest, TestIdempotentWrite) {
    GTEST_SKIP() << "TODO: idempotent write belongs to "
                    "EtcdOpLogStore::WriteOpLog tests.";
}

// ========== 2.1.5 Boundary condition tests ==========

TEST_F(OpLogManagerTest, TestSequenceIdWrapAround) {
    // Theoretical wrap-around test: set initial value near UINT64_MAX and
    // verify wrap-around semantics
    uint64_t near_max = std::numeric_limits<uint64_t>::max() - 2;
    M().SetInitialSequenceId(near_max);

    std::vector<uint64_t> ids;
    ids.push_back(M().Append(OpType::PUT_END, "k1", "v1"));  // max-1
    ids.push_back(M().Append(OpType::PUT_END, "k2", "v2"));  // max
    ids.push_back(M().Append(OpType::PUT_END, "k3", "v3"));  // 0 (wrap)

    ASSERT_EQ(3u, ids.size());
    // Use wrap-around-safe comparison helpers to verify monotonic increase
    EXPECT_TRUE(IsSequenceNewer(ids[1], ids[0]));
    EXPECT_TRUE(IsSequenceNewer(
        ids[2], ids[1]));  // 0 is considered newer than UINT64_MAX
}

TEST_F(OpLogManagerTest, TestConcurrentAppend) {
    constexpr int kThreads = 8;
    constexpr int kPerThread = 1000;

    std::vector<uint64_t> ids;
    ids.reserve(kThreads * kPerThread);
    std::mutex m;

    auto worker = [&]() {
        for (int i = 0; i < kPerThread; ++i) {
            uint64_t id = M().Append(OpType::PUT_END, "key", "value");
            std::lock_guard<std::mutex> lock(m);
            ids.push_back(id);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(static_cast<size_t>(kThreads * kPerThread), ids.size());

    std::sort(ids.begin(), ids.end());
    // Ensure there are no duplicates and sequence IDs are strictly increasing
    for (size_t i = 1; i < ids.size(); ++i) {
        EXPECT_GT(ids[i], ids[i - 1]);
    }
}

TEST_F(OpLogManagerTest, TestLargePayload) {
    // Construct a payload close to the upper limit and verify it passes
    // validation and appends successfully
    std::string key = "large-payload-key";
    std::string payload(OpLogManager::kMaxPayloadSize - 1, 'x');

    OpLogEntry entry;
    entry.object_key = key;
    entry.payload = payload;

    std::string reason;
    EXPECT_TRUE(OpLogManager::ValidateEntrySize(entry, &reason));

    uint64_t id = M().Append(OpType::PUT_END, key, payload);
    EXPECT_LT(0u, id);
    EXPECT_EQ(1u, M().GetEntryCount());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
