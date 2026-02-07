#include "etcd_oplog_store.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "etcd_helper.h"

DEFINE_string(etcd_endpoints, "0.0.0.0:2379",
              "Etcd endpoints for EtcdOpLogStoreTest");

namespace mooncake::test {

class EtcdOpLogStoreTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
#ifdef STORE_USE_ETCD
        google::InitGoogleLogging("EtcdOpLogStoreTest");
        FLAGS_logtostderr = 1;

        ASSERT_EQ(ErrorCode::OK,
                  EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints))
            << "Failed to connect to etcd at " << FLAGS_etcd_endpoints;
#endif
    }

    static void TearDownTestSuite() {
#ifdef STORE_USE_ETCD
        google::ShutdownGoogleLogging();
#endif
    }

    void SetUp() override {
#ifndef STORE_USE_ETCD
        GTEST_SKIP()
            << "STORE_USE_ETCD is disabled, skipping EtcdOpLogStore tests.";
#else
        cluster_id_ = "test_cluster_etcd_oplog_store";
        store_ = std::make_unique<EtcdOpLogStore>(
            cluster_id_,
            /*enable_latest_seq_batch_update=*/false);
        CleanupTestData();
#endif
    }

    void TearDown() override {
#ifdef STORE_USE_ETCD
        CleanupTestData();
        store_.reset();
#endif
    }

    std::string cluster_id_;
    std::unique_ptr<EtcdOpLogStore> store_;

    void CleanupTestData() {
#ifdef STORE_USE_ETCD
        // Delete all keys under /oplog/{cluster_id_}/ prefix
        std::string prefix = std::string("/oplog/") + cluster_id_ + "/";

        auto prefix_end = [](std::string p) -> std::string {
            for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i) {
                unsigned char c = static_cast<unsigned char>(p[i]);
                if (c < 0xFF) {
                    p[i] = static_cast<char>(c + 1);
                    p.resize(i + 1);
                    return p;
                }
            }
            return std::string(1, '\0');
        };
        std::string end_key = prefix_end(prefix);

        (void)EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(),
                                      end_key.c_str(), end_key.size());
#endif
    }

    static OpLogEntry MakeEntry(uint64_t seq, OpType type,
                                const std::string& key,
                                const std::string& payload) {
        OpLogEntry e;
        e.sequence_id = seq;
        e.timestamp_ms = 123456;
        e.op_type = type;
        e.object_key = key;
        e.payload = payload;
        e.checksum = 0;
        e.prefix_hash = 0;
        return e;
    }
};

// ========== 3.1.1 Basic CRUD tests ==========

TEST_F(EtcdOpLogStoreTest, TestWriteOpLog) {
    OpLogEntry e = MakeEntry(1, OpType::PUT_END, "key1", "value1");

    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));

    // Latest sequence ID should be updated to 1
    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(1u, latest);
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLog) {
    OpLogEntry e = MakeEntry(2, OpType::PUT_END, "key2", "value2");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));

    OpLogEntry out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(2, out));

    EXPECT_EQ(2u, out.sequence_id);
    EXPECT_EQ(OpType::PUT_END, out.op_type);
    EXPECT_EQ("key2", out.object_key);
    EXPECT_EQ("value2", out.payload);
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince) {
    // Write multiple entries
    for (uint64_t i = 10; i < 15; ++i) {
        OpLogEntry e = MakeEntry(i, OpType::PUT_END, "key_" + std::to_string(i),
                                 "value_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(11, 10, entries));

    // Expect entries with seq > 11: 12,13,14
    ASSERT_EQ(3u, entries.size());
    EXPECT_EQ(12u, entries[0].sequence_id);
    EXPECT_EQ(13u, entries[1].sequence_id);
    EXPECT_EQ(14u, entries[2].sequence_id);
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_Empty) {
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(1000, 10, entries));
    EXPECT_TRUE(entries.empty());
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_Limit) {
    for (uint64_t i = 1; i <= 5; ++i) {
        OpLogEntry e = MakeEntry(i, OpType::PUT_END, "key_" + std::to_string(i),
                                 "value_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(0, 3, entries));
    ASSERT_EQ(3u, entries.size());
    EXPECT_EQ(1u, entries[0].sequence_id);
    EXPECT_EQ(2u, entries[1].sequence_id);
    EXPECT_EQ(3u, entries[2].sequence_id);
}

// ========== 3.1.2 Serialization tests ==========

TEST_F(EtcdOpLogStoreTest, TestSerializeDeserializeRoundTrip) {
    OpLogEntry in =
        MakeEntry(42, OpType::PUT_END, "roundtrip-key", "roundtrip-value");

    // Indirectly verify serialization / deserialization via WriteOpLog +
    // ReadOpLog
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(in));

    OpLogEntry out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(42, out));

    EXPECT_EQ(in.sequence_id, out.sequence_id);
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.object_key, out.object_key);
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(EtcdOpLogStoreTest, TestDeserializeInvalidJson) {
    // Write invalid JSON directly into etcd; subsequent ReadOpLog should return
    // INTERNAL_ERROR
    std::string key = "/oplog/" + cluster_id_ + "/00000000000000000077";
    std::string bad_json = "{ this is not valid json }";
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Put(key.c_str(), key.size(), bad_json.c_str(),
                              bad_json.size()));

    OpLogEntry out;
    ASSERT_EQ(ErrorCode::INTERNAL_ERROR, store_->ReadOpLog(77, out));
}

// ========== 3.1.3 Fencing tests ==========

TEST_F(EtcdOpLogStoreTest, TestWriteOpLog_Fencing) {
    OpLogEntry e1 = MakeEntry(100, OpType::PUT_END, "key_fence", "value1");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e1));

    // Same seq, same content => idempotent (OK)
    OpLogEntry e2 = e1;
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e2));

    // Same seq, different content => conflict (ETCD_OPERATION_ERROR)
    OpLogEntry e3 = e1;
    e3.payload = "value2";
    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, store_->WriteOpLog(e3));
}

TEST_F(EtcdOpLogStoreTest, TestWriteOpLog_Idempotent) {
    OpLogEntry e = MakeEntry(200, OpType::PUT_END, "key_idem", "v");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));

    // Repeatedly writing the exact same entry should return OK (idempotent)
    // even if the underlying Create operation reports a transaction failure.
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
}

// ========== 3.1.4 Sequence ID management tests ==========

TEST_F(EtcdOpLogStoreTest, TestGetLatestSequenceId) {
    OpLogEntry e1 = MakeEntry(1, OpType::PUT_END, "k1", "v1");
    OpLogEntry e2 = MakeEntry(2, OpType::PUT_END, "k2", "v2");
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e1));
    ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e2));

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(2u, latest);
}

TEST_F(EtcdOpLogStoreTest, TestGetMaxSequenceIdAndEmpty) {
    uint64_t max_seq = 0;

    // Empty cluster: after cleanup, GetMaxSequenceId should return
    // ETCD_KEY_NOT_EXIST
    CleanupTestData();
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, store_->GetMaxSequenceId(max_seq));

    // After writing several entries, MaxSequenceId should equal the last
    // entry's seq
    for (uint64_t i = 10; i <= 15; ++i) {
        OpLogEntry e = MakeEntry(i, OpType::PUT_END, "key_" + std::to_string(i),
                                 "value_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    ASSERT_EQ(ErrorCode::OK, store_->GetMaxSequenceId(max_seq));
    EXPECT_EQ(15u, max_seq);
}

TEST_F(EtcdOpLogStoreTest, TestUpdateLatestSequenceId) {
    // Directly call UpdateLatestSequenceId, then GetLatestSequenceId should
    // match
    ASSERT_EQ(ErrorCode::OK, store_->UpdateLatestSequenceId(12345));

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(12345u, latest);
}

// ========== 3.1.5 Batch update tests ==========

TEST_F(EtcdOpLogStoreTest, TestBatchUpdate_EnabledAndThreshold) {
    // Use a store with batch enabled, then verify /latest is updated to the max
    // seq
    EtcdOpLogStore writer(cluster_id_,
                          /*enable_latest_seq_batch_update=*/true);

    const uint64_t base_seq = 1000;
    const int kEntries = 5;
    for (int i = 0; i < kEntries; ++i) {
        OpLogEntry e = MakeEntry(base_seq + i, OpType::PUT_END,
                                 "batch_key_" + std::to_string(i),
                                 "batch_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, writer.WriteOpLog(e));
    }

    // Wait a short period to give the batch thread a chance to flush `/latest`
    std::this_thread::sleep_for(std::chrono::milliseconds(2 * 1000));

    uint64_t latest = 0;
    ASSERT_EQ(ErrorCode::OK, store_->GetLatestSequenceId(latest));
    EXPECT_EQ(base_seq + kEntries - 1, latest);
}

TEST_F(EtcdOpLogStoreTest, TestBatchUpdate_FailurePlaceholder) {
    GTEST_SKIP() << "Batch failure scenarios are better tested with a "
                    "fault-injection etcd wrapper.";
}

// ========== 3.1.6 Cleanup tests ==========

TEST_F(EtcdOpLogStoreTest, TestCleanupOpLogBeforeAndBoundary) {
    // Write seq 1..5
    for (uint64_t i = 1; i <= 5; ++i) {
        OpLogEntry e =
            MakeEntry(i, OpType::PUT_END, "cleanup_key_" + std::to_string(i),
                      "cleanup_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    // Cleanup seq < 3 => 1,2 should be deleted; 3,4,5 should remain
    ASSERT_EQ(ErrorCode::OK, store_->CleanupOpLogBefore(3));

    OpLogEntry out;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, store_->ReadOpLog(1, out));
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST, store_->ReadOpLog(2, out));

    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(3, out));
    EXPECT_EQ(3u, out.sequence_id);

    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(5, out));
    EXPECT_EQ(5u, out.sequence_id);
}

TEST_F(EtcdOpLogStoreTest, TestCleanupOpLogBefore_Empty) {
    // Cleanup on an empty cluster should return OK
    CleanupTestData();
    EXPECT_EQ(ErrorCode::OK, store_->CleanupOpLogBefore(100));
}

// ========== 3.1.7 Cluster ID validation tests ==========

TEST_F(EtcdOpLogStoreTest, TestInvalidClusterId_Rejected) {
    // Invalid cluster_id (containing slashes) should trigger LOG(FATAL) and
    // terminate
    EXPECT_DEATH(
        {
            EtcdOpLogStore bad_store("invalid/cluster", false);
            (void)bad_store;
        },
        "Invalid cluster_id");
}

TEST_F(EtcdOpLogStoreTest, TestClusterIdNormalization) {
    // Trailing slashes should be normalized away from the cluster_id
    std::string raw_cluster = cluster_id_ + "///";
    EtcdOpLogStore normalized_store(raw_cluster, false);

    OpLogEntry e = MakeEntry(999, OpType::PUT_END, "norm-key", "norm-val");
    ASSERT_EQ(ErrorCode::OK, normalized_store.WriteOpLog(e));

    // Read the same seq via the current store_ to confirm the normalized
    // cluster_id is used
    OpLogEntry out;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLog(999, out));
    EXPECT_EQ("norm-key", out.object_key);
    EXPECT_EQ("norm-val", out.payload);
}

// ========== 3.1.8 Pagination tests ==========

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_Pagination) {
    // Write 20 entries and verify pagination via limit
    for (uint64_t i = 1; i <= 20; ++i) {
        OpLogEntry e =
            MakeEntry(i, OpType::PUT_END, "page_key_" + std::to_string(i),
                      "page_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(0, 20, entries));
    ASSERT_EQ(20u, entries.size());
    for (uint64_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(i + 1, entries[i].sequence_id);
    }
}

TEST_F(EtcdOpLogStoreTest, TestReadOpLogSince_LargeDataset) {
    // Write a larger number of entries to verify ReadOpLogSince returns the
    // first N correctly
    const uint64_t total = 200;
    const uint64_t limit = 150;
    CleanupTestData();
    for (uint64_t i = 1; i <= total; ++i) {
        OpLogEntry e =
            MakeEntry(i, OpType::PUT_END, "large_key_" + std::to_string(i),
                      "large_val_" + std::to_string(i));
        ASSERT_EQ(ErrorCode::OK, store_->WriteOpLog(e));
    }

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, store_->ReadOpLogSince(0, limit, entries));
    ASSERT_EQ(limit, entries.size());
    for (uint64_t i = 0; i < limit; ++i) {
        EXPECT_EQ(i + 1, entries[i].sequence_id);
    }
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
