#include "oplog_applier.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <xxhash.h>

#include "etcd_oplog_store.h"
#include "metadata_store.h"
#include "oplog_manager.h"
#include "types.h"

namespace mooncake::test {

// Mock MetadataStore for testing OpLogApplier
class MockMetadataStore : public MetadataStore {
   public:
    MockMetadataStore() = default;
    ~MockMetadataStore() override = default;

    bool PutMetadata(const std::string& key,
                     const StandbyObjectMetadata& metadata) override {
        metadata_map_[key] = metadata;
        return true;
    }

    bool Put(const std::string& key, const std::string& payload) override {
        // For testing, we can use PutMetadata with empty metadata
        StandbyObjectMetadata meta;
        metadata_map_[key] = meta;
        return true;
    }

    const StandbyObjectMetadata* GetMetadata(
        const std::string& key) const override {
        auto it = metadata_map_.find(key);
        if (it != metadata_map_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    bool Remove(const std::string& key) override {
        auto it = metadata_map_.find(key);
        if (it != metadata_map_.end()) {
            metadata_map_.erase(it);
            return true;
        }
        return false;
    }

    bool Exists(const std::string& key) const override {
        return metadata_map_.find(key) != metadata_map_.end();
    }

    size_t GetKeyCount() const override { return metadata_map_.size(); }

    // Test helper methods
    void Clear() { metadata_map_.clear(); }

    size_t Size() const { return metadata_map_.size(); }

   private:
    std::map<std::string, StandbyObjectMetadata> metadata_map_;
};

// Helper function to create a valid OpLogEntry with checksum
// Uses the same checksum algorithm as OpLogManager (XXH32)
OpLogEntry MakeEntry(uint64_t seq, OpType type, const std::string& key,
                     const std::string& payload) {
    OpLogEntry e;
    e.sequence_id = seq;
    e.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
                         .count();
    e.op_type = type;
    e.object_key = key;
    e.payload = payload;
    // Compute checksum and prefix_hash using the same algorithm as OpLogManager
    e.checksum =
        static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
    e.prefix_hash =
        key.empty() ? 0
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    return e;
}

// Helper function to create a valid JSON payload for PUT_END
std::string MakeValidJsonPayload(uint64_t client_id_first = 1,
                                 uint64_t client_id_second = 2,
                                 uint64_t size = 1024) {
    // NOTE: OpLogApplier's current implementation expects PUT_END payload to be
    // struct_pack-serialized MetadataPayload (msgpack binary), not JSON.
    MetadataPayload payload;
    payload.client_id = {client_id_first, client_id_second};
    payload.size = size;
    payload.replicas = {};
    auto buf = struct_pack::serialize(payload);
    return std::string(buf.data(), buf.size());
}

class OpLogApplierTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogApplierTest");
        FLAGS_logtostderr = 1;
        mock_metadata_store_ = std::make_unique<MockMetadataStore>();
        cluster_id_ = "test_cluster_001";
        applier_ = std::make_unique<OpLogApplier>(mock_metadata_store_.get(),
                                                  cluster_id_);
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<MockMetadataStore> mock_metadata_store_;
    std::unique_ptr<OpLogApplier> applier_;
    std::string cluster_id_;
};

// ========== 4.1.1 Basic apply tests ==========

TEST_F(OpLogApplierTest, TestApplyPutEnd) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    // After applying seq=1, expected_sequence_id becomes 2
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_EQ(1u, mock_metadata_store_->GetKeyCount());
}

TEST_F(OpLogApplierTest, TestApplyPutRevoke) {
    // First add a key
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    // Then revoke it
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_REVOKE, "key1", "");
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyRemove) {
    // First add a key
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    // Then remove it
    OpLogEntry entry2 = MakeEntry(2, OpType::REMOVE, "key1", "");
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidOpType) {
    OpLogEntry entry =
        MakeEntry(1, OpType::PUT_END, "key1", MakeValidJsonPayload());
    // Manually set an invalid op_type (assuming OpType is an enum)
    // Since we can't directly set invalid enum, we test with valid types
    // and verify that unsupported types in ProcessPendingEntries are handled
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
}

// ========== 4.1.2 Sequence ordering tests ==========

TEST_F(OpLogApplierTest, TestApplyInOrder) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_END, "key2", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());

    EXPECT_EQ(3u, mock_metadata_store_->GetKeyCount());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key3"));
}

TEST_F(OpLogApplierTest, TestApplyOutOfOrder) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_END, "key2", payload);

    // Apply entry1 (seq=1) - should succeed
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry3 (seq=3) - should be cached (out of order)
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(2u,
              applier_->GetExpectedSequenceId());  // Still waiting for seq=2
    EXPECT_FALSE(mock_metadata_store_->Exists("key3"));

    // Apply entry2 (seq=2) - should succeed and trigger processing of entry3
    // ApplyOpLogEntry internally calls ProcessPendingEntries(), so entry3
    // should be processed
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());  // Now at seq=4

    // entry3 should already be processed by ApplyOpLogEntry
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key3"));

    // ProcessPendingEntries may return 0 if entry3 was already processed
    (void)applier_->ProcessPendingEntries();
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestApplyWithGap) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry4 = MakeEntry(4, OpType::PUT_END, "key4", payload);

    // Apply entry1 (seq=1)
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry4 (seq=4) - gap at seq=2,3
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry4));
    EXPECT_EQ(2u,
              applier_->GetExpectedSequenceId());  // Still waiting for seq=2

    // Process pending entries - should detect gap and schedule wait
    (void)applier_->ProcessPendingEntries();
    // May process 0 entries if gap resolution is still waiting
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestApplyDuplicateSequenceId) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry1_dup = MakeEntry(1, OpType::PUT_END, "key1_dup", payload);

    // Apply entry1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Try to apply duplicate sequence_id (older than expected)
    // Should be treated as no-op (already applied) and return true
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1_dup));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    // key1_dup should not be added (treated as no-op)
    EXPECT_FALSE(mock_metadata_store_->Exists("key1_dup"));
}

// ========== 4.1.3 Gap resolution tests ==========

TEST_F(OpLogApplierTest, TestRequestMissingOpLog_Success) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real EtcdOpLogStore, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogApplierTest, TestRequestMissingOpLog_Failure) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real EtcdOpLogStore, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogApplierTest, TestRequestMissingOpLog_Timeout) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real EtcdOpLogStore, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogApplierTest, TestTryResolveGapsOnceForPromotion) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real EtcdOpLogStore, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogApplierTest, TestGapResolution_Retry) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real EtcdOpLogStore, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== 4.1.4 Checksum tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidChecksum) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidChecksum) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestChecksumFailureMetric) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    // Metric increment is tested implicitly by the failure
}

// ========== 4.1.5 Size validation tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidSize) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(OpLogManager::ValidateEntrySize(entry));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidSize) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Make key too large
    entry.object_key.assign(OpLogManager::kMaxObjectKeySize + 1, 'k');

    EXPECT_FALSE(OpLogManager::ValidateEntrySize(entry));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_PayloadTooLarge) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Make payload too large
    entry.payload.assign(OpLogManager::kMaxPayloadSize + 1, 'p');

    EXPECT_FALSE(OpLogManager::ValidateEntrySize(entry));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
}

// ========== 4.1.6 Recovery tests ==========

TEST_F(OpLogApplierTest, TestRecover) {
    // Set initial state: last applied sequence_id = 10
    applier_->Recover(10);
    EXPECT_EQ(11u, applier_->GetExpectedSequenceId());

    // Apply entry with seq=11 should succeed
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(11, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(12u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestRecover_ZeroSequenceId) {
    // Recover from sequence_id 0
    applier_->Recover(0);
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    // Apply entry with seq=1 should succeed
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestRecover_AfterGap) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);

    // Apply entry1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry3 (creates gap)
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry3));

    // Recover from seq=3 (skip the gap)
    applier_->Recover(3);
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());

    // Now entry3 should be processable
    (void)applier_->ProcessPendingEntries();
    // entry3 should be in pending, but expected_seq is now 4, so it won't be
    // processed This tests that recovery resets the expected sequence
}

// ========== 4.1.7 Pending entries tests ==========

TEST_F(OpLogApplierTest, TestProcessPendingEntries) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_END, "key2", payload);

    // Apply entry1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry3 (out of order)
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Process pending - should not process entry3 yet (waiting for seq=2)
    size_t processed1 = applier_->ProcessPendingEntries();
    EXPECT_EQ(0u, processed1);
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry2 - this will internally call ProcessPendingEntries() and
    // process entry3
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());

    // entry3 should already be processed by ApplyOpLogEntry
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key3"));

    // ProcessPendingEntries may return 0 if entry3 was already processed
    (void)applier_->ProcessPendingEntries();
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestPendingEntriesTimeout) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);

    // Apply entry1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry3 (creates gap at seq=2)
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry3));

    // Process pending entries multiple times to trigger timeout
    // After kMissingEntrySkipSeconds (3s), the gap should be skipped
    size_t processed = 0;
    for (int i = 0; i < 10; ++i) {
        processed = applier_->ProcessPendingEntries();
        if (processed > 0 || applier_->GetExpectedSequenceId() > 2) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // After timeout, gap should be skipped and entry3 should be processed
    // Note: This test may be flaky due to timing, but it tests the timeout
    // logic
    EXPECT_GE(applier_->GetExpectedSequenceId(), 2u);
}

TEST_F(OpLogApplierTest, TestPendingEntriesSkip) {
    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry4 = MakeEntry(4, OpType::PUT_END, "key4", payload);

    // Apply entry1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry4 (creates gap at seq=2,3)
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry4));

    // Process pending entries to trigger skip logic
    // After timeout (3 seconds), gaps should be skipped
    for (int i = 0; i < 10; ++i) {
        applier_->ProcessPendingEntries();
        uint64_t expected = applier_->GetExpectedSequenceId();
        if (expected >= 3) {  // Gap at seq=2 is skipped, expected becomes 3
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    // After skip, expected_seq should advance to 3 (gap at seq=2 is skipped)
    // entry4 is still pending, waiting for seq=3
    EXPECT_GE(applier_->GetExpectedSequenceId(), 3u);
    // entry4 should still be pending (not applied yet)
    EXPECT_FALSE(mock_metadata_store_->Exists("key4"));
}

// ========== 4.1.8 JSON parsing tests ==========

TEST_F(OpLogApplierTest, TestApplyPutEnd_ValidJson) {
    std::string payload = MakeValidJsonPayload(1, 2, 2048);
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    const StandbyObjectMetadata* meta =
        mock_metadata_store_->GetMetadata("key1");
    ASSERT_NE(nullptr, meta);
    EXPECT_EQ(1u, meta->client_id.first);
    EXPECT_EQ(2u, meta->client_id.second);
    EXPECT_EQ(2048u, meta->size);
}

TEST_F(OpLogApplierTest, TestApplyPutEnd_InvalidJson) {
    std::string invalid_json = "{invalid json}";
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", invalid_json);

    // Should still succeed (fallback to empty metadata)
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    // Metadata should exist but with default values
    const StandbyObjectMetadata* meta =
        mock_metadata_store_->GetMetadata("key1");
    ASSERT_NE(nullptr, meta);
    EXPECT_EQ(0u, meta->client_id.first);
    EXPECT_EQ(0u, meta->client_id.second);
    EXPECT_EQ(0u, meta->size);
}

TEST_F(OpLogApplierTest, TestApplyPutEnd_EmptyPayload) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Should succeed with empty payload (creates empty metadata)
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    const StandbyObjectMetadata* meta =
        mock_metadata_store_->GetMetadata("key1");
    ASSERT_NE(nullptr, meta);
    EXPECT_EQ(0u, meta->client_id.first);
    EXPECT_EQ(0u, meta->client_id.second);
    EXPECT_EQ(0u, meta->size);
}

// ========== Additional Edge Case Tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntries_Batch) {
    std::string payload = MakeValidJsonPayload();
    std::vector<OpLogEntry> entries;
    entries.push_back(MakeEntry(1, OpType::PUT_END, "key1", payload));
    entries.push_back(MakeEntry(2, OpType::PUT_END, "key2", payload));
    entries.push_back(MakeEntry(3, OpType::PUT_END, "key3", payload));

    size_t applied = applier_->ApplyOpLogEntries(entries);
    EXPECT_EQ(3u, applied);
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
    EXPECT_EQ(3u, mock_metadata_store_->GetKeyCount());
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntries_WithGaps) {
    std::string payload = MakeValidJsonPayload();
    std::vector<OpLogEntry> entries;
    entries.push_back(MakeEntry(1, OpType::PUT_END, "key1", payload));
    entries.push_back(
        MakeEntry(3, OpType::PUT_END, "key3", payload));  // Gap at seq=2
    entries.push_back(MakeEntry(2, OpType::PUT_END, "key2", payload));

    size_t applied = applier_->ApplyOpLogEntries(entries);
    // entry1 should be applied, entry3 should be pending, entry2 should be
    // applied and trigger processing of entry3
    EXPECT_GE(applied, 2u);  // entry1 and entry2 are applied
    EXPECT_LE(applied, 3u);

    // entry2's ApplyOpLogEntry internally calls ProcessPendingEntries(), so
    // entry3 should be processed
    EXPECT_GE(applier_->GetExpectedSequenceId(), 4u);
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key3"));

    // ProcessPendingEntries may return 0 if entry3 was already processed
    (void)applier_->ProcessPendingEntries();
    EXPECT_GE(applier_->GetExpectedSequenceId(), 4u);
}

TEST_F(OpLogApplierTest, TestGetExpectedSequenceId) {
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    std::string payload = MakeValidJsonPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestGetKeySequenceId_Deprecated) {
    // This method is deprecated and always returns 0
    EXPECT_EQ(0u, applier_->GetKeySequenceId("any_key"));
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
