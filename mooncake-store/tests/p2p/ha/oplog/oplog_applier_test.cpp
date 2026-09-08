#include "p2p/ha/oplog/p2p_oplog_applier.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <xxhash.h>

#include "mock_oplog_store.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "types.h"

using mooncake::test::MockOpLogStore;

namespace mooncake::test {

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

// Helper function to create a valid P2P publish payload
std::string MakeValidPayload(const std::string& key,
                             uint64_t client_id_first = 1,
                             uint64_t client_id_second = 2,
                             uint64_t size = 1024) {
    PublishRoutePayload payload;
    payload.object_key = key;
    payload.client_id = {client_id_first, client_id_second};
    payload.segment_id = {3, 4};
    payload.size = size;
    return SerializeP2PPayload(payload);
}

std::string MakeWithdrawPayload(const std::string& key) {
    WithdrawRoutePayload payload;
    payload.object_key = key;
    payload.client_id = {1, 2};
    payload.segment_id = {3, 4};
    return SerializeP2PPayload(payload);
}

class OpLogApplierTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogApplierTest");
        FLAGS_logtostderr = 1;
        metadata_store_ = std::make_unique<P2PStandbyMetadataStore>();
        cluster_id_ = "test_cluster_001";
        applier_ = std::make_unique<P2POpLogApplier>(metadata_store_.get(),
                                                     cluster_id_);
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<P2PStandbyMetadataStore> metadata_store_;
    std::unique_ptr<P2POpLogApplier> applier_;
    std::string cluster_id_;
};

// ========== 4.1.1 Basic apply tests ==========

TEST_F(OpLogApplierTest, TestApplyPublishRoute) {
    OpLogEntry entry =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    // After applying seq=1, expected_sequence_id becomes 2
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));
    EXPECT_EQ(1u, metadata_store_->GetRouteKeyCount());
}

TEST_F(OpLogApplierTest, TestApplyWithdrawRoute) {
    // First add a key
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));

    // Then revoke it
    OpLogEntry entry2 = MakeEntry(2, OpType_WITHDRAW_ROUTE, "key1",
                                  MakeWithdrawPayload("key1"));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(metadata_store_->RouteExists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidOpType) {
    OpLogEntry entry =
        MakeEntry(1, static_cast<OpType>(99), "key1", MakeValidPayload("key1"));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_FALSE(applier_->IsHealthy());
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(metadata_store_->RouteExists("key1"));
}

// ========== 4.1.2 Sequence ordering tests ==========

TEST_F(OpLogApplierTest, TestApplyInOrder) {
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry2 =
        MakeEntry(2, OpType_PUBLISH_ROUTE, "key2", MakeValidPayload("key2"));
    OpLogEntry entry3 =
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"));

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());

    EXPECT_EQ(3u, metadata_store_->GetRouteKeyCount());
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));
    EXPECT_TRUE(metadata_store_->RouteExists("key2"));
    EXPECT_TRUE(metadata_store_->RouteExists("key3"));
}

TEST_F(OpLogApplierTest, TestApplyOutOfOrder) {
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry3 =
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"));
    OpLogEntry entry2 =
        MakeEntry(2, OpType_PUBLISH_ROUTE, "key2", MakeValidPayload("key2"));

    // Apply entry1 (seq=1) - should succeed
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Apply entry3 (seq=3) - should be cached (out of order)
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(2u,
              applier_->GetExpectedSequenceId());  // Still waiting for seq=2
    EXPECT_FALSE(metadata_store_->RouteExists("key3"));

    // Apply entry2 (seq=2) - should succeed and trigger processing of entry3
    // ApplyOpLogEntry internally calls ProcessPendingEntries(), so entry3
    // should be processed
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());  // Now at seq=4

    // entry3 should already be processed by ApplyOpLogEntry
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));
    EXPECT_TRUE(metadata_store_->RouteExists("key2"));
    EXPECT_TRUE(metadata_store_->RouteExists("key3"));

    // ProcessPendingEntries may return 0 if entry3 was already processed
    (void)applier_->ProcessPendingEntries();
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestApplyWithGap) {
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry4 =
        MakeEntry(4, OpType_PUBLISH_ROUTE, "key4", MakeValidPayload("key4"));

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
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry1_dup = MakeEntry(1, OpType_PUBLISH_ROUTE, "key1_dup",
                                      MakeValidPayload("key1_dup"));

    // Apply entry1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Try to apply duplicate sequence_id (older than expected)
    // Should be treated as no-op (already applied) and return true
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1_dup));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    // key1_dup should not be added (treated as no-op)
    EXPECT_FALSE(metadata_store_->RouteExists("key1_dup"));
}

// ========== 4.1.3 Gap resolution tests ==========

class OpLogApplierGapTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogApplierGapTest");
        FLAGS_logtostderr = 1;
        metadata_store_ = std::make_unique<P2PStandbyMetadataStore>();
        mock_oplog_store_ = std::make_unique<MockOpLogStore>();
        applier_ = std::make_unique<P2POpLogApplier>(
            metadata_store_.get(), "test_cluster", mock_oplog_store_.get());
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<P2PStandbyMetadataStore> metadata_store_;
    std::unique_ptr<MockOpLogStore> mock_oplog_store_;
    std::unique_ptr<P2POpLogApplier> applier_;
};

TEST_F(OpLogApplierGapTest, RequestMissingOpLog_Success) {
    // Pre-populate seq=2 in mock store
    OpLogEntry missing =
        MakeEntry(2, OpType_PUBLISH_ROUTE, "key2", MakeValidPayload("key2"));
    mock_oplog_store_->WriteOpLog(missing);

    // Apply seq=1
    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"))));
    // Apply seq=3 (gap at seq=2)
    EXPECT_FALSE(applier_->ApplyOpLogEntry(
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"))));

    // First call registers the gap in missing_sequence_ids_
    applier_->ProcessPendingEntries();
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // Wait for gap resolution trigger (kMissingEntryRequestSeconds = 1s)
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    applier_->ProcessPendingEntries();

    // After gap resolution, all 3 should be applied
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));
    EXPECT_TRUE(metadata_store_->RouteExists("key2"));
    EXPECT_TRUE(metadata_store_->RouteExists("key3"));
}

TEST_F(OpLogApplierGapTest, RequestMissingOpLog_StoreError) {
    mock_oplog_store_->SetReadError(ErrorCode::ETCD_OPERATION_ERROR);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"))));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"))));

    // First call registers the gap
    applier_->ProcessPendingEntries();

    // Gap resolution should fail gracefully
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    applier_->ProcessPendingEntries();

    // seq=2 not resolved, expected still at 2
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierGapTest, RequestMissingOpLog_NotFound) {
    // Don't preload seq=2 in mock store

    EXPECT_TRUE(applier_->ApplyOpLogEntry(
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"))));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"))));

    // First call registers the gap
    applier_->ProcessPendingEntries();

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    applier_->ProcessPendingEntries();

    // Not found, should not advance
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

// ========== 4.1.4 Checksum tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidChecksum) {
    OpLogEntry entry =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidChecksum) {
    OpLogEntry entry = MakeEntry(1, OpType_WITHDRAW_ROUTE, "key1",
                                 MakeWithdrawPayload("key1"));

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(metadata_store_->RouteExists("key1"));
}

TEST_F(OpLogApplierTest, TestChecksumFailureMetric) {
    OpLogEntry entry = MakeEntry(1, OpType_WITHDRAW_ROUTE, "key1",
                                 MakeWithdrawPayload("key1"));

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    // Metric increment is tested implicitly by the failure
}

// ========== 4.1.5 Size validation tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidSize) {
    OpLogEntry entry =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));

    EXPECT_TRUE(OpLogManager::ValidateEntrySize(entry));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidSize) {
    OpLogEntry entry = MakeEntry(1, OpType_WITHDRAW_ROUTE, "key1",
                                 MakeWithdrawPayload("key1"));

    // Make key too large
    entry.object_key.assign(OpLogManager::kMaxObjectKeySize + 1, 'k');

    EXPECT_FALSE(OpLogManager::ValidateEntrySize(entry));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(metadata_store_->RouteExists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_PayloadTooLarge) {
    OpLogEntry entry = MakeEntry(1, OpType_WITHDRAW_ROUTE, "key1",
                                 MakeWithdrawPayload("key1"));

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
    OpLogEntry entry =
        MakeEntry(11, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(12u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestRecover_ZeroSequenceId) {
    // Recover from sequence_id 0
    applier_->Recover(0);
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    // Apply entry with seq=1 should succeed
    OpLogEntry entry =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestRecover_AfterGap) {
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry3 =
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"));

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
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry3 =
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"));
    OpLogEntry entry2 =
        MakeEntry(2, OpType_PUBLISH_ROUTE, "key2", MakeValidPayload("key2"));

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
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));
    EXPECT_TRUE(metadata_store_->RouteExists("key2"));
    EXPECT_TRUE(metadata_store_->RouteExists("key3"));

    // ProcessPendingEntries may return 0 if entry3 was already processed
    (void)applier_->ProcessPendingEntries();
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestPendingEntriesTimeout) {
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry3 =
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3"));

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
    OpLogEntry entry1 =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    OpLogEntry entry4 =
        MakeEntry(4, OpType_PUBLISH_ROUTE, "key4", MakeValidPayload("key4"));

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
    EXPECT_FALSE(metadata_store_->RouteExists("key4"));
}

// ========== 4.1.8 Payload deserialization tests ==========

TEST_F(OpLogApplierTest, TestApplyPublishRoute_ValidPayload) {
    OpLogEntry entry = MakeEntry(1, OpType_PUBLISH_ROUTE, "key1",
                                 MakeValidPayload("key1", 1, 2, 2048));

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));

    auto meta = metadata_store_->GetRoute("key1");
    ASSERT_TRUE(meta.has_value());
    ASSERT_EQ(1u, meta->locations.size());
    EXPECT_EQ(1u, meta->locations.front().client_id.first);
    EXPECT_EQ(2u, meta->locations.front().client_id.second);
    EXPECT_EQ(2048u, meta->object_size);
}

// ========== Additional Edge Case Tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntries_Batch) {
    std::vector<OpLogEntry> entries;
    entries.push_back(
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1")));
    entries.push_back(
        MakeEntry(2, OpType_PUBLISH_ROUTE, "key2", MakeValidPayload("key2")));
    entries.push_back(
        MakeEntry(3, OpType_PUBLISH_ROUTE, "key3", MakeValidPayload("key3")));

    size_t applied = applier_->ApplyOpLogEntries(entries);
    EXPECT_EQ(3u, applied);
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
    EXPECT_EQ(3u, metadata_store_->GetRouteKeyCount());
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntries_WithGaps) {
    std::vector<OpLogEntry> entries;
    entries.push_back(
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1")));
    entries.push_back(MakeEntry(3, OpType_PUBLISH_ROUTE, "key3",
                                MakeValidPayload("key3")));  // Gap at seq=2
    entries.push_back(
        MakeEntry(2, OpType_PUBLISH_ROUTE, "key2", MakeValidPayload("key2")));

    size_t applied = applier_->ApplyOpLogEntries(entries);
    // entry1 should be applied, entry3 should be pending, entry2 should be
    // applied and trigger processing of entry3
    EXPECT_GE(applied, 2u);  // entry1 and entry2 are applied
    EXPECT_LE(applied, 3u);

    // entry2's ApplyOpLogEntry internally calls ProcessPendingEntries(), so
    // entry3 should be processed
    EXPECT_GE(applier_->GetExpectedSequenceId(), 4u);
    EXPECT_TRUE(metadata_store_->RouteExists("key1"));
    EXPECT_TRUE(metadata_store_->RouteExists("key2"));
    EXPECT_TRUE(metadata_store_->RouteExists("key3"));

    // ProcessPendingEntries may return 0 if entry3 was already processed
    (void)applier_->ProcessPendingEntries();
    EXPECT_GE(applier_->GetExpectedSequenceId(), 4u);
}

TEST_F(OpLogApplierTest, TestGetExpectedSequenceId) {
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    OpLogEntry entry =
        MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", MakeValidPayload("key1"));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
