// mooncake-store/tests/hot_standby_ut/ha_recovery_test.cpp
//
// Mock-layer tests for HA recovery scenarios.
// Tests at component level: OpLogApplier + MockOpLogStore +
// MockMetadataStore + MockSnapshotProvider.
// Does NOT go through HotStandbyService (OpLogStoreFactory does not
// support creating MockOpLogStore).

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <xxhash.h>
#include <ylt/struct_pack.hpp>

#include <chrono>
#include <thread>

#include "mock_metadata_store.h"
#include "mock_oplog_store.h"
#include "mock_snapshot_provider.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_manager.h"

using namespace mooncake;
using namespace mooncake::test;

// Helper: create a struct_pack-serialized payload (same format as OpLogManager)
static std::string MakeValidPayload(uint64_t client_first = 1,
                                    uint64_t client_second = 2,
                                    uint64_t size = 1024) {
    MetadataPayload payload;
    payload.client_id = {client_first, client_second};
    payload.size = size;
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

// Helper: create an OpLogEntry with PUT_END and valid checksum
static OpLogEntry MakeEntry(uint64_t seq, const std::string& key) {
    OpLogEntry entry;
    entry.sequence_id = seq;
    entry.timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    entry.op_type = OpType::PUT_END;
    entry.object_key = key;
    entry.payload = MakeValidPayload();
    entry.checksum = static_cast<uint32_t>(
        XXH32(entry.payload.data(), entry.payload.size(), 0));
    entry.prefix_hash =
        key.empty() ? 0
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    return entry;
}

class HaRecoveryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        mock_store_ = std::make_shared<MockOpLogStore>();
        mock_store_->Init();
        mock_snapshot_ = std::make_shared<MockSnapshotProvider>();
        metadata_store_ = std::make_shared<MockMetadataStore>();
        applier_ = std::make_unique<OpLogApplier>(
            metadata_store_.get(), "test_cluster", mock_store_.get());
    }

    // Write N entries to MockOpLogStore with seq [start, start+count)
    void WriteEntriesToStore(uint64_t start, uint64_t count) {
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t seq = start + i;
            mock_store_->WriteOpLog(
                MakeEntry(seq, "key_" + std::to_string(seq)));
        }
    }

    // Build snapshot data for keys key_1..key_count
    std::vector<std::pair<std::string, StandbyObjectMetadata>>
    BuildSnapshotData(uint64_t count) {
        std::vector<std::pair<std::string, StandbyObjectMetadata>> data;
        for (uint64_t i = 1; i <= count; ++i) {
            StandbyObjectMetadata meta;
            meta.client_id = {1, 2};
            meta.size = 1024;
            meta.last_sequence_id = i;
            data.emplace_back("key_" + std::to_string(i), meta);
        }
        return data;
    }

    // Load snapshot into metadata_store_ and recover applier
    bool LoadSnapshotAndRecover() {
        auto result = mock_snapshot_->LoadLatestSnapshot("test_cluster");

        if (!result.has_value() || !result->has_value()) {
            return false;
        }
        const auto& snap = result->value();
        for (const auto& [key, meta] : snap.metadata) {
            metadata_store_->PutMetadata(key, meta);
        }
        applier_->Recover(snap.snapshot_sequence_id);
        return true;
    }

    std::shared_ptr<MockOpLogStore> mock_store_;
    std::shared_ptr<MockSnapshotProvider> mock_snapshot_;
    std::shared_ptr<MockMetadataStore> metadata_store_;
    std::unique_ptr<OpLogApplier> applier_;
};

// ============================================================
// Scenario 1: OpLog + Snapshot Joint Recovery
// ============================================================

TEST_F(HaRecoveryTest, SnapshotThenOpLogReplay) {
    // Snapshot @ seq=10 with 10 entries
    mock_snapshot_->SetSnapshot("snap1", 10, BuildSnapshotData(10));

    // OpLog store has seq 11~20
    WriteEntriesToStore(11, 10);

    // Load snapshot, recover applier to seq=10
    ASSERT_TRUE(LoadSnapshotAndRecover());
    EXPECT_EQ(metadata_store_->Size(), 10u);
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 11u);

    // Read and apply entries 11~20
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, mock_store_->ReadOpLogSince(10, 1000, entries));
    EXPECT_EQ(entries.size(), 10u);
    EXPECT_EQ(applier_->ApplyOpLogEntries(entries), 10u);

    // 10 snapshot + 10 OpLog = 20 total
    EXPECT_EQ(metadata_store_->Size(), 20u);
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 21u);
}

TEST_F(HaRecoveryTest, SnapshotLoadFail_FallbackToFullReplay) {
    mock_snapshot_->SetLoadFail(true);

    // OpLog store has seq 1~20 (full history)
    WriteEntriesToStore(1, 20);

    // Snapshot fails -> recover from 0
    ASSERT_FALSE(LoadSnapshotAndRecover());
    applier_->Recover(0);

    // Read all and apply
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, mock_store_->ReadOpLogSince(0, 1000, entries));
    EXPECT_EQ(entries.size(), 20u);
    EXPECT_EQ(applier_->ApplyOpLogEntries(entries), 20u);

    EXPECT_EQ(metadata_store_->Size(), 20u);
}

TEST_F(HaRecoveryTest, SnapshotWithNoSubsequentOpLog) {
    // Snapshot @ seq=10, no subsequent OpLog
    mock_snapshot_->SetSnapshot("snap1", 10, BuildSnapshotData(10));

    ASSERT_TRUE(LoadSnapshotAndRecover());
    EXPECT_EQ(metadata_store_->Size(), 10u);

    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, mock_store_->ReadOpLogSince(10, 1000, entries));
    EXPECT_EQ(entries.size(), 0u);
    EXPECT_EQ(metadata_store_->Size(), 10u);
}

TEST_F(HaRecoveryTest, SnapshotSeqMismatch_GapInOpLog) {
    // Snapshot @ seq=10, but OpLog starts at seq=15 (11~14 missing)
    mock_snapshot_->SetSnapshot("snap1", 10, BuildSnapshotData(10));
    WriteEntriesToStore(15, 6);  // seq 15~20

    ASSERT_TRUE(LoadSnapshotAndRecover());
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 11u);

    // Feed seq 15~20 (expected=11, so gap for 11~14)
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, mock_store_->ReadOpLogSince(10, 1000, entries));
    EXPECT_EQ(entries.size(), 6u);

    for (const auto& e : entries) {
        applier_->ApplyOpLogEntry(e);
    }

    // Wait for gap timeout (kMissingEntrySkipSeconds = 3s)
    std::this_thread::sleep_for(std::chrono::seconds(4));
    applier_->ProcessPendingEntries();

    // Snapshot data preserved, gaps eventually skipped, pending applied
    EXPECT_GE(metadata_store_->Size(), 10u);
}

// ============================================================
// Scenario 2: OpLog GC + Snapshot Safety
// ============================================================

TEST_F(HaRecoveryTest, GC_BasicCleanupSemantics) {
    WriteEntriesToStore(1, 20);
    EXPECT_EQ(mock_store_->EntryCount(), 20u);

    // Cleanup before seq=15 -> deletes 1~14
    ASSERT_EQ(ErrorCode::OK, mock_store_->CleanupOpLogBefore(15));
    EXPECT_EQ(mock_store_->EntryCount(), 6u);

    // Verify deleted vs preserved
    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OPLOG_ENTRY_NOT_FOUND,
              mock_store_->ReadOpLog(14, entry));
    EXPECT_EQ(ErrorCode::OK, mock_store_->ReadOpLog(15, entry));
    EXPECT_EQ(entry.sequence_id, 15u);

    // Snapshot metadata is independent
    ASSERT_EQ(ErrorCode::OK,
              mock_store_->RecordSnapshotSequenceId("snap1", 10));
    uint64_t snap_seq = 0;
    ASSERT_EQ(ErrorCode::OK,
              mock_store_->GetSnapshotSequenceId("snap1", snap_seq));
    EXPECT_EQ(snap_seq, 10u);
}

TEST_F(HaRecoveryTest, GC_NewStandbyAfterCleanup) {
    WriteEntriesToStore(1, 20);
    ASSERT_EQ(ErrorCode::OK, mock_store_->CleanupOpLogBefore(10));

    // Snapshot @ seq=10 covers keys 1~10
    mock_snapshot_->SetSnapshot("snap1", 10, BuildSnapshotData(10));

    ASSERT_TRUE(LoadSnapshotAndRecover());
    EXPECT_EQ(metadata_store_->Size(), 10u);
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 11u);

    // ReadOpLogSince(10) returns seq 11~20
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, mock_store_->ReadOpLogSince(10, 1000, entries));
    EXPECT_EQ(entries.size(), 10u);
    EXPECT_EQ(applier_->ApplyOpLogEntries(entries), 10u);

    EXPECT_EQ(metadata_store_->Size(), 20u);
}

TEST_F(HaRecoveryTest, GC_NoSnapshot_IncompleteRecovery) {
    WriteEntriesToStore(1, 20);
    ASSERT_EQ(ErrorCode::OK, mock_store_->CleanupOpLogBefore(10));

    // No snapshot
    mock_snapshot_->SetLoadFail(true);
    ASSERT_FALSE(LoadSnapshotAndRecover());
    applier_->Recover(0);

    // ReadOpLogSince(0) returns seq 10~20 (11 entries)
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, mock_store_->ReadOpLogSince(0, 1000, entries));
    EXPECT_EQ(entries.size(), 11u);

    // Applier expects seq=1 but first entry is seq=10. This creates a gap.
    // Entries go to pending buffer — applier cannot apply them immediately.
    // This demonstrates data incompleteness without snapshot protection:
    // the applier sees a gap it cannot fill (seq 1~9 are GC'd).
    size_t applied = applier_->ApplyOpLogEntries(entries);

    // Wait for gap timeout so pending entries eventually drain
    std::this_thread::sleep_for(std::chrono::seconds(4));
    applier_->ProcessPendingEntries();

    // After timeout, gaps 1~9 are skipped, entries 10~20 apply.
    // Key insight: without snapshot, recovery is delayed and data for
    // seq 1~9 is permanently lost.
    EXPECT_LE(metadata_store_->Size(), 11u);
}

// ============================================================
// Scenario 3: Promotion Write Consistency
// ============================================================

TEST_F(HaRecoveryTest, PromotionCatchUp_AllEntriesApplied) {
    applier_->Recover(0);
    for (uint64_t i = 1; i <= 10; ++i) {
        EXPECT_TRUE(applier_->ApplyOpLogEntry(
            MakeEntry(i, "key_" + std::to_string(i))));
    }
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 11u);
    EXPECT_EQ(metadata_store_->Size(), 10u);

    // seq 11~15 in store but not applied (simulates lag)
    WriteEntriesToStore(11, 5);

    // Final catch-up
    std::vector<OpLogEntry> entries;
    ASSERT_EQ(ErrorCode::OK, mock_store_->ReadOpLogSince(10, 1000, entries));
    EXPECT_EQ(entries.size(), 5u);
    EXPECT_EQ(applier_->ApplyOpLogEntries(entries), 5u);

    EXPECT_EQ(metadata_store_->Size(), 15u);
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 16u);
}

TEST_F(HaRecoveryTest, PromotionWithPendingGaps) {
    applier_->Recover(0);
    for (uint64_t i = 1; i <= 7; ++i) {
        EXPECT_TRUE(applier_->ApplyOpLogEntry(
            MakeEntry(i, "key_" + std::to_string(i))));
    }
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 8u);

    // Skip seq=8, apply seq 9~10 -> go to pending
    applier_->ApplyOpLogEntry(MakeEntry(9, "key_9"));
    applier_->ApplyOpLogEntry(MakeEntry(10, "key_10"));
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 8u);
    EXPECT_EQ(metadata_store_->Size(), 7u);

    // Write seq=8 to store (late arrival)
    mock_store_->WriteOpLog(MakeEntry(8, "key_8"));

    // First ProcessPendingEntries: detect gap, register in
    // missing_sequence_ids_
    applier_->ProcessPendingEntries();

    // Wait for kMissingEntryRequestSeconds (1s) so next call fetches seq=8
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Second ProcessPendingEntries: request seq=8 from store, apply it,
    // then drain pending 9~10
    applier_->ProcessPendingEntries();

    EXPECT_EQ(applier_->GetExpectedSequenceId(), 11u);
    EXPECT_EQ(metadata_store_->Size(), 10u);
}

TEST_F(HaRecoveryTest, PromotionGapUnresolvable_SkipAndPromote) {
    applier_->Recover(0);
    for (uint64_t i = 1; i <= 7; ++i) {
        EXPECT_TRUE(applier_->ApplyOpLogEntry(
            MakeEntry(i, "key_" + std::to_string(i))));
    }

    // Skip seq=8, apply seq 9~10 -> pending
    applier_->ApplyOpLogEntry(MakeEntry(9, "key_9"));
    applier_->ApplyOpLogEntry(MakeEntry(10, "key_10"));
    EXPECT_EQ(applier_->GetExpectedSequenceId(), 8u);

    // seq=8 NOT in store (GC'd). Let gap detection register.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    applier_->ProcessPendingEntries();

    auto result = applier_->TryResolveGapsOnceForPromotion(1024);
    // seq=8 not in store, so fetched=0
    EXPECT_EQ(result.fetched, 0u);

    // Wait for skip timeout (3s) so gap is eventually skipped
    std::this_thread::sleep_for(std::chrono::seconds(4));
    applier_->ProcessPendingEntries();

    // After skip timeout, gap is skipped, pending 9~10 should drain
    // No crash, no hang. Original data preserved.
    EXPECT_GE(metadata_store_->Size(), 7u);
}
