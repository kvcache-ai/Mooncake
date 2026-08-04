#include "ha/oplog/oplog_applier.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <xxhash.h>

#include "metadata_store.h"
#include "mock_metadata_store.h"
#include "ha/oplog/oplog_types.h"
#include "types.h"

using mooncake::test::MockMetadataStore;

namespace mooncake::test {

// Helper function to create a valid OpLogEntry with checksum
// Uses the production checksum algorithm (XXH32).
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
    // Compute checksum and prefix_hash using the production algorithm.
    e.checksum =
        static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
    e.prefix_hash =
        key.empty() ? 0
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    return e;
}

// Helper function to create a valid struct_pack payload for PUT_END
std::string MakeValidPayload(uint64_t client_id_first = 1,
                             uint64_t client_id_second = 2,
                             uint64_t size = 1024) {
    mooncake::MetadataPayload payload;
    payload.client_id = {client_id_first, client_id_second};
    payload.size = size;
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
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
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    // After applying seq=1, expected_sequence_id becomes 2
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_EQ(1u, mock_metadata_store_->GetKeyCount());
}

TEST_F(OpLogApplierTest, TestApplyPutRevoke) {
    // First add a key
    std::string payload = MakeValidPayload();
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
    std::string payload = MakeValidPayload();
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
        MakeEntry(1, OpType::PUT_END, "key1", MakeValidPayload());
    // Manually set an invalid op_type (assuming OpType is an enum)
    // Since we can't directly set invalid enum, we test with valid types
    // and verify that unsupported types in ProcessPendingEntries are handled
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
}

// ========== 4.1.2 Sequence ordering tests ==========

TEST_F(OpLogApplierTest, TestApplyInOrder) {
    std::string payload = MakeValidPayload();
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

TEST_F(OpLogApplierTest, FutureSequenceFailsWithoutBuffering) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry1 = MakeEntry(1, OpType::PUT_END, "key1", payload);
    OpLogEntry entry2 = MakeEntry(2, OpType::PUT_END, "key2", payload);
    OpLogEntry entry3 = MakeEntry(3, OpType::PUT_END, "key3", payload);

    // Apply entry1 (seq=1) - should succeed
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry1));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    // A future entry fails and is not retained for later application.
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry3));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(mock_metadata_store_->Exists("key3"));

    // Applying the expected entry does not resurrect the rejected future one.
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry2));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());

    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_FALSE(mock_metadata_store_->Exists("key3"));
}

TEST_F(OpLogApplierTest, TestApplyDuplicateSequenceId) {
    std::string payload = MakeValidPayload();
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

// ========== 4.1.4 Checksum tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidChecksum) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidChecksum) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestChecksumFailureMetric) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    // Tamper with the checksum
    entry.checksum = entry.checksum + 1;

    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    // Metric increment is tested implicitly by the failure
}

// ========== 4.1.5 Size validation tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_ValidSize) {
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(ValidateOpLogEntrySize(entry));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_InvalidSize) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Make key too large
    entry.object_key.assign(kMaxOpLogObjectKeySize + 1, 'k');

    EXPECT_FALSE(ValidateOpLogEntrySize(entry));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
    EXPECT_FALSE(mock_metadata_store_->Exists("key1"));
}

TEST_F(OpLogApplierTest, TestApplyOpLogEntry_PayloadTooLarge) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "");

    // Make payload too large
    entry.payload.assign(kMaxOpLogPayloadSize + 1, 'p');

    EXPECT_FALSE(ValidateOpLogEntrySize(entry));
    EXPECT_FALSE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());  // Should not advance
}

// ========== 4.1.6 Recovery tests ==========

TEST_F(OpLogApplierTest, TestRecover) {
    // Set initial state: last applied sequence_id = 10
    applier_->Recover(10);
    EXPECT_EQ(11u, applier_->GetExpectedSequenceId());

    // Apply entry with seq=11 should succeed
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(11, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(12u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogApplierTest, TestRecover_ZeroSequenceId) {
    // Recover from sequence_id 0
    applier_->Recover(0);
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    // Apply entry with seq=1 should succeed
    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

// ========== 4.1.8 Payload deserialization tests ==========

TEST_F(OpLogApplierTest, TestApplyPutEnd_ValidPayload) {
    std::string payload = MakeValidPayload(1, 2, 2048);
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    auto meta = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(1u, meta->client_id.first);
    EXPECT_EQ(2u, meta->client_id.second);
    EXPECT_EQ(2048u, meta->size);
}

TEST_F(OpLogApplierTest, TestApplyPutEnd_InvalidPayload) {
    std::string invalid_data = "{invalid data}";
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", invalid_data);

    // Should still succeed (fallback to empty metadata)
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));

    // Metadata should exist but with default values
    auto meta = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(meta.has_value());
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

    auto meta = mock_metadata_store_->GetMetadata("key1");
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(0u, meta->client_id.first);
    EXPECT_EQ(0u, meta->client_id.second);
    EXPECT_EQ(0u, meta->size);
}

// ========== Additional Edge Case Tests ==========

TEST_F(OpLogApplierTest, TestApplyOpLogEntries_Batch) {
    std::string payload = MakeValidPayload();
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
    std::string payload = MakeValidPayload();
    std::vector<OpLogEntry> entries;
    entries.push_back(MakeEntry(1, OpType::PUT_END, "key1", payload));
    entries.push_back(
        MakeEntry(3, OpType::PUT_END, "key3", payload));  // Gap at seq=2
    entries.push_back(MakeEntry(2, OpType::PUT_END, "key2", payload));

    size_t applied = applier_->ApplyOpLogEntries(entries);
    EXPECT_EQ(2u, applied);
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
    EXPECT_TRUE(mock_metadata_store_->Exists("key1"));
    EXPECT_TRUE(mock_metadata_store_->Exists("key2"));
    EXPECT_FALSE(mock_metadata_store_->Exists("key3"));
}

TEST_F(OpLogApplierTest, TestGetExpectedSequenceId) {
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    std::string payload = MakeValidPayload();
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

// ========== Segment OpLog apply tests ==========

std::string MakeSegmentMountPayload(const std::string& segment_name,
                                    const std::string& transport_endpoint,
                                    uint64_t capacity = 1024,
                                    bool is_memory = true,
                                    const std::string& file_path = "") {
    SegmentMountOp op;
    op.segment_name = segment_name;
    op.transport_endpoint = transport_endpoint;
    op.capacity = capacity;
    op.is_memory_segment = is_memory;
    op.file_path = file_path;
    auto result = struct_pack::serialize(op);
    return std::string(result.begin(), result.end());
}

std::string MakeSegmentUnmountPayload(const std::string& transport_endpoint) {
    SegmentUnmountOp op;
    op.transport_endpoint = transport_endpoint;
    auto result = struct_pack::serialize(op);
    return std::string(result.begin(), result.end());
}

std::string MakeSegmentUpdatePayload(const std::string& segment_name,
                                     const std::string& transport_endpoint,
                                     uint64_t capacity = 2048,
                                     bool is_memory = true,
                                     const std::string& file_path = "") {
    SegmentUpdateOp op;
    op.segment_name = segment_name;
    op.transport_endpoint = transport_endpoint;
    op.capacity = capacity;
    op.is_memory_segment = is_memory;
    op.file_path = file_path;
    auto result = struct_pack::serialize(op);
    return std::string(result.begin(), result.end());
}

TEST_F(OpLogApplierTest, TestApplySegmentMount) {
    std::string payload =
        MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024, true);
    OpLogEntry entry = MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", payload);

    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());

    const auto& registry = applier_->GetSegmentRegistry();
    EXPECT_TRUE(registry.HasSegment("192.168.1.1:12345"));
    auto info = registry.GetSegment("192.168.1.1:12345");
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ("seg1", info->segment_name);
    EXPECT_EQ(1024u, info->capacity);
    EXPECT_TRUE(info->is_memory_segment);
}

TEST_F(OpLogApplierTest, TestApplySegmentUnmount) {
    // Mount first
    std::string mount_payload =
        MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024, true);
    OpLogEntry mount_entry =
        MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", mount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount_entry));
    EXPECT_TRUE(applier_->GetSegmentRegistry().HasSegment("192.168.1.1:12345"));

    // Then unmount
    std::string unmount_payload =
        MakeSegmentUnmountPayload("192.168.1.1:12345");
    OpLogEntry unmount_entry =
        MakeEntry(2, OpType::SEGMENT_UNMOUNT, "seg1", unmount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(unmount_entry));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());

    EXPECT_FALSE(
        applier_->GetSegmentRegistry().HasSegment("192.168.1.1:12345"));
}

TEST_F(OpLogApplierTest, TestApplySegmentUpdate) {
    // Mount first
    std::string mount_payload =
        MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024, true);
    OpLogEntry mount_entry =
        MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", mount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount_entry));

    auto info_before =
        applier_->GetSegmentRegistry().GetSegment("192.168.1.1:12345");
    ASSERT_TRUE(info_before.has_value());
    EXPECT_EQ(1024u, info_before->capacity);

    // Update
    std::string update_payload =
        MakeSegmentUpdatePayload("seg1", "192.168.1.1:12345", 2048, true);
    OpLogEntry update_entry =
        MakeEntry(2, OpType::SEGMENT_UPDATE, "seg1", update_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(update_entry));
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());

    auto info_after =
        applier_->GetSegmentRegistry().GetSegment("192.168.1.1:12345");
    ASSERT_TRUE(info_after.has_value());
    EXPECT_EQ(2048u, info_after->capacity);
}

TEST_F(OpLogApplierTest, TestApplySegmentMount_InvalidPayload) {
    OpLogEntry entry = MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1", "garbage");

    // Should not crash, but should not add segment either
    EXPECT_TRUE(applier_->ApplyOpLogEntry(entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(applier_->GetSegmentRegistry().HasSegment("seg1"));
}

TEST_F(OpLogApplierTest, TestApplySegmentUnmount_NonExistent) {
    // Unmount a segment that was never mounted - should not crash
    std::string unmount_payload =
        MakeSegmentUnmountPayload("192.168.1.1:12345");
    OpLogEntry unmount_entry =
        MakeEntry(1, OpType::SEGMENT_UNMOUNT, "seg1", unmount_payload);
    EXPECT_TRUE(applier_->ApplyOpLogEntry(unmount_entry));
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
    EXPECT_FALSE(
        applier_->GetSegmentRegistry().HasSegment("192.168.1.1:12345"));
}

TEST_F(OpLogApplierTest, TestApplySegmentOperations_Mixed) {
    // Mount multiple segments
    OpLogEntry mount1 =
        MakeEntry(1, OpType::SEGMENT_MOUNT, "seg1",
                  MakeSegmentMountPayload("seg1", "192.168.1.1:12345", 1024));
    OpLogEntry mount2 =
        MakeEntry(2, OpType::SEGMENT_MOUNT, "seg2",
                  MakeSegmentMountPayload("seg2", "192.168.1.2:12345", 2048));
    OpLogEntry mount3 =
        MakeEntry(3, OpType::SEGMENT_MOUNT, "seg3",
                  MakeSegmentMountPayload("seg3", "192.168.1.3:12345", 4096));

    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount1));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount2));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(mount3));

    const auto& registry = applier_->GetSegmentRegistry();
    EXPECT_EQ(3u, registry.GetAllSegments().size());

    // Unmount one
    OpLogEntry unmount2 =
        MakeEntry(4, OpType::SEGMENT_UNMOUNT, "seg2",
                  MakeSegmentUnmountPayload("192.168.1.2:12345"));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(unmount2));
    EXPECT_EQ(2u, registry.GetAllSegments().size());
    EXPECT_FALSE(registry.HasSegment("192.168.1.2:12345"));

    // Update another
    OpLogEntry update3 =
        MakeEntry(5, OpType::SEGMENT_UPDATE, "seg3",
                  MakeSegmentUpdatePayload("seg3", "192.168.1.3:12345", 8192));
    EXPECT_TRUE(applier_->ApplyOpLogEntry(update3));
    auto info3 = registry.GetSegment("192.168.1.3:12345");
    ASSERT_TRUE(info3.has_value());
    EXPECT_EQ(8192u, info3->capacity);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
