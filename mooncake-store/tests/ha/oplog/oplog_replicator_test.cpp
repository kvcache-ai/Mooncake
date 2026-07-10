#include "ha/oplog/oplog_replicator.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <xxhash.h>

#include "metadata_store.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_change_notifier.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_serializer.h"
#include "types.h"

namespace mooncake::test {

// Minimal MetadataStore implementation for OpLogApplier
class MinimalMockMetadataStore : public MetadataStore {
   public:
    bool PutMetadata(const std::string&,
                     const StandbyObjectMetadata&) override {
        return true;
    }
    bool Put(const std::string&, const std::string&) override { return true; }
    std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string&) const override {
        return std::nullopt;
    }
    bool Remove(const std::string&) override { return true; }
    bool Exists(const std::string&) const override { return false; }
    size_t GetKeyCount() const override { return 0; }
};

// In-memory MockOpLogChangeNotifier for tests
class MockOpLogChangeNotifier : public OpLogChangeNotifier {
   public:
    ErrorCode Start(uint64_t /*start_seq*/, EntryCallback on_entry,
                    ErrorCallback on_error) override {
        on_entry_ = std::move(on_entry);
        on_error_ = std::move(on_error);
        healthy_ = true;
        return ErrorCode::OK;
    }
    void Stop() override { healthy_ = false; }
    bool IsHealthy() const override { return healthy_; }

    // Test helpers
    void InjectEntry(const OpLogEntry& entry) {
        if (on_entry_) on_entry_(entry);
    }
    void InjectError(ErrorCode err) {
        if (on_error_) on_error_(err);
    }

   private:
    EntryCallback on_entry_;
    ErrorCallback on_error_;
    bool healthy_{false};
};

// Helper function to create a valid OpLogEntry with checksum
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
    e.checksum =
        static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
    e.prefix_hash =
        key.empty() ? 0
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    return e;
}

class OpLogReplicatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogReplicatorTest");
        FLAGS_logtostderr = 1;
        metadata_store_ = std::make_unique<MinimalMockMetadataStore>();
        applier_ =
            std::make_unique<OpLogApplier>(metadata_store_.get(), "test");
        notifier_ = std::make_unique<MockOpLogChangeNotifier>();
        replicator_ =
            std::make_unique<OpLogReplicator>(notifier_.get(), applier_.get());
    }

    void TearDown() override {
        if (replicator_) {
            replicator_->Stop();
        }
        google::ShutdownGoogleLogging();
    }

    MockOpLogChangeNotifier& Notifier() { return *notifier_; }

    std::unique_ptr<MinimalMockMetadataStore> metadata_store_;
    std::unique_ptr<OpLogApplier> applier_;
    std::unique_ptr<MockOpLogChangeNotifier> notifier_;
    std::unique_ptr<OpLogReplicator> replicator_;
};

// ========== Start/Stop tests ==========

TEST_F(OpLogReplicatorTest, TestStartStop) {
    EXPECT_TRUE(replicator_->StartFromSequenceId(0));
    EXPECT_TRUE(replicator_->IsHealthy());
    replicator_->Stop();
    EXPECT_FALSE(replicator_->IsHealthy());
}

TEST_F(OpLogReplicatorTest, TestStartFromSequenceId) {
    EXPECT_TRUE(replicator_->StartFromSequenceId(100));
    EXPECT_TRUE(replicator_->IsHealthy());
}

// ========== Entry delivery tests ==========

TEST_F(OpLogReplicatorTest, InjectEntry_Applied) {
    replicator_->StartFromSequenceId(0);

    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    Notifier().InjectEntry(entry);

    EXPECT_EQ(1u, replicator_->GetLastProcessedSequenceId());
    EXPECT_EQ(2u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogReplicatorTest, InjectMultipleEntries_SequenceTracking) {
    replicator_->StartFromSequenceId(0);

    Notifier().InjectEntry(MakeEntry(1, OpType::PUT_END, "k1", "p1"));
    Notifier().InjectEntry(MakeEntry(2, OpType::PUT_END, "k2", "p2"));
    Notifier().InjectEntry(MakeEntry(3, OpType::REMOVE, "k3", ""));

    EXPECT_EQ(3u, replicator_->GetLastProcessedSequenceId());
    EXPECT_EQ(4u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogReplicatorTest, InjectError_NotifiesCallback) {
    bool error_received = false;
    replicator_->SetStateCallback([&](StandbyEvent event) {
        if (event == StandbyEvent::WATCH_BROKEN) {
            error_received = true;
        }
    });

    replicator_->StartFromSequenceId(0);
    Notifier().InjectError(ErrorCode::ETCD_OPERATION_ERROR);

    EXPECT_TRUE(error_received);
}

// ========== Utility tests ==========

TEST_F(OpLogReplicatorTest, GetLastProcessedSequenceId_InitiallyZero) {
    EXPECT_EQ(0u, replicator_->GetLastProcessedSequenceId());
}

TEST_F(OpLogReplicatorTest, IsHealthy_FalseBeforeStart) {
    EXPECT_FALSE(replicator_->IsHealthy());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
