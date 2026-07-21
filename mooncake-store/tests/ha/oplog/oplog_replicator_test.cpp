#include "ha/oplog/oplog_replicator.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <xxhash.h>

#include "metadata_store.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_change_notifier.h"
#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_serializer.h"
#include "ha/oplog/polling_oplog_change_notifier.h"
#include "mock_oplog_store.h"
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
                    ErrorCallback on_error,
                    MaintenanceCallback on_maintenance = {}) override {
        on_entry_ = std::move(on_entry);
        on_error_ = std::move(on_error);
        on_maintenance_ = std::move(on_maintenance);
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
    void RunMaintenance(const std::vector<uint64_t>& missing_sequences = {}) {
        if (on_maintenance_) on_maintenance_(missing_sequences);
    }

   private:
    EntryCallback on_entry_;
    ErrorCallback on_error_;
    MaintenanceCallback on_maintenance_;
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

class SparseReadOpLogStore : public MockOpLogStore {
   public:
    ErrorCode ReadOpLogSinceWithProgress(uint64_t start_sequence_id,
                                         size_t /*limit*/,
                                         std::vector<OpLogEntry>& entries,
                                         OpLogReadProgress& progress) override {
        entries.clear();
        progress.last_scanned_sequence_id = start_sequence_id;
        if (start_sequence_id == 0) {
            entries.push_back(MakeEntry(1, OpType::REMOVE, "key1", ""));
            entries.push_back(MakeEntry(3, OpType::REMOVE, "key3", ""));
            progress.last_scanned_sequence_id = 4;
        }
        return ErrorCode::OK;
    }
};

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

TEST_F(OpLogReplicatorTest, FatalApplyFailureMarksReplicatorUnhealthy) {
    bool fatal_error_received = false;
    replicator_->SetStateCallback([&](StandbyEvent event) {
        if (event == StandbyEvent::FATAL_ERROR) {
            fatal_error_received = true;
        }
    });

    replicator_->StartFromSequenceId(0);
    Notifier().InjectEntry(
        MakeEntry(1, static_cast<OpType>(99), "key1", "payload"));

    EXPECT_TRUE(fatal_error_received);
    EXPECT_FALSE(replicator_->IsHealthy());
    EXPECT_EQ(0u, replicator_->GetLastProcessedSequenceId());
    EXPECT_EQ(1u, applier_->GetFailedSequenceId());
}

TEST_F(OpLogReplicatorTest, MaintenanceProcessesFinalGap) {
    replicator_->StartFromSequenceId(0);

    Notifier().InjectEntry(MakeEntry(2, OpType::REMOVE, "key2", ""));
    EXPECT_EQ(0u, replicator_->GetLastProcessedSequenceId());
    EXPECT_EQ(1u, applier_->GetExpectedSequenceId());

    Notifier().RunMaintenance();
    std::this_thread::sleep_for(std::chrono::milliseconds(3100));
    Notifier().RunMaintenance();

    EXPECT_EQ(2u, replicator_->GetLastProcessedSequenceId());
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
}

TEST_F(OpLogReplicatorTest, ConfirmedGapIsProcessedImmediately) {
    replicator_->StartFromSequenceId(0);

    Notifier().InjectEntry(MakeEntry(2, OpType::REMOVE, "key2", ""));
    Notifier().RunMaintenance({1});

    EXPECT_EQ(2u, replicator_->GetLastProcessedSequenceId());
    EXPECT_EQ(3u, applier_->GetExpectedSequenceId());
}

TEST(PollingOpLogChangeNotifierTest, ReportsMiddleAndTrailingGaps) {
    SparseReadOpLogStore store;
    PollingOpLogChangeNotifier notifier(&store, /*poll_interval_ms=*/10);
    std::atomic<int> maintenance_count{0};
    std::atomic<size_t> max_missing_count{0};

    ASSERT_EQ(
        ErrorCode::OK,
        notifier.Start(/*start_sequence_id=*/0, [](const OpLogEntry&) {},
                       [](ErrorCode) {},
                       [&](const std::vector<uint64_t>& missing) {
                           maintenance_count.fetch_add(1);
                           size_t current = max_missing_count.load();
                           while (current < missing.size() &&
                                  !max_missing_count.compare_exchange_weak(
                                      current, missing.size())) {
                           }
                       }));

    for (int i = 0; i < 50 && maintenance_count.load() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    notifier.Stop();

    EXPECT_GT(maintenance_count.load(), 0);
    EXPECT_EQ(2u, max_missing_count.load());
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
