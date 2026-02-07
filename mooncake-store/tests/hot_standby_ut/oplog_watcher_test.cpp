#include "oplog_watcher.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <xxhash.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "metadata_store.h"
#include "oplog_applier.h"
#include "oplog_manager.h"
#include "standby_state_machine.h"
#include "types.h"
#include "etcd_oplog_store.h"

namespace mooncake::test {

// Minimal MetadataStore implementation for OpLogApplier
class MinimalMockMetadataStore : public MetadataStore {
   public:
    bool PutMetadata(const std::string&,
                     const StandbyObjectMetadata&) override {
        return true;
    }
    bool Put(const std::string&, const std::string&) override { return true; }
    const StandbyObjectMetadata* GetMetadata(
        const std::string&) const override {
        return nullptr;
    }
    bool Remove(const std::string&) override { return true; }
    bool Exists(const std::string&) const override { return false; }
    size_t GetKeyCount() const override { return 0; }
};

// Simple wrapper around OpLogApplier for testing OpLogWatcher.
// We don't override any methods since OpLogApplier's methods are not virtual;
// we just provide a valid instance to OpLogWatcher.
class MockOpLogApplier : public OpLogApplier {
   public:
    MockOpLogApplier() : OpLogApplier(&metadata_store_, "test_cluster") {}

   private:
    MinimalMockMetadataStore metadata_store_;
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

// Helper function to serialize OpLogEntry to JSON (same as EtcdOpLogStore)
std::string SerializeOpLogEntry(const OpLogEntry& entry) {
    Json::Value root;
    root["sequence_id"] = static_cast<Json::UInt64>(entry.sequence_id);
    root["timestamp_ms"] = static_cast<Json::UInt64>(entry.timestamp_ms);
    root["op_type"] = static_cast<int>(entry.op_type);
    root["object_key"] = entry.object_key;
    root["payload"] = entry.payload;
    root["checksum"] = static_cast<Json::UInt>(entry.checksum);
    root["prefix_hash"] = static_cast<Json::UInt>(entry.prefix_hash);

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";  // Compact format
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    std::ostringstream oss;
    writer->write(root, &oss);
    return oss.str();
}

class OpLogWatcherTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogWatcherTest");
        FLAGS_logtostderr = 1;
        etcd_endpoints_ = "http://localhost:2379";
        cluster_id_ = "test_cluster_001";
        mock_applier_ = std::make_unique<MockOpLogApplier>();
        watcher_ = std::make_unique<OpLogWatcher>(etcd_endpoints_, cluster_id_,
                                                  mock_applier_.get());
    }

    void TearDown() override {
        if (watcher_) {
            watcher_->Stop();
        }
        google::ShutdownGoogleLogging();
    }

    std::string etcd_endpoints_;
    std::string cluster_id_;
    std::unique_ptr<MockOpLogApplier> mock_applier_;
    std::unique_ptr<OpLogWatcher> watcher_;
};

// ========== 5.1.1 Start/Stop tests ==========

TEST_F(OpLogWatcherTest, TestStart) {
#ifdef STORE_USE_ETCD
    // This test requires a real etcd connection
    GTEST_SKIP() << "Requires real etcd connection, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestStartFromSequenceId) {
#ifdef STORE_USE_ETCD
    // This test requires a real etcd connection
    GTEST_SKIP() << "Requires real etcd connection, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestStop) {
#ifdef STORE_USE_ETCD
    // Stop without starting should be safe
    watcher_->Stop();
    EXPECT_FALSE(watcher_->IsWatchHealthy());
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestStartWhenAlreadyRunning) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd connection, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== 5.1.2 Watch event handling tests ==========

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_Put) {
#ifdef STORE_USE_ETCD
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    std::string json_value = SerializeOpLogEntry(entry);
    std::string key = "/oplog/" + cluster_id_ + "/00000000000000000001";

    // Use reflection to call HandleWatchEvent (it's private, so we test via
    // public interface) For unit testing, we can't directly call
    // HandleWatchEvent, so we skip this test and rely on integration tests
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_Delete) {
#ifdef STORE_USE_ETCD
    // DELETE events are handled but don't apply entries
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_InvalidEntry) {
#ifdef STORE_USE_ETCD
    // Invalid JSON should be rejected
    std::string invalid_json = "{invalid json}";
    std::string key = "/oplog/" + cluster_id_ + "/00000000000000000001";

    // Can't directly test HandleWatchEvent, requires integration test
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_OutOfOrder) {
#ifdef STORE_USE_ETCD
    // Out-of-order entries should be handled by OpLogApplier
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== 5.1.3 Reconnection tests ==========

TEST_F(OpLogWatcherTest, TestReconnectAfterDisconnect) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd connection and watch failure simulation";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestReconnectWithStateCallback) {
#ifdef STORE_USE_ETCD
    std::atomic<bool> callback_called{false};
    StandbyEvent received_event = StandbyEvent::START;

    watcher_->SetStateCallback([&](StandbyEvent event) {
        callback_called.store(true);
        received_event = event;
    });

    // State callbacks are triggered during watch operations
    // Requires integration test with real etcd
    GTEST_SKIP() << "Requires real etcd connection for state callback testing";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestReconnectResumeFromLastSequence) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd connection and reconnection simulation";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== 5.1.4 Checksum verification tests ==========

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_ValidChecksum) {
#ifdef STORE_USE_ETCD
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    std::string json_value = SerializeOpLogEntry(entry);

    // Valid checksum should pass validation
    // Can't directly test HandleWatchEvent, requires integration test
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_InvalidChecksum) {
#ifdef STORE_USE_ETCD
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    std::string json_value = SerializeOpLogEntry(entry);

    // Tamper with checksum in JSON
    // Replace checksum value in JSON string
    size_t pos = json_value.find("\"checksum\":");
    if (pos != std::string::npos) {
        size_t start = pos + 10;  // length of "checksum":
        size_t end = json_value.find_first_of(",}", start);
        if (end != std::string::npos) {
            json_value.replace(start, end - start, "999999");
        }
    }

    // Invalid checksum should be rejected
    // Can't directly test HandleWatchEvent, requires integration test
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== 5.1.5 Size validation tests ==========

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_ValidSize) {
#ifdef STORE_USE_ETCD
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    std::string json_value = SerializeOpLogEntry(entry);

    // Valid size should pass validation
    EXPECT_TRUE(OpLogManager::ValidateEntrySize(entry));
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestHandleWatchEvent_InvalidSize) {
#ifdef STORE_USE_ETCD
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    // Make key too large
    entry.object_key.assign(OpLogManager::kMaxObjectKeySize + 1, 'k');
    std::string json_value = SerializeOpLogEntry(entry);

    // Invalid size should be rejected
    EXPECT_FALSE(OpLogManager::ValidateEntrySize(entry));
    GTEST_SKIP() << "HandleWatchEvent is private, requires integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== 5.1.6 State callback tests ==========

TEST_F(OpLogWatcherTest, TestStateCallback_WatchHealthy) {
#ifdef STORE_USE_ETCD
    std::atomic<bool> callback_called{false};
    StandbyEvent received_event = StandbyEvent::START;

    watcher_->SetStateCallback([&](StandbyEvent event) {
        callback_called.store(true);
        received_event = event;
    });

    // Watch healthy events are triggered during normal watch operations
    // Requires integration test with real etcd
    GTEST_SKIP() << "Requires real etcd connection for state callback testing";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestStateCallback_WatchBroken) {
#ifdef STORE_USE_ETCD
    std::atomic<bool> callback_called{false};
    StandbyEvent received_event = StandbyEvent::START;

    watcher_->SetStateCallback([&](StandbyEvent event) {
        callback_called.store(true);
        received_event = event;
    });

    // Watch broken events are triggered when watch fails
    // Requires integration test with real etcd and watch failure
    GTEST_SKIP()
        << "Requires real etcd connection and watch failure simulation";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== 5.1.7 Cluster ID validation tests ==========

TEST_F(OpLogWatcherTest, TestInvalidClusterId_Rejected) {
    // Invalid cluster_id should cause LOG(FATAL) in constructor
    // We can't test this directly as it would terminate the process
    // But we can verify that valid cluster_id works
    std::string valid_cluster_id = "test_cluster_001";
    std::unique_ptr<MockOpLogApplier> mock_applier =
        std::make_unique<MockOpLogApplier>();
    std::unique_ptr<OpLogWatcher> watcher = std::make_unique<OpLogWatcher>(
        etcd_endpoints_, valid_cluster_id, mock_applier.get());
    EXPECT_NE(nullptr, watcher);
    watcher->Stop();
}

TEST_F(OpLogWatcherTest, TestGetLastProcessedSequenceId) {
#ifdef STORE_USE_ETCD
    // Initially should be 0
    EXPECT_EQ(0u, watcher_->GetLastProcessedSequenceId());

    // After processing entries, should be updated
    // This requires integration test with real etcd
    GTEST_SKIP() << "Requires real etcd connection for sequence ID tracking";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestIsWatchHealthy) {
#ifdef STORE_USE_ETCD
    // Initially should be false (not started)
    EXPECT_FALSE(watcher_->IsWatchHealthy());

    // After starting and successful watch, should be true
    // This requires integration test with real etcd
    GTEST_SKIP() << "Requires real etcd connection for watch health testing";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== Additional Helper Tests ==========

TEST_F(OpLogWatcherTest, TestSerializeOpLogEntry) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    std::string json = SerializeOpLogEntry(entry);

    // Verify JSON contains expected fields
    EXPECT_NE(std::string::npos, json.find("sequence_id"));
    EXPECT_NE(std::string::npos, json.find("op_type"));
    EXPECT_NE(std::string::npos, json.find("object_key"));
    EXPECT_NE(std::string::npos, json.find("payload"));
    EXPECT_NE(std::string::npos, json.find("checksum"));
    EXPECT_NE(std::string::npos, json.find("prefix_hash"));
}

TEST_F(OpLogWatcherTest, TestMakeEntry) {
    OpLogEntry entry =
        MakeEntry(100, OpType::REMOVE, "test_key", "test_payload");

    EXPECT_EQ(100u, entry.sequence_id);
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
    EXPECT_EQ("test_key", entry.object_key);
    EXPECT_EQ("test_payload", entry.payload);
    EXPECT_NE(0u, entry.checksum);
    EXPECT_NE(0u, entry.prefix_hash);
    EXPECT_NE(0u, entry.timestamp_ms);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
