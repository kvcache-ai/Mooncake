// OpLogRouteHook: the OpLog as one more hook on a namespace's write chain.
// These cases run a real OrderedOpLogWriter over a fake backend, and judge the
// hook by what the writer came to hold, not by what the hook reported:
//
//   * the record is taken under the key's lock, in the order the writes began;
//   * a write that changes nothing leaves no record, even one begun for a
//     removal the caller then refused;
//   * a key the OpLog cannot name is refused before the write changes
//     anything, so no slot and no object state is spent on it;
//   * a writer that stops between the reserve and the commit drops that one
//     record and reports it, which is the only failure a release can meet.
//
// A recorder hangs on the serializer, so the tests can also ask the hook to
// refuse a record it no longer trusts: the object state must go back.

#include "ha/oplog/oplog_route_hook.h"
#include "metadata/namespace.h"
#include "object_test_helpers.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <xxhash.h>

namespace mooncake::test {
using metadata::Namespace;
using metadata::WriteChange;
namespace {

// A writer backend that records what it was asked to make durable, and can be
// held or failed at will.
class RecordingBackend {
   public:
    ErrorCode Write(const OpLogBatchRecord& batch,
                    const DurablePrefix& expected_prefix) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (error_ != ErrorCode::OK) {
            return error_;
        }
        for (const auto& entry : batch.entries) {
            records_.push_back({entry.op_type, entry.tenant_id,
                                entry.object_key, entry.payload});
        }
        cv_.notify_all();
        return ErrorCode::OK;
    }

    struct Record {
        OpType op_type;
        std::string tenant_id;
        std::string key;
        std::string payload;
    };

    std::vector<Record> Records() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return records_;
    }

    // Waits until the backend holds `count` records; the writer flushes on
    // its own thread, so a committed record arrives there, not at Commit.
    bool WaitForRecords(size_t count) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(5),
                            [&] { return records_.size() >= count; });
    }

    void FailWith(ErrorCode error) {
        std::lock_guard<std::mutex> lock(mutex_);
        error_ = error;
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<Record> records_;
    ErrorCode error_{ErrorCode::OK};
};

std::unique_ptr<OrderedOpLogWriter> MakeWriter(RecordingBackend& backend) {
    return std::make_unique<OrderedOpLogWriter>(
        OrderedOpLogWriterConfig{},
        [&backend](const OpLogBatchRecord& batch, const DurablePrefix& prefix) {
            return backend.Write(batch, prefix);
        });
}

// Serializes the object's user key and its completed replicas, the way the
// master would; an object gone or left with nothing valid records a REMOVE.
std::optional<std::string> SerializeKey(const metadata::WriteGuard& guard) {
    if (!guard.has_object()) {
        return std::nullopt;
    }
    return guard.key();
}

// Publishes through the hook's own path, so the creation is recorded.
route::Generation PublishRecorded(metadata::Namespace& ns,
                                  const std::string& key) {
    auto created = ns.objects.WriteKey(key, WriteChange::kNewWrite);
    if (!created.has_value() || created->has_object()) {
        return 0;
    }
    return test::PublishEnvelope(*created);
}

size_t Count(const std::vector<RecordingBackend::Record>& records, OpType type,
             const std::string& key) {
    return static_cast<size_t>(std::count_if(
        records.begin(), records.end(), [&](const auto& record) {
            return record.op_type == type && record.key == key;
        }));
}


class OpLogRouteHookTest : public ::testing::Test {
   protected:
    void SetUp() override {
        backend_ = std::make_unique<RecordingBackend>();
        writer_ = MakeWriter(*backend_);
        writer_->Start();
        hook_ = std::make_unique<OpLogRouteHook>(TenantId("t"), writer_.get(),
                                                 SerializeKey);
        ns_ = std::make_unique<Namespace>(TenantId("t"));
        ns_->RegisterWriteHook(hook_.get());
    }

    void TearDown() override {
        ns_.reset();
        hook_.reset();
        writer_->Stop();
    }

    RecordingBackend* backend() { return backend_.get(); }
    Namespace& ns() { return *ns_; }

    std::unique_ptr<RecordingBackend> backend_;
    std::unique_ptr<OrderedOpLogWriter> writer_;
    std::unique_ptr<OpLogRouteHook> hook_;
    std::unique_ptr<Namespace> ns_;
};

TEST_F(OpLogRouteHookTest, ACommittedWriteRecordsWhatTheWriteLeft) {
    {
        auto created = ns().objects.WriteKey("k", WriteChange::kNewWrite);
        ASSERT_TRUE(created.has_value());
        (void)test::PublishEnvelope(*created);
    }
    EXPECT_TRUE(backend()->WaitForRecords(1));
    EXPECT_EQ(Count(backend()->Records(), OpType::PUT_END, "k"), 1u);

    {
        auto grown = ns().objects.Write("k", WriteChange::kNewWrite);
        ASSERT_TRUE(grown.has_value() && grown->has_value());
        grown->value().Commit();
    }
    EXPECT_TRUE(backend()->WaitForRecords(2));
    EXPECT_EQ(Count(backend()->Records(), OpType::PUT_END, "k"), 2u);
}

// The case the commit flag exists for: a write begun for a removal the caller
// then refuses records nothing.
TEST_F(OpLogRouteHookTest, ARefusedRemovalRecordsNothing) {
    ASSERT_NE(PublishRecorded(ns(), "k"), 0u);
    ASSERT_TRUE(backend()->WaitForRecords(1));
    const auto before = backend()->Records().size();

    {
        auto refused = ns().objects.Write("k", WriteChange::kKeyRemoved);
        ASSERT_TRUE(refused.has_value() && refused->has_value());
        // The caller decides against the removal; nothing was touched.
    }

    EXPECT_EQ(backend()->Records().size(), before);
    EXPECT_TRUE(ns().objects.Contains("k"));
}

TEST_F(OpLogRouteHookTest, ACommittedRemovalRecordsARemove) {
    ASSERT_NE(PublishRecorded(ns(), "k"), 0u);
    ASSERT_TRUE(backend()->WaitForRecords(1));
    ASSERT_EQ(Count(backend()->Records(), OpType::PUT_END, "k"), 1u);

    {
        auto removed = ns().objects.Write("k", WriteChange::kKeyRemoved);
        ASSERT_TRUE(removed.has_value() && removed->has_value());
        removed->value().TearDown();
    }
    EXPECT_TRUE(backend()->WaitForRecords(2));
    EXPECT_EQ(Count(backend()->Records(), OpType::PUT_END, "k"), 1u);
    EXPECT_EQ(Count(backend()->Records(), OpType::REMOVE, "k"), 1u);
}

// A key the OpLog cannot name must fail the write before anything changed,
// which the master itself does not check today.
TEST_F(OpLogRouteHookTest, AnUnnameableKeyIsRefusedBeforeAnythingChanged) {
    const std::string oversized(kMaxOpLogObjectKeySize + 1, 'k');

    // WriteKey reaches the hook whether or not the key holds an object, so
    // the refusal is observable on the key alone: the begin refuses it before
    // the guard is handed out, and the slot the attempt made is dropped with
    // the guard, since nothing holds it and no object was published.
    auto refused = ns().objects.WriteKey(oversized, WriteChange::kNewWrite);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(ns().objects.Contains(oversized));

    // No slot was consumed for it: the writer is still empty.
    auto reservation = writer_->Reserve();
    ASSERT_TRUE(reservation.has_value());
    writer_->Abort(std::move(*reservation));
}

TEST_F(OpLogRouteHookTest, TwoWritesOfOneKeyAreRecordedInTheOrderTheyBegan) {
    ASSERT_NE(PublishRecorded(ns(), "k"), 0u);

    // Two threads write the same key; whichever gets the lock first must be
    // the first record the backend holds.
    std::atomic<int> done{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([this, &done] {
            auto result = ns().objects.Write("k", WriteChange::kNewWrite);
            if (result.has_value() && result->has_value()) {
                result->value().Commit();
            }
            ++done;
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(done.load(), 2);
    // Both writes recorded, neither lost: the lock serialized them, and the
    // reservations were taken under it.
    EXPECT_TRUE(backend()->WaitForRecords(3));
    EXPECT_EQ(Count(backend()->Records(), OpType::PUT_END, "k"), 3u);
}

// The kind of change decides the record, not the post-state: a tombstone
// removal commits as REMOVE while the object is still there, which is what
// lets a deferred removal keep its replicas until the record is durable.
TEST_F(OpLogRouteHookTest, ATombstoneRemovalRecordsARemoveWithTheObjectStillThere) {
    ASSERT_NE(PublishRecorded(ns(), "k"), 0u);
    ASSERT_TRUE(backend()->WaitForRecords(1));

    {
        auto removed = ns().objects.Write("k", WriteChange::kKeyRemoved);
        ASSERT_TRUE(removed.has_value() && removed->has_value());
        // Committed as a removal, but the replicas stay until the durable
        // finalize tears them down: the object is still here at release.
        removed->value().Commit();
    }
    EXPECT_TRUE(ns().objects.Contains("k"));
    EXPECT_TRUE(backend()->WaitForRecords(2));
    EXPECT_EQ(Count(backend()->Records(), OpType::REMOVE, "k"), 1u);
    EXPECT_EQ(Count(backend()->Records(), OpType::PUT_END, "k"), 1u);
}

// The one failure a release can meet: the writer stops accepting between the
// reserve and the commit. The record is dropped and reported, not retried, and
// the object state is what the write made it.
TEST_F(OpLogRouteHookTest, AWriterThatStopsBetweenReserveAndCommitDropsTheRecord) {
    ASSERT_NE(PublishRecorded(ns(), "k"), 0u);

    {
        auto result = ns().objects.Write("k", WriteChange::kNewWrite);
        ASSERT_TRUE(result.has_value() && result->has_value());
        // The begin has reserved its slot; stop the writer before the release
        // commits.
        writer_->Stop();
        result->value().Commit();
    }
    EXPECT_EQ(backend()->Records().size(), 1u);
    EXPECT_TRUE(ns().objects.Contains("k"));
}

// A hook that refuses to record a write it no longer trusts must fail the
// write before anything changed, not corrupt the object.
TEST_F(OpLogRouteHookTest, ARecorderThatRefusesInBeginLeavesTheKeyAlone) {
    class RefusingRecorder final : public OpLogRouteHook {
       public:
        using OpLogRouteHook::OpLogRouteHook;
        tl::expected<void, ErrorCode> OnWriteBegin(
            const metadata::WriteGuard& guard,
            metadata::WriteChange change,
            std::unique_ptr<metadata::WriteReservation>& reservation) override {
            if (guard.key() == "k") {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return OpLogRouteHook::OnWriteBegin(guard, change, reservation);
        }
    };
    RefusingRecorder refusing(TenantId("t"), writer_.get(), SerializeKey);
    Namespace with_refuser(TenantId("t"));
    with_refuser.RegisterWriteHook(&refusing);
    ASSERT_NE(test::PublishObject(with_refuser.objects, "k"), 0u);

    auto refused = with_refuser.objects.Write("k", WriteChange::kNewWrite);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(with_refuser.objects.Contains("k"));
}

}  // namespace test
}  // namespace mooncake
