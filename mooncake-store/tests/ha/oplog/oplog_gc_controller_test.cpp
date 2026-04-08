#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <ylt/util/tl/expected.hpp>

#include "ha/oplog/oplog_gc_controller.h"
#include "ha/oplog/oplog_store.h"
#include "ha/progress/standby_progress_store.h"

namespace mooncake {
namespace test {
namespace {

int64_t CurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

class FakeOpLogStore final : public ha::OpLogStore {
   public:
    tl::expected<ha::OpLogSequenceId, ErrorCode> Append(
        const ha::OpLogAppendRequest&) override {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    tl::expected<ha::OpLogPollResult, ErrorCode> PollFrom(
        ha::OpLogSequenceId, size_t, std::chrono::milliseconds) override {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    tl::expected<ha::OpLogSequenceId, ErrorCode> GetLatestSequence() override {
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }

    ErrorCode CleanupBefore(ha::OpLogSequenceId before_sequence_id) override {
        cleanup_calls.push_back(before_sequence_id);
        return cleanup_result;
    }

    ErrorCode cleanup_result{ErrorCode::OK};
    std::vector<ha::OpLogSequenceId> cleanup_calls;
};

class FakeStandbyProgressStore final : public ha::StandbyProgressStore {
   public:
    ErrorCode Publish(const ha::StandbyProgressRecord&) override {
        return ErrorCode::OK;
    }

    tl::expected<std::vector<ha::StandbyProgressRecord>, ErrorCode> List()
        override {
        if (list_error != ErrorCode::OK) {
            return tl::make_unexpected(list_error);
        }
        return records;
    }

    ErrorCode Delete(std::string_view) override { return ErrorCode::OK; }

    ErrorCode list_error{ErrorCode::OK};
    std::vector<ha::StandbyProgressRecord> records;
};

ha::SnapshotDescriptor MakeSnapshotDescriptor(ha::OpLogSequenceId seq) {
    ha::SnapshotDescriptor descriptor;
    descriptor.snapshot_id = "snapshot-001";
    descriptor.last_included_seq = seq;
    return descriptor;
}

ha::StandbyProgressRecord MakeProgressRecord(
    std::string standby_id, ha::StandbyProgressMode mode,
    ha::OpLogSequenceId applied_seq_id, ha::OpLogSequenceId required_seq_id,
    int64_t updated_at_ms) {
    ha::StandbyProgressRecord record;
    record.standby_id = std::move(standby_id);
    record.mode = mode;
    record.applied_seq_id = applied_seq_id;
    record.required_seq_id = required_seq_id;
    record.updated_at_ms = updated_at_ms;
    return record;
}

TEST(OpLogGcControllerTest, UsesSnapshotBoundaryWithoutBlockingStandby) {
    auto oplog_store = std::make_shared<FakeOpLogStore>();
    auto progress_store = std::make_shared<FakeStandbyProgressStore>();
    const int64_t now_ms = CurrentTimeMs();
    progress_store->records.push_back(MakeProgressRecord(
        "standby-snapshot", ha::StandbyProgressMode::kSnapshotOnly, 100, 0,
        now_ms));

    ha::OpLogGcController controller(oplog_store, progress_store);

    EXPECT_EQ(ErrorCode::OK,
              controller.CleanupAfterSnapshot(MakeSnapshotDescriptor(10)));
    ASSERT_EQ(1u, oplog_store->cleanup_calls.size());
    EXPECT_EQ(11u, oplog_store->cleanup_calls.front());
}

TEST(OpLogGcControllerTest, UsesFreshFollowerRequirementAsGcBoundary) {
    auto oplog_store = std::make_shared<FakeOpLogStore>();
    auto progress_store = std::make_shared<FakeStandbyProgressStore>();
    const int64_t now_ms = CurrentTimeMs();
    progress_store->records.push_back(MakeProgressRecord(
        "standby-following", ha::StandbyProgressMode::kOplogFollowing, 5, 6,
        now_ms));
    progress_store->records.push_back(MakeProgressRecord(
        "standby-snapshot", ha::StandbyProgressMode::kSnapshotOnly, 10, 0,
        now_ms));

    ha::OpLogGcController controller(oplog_store, progress_store);

    EXPECT_EQ(ErrorCode::OK,
              controller.CleanupAfterSnapshot(MakeSnapshotDescriptor(20)));
    ASSERT_EQ(1u, oplog_store->cleanup_calls.size());
    EXPECT_EQ(6u, oplog_store->cleanup_calls.front());
}

TEST(OpLogGcControllerTest, IgnoresStaleFollowerProgress) {
    auto oplog_store = std::make_shared<FakeOpLogStore>();
    auto progress_store = std::make_shared<FakeStandbyProgressStore>();
    const auto stale_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            ha::standby_progress_store_detail::kStandbyProgressFreshness)
            .count() +
        1000;
    progress_store->records.push_back(MakeProgressRecord(
        "stale-following", ha::StandbyProgressMode::kOplogFollowing, 2, 3,
        CurrentTimeMs() - stale_ms));

    ha::OpLogGcController controller(oplog_store, progress_store);

    EXPECT_EQ(ErrorCode::OK,
              controller.CleanupAfterSnapshot(MakeSnapshotDescriptor(8)));
    ASSERT_EQ(1u, oplog_store->cleanup_calls.size());
    EXPECT_EQ(9u, oplog_store->cleanup_calls.front());
}

TEST(OpLogGcControllerTest, ReturnsProgressStoreErrorWithoutCleanup) {
    auto oplog_store = std::make_shared<FakeOpLogStore>();
    auto progress_store = std::make_shared<FakeStandbyProgressStore>();
    progress_store->list_error = ErrorCode::PERSISTENT_FAIL;

    ha::OpLogGcController controller(oplog_store, progress_store);

    EXPECT_EQ(ErrorCode::PERSISTENT_FAIL,
              controller.CleanupAfterSnapshot(MakeSnapshotDescriptor(8)));
    EXPECT_TRUE(oplog_store->cleanup_calls.empty());
}

}  // namespace
}  // namespace test
}  // namespace mooncake
