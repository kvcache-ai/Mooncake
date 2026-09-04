#include "storage/local/log_structured/index.h"

#include <gtest/gtest.h>

namespace mooncake::logstructured {
namespace {

RecordIdentity Identity(std::string key, uint64_t incarnation) {
    return RecordIdentity{
        .tenant_id = "tenant-a",
        .object_key = std::move(key),
        .incarnation = ObjectIncarnation{.high = 9, .low = incarnation},
    };
}

PhysicalRecord Physical(uint64_t segment, uint64_t offset) {
    return PhysicalRecord{.segment_id = segment,
                          .record_offset = offset,
                          .value_offset = offset + 64,
                          .value_length = 32,
                          .total_length = 128};
}

TEST(LogStructuredIndexTest, PreparedValueIsInvisibleUntilCommit) {
    VersionIndex index;
    const auto identity = Identity("key", 1);
    ASSERT_TRUE(index.Prepare(identity, Physical(1, 0), 10).has_value());
    EXPECT_FALSE(index.LookupCommitted(identity).has_value());

    ASSERT_TRUE(index.Commit(identity, 10).has_value());
    auto committed = index.LookupCommitted(identity);
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ(committed->physical, Physical(1, 0));
    EXPECT_EQ(committed->state, VersionState::kCommitted);
}

TEST(LogStructuredIndexTest, FailedReplacementPreservesOldCommit) {
    VersionIndex index;
    const auto old_identity = Identity("key", 1);
    const auto new_identity = Identity("key", 2);
    ASSERT_TRUE(index.Prepare(old_identity, Physical(1, 0), 10).has_value());
    ASSERT_TRUE(index.Commit(old_identity, 10).has_value());
    ASSERT_TRUE(index.Prepare(new_identity, Physical(1, 128), 20).has_value());
    ASSERT_TRUE(index.Abort(new_identity, 20).has_value());

    EXPECT_TRUE(index.LookupCommitted(old_identity).has_value());
    EXPECT_FALSE(index.LookupCommitted(new_identity).has_value());
}

TEST(LogStructuredIndexTest, NewCommitMakesOldIncarnationObsolete) {
    VersionIndex index;
    const auto old_identity = Identity("key", 1);
    const auto new_identity = Identity("key", 2);
    ASSERT_TRUE(index.Prepare(old_identity, Physical(1, 0), 10).has_value());
    ASSERT_TRUE(index.Commit(old_identity, 10).has_value());
    ASSERT_TRUE(index.Prepare(new_identity, Physical(1, 128), 20).has_value());
    ASSERT_TRUE(index.Commit(new_identity, 20).has_value());

    EXPECT_FALSE(index.LookupCommitted(old_identity).has_value());
    EXPECT_EQ(index.Lookup(old_identity)->state, VersionState::kObsolete);
    EXPECT_TRUE(index.LookupCommitted(new_identity).has_value());
}

TEST(LogStructuredIndexTest, StaleDeleteCannotRemoveRecreatedObject) {
    VersionIndex index;
    const auto old_identity = Identity("key", 1);
    const auto new_identity = Identity("key", 2);
    ASSERT_TRUE(index.Prepare(old_identity, Physical(1, 0), 10).has_value());
    ASSERT_TRUE(index.Commit(old_identity, 10).has_value());
    ASSERT_TRUE(index.Prepare(new_identity, Physical(1, 128), 20).has_value());
    ASSERT_TRUE(index.Commit(new_identity, 20).has_value());

    ASSERT_TRUE(index.ApplyTombstone(old_identity, 30).has_value());
    EXPECT_TRUE(index.LookupCommitted(new_identity).has_value());
    EXPECT_EQ(index.Lookup(old_identity)->state, VersionState::kTombstoned);
}

TEST(LogStructuredIndexTest, CompactionInstallUsesPhysicalAndEpochCas) {
    VersionIndex index;
    const auto identity = Identity("key", 1);
    const auto source = Physical(1, 0);
    const auto target = Physical(2, 0);
    ASSERT_TRUE(index.Prepare(identity, source, 10).has_value());
    ASSERT_TRUE(index.Commit(identity, 10).has_value());
    const auto committed = index.LookupCommitted(identity);
    ASSERT_TRUE(committed.has_value());

    EXPECT_EQ(index
                  .InstallCompactionCopy(identity, Physical(9, 0),
                                         committed->mutation_epoch, target)
                  .error(),
              IndexError::kPhysicalMismatch);
    ASSERT_TRUE(index
                    .InstallCompactionCopy(identity, source,
                                           committed->mutation_epoch, target)
                    .has_value());
    EXPECT_EQ(index.LookupCommitted(identity)->physical, target);
    EXPECT_EQ(index
                  .InstallCompactionCopy(identity, source,
                                         committed->mutation_epoch, target)
                  .error(),
              IndexError::kStaleSequence);
}

TEST(LogStructuredIndexTest, LatestLookupTracksNewestCommittedIncarnation) {
    VersionIndex index;
    const auto first = Identity("key", 1);
    const auto second = Identity("key", 2);
    ASSERT_TRUE(index.Prepare(first, Physical(1, 0), 1).has_value());
    ASSERT_TRUE(index.Commit(first, 1).has_value());
    ASSERT_TRUE(index.Prepare(second, Physical(1, 128), 2).has_value());
    ASSERT_TRUE(index.Commit(second, 2).has_value());

    auto current = index.LookupCurrent("tenant-a", "key");
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(current->identity, second);
    ASSERT_EQ(index.CurrentSnapshot().size(), size_t{1});
}

TEST(LogStructuredIndexTest, ReplayOperationsAreIdempotent) {
    VersionIndex index;
    const auto identity = Identity("key", 1);
    const auto physical = Physical(1, 0);
    ASSERT_TRUE(index.Prepare(identity, physical, 10).has_value());
    ASSERT_TRUE(index.Prepare(identity, physical, 10).has_value());
    ASSERT_TRUE(index.Commit(identity, 10).has_value());
    ASSERT_TRUE(index.Commit(identity, 10).has_value());
    ASSERT_TRUE(index.ApplyTombstone(identity, 20).has_value());
    ASSERT_TRUE(index.ApplyTombstone(identity, 20).has_value());
}

TEST(LogStructuredIndexTest, CompactionBatchIsAllOrNothing) {
    VersionIndex index;
    const auto first = Identity("first", 1);
    const auto second = Identity("second", 1);
    const auto first_source = Physical(1, 0);
    const auto second_source = Physical(1, 128);
    ASSERT_TRUE(index.Prepare(first, first_source, 10).has_value());
    ASSERT_TRUE(index.Commit(first, 10).has_value());
    ASSERT_TRUE(index.Prepare(second, second_source, 20).has_value());
    ASSERT_TRUE(index.Commit(second, 20).has_value());
    const auto first_epoch = index.LookupCommitted(first)->mutation_epoch;
    const auto second_epoch = index.LookupCommitted(second)->mutation_epoch;

    auto result =
        index.InstallCompactionCopies({{.identity = first,
                                        .expected_source = first_source,
                                        .expected_epoch = first_epoch,
                                        .target = Physical(2, 0)},
                                       {.identity = second,
                                        .expected_source = Physical(9, 0),
                                        .expected_epoch = second_epoch,
                                        .target = Physical(2, 128)}});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(index.LookupCommitted(first)->physical, first_source);
    EXPECT_EQ(index.LookupCommitted(second)->physical, second_source);
}

TEST(LogStructuredIndexTest, ReclaimClearsOnlyDeadPhysicalMappings) {
    VersionIndex index;
    const auto old_identity = Identity("key", 1);
    const auto current_identity = Identity("key", 2);
    ASSERT_TRUE(index.Prepare(old_identity, Physical(1, 0), 10).has_value());
    ASSERT_TRUE(index.Commit(old_identity, 10).has_value());
    ASSERT_TRUE(
        index.Prepare(current_identity, Physical(2, 0), 20).has_value());
    ASSERT_TRUE(index.Commit(current_identity, 20).has_value());

    index.ReclaimNonCurrentVersionsInSegments({1});
    auto old = index.Lookup(old_identity);
    ASSERT_TRUE(old.has_value());
    EXPECT_EQ(old->state, VersionState::kReclaimable);
    EXPECT_EQ(old->physical, PhysicalRecord{});
    EXPECT_TRUE(index.LookupCommitted(current_identity).has_value());
}

TEST(LogStructuredIndexTest, RestoreRejectsUnknownVersionState) {
    VersionIndex index;
    auto version = VersionEntry{.physical = Physical(1, 0),
                                .state = static_cast<VersionState>(255),
                                .sequence = 1,
                                .mutation_epoch = 1};
    auto restored =
        index.Restore({{.identity = Identity("key", 1), .version = version}});
    ASSERT_FALSE(restored.has_value());
    EXPECT_EQ(restored.error(), IndexError::kInvalidTransition);
}

TEST(LogStructuredIndexTest, RestoreRejectsMultipleCommittedIncarnations) {
    VersionIndex index;
    const auto first = IndexSnapshotEntry{
        .identity = Identity("key", 1),
        .version = VersionEntry{.physical = Physical(1, 0),
                                .state = VersionState::kCommitted,
                                .sequence = 1,
                                .mutation_epoch = 1}};
    const auto second = IndexSnapshotEntry{
        .identity = Identity("key", 2),
        .version = VersionEntry{.physical = Physical(1, 128),
                                .state = VersionState::kCommitted,
                                .sequence = 2,
                                .mutation_epoch = 1}};
    auto restored = index.Restore({first, second});
    ASSERT_FALSE(restored.has_value());
    EXPECT_EQ(restored.error(), IndexError::kInvalidTransition);
}

}  // namespace
}  // namespace mooncake::logstructured
