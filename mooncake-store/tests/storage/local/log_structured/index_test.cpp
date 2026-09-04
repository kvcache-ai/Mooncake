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

}  // namespace
}  // namespace mooncake::logstructured
