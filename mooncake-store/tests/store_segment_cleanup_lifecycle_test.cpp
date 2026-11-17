#include "store_segment_cleanup_fixture.h"

using namespace mooncake;
using namespace mooncake::cleanup_test;

namespace {
TEST(StoreSegmentCleanup, LocalPoolPublicationFailureRetainsOwnersUntilRetry) {
    Scenario s;
    s.Start(true, true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    ASSERT_TRUE(s.real->client_->UnmountSegmentById(*s.pools[0].segment));
    s.publication->fail_publication = true;

    EXPECT_NE(s.real->tearDownAll(), 0);
    EXPECT_NE(s.real->tearDownAll(), 0);
    EXPECT_FALSE(s.Contains(
        s.engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID),
        s.pools[1].base));
    EXPECT_TRUE(s.Contains(s.engine->getMetadata()->getSegmentDesc(s.name),
                           s.pools[1].base));
    for (const auto& pool : s.pools) EXPECT_EQ(pool.attempts, 0);

    s.publication->fail_publication = false;
    EXPECT_EQ(s.real->tearDownAll(), 0);
    for (const auto& pool : s.pools) {
        EXPECT_TRUE(s.PoolSafe(pool));
        EXPECT_EQ(pool.released, 1);
    }
    EXPECT_EQ(s.real->tearDownAll(), 0);
}

class RealDeferredClose : public ::testing::TestWithParam<bool> {};
TEST_P(RealDeferredClose, CallbackEmptiedMapStillFinishesDeferredResourceTail) {
    Scenario s;
    s.Start(true, true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount(GetParam());
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.GraceReal(60);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.fault = Fault::Transfer;
    EXPECT_NE(s.real->tearDownAll(), 0);
    EXPECT_EQ(s.release_attempts, 0);
    for (const auto& pool : s.pools) EXPECT_EQ(pool.attempts, 0);

    s.fault = Fault::None;
    ASSERT_NE(s.real->client_, nullptr);
    EXPECT_TRUE(s.real->client_->UnmountSegmentById(s.id));
    EXPECT_EQ(s.released, 1);
    EXPECT_EQ(s.release_attempts, 1);
    EXPECT_EQ(s.real->tearDownAll(), 0);

    EXPECT_TRUE(s.retained_client.expired());
    EXPECT_EQ(s.real->client_buffer_allocator_, nullptr);
    for (const auto& pool : s.pools) {
        EXPECT_TRUE(s.PoolSafe(pool));
        EXPECT_EQ(pool.released, 1);
    }
    EXPECT_EQ(s.real->tearDownAll(), 0);
    EXPECT_EQ(s.release_attempts, 1);
}

TEST_P(RealDeferredClose,
       SuccessfulCloseCleansPoolsWithExternalClientRetained) {
    Scenario s;
    s.Start(true, true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount(GetParam());
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    auto external = s.real->client_;
    s.pools_require_client_destruction = false;

    EXPECT_EQ(s.real->tearDownAll(), 0);

    EXPECT_EQ(s.released, 1);
    EXPECT_EQ(s.unsafe_releases, 0);
    EXPECT_EQ(s.real->client_, nullptr);
    EXPECT_FALSE(s.retained_client.expired());
    for (const auto& pool : s.pools) {
        EXPECT_TRUE(s.PoolSafe(pool));
        EXPECT_EQ(pool.released, 1);
    }
    external.reset();
    EXPECT_EQ(s.real->tearDownAll(), 0);
    for (const auto& pool : s.pools) EXPECT_EQ(pool.released, 1);
}

TEST_P(RealDeferredClose, PersistentFailureDestructionRetainsRegisteredPools) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            {
                Scenario s;
                s.Start(true, true);
                if (::testing::Test::HasFatalFailure()) _exit(2);
                s.Mount(GetParam());
                if (::testing::Test::HasFatalFailure()) _exit(2);
                s.GraceReal(60);
                if (::testing::Test::HasFatalFailure()) _exit(2);
                s.fault = Fault::Transfer;
                EXPECT_NE(s.real->tearDownAll(), 0);
                s.real.reset();
                EXPECT_EQ(s.release_attempts, 0);
                EXPECT_TRUE(s.LocalContains());
                EXPECT_TRUE(s.RemoteContains());
                for (const auto& pool : s.pools) {
                    EXPECT_EQ(pool.attempts, 0);
                    EXPECT_EQ(pool.released, 0);
                }
                std::cerr << "persistent destruction: dynamic_attempts="
                          << s.release_attempts
                          << " setup_attempts=" << s.pools[0].attempts
                          << " local_attempts=" << s.pools[1].attempts
                          << " local_registered="
                          << s.Contains(
                                 s.engine->getMetadata()->getSegmentDescByID(
                                     LOCAL_SEGMENT_ID),
                                 s.pools[1].base)
                          << "\n";
            }
            _exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}
INSTANTIATE_TEST_SUITE_P(AllocatedAndMapped, RealDeferredClose,
                         ::testing::Bool());
}  // namespace
