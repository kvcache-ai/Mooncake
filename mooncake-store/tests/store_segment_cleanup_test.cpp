#include "store_segment_cleanup_fixture.h"

using namespace mooncake;
using namespace mooncake::cleanup_test;

namespace {
class ClientClose : public ::testing::TestWithParam<Fault> {};
TEST_P(ClientClose, CancelsGraceAndReleasesOnlyAfterConfirmedCleanup) {
    Scenario s;
    s.Start(false);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Grace(60000);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.fault = GetParam();

    s.client.reset();

    EXPECT_EQ(s.unsafe_releases, 0);
    EXPECT_EQ(s.callbacks, GetParam() == Fault::None ? 1 : 0);
    EXPECT_EQ(s.released, GetParam() == Fault::None ? 1 : 0);
    if (GetParam() == Fault::Master) {
        EXPECT_FALSE(s.MasterRemoved());
    }
    if (GetParam() == Fault::Transfer) {
        EXPECT_TRUE(s.LocalContains());
    }
}
INSTANTIATE_TEST_SUITE_P(Boundaries, ClientClose,
                         ::testing::Values(Fault::None, Fault::Master,
                                           Fault::Transfer));

class GraceCancellation : public ::testing::TestWithParam<bool> {};
TEST_P(GraceCancellation, SynchronousUnmountRetriesBarrierAndReleasesOnce) {
    Scenario s;
    s.Start(false);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Grace(60000);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    const auto unmount = [&] {
        return GetParam() ? s.client->UnmountSegment(s.base, s.size)
                          : s.client->UnmountSegmentById(s.id);
    };
    s.fault = Fault::Transfer;
    EXPECT_FALSE(unmount());
    EXPECT_EQ(s.callbacks, 0);
    EXPECT_EQ(s.release_attempts, 0);
    s.fault = Fault::None;
    EXPECT_TRUE(unmount());
    s.client.reset();
    EXPECT_EQ(s.unsafe_releases, 0);
    EXPECT_EQ(s.callbacks, 1);
    EXPECT_EQ(s.released, 1);
    EXPECT_EQ(s.release_attempts, 1);
}
INSTANTIATE_TEST_SUITE_P(AddressForms, GraceCancellation, ::testing::Bool());

TEST(StoreSegmentCleanup, GracefulUnregisterFailureRetainsCallbackForRecovery) {
    Scenario s;
    s.Start(false);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.fault = Fault::Transfer;
    s.Grace(1);  // Keep the production grace + ten-second timer schedule.
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    ASSERT_TRUE(s.WaitForUnregisterFailure())
        << "Graceful timer did not reach TE";
    EXPECT_TRUE(s.MasterRemoved());
    EXPECT_TRUE(s.LocalContains());
    EXPECT_TRUE(s.RemoteContains());

    s.fault = Fault::None;
    s.client.reset();  // Joins the timer and retries retained cleanup on close.

    EXPECT_EQ(s.unsafe_releases, 0);
    EXPECT_EQ(s.callbacks, 1);
    EXPECT_EQ(s.released, 1);
    EXPECT_EQ(s.release_attempts, 1);
}

TEST(StoreSegmentCleanup, LocalMissCannotConfirmFailedMetadataPublication) {
    Scenario s;
    s.Start(true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    const std::vector<std::string> ids{UuidToString(s.id)};
    s.publication->fail_publication = true;

    EXPECT_NE(s.real->unmountAndFreeSegment(ids), 0);
    EXPECT_EQ(s.publication->failed_publications, 1);
    EXPECT_TRUE(s.MasterRemoved());
    EXPECT_FALSE(s.LocalContains());
    EXPECT_TRUE(s.RemoteContains());
    EXPECT_NE(s.real->unmountAndFreeSegment(ids), 0);
    EXPECT_EQ(s.unsafe_releases, 0);
    EXPECT_EQ(s.release_attempts, 0);
    EXPECT_TRUE(s.RemoteContains());

    s.publication->fail_publication = false;
    EXPECT_EQ(s.real->unmountAndFreeSegment(ids), 0);
    EXPECT_EQ(s.released, 1);
    EXPECT_EQ(s.release_attempts, 1);
}

struct CloseCase {
    Fault fault;
    bool mapped;
    bool graceful;
};
class RealClose : public ::testing::TestWithParam<CloseCase> {};
TEST_P(RealClose, FailedCloseRetainsBackingAndKeepsReportingPendingFailure) {
    Scenario s;
    s.Start(true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount(GetParam().mapped);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    if (GetParam().graceful) s.GraceReal(60);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.fault = GetParam().fault;

    EXPECT_NE(s.real->tearDownAll(), 0);
    EXPECT_NE(s.real->tearDownAll(), 0);

    EXPECT_EQ(s.release_attempts, 0);
    EXPECT_EQ(s.released, 0);
    EXPECT_TRUE(s.LocalContains());
    EXPECT_TRUE(s.RemoteContains());
    if (GetParam().mapped) {
        unsigned char residency = 0;
        EXPECT_EQ(mincore(s.base, sysconf(_SC_PAGESIZE), &residency), 0);
    }
    s.fault = Fault::None;
    EXPECT_EQ(s.real->tearDownAll(), 0);
    EXPECT_EQ(s.released, 1);
    EXPECT_EQ(s.release_attempts, 1);
}
INSTANTIATE_TEST_SUITE_P(
    BackingAndBoundaries, RealClose,
    ::testing::Values(CloseCase{Fault::Master, false, false},
                      CloseCase{Fault::Transfer, false, false},
                      CloseCase{Fault::Master, true, false},
                      CloseCase{Fault::Transfer, true, false},
                      CloseCase{Fault::Master, false, true},
                      CloseCase{Fault::Transfer, false, true},
                      CloseCase{Fault::Master, true, true},
                      CloseCase{Fault::Transfer, true, true}));

class RealPoolClose : public ::testing::TestWithParam<Fault> {};
TEST_P(RealPoolClose, FailedDynamicCloseDefersPoolsUntilCompleteRecovery) {
    Scenario s;
    s.Start(true, true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    const std::weak_ptr<ClientBufferAllocator> allocator =
        s.real->client_buffer_allocator_;
    s.Mount();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.fault = GetParam();

    EXPECT_NE(s.real->tearDownAll(), 0);
    EXPECT_NE(s.real->tearDownAll(), 0);

    EXPECT_FALSE(s.retained_client.expired());
    EXPECT_FALSE(allocator.expired());
    EXPECT_EQ(s.release_attempts, 0);
    for (const auto& pool : s.pools) {
        EXPECT_EQ(pool.attempts, 0) << "pool=" << pool.base;
        EXPECT_EQ(pool.released, 0);
    }
    EXPECT_TRUE(s.LocalContains());
    EXPECT_TRUE(s.RemoteContains());
    s.fault = Fault::None;
    EXPECT_EQ(s.real->tearDownAll(), 0);
    EXPECT_TRUE(s.retained_client.expired());
    EXPECT_TRUE(allocator.expired());
    EXPECT_EQ(s.released, 1);
    for (const auto& pool : s.pools) {
        EXPECT_TRUE(s.PoolSafe(pool));
        EXPECT_EQ(pool.released, 1) << "pool=" << pool.base;
    }
}
INSTANTIATE_TEST_SUITE_P(NonzeroPools, RealPoolClose,
                         ::testing::Values(Fault::Master, Fault::Transfer));

// Build the entire runtime inside the child (never fork a running Master).
// On baseline, stop at the early-release assertion before invoking a dangling
// raw-this callback. _exit then lets the OS reclaim the guarded child
// resources.
class RealLifetime : public ::testing::TestWithParam<bool> {};
TEST_P(RealLifetime, GracefulCallbackOutlivesRealClientOwner) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            {
                Scenario s;
                s.Start(true);
                if (::testing::Test::HasFailure()) _exit(1);
                s.Mount(GetParam());
                if (::testing::Test::HasFailure()) _exit(1);
                auto retained_client = s.real->client_;
                s.GraceReal(60);
                if (::testing::Test::HasFailure()) _exit(1);
                s.fault = Fault::Master;
                s.real.reset();
                EXPECT_EQ(s.release_attempts, 0);
                std::cerr << "after RealClient destruction: release_attempts="
                          << s.release_attempts << "\n";
                if (::testing::Test::HasFailure()) _exit(1);
                s.fault = Fault::None;
                retained_client.reset();
                EXPECT_EQ(s.unsafe_releases, 0);
                EXPECT_EQ(s.release_attempts, 1);
                EXPECT_EQ(s.released, 1);
            }
            _exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}
INSTANTIATE_TEST_SUITE_P(Backing, RealLifetime, ::testing::Bool());
}  // namespace
