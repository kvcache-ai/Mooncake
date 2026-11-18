#include "store_segment_cleanup_worker_fixture.h"

using namespace mooncake;
using namespace mooncake::cleanup_test;

namespace {
struct WorkerCase {
    bool move;
    bool mapped;
};
class RealWorkerClose : public ::testing::TestWithParam<WorkerCase> {};
TEST_P(RealWorkerClose, DrainsMasterDispatchedMigrationBeforeReleasingSource) {
    Scenario s;
    s.Start(true);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    s.Mount(GetParam().mapped);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    MigrationDestination destination;
    destination.Start(s);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    std::vector<char> payload(4096, 'w');
    std::vector<Slice> slices{{payload.data(), payload.size()}};
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = s.name;
    ASSERT_TRUE(s.real->client_->Put("blocked-migration", slices, config));
    auto gate = std::make_shared<MigrationGate>(s.base);
    MigrationScope scope(gate);
    s.before_release = [gate] { return gate->ObserveRelease(); };
    const auto task = GetParam().move
                          ? s.real->client_->CreateMoveTask(
                                "blocked-migration", s.name, destination.name)
                          : s.real->client_->CreateCopyTask("blocked-migration",
                                                            {destination.name});
    EXPECT_TRUE(task);
    const bool captured = task && gate->WaitCaptured();
    EXPECT_TRUE(captured)
        << "Master-dispatched worker did not capture source slices";

    int close_result = -1;
    std::thread close([&] {
        gate->BeginClose();
        close_result = s.real->tearDownAll();
    });
    const bool close_reached = gate->WaitCloseBoundary();
    EXPECT_TRUE(close_reached)
        << "Close reached neither release nor worker drain";
    EXPECT_EQ(gate->PrematureReleases(), 0);
    gate->Unblock();  // Always release before joining, including assertion
                      // failures.
    close.join();

    EXPECT_EQ(close_result, 0);
    EXPECT_EQ(s.unsafe_releases, 0);
    EXPECT_EQ(s.released, 1);
    EXPECT_TRUE(s.retained_client.expired());
}
INSTANTIATE_TEST_SUITE_P(CopyMoveAndBacking, RealWorkerClose,
                         ::testing::Values(WorkerCase{false, false},
                                           WorkerCase{true, false},
                                           WorkerCase{false, true},
                                           WorkerCase{true, true}));
}  // namespace
