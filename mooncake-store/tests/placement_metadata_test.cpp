#include <gtest/gtest.h>
#include <glog/logging.h>

#include "replica.h"
#include "types.h"

namespace mooncake {

class PlacementMetadataTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PlacementMetadataTest");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(PlacementMetadataTest, DefaultPlacementStatusIsActive) {
    Replica replica("/tmp/placeholder", 0, ReplicaStatus::COMPLETE);
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::ACTIVE);
}

TEST_F(PlacementMetadataTest, SetPlacementStatus) {
    Replica replica("/tmp/placeholder", 0, ReplicaStatus::COMPLETE);

    replica.set_placement_status(PlacementStatus::MOVING_OUT);
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::MOVING_OUT);

    replica.set_placement_status(PlacementStatus::MOVING_IN);
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::MOVING_IN);

    replica.set_placement_status(PlacementStatus::FAILED);
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::FAILED);

    replica.set_placement_status(PlacementStatus::ACTIVE);
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::ACTIVE);
}

TEST_F(PlacementMetadataTest, MarkPlacementHelpers) {
    Replica replica("/tmp/placeholder", 0, ReplicaStatus::COMPLETE);

    replica.mark_placement_moving_out();
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::MOVING_OUT);

    replica.mark_placement_moving_in();
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::MOVING_IN);

    replica.mark_placement_failed();
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::FAILED);

    replica.mark_placement_active();
    EXPECT_EQ(replica.get_placement_status(), PlacementStatus::ACTIVE);
}

TEST_F(PlacementMetadataTest, MoveConstructorPreservesPlacementStatus) {
    Replica replica("/tmp/placeholder", 0, ReplicaStatus::COMPLETE);
    replica.mark_placement_moving_out();

    Replica moved(std::move(replica));
    EXPECT_EQ(moved.get_placement_status(), PlacementStatus::MOVING_OUT);
}

TEST_F(PlacementMetadataTest, MoveAssignmentCopiesPlacementStatus) {
    Replica a("/tmp/a", 0, ReplicaStatus::COMPLETE);
    Replica b("/tmp/b", 0, ReplicaStatus::COMPLETE);
    a.mark_placement_moving_in();

    b = std::move(a);
    EXPECT_EQ(b.get_placement_status(), PlacementStatus::MOVING_IN);
}

}  // namespace mooncake
