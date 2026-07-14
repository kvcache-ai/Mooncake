#pragma once

// Shared fixture and helpers for snapshot-aware MasterService SSD tests.

#include "ha/snapshot/master_service_test_for_snapshot_base.h"

namespace mooncake::test {

class MasterServiceSSDSnapshotTest : public MasterServiceSnapshotTestBase {
   protected:
    static bool glog_initialized_;

    void SetUp() override {
        // Call base class SetUp first to reset MasterMetricManager state
        MasterServiceSnapshotTestBase::SetUp();

        if (!glog_initialized_) {
            google::InitGoogleLogging("MasterServiceSSDSnapshotTest");
            FLAGS_logtostderr = true;
            glog_initialized_ = true;
        }
    }

    // Create MasterService with SSD feature and assign to class member
    void CreateMasterServiceWithSSDFeat(const std::string& root_fs_dir) {
        service_ =
            std::make_unique<MasterService>(MasterServiceConfig::builder()
                                                .set_root_fs_dir(root_fs_dir)
                                                .build());
    }

    // Create MasterService with SSD feature and custom config
    void CreateMasterServiceWithSSDFeatAndConfig(
        const MasterServiceConfig& config) {
        service_ = std::make_unique<MasterService>(config);
    }
};

bool MasterServiceSSDSnapshotTest::glog_initialized_ = false;

}  // namespace mooncake::test
