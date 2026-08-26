#include "master_config.h"

#include <gtest/gtest.h>

namespace mooncake::test {

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesDefaultsTo1024) {
    MasterConfig master_config;
    EXPECT_EQ(1024u, master_config.oplog_batch_max_entries);

    MasterServiceConfig service_config;
    EXPECT_EQ(1024u, service_config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, OplogIsDisabledByDefault) {
    MasterConfig master_config;
    EXPECT_FALSE(master_config.enable_oplog);

    MasterServiceConfig service_config;
    EXPECT_FALSE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogBuilderOverrideIsRespected) {
    auto config = MasterServiceConfig::builder().set_enable_oplog(true).build();

    EXPECT_TRUE(config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogEnablementPropagatesToServingConfig) {
    MasterConfig master_config{};
    master_config.enable_oplog = true;
    MasterServiceSupervisorConfig supervisor_config(master_config);

    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_TRUE(supervisor_config.enable_oplog);
    EXPECT_TRUE(wrapped_config.enable_oplog);
    EXPECT_TRUE(service_config.enable_oplog);
}

TEST(MasterServiceConfigTest, OplogBatchMaxEntriesBuilderOverrideRespected) {
    auto config =
        MasterServiceConfig::builder().set_oplog_batch_max_entries(17).build();

    EXPECT_EQ(17u, config.oplog_batch_max_entries);
}

TEST(MasterServiceConfigTest, SegmentAdmissionDefaultsToObserve) {
    MasterConfig master_config;
    EXPECT_EQ("observe", master_config.segment_write_admission_mode);

    MasterServiceConfig service_config;
    EXPECT_EQ(SegmentWriteAdmissionMode::OBSERVE,
              service_config.segment_admission_config.mode);
    EXPECT_EQ(60u,
              service_config.segment_admission_config.ramp_up_duration_sec);
    EXPECT_DOUBLE_EQ(
        0.05, service_config.segment_admission_config.ramp_initial_ratio);
}

TEST(MasterServiceConfigTest, SegmentAdmissionPropagatesThroughHaConfig) {
    MasterConfig master_config{};
    master_config.segment_write_admission_mode = "enforce";
    master_config.segment_ramp_up_duration_sec = 23;
    master_config.segment_ramp_initial_ratio = 0.2;
    master_config.segment_failure_threshold = 7;

    MasterServiceSupervisorConfig supervisor_config(master_config);
    WrappedMasterServiceConfig wrapped_config(supervisor_config, 1);
    MasterServiceConfig service_config(wrapped_config);

    EXPECT_EQ(SegmentWriteAdmissionMode::ENFORCE,
              service_config.segment_admission_config.mode);
    EXPECT_EQ(23u,
              service_config.segment_admission_config.ramp_up_duration_sec);
    EXPECT_DOUBLE_EQ(
        0.2, service_config.segment_admission_config.ramp_initial_ratio);
    EXPECT_EQ(7u, service_config.segment_admission_config.failure_threshold);
}

TEST(MasterServiceConfigTest, SegmentAdmissionRejectsInvalidMode) {
    MasterConfig master_config{};
    master_config.segment_write_admission_mode = "Observe";
    EXPECT_THROW(MasterServiceSupervisorConfig(master_config),
                 std::invalid_argument);
}

TEST(MasterServiceConfigTest, SegmentAdmissionBuilderOverrideIsRespected) {
    SegmentAdmissionConfig admission;
    admission.mode = SegmentWriteAdmissionMode::DISABLED;
    admission.ramp_initial_ratio = 0.25;
    auto config = MasterServiceConfig::builder()
                      .set_segment_admission_config(admission)
                      .build();

    EXPECT_EQ(SegmentWriteAdmissionMode::DISABLED,
              config.segment_admission_config.mode);
    EXPECT_DOUBLE_EQ(0.25, config.segment_admission_config.ramp_initial_ratio);
}

}  // namespace mooncake::test
