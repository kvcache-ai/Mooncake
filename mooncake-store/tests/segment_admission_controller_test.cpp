#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "segment_admission_controller.h"
#include "master_service.h"

namespace mooncake::test {
namespace {

class FakeSegmentAdmissionClock final : public SegmentAdmissionClock {
   public:
    TimePoint Now() const override { return now_; }

    template <typename Rep, typename Period>
    void Advance(std::chrono::duration<Rep, Period> duration) {
        now_ += std::chrono::duration_cast<TimePoint::duration>(duration);
    }

   private:
    TimePoint now_{};
};

Segment MakeSegment(UUID id, std::string name, std::string host_id) {
    Segment segment;
    segment.id = id;
    segment.name = std::move(name);
    segment.host_id = std::move(host_id);
    segment.size = 128ULL * 1024ULL * 1024ULL * 1024ULL;
    return segment;
}

SegmentAdmissionConfig TestConfig() {
    SegmentAdmissionConfig config;
    config.ramp_up_duration_sec = 10;
    config.ramp_initial_ratio = 0.1;
    config.ramp_min_successful_remote_writes = 4;
    config.failure_window_sec = 5;
    config.failure_threshold = 3;
    config.quarantine_duration_sec = 10;
    return config;
}

TEST(SegmentAdmissionConfigTest, RejectsInvalidValues) {
    auto config = TestConfig();
    config.ramp_initial_ratio = 0.0;
    EXPECT_THROW(config.Validate(), std::invalid_argument);

    config = TestConfig();
    config.failure_threshold = 0;
    EXPECT_THROW(config.Validate(), std::invalid_argument);

    EXPECT_FALSE(ParseSegmentWriteAdmissionMode("OBSERVE"));
    auto enforce = ParseSegmentWriteAdmissionMode("enforce");
    ASSERT_TRUE(enforce);
    EXPECT_EQ(*enforce, SegmentWriteAdmissionMode::ENFORCE);
}

TEST(SegmentAdmissionControllerTest, FirstSegmentIsActiveAndNextRamps) {
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(TestConfig(), clock);
    const UUID owner_a{1, 1};
    const UUID owner_b{2, 2};
    auto first = controller.OnMount(MakeSegment({11, 1}, "segment-a", "host-a"),
                                    owner_a);
    ASSERT_TRUE(first);
    EXPECT_EQ(first->state, SegmentAdmissionState::ACTIVE);
    EXPECT_DOUBLE_EQ(first->effective_ratio, 1.0);

    auto second = controller.OnMount(
        MakeSegment({22, 2}, "segment-b", "host-b"), owner_b);
    ASSERT_TRUE(second);
    EXPECT_EQ(second->state, SegmentAdmissionState::RAMPING);
    EXPECT_DOUBLE_EQ(second->effective_ratio, 0.1);
}

TEST(SegmentAdmissionControllerTest,
     TimeAndSuccessFactorsIndependentlyLimitRampUp) {
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(TestConfig(), clock);
    controller.OnMount(MakeSegment({1, 1}, "first", "host-a"), {10, 10});
    const UUID ramping_id{2, 2};
    controller.OnMount(MakeSegment(ramping_id, "ramping", "host-b"), {20, 20});

    clock->Advance(std::chrono::seconds(10));
    auto time_only = controller.GetSnapshot(ramping_id);
    ASSERT_TRUE(time_only);
    EXPECT_EQ(time_only->state, SegmentAdmissionState::RAMPING);
    EXPECT_DOUBLE_EQ(time_only->effective_ratio, 0.1);

    for (int i = 0; i < 3; ++i) {
        controller.RecordRemoteWriteSuccess(ramping_id);
    }
    auto success_limited = controller.GetSnapshot(ramping_id);
    ASSERT_TRUE(success_limited);
    EXPECT_EQ(success_limited->state, SegmentAdmissionState::RAMPING);
    EXPECT_NEAR(success_limited->effective_ratio, 0.775, 1e-9);

    auto active = controller.RecordRemoteWriteSuccess(ramping_id);
    ASSERT_TRUE(active);
    EXPECT_EQ(active->state, SegmentAdmissionState::ACTIVE);
    EXPECT_DOUBLE_EQ(active->effective_ratio, 1.0);
}

TEST(SegmentAdmissionControllerTest,
     QuarantineRecoveryRequiresCooldownAndNewHeartbeat) {
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(TestConfig(), clock);
    const UUID segment_id{1, 1};
    const UUID owner_id{10, 10};
    controller.OnMount(MakeSegment(segment_id, "segment", "host"), owner_id);

    controller.RecordRemoteWriteFailure(segment_id);
    controller.RecordRemoteWriteFailure(segment_id);
    auto quarantined = controller.RecordRemoteWriteFailure(segment_id);
    ASSERT_TRUE(quarantined);
    EXPECT_EQ(quarantined->state, SegmentAdmissionState::QUARANTINED);

    clock->Advance(std::chrono::seconds(11));
    auto without_heartbeat = controller.GetSnapshot(segment_id);
    ASSERT_TRUE(without_heartbeat);
    EXPECT_EQ(without_heartbeat->state, SegmentAdmissionState::QUARANTINED);

    auto recovered = controller.OnOwnerHeartbeat(owner_id);
    ASSERT_EQ(recovered.size(), 1);
    EXPECT_EQ(recovered[0].state, SegmentAdmissionState::RAMPING);
    EXPECT_DOUBLE_EQ(recovered[0].effective_ratio, 0.1);
}

TEST(SegmentAdmissionControllerTest, FailureWindowExpiresOldFailures) {
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(TestConfig(), clock);
    const UUID segment_id{1, 1};
    controller.OnMount(MakeSegment(segment_id, "segment", "host"), {10, 10});

    controller.RecordRemoteWriteFailure(segment_id);
    controller.RecordRemoteWriteFailure(segment_id);
    clock->Advance(std::chrono::seconds(6));
    auto snapshot = controller.RecordRemoteWriteFailure(segment_id);
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->state, SegmentAdmissionState::ACTIVE);
    EXPECT_EQ(snapshot->recent_failures, 1);
}

TEST(SegmentAdmissionControllerTest,
     ObserveCountsWouldRejectWithoutChangingRuntimeBudget) {
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(TestConfig(), clock);
    const UUID segment_id{1, 1};
    controller.OnMount(MakeSegment(segment_id, "segment", "host-a"), {10, 10});
    controller.RecordRemoteWriteFailure(segment_id);
    controller.RecordRemoteWriteFailure(segment_id);
    controller.RecordRemoteWriteFailure(segment_id);

    auto local = controller.ObserveRemoteWrite("segment", "host-a", 4096);
    EXPECT_TRUE(local.would_admit);
    EXPECT_EQ(local.snapshot.observed_remote_writes, 0);

    auto remote = controller.ObserveRemoteWrite("segment", "host-b", 4096);
    EXPECT_FALSE(remote.would_admit);
    EXPECT_EQ(remote.reason, SegmentAdmissionRejectReason::QUARANTINED);
    EXPECT_EQ(remote.snapshot.observed_remote_writes, 1);
    EXPECT_EQ(remote.snapshot.observed_would_reject, 1);
    EXPECT_EQ(remote.snapshot.inflight_remote_write_ops, 0);
    EXPECT_EQ(remote.snapshot.inflight_remote_write_bytes, 0);
}

TEST(SegmentAdmissionControllerTest, UnmountDropsRuntimeBySegmentId) {
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(TestConfig(), clock);
    const UUID old_id{1, 1};
    controller.OnMount(MakeSegment(old_id, "segment", "host"), {10, 10});
    EXPECT_TRUE(controller.OnUnmount(old_id));
    EXPECT_EQ(controller.size(), 0);

    const UUID new_id{2, 2};
    auto replacement =
        controller.OnMount(MakeSegment(new_id, "segment", "host"), {20, 20});
    ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->segment_id, new_id);
    EXPECT_FALSE(controller.GetSnapshot(old_id));
}

TEST(SegmentAdmissionControllerTest, RemountRefreshesOwnerHeartbeatIndex) {
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(TestConfig(), clock);
    const UUID segment_id{1, 1};
    auto segment = MakeSegment(segment_id, "segment", "host");
    controller.OnMount(segment, {10, 10});
    controller.OnMount(segment, {20, 20}, true);

    EXPECT_TRUE(controller.OnOwnerHeartbeat({10, 10}).empty());
    auto new_owner = controller.OnOwnerHeartbeat({20, 20});
    ASSERT_EQ(new_owner.size(), 1);
    EXPECT_EQ(new_owner.front().owner_client_id, UUID(20, 20));
}

TEST(SegmentAdmissionControllerTest, DisabledModeCreatesNoRuntime) {
    auto config = TestConfig();
    config.mode = SegmentWriteAdmissionMode::DISABLED;
    auto clock = std::make_shared<FakeSegmentAdmissionClock>();
    SegmentAdmissionController controller(config, clock);
    EXPECT_FALSE(
        controller.OnMount(MakeSegment({1, 1}, "segment", "host"), {10, 10}));
    EXPECT_EQ(controller.size(), 0);
    EXPECT_TRUE(
        controller.ObserveRemoteWrite("segment", "remote", 4096).would_admit);
}

TEST(SegmentAdmissionObserveIntegrationTest,
     WouldRejectMetricDoesNotChangePreferredPlacement) {
    auto admission = TestConfig();
    admission.max_inflight_remote_write_bytes = 1024;
    auto service_config = MasterServiceConfig::builder()
                              .set_segment_admission_config(admission)
                              .build();
    MasterService service(service_config);

    auto active = MakeSegment({1, 1}, "active", "host-a");
    active.base = 0x300000000;
    active.size = 16ULL * 1024ULL * 1024ULL;
    active.te_endpoint = active.name;
    auto ramping = MakeSegment({2, 2}, "ramping", "host-b");
    ramping.base = 0x400000000;
    ramping.size = 16ULL * 1024ULL * 1024ULL;
    ramping.te_endpoint = ramping.name;
    ASSERT_TRUE(service.MountSegment(active, {10, 10}));
    ASSERT_TRUE(service.MountSegment(ramping, {20, 20}));

    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;
    replicate_config.preferred_segment = ramping.name;
    auto put = service.PutStart({30, 30}, "observe-only-key",
                                TenantId::Default(), 2048, replicate_config);
    ASSERT_TRUE(put);
    ASSERT_EQ(put->size(), 1);
    EXPECT_EQ(put->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              ramping.name);

    auto details = service.GetSegmentsDetail();
    ASSERT_TRUE(details);
    auto detail = std::find_if(
        details->begin(), details->end(),
        [&](const auto& item) { return item.segment_id == ramping.id; });
    ASSERT_NE(detail, details->end());
    EXPECT_EQ(detail->admission_state, "RAMPING");
    EXPECT_EQ(detail->admission_observed_remote_writes, 1);
    EXPECT_EQ(detail->admission_observed_would_reject, 1);
    EXPECT_EQ(detail->inflight_remote_write_ops, 0);
    EXPECT_EQ(detail->inflight_remote_write_bytes, 0);

    EXPECT_TRUE(service.PutRevoke({30, 30}, "observe-only-key",
                                  TenantId::Default(), ReplicaType::MEMORY));
}

}  // namespace
}  // namespace mooncake::test
