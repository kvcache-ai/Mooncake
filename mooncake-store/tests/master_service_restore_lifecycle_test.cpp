// Restore lifecycle state machine tests (RFC #4700, slice 1).
//
// The machine under test:
//   Constructed -> Recovering -> Ready -> Serving
//   (non-HA: Constructed -> Ready via MarkReadyForServing)
//   Recovering -> Failed on a failed restore.
//
// Slice 1 changes no locking behavior: master_state_mutex_ stays exactly
// where it was. What it adds is (a) explicit transitions with a single
// restore admission point and (b) worker admission that pauses business
// background threads during the restore window, independent of that lock.

#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "rpc_service.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "master_service_test_fixture.h"
#include "types.h"

namespace mooncake::test {

using RestoreLifecycleState = MasterService::RestoreLifecycleState;

namespace {

StandbyObjectEntry MakeStandbyEntry(const std::string& key, uint64_t obj_size,
                                    uintptr_t addr,
                                    const std::string& endpoint) {
    StandbyObjectEntry entry;
    entry.tenant_id = TenantId::Default().value();
    entry.key = key;
    entry.metadata.client_id = generate_uuid();
    entry.metadata.size = obj_size;
    AllocatedBuffer::Descriptor buf;
    buf.size_ = obj_size;
    buf.buffer_address_ = addr;
    buf.transport_endpoint_ = endpoint;
    Replica::Descriptor desc;
    desc.id = addr;
    desc.descriptor_variant = MemoryDescriptor{buf};
    desc.status = ReplicaStatus::COMPLETE;
    entry.metadata.replicas.push_back(desc);
    return entry;
}

StandbySegmentInfo MakeStandbySegment(const std::string& name,
                                      const std::string& endpoint,
                                      uint64_t capacity) {
    StandbySegmentInfo seg;
    seg.segment_name = name;
    seg.transport_endpoint = endpoint;
    seg.capacity = capacity;
    seg.is_memory_segment = true;
    return seg;
}

}  // namespace

TEST_F(MasterServiceTest, RestoreLifecycleFreshServiceStartsConstructed) {
    MasterService service(MakeStrictTenantConfig({"default"}));
    MasterServiceTestPeer peer(service);
    EXPECT_EQ(RestoreLifecycleState::kConstructed,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
    EXPECT_TRUE(peer.AdmitsBusinessWorkersForTesting());
}

TEST_F(MasterServiceTest, RestoreLifecycleSuccessfulRestoreReachesReady) {
    MasterService service(MakeStrictTenantConfig({"default"}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service, "ep1");
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto entry = MakeStandbyEntry("restored_key", 128, 0x1000, "ep1");

    auto result = service.RestoreFromStandbySnapshot(
        {entry}, /*initial_oplog_sequence_id=*/0, {seg});
    ASSERT_TRUE(result.has_value());

    // Ready means the restore window is over: workers are admitted again,
    // and serving has not been activated yet.
    EXPECT_EQ(RestoreLifecycleState::kReady,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
    EXPECT_TRUE(
        MasterServiceTestPeer(service).AdmitsBusinessWorkersForTesting());
    EXPECT_TRUE(
        service.ExistKey("restored_key", TenantId::Default()).value_or(false));
}

TEST_F(MasterServiceTest, RestoreLifecycleRestoreAfterServingIsRefused) {
    MasterService service(MakeStrictTenantConfig({"default"}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service, "ep1");
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto entry = MakeStandbyEntry("restored_key", 128, 0x1000, "ep1");

    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {entry}, /*initial_oplog_sequence_id=*/0, {seg})
                    .has_value());
    service.MarkReadyForServing();
    service.ActivateServing();
    ASSERT_EQ(RestoreLifecycleState::kServing,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));

    // The RFC's proposed constraint: no Serving -> Recovering edge. A
    // restore may only enter before serving was ever activated.
    auto late = service.RestoreFromStandbySnapshot(
        {entry}, /*initial_oplog_sequence_id=*/0, {seg});
    ASSERT_FALSE(late.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, late.error());
    EXPECT_EQ(RestoreLifecycleState::kServing,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
}

TEST_F(MasterServiceTest, RestoreLifecycleRetryAfterFailedRestoreReachesReady) {
    MasterService service(MakeStrictTenantConfig({"default"}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service, "ep1");

    // A memory segment with no name fails validation inside the restore.
    StandbySegmentInfo invalid_seg;
    invalid_seg.is_memory_segment = true;
    ASSERT_FALSE(service
                     .RestoreFromStandbySnapshot(
                         {}, /*initial_oplog_sequence_id=*/0, {invalid_seg})
                     .has_value());
    EXPECT_EQ(RestoreLifecycleState::kFailed,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));

    // Replacement restore semantics are unchanged: a retry from Failed is
    // allowed (the HA discard-repair retry tests rely on this) and a
    // successful retry reaches Ready.
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto entry = MakeStandbyEntry("restored_key", 128, 0x1000, "ep1");
    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {entry}, /*initial_oplog_sequence_id=*/0, {seg})
                    .has_value());
    EXPECT_EQ(RestoreLifecycleState::kReady,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
}

TEST_F(MasterServiceTest, RestoreLifecycleFailedRestoreReachesFailed) {
    MasterService service(MakeStrictTenantConfig({"default"}));
    // A memory segment with no name fails validation inside the restore.
    StandbySegmentInfo invalid_seg;
    invalid_seg.is_memory_segment = true;

    auto result = service.RestoreFromStandbySnapshot(
        {}, /*initial_oplog_sequence_id=*/0, {invalid_seg});
    ASSERT_FALSE(result.has_value());

    // Failed is terminal for the candidate: workers stay paused until the
    // supervisor destroys the service.
    EXPECT_EQ(RestoreLifecycleState::kFailed,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
    EXPECT_FALSE(
        MasterServiceTestPeer(service).AdmitsBusinessWorkersForTesting());
}

TEST_F(MasterServiceTest, RestoreLifecycleActivateServingRequiresReady) {
    MasterService service(MakeStrictTenantConfig({"default"}));

    // Activating a Constructed (never-restored) service must not open the
    // lifecycle: only a successful restore (Ready) may reach Serving.
    service.ActivateServing();
    EXPECT_EQ(RestoreLifecycleState::kConstructed,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));

    // The non-HA path: Constructed -> Ready -> Serving, explicitly.
    service.MarkReadyForServing();
    EXPECT_EQ(RestoreLifecycleState::kReady,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
    service.ActivateServing();
    EXPECT_EQ(RestoreLifecycleState::kServing,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
}

TEST_F(MasterServiceTest, RestoreLifecycleMarkReadyRequiresConstructed) {
    MasterService service(MakeStrictTenantConfig({"default"}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service, "ep1");
    const auto seg = MakeStandbySegment("seg", "ep1", 4096);
    auto entry = MakeStandbyEntry("restored_key", 128, 0x1000, "ep1");
    ASSERT_TRUE(service
                    .RestoreFromStandbySnapshot(
                        {entry}, /*initial_oplog_sequence_id=*/0, {seg})
                    .has_value());

    // MarkReadyForServing is the non-HA initialization edge only; calling
    // it after a restore must not undo the Ready state or re-open anything.
    service.MarkReadyForServing();
    EXPECT_EQ(RestoreLifecycleState::kReady,
              MasterServiceTestPeer::GetRestoreLifecycleState(service));
}

TEST_F(MasterServiceTest, RestoreLifecycleWorkersPausedOnlyInRestoreWindows) {
    MasterService service(MakeStrictTenantConfig({"default"}));
    MasterServiceTestPeer peer(service);

    struct Case {
        RestoreLifecycleState state;
        bool admitted;
    };
    const std::vector<Case> cases = {
        {RestoreLifecycleState::kConstructed, true},
        {RestoreLifecycleState::kRecovering, false},
        {RestoreLifecycleState::kReady, true},
        {RestoreLifecycleState::kServing, true},
        {RestoreLifecycleState::kFailed, false},
    };

    for (const auto& test_case : cases) {
        MasterServiceTestPeer::SetRestoreLifecycleStateForTesting(
            service, test_case.state);
        EXPECT_EQ(test_case.state,
                  MasterServiceTestPeer::GetRestoreLifecycleState(service));
        EXPECT_EQ(test_case.admitted, peer.AdmitsBusinessWorkersForTesting())
            << "state=" << static_cast<int>(test_case.state);
    }
}

}  // namespace mooncake::test
