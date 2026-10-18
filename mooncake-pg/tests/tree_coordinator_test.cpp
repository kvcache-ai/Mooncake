#include "control_plane/coordinator.h"

#include <algorithm>
#include <array>
#include <gtest/gtest.h>
#include <ylt/struct_pack.hpp>

#include "device_comm/device_collective/algorithms/ring/ring_types.cuh"
#include "device_comm/device_collective/algorithms/oneshot/one_shot_types.cuh"
#include "device_comm/device_collective/algorithms/tree/tree_types.cuh"
#include "device_comm/device_collective/device_control_update.h"

namespace mooncake {
namespace {

class CoordinatorFixture {
   public:
    explicit CoordinatorFixture(
        std::optional<DeviceAllReduceAlgorithm> algorithm)
        : coordinator(3, std::chrono::microseconds(50000), algorithm) {
        for (GlobalRank rank = 0; rank < 3; ++rank) {
            const auto response =
                coordinator.handleRegisterAgent(agent(rank)).response;
            EXPECT_TRUE(response.success);
        }
        for (GlobalRank rank = 0; rank < 3; ++rank) {
            LinkEventReport report;
            report.reporter_rank = rank;
            report.agent_session_id = 100 + rank;
            report.reporter_rank_epoch = 1;
            report.report_id = 1;
            report.events.assign(3, LinkEvent::EventType::Success);
            report.target_rank_epochs.assign(3, 1);
            coordinator.handleLinkEventReport(report);
        }
    }

    RegisterAgentRequest agent(GlobalRank rank) {
        RegisterAgentRequest request;
        request.rank = rank;
        request.agent_session_id = 100 + rank;
        request.transfer_service_endpoint =
            DeviceTransferEndpoint{4096, 128ull << 20, {}};
        request.collective_workspace_endpoint =
            DeviceCollectiveWorkspaceEndpoint{0, 32ull << 20};
        return request;
    }

    RegisterGroupRequest registration(
        GlobalRank rank, std::optional<GpuCollectiveBackend> preference =
                             GpuCollectiveBackend::New) {
        RegisterGroupRequest request;
        request.rank = rank;
        request.agent_session_id = 100 + rank;
        request.group_bootstrap_id = "test-tree";
        request.max_group_size = 3;
        request.rank_order = rank == 2 ? std::vector<GlobalRank>{0, 1, 2}
                                       : std::vector<GlobalRank>{0, 1};
        request.preferred_gpu_collective_backend = preference;
        request.resolve_policy =
            rank == 2 ? GroupBootstrapIdResolvePolicy::AttachOrExtend
                      : GroupBootstrapIdResolvePolicy::CreateOrAttach;
        return request;
    }

    GroupEndpointInfo endpoint(GlobalRank rank, bool atomic = false) {
        GroupEndpointInfo info;
        auto& endpoint = info.device_collective;
        endpoint.view_epoch_signal = 32ull << 20;
        endpoint.view_epoch_signal_count = 3;
        endpoint.simple.emplace(SimpleEndpoint{(32ull << 20) + 512, 640});
        endpoint.tree.emplace();
        if (atomic) {
            LLEndpoint ll;
            ll.device_uuid[0] = rank;
            for (GlobalRank peer = 0; peer < 3; ++peer) {
                if (peer == rank) continue;
                DeviceUUID uuid{};
                uuid[0] = peer;
                ll.native_atomic_peer_uuids.push_back(uuid);
            }
            endpoint.ll.emplace(ll);
            endpoint.one_shot.emplace(OneShotEndpoint{64ull << 20, 4ull << 20});
        }
        return info;
    }

    PublishEndpointResponse publish(GlobalRank rank, GroupEndpointInfo info) {
        PublishEndpointRequest request;
        request.rank = rank;
        request.agent_session_id = 100 + rank;
        request.endpoints.push_back({group, std::move(info)});
        const auto result = coordinator.handlePublishEndpoint(request);
        for (const auto& effect : result.effects) {
            const auto* update = std::get_if<PushViewUpdate>(&effect);
            if (!update || update->view.status != GroupStatus::BootstrapSyncing)
                continue;
            const auto epoch = update->view.epoch;
            coordinator.handleViewUpdateAck(group, 0, epoch, true);
            coordinator.handleViewUpdateAck(group, 1, epoch, true);
        }
        return result.response;
    }

    GroupView snapshot() {
        const auto response =
            coordinator.handleRegisterAgent(agent(0)).response;
        for (const auto& view : response.groups)
            if (view.group_id == group) return view;
        ADD_FAILURE() << "group snapshot missing";
        return {};
    }

    void ready(bool atomic = false,
               std::optional<GpuCollectiveBackend> preference =
                   GpuCollectiveBackend::New) {
        auto first =
            coordinator.handleRegisterGroup(registration(0, preference))
                .response;
        ASSERT_TRUE(first.success) << first.reject_reason;
        group = first.view.group_id;
        ASSERT_TRUE(coordinator.handleRegisterGroup(registration(1, preference))
                        .response.success);
        ASSERT_TRUE(
            publish(0, preference ? endpoint(0, atomic) : GroupEndpointInfo{})
                .success);
        ASSERT_TRUE(
            publish(1, preference ? endpoint(1, atomic) : GroupEndpointInfo{})
                .success);
        ASSERT_EQ(snapshot().status, GroupStatus::Ready);
    }

    CentralizedCoordinatorStateMachine coordinator;
    GroupId group;
};

TEST(TreeCoordinatorTest,
     ExplicitTreeSelectsTheWholeByteRangeWithoutAtomicPeers) {
    CoordinatorFixture fixture(DeviceAllReduceAlgorithm::Tree);
    fixture.ready();
    const auto view = fixture.snapshot();
    EXPECT_EQ(view.gpu_collective_backend, GpuCollectiveBackend::New);
    ASSERT_EQ(view.all_reduce_algorithm_choices.size(), 1u);
    EXPECT_EQ(view.all_reduce_algorithm_choices[0].algorithm,
              DeviceAllReduceAlgorithm::Tree);
    EXPECT_EQ(view.all_reduce_algorithm_choices[0].max_bytes, UINT64_MAX);
}

TEST(TreeCoordinatorTest, LegacyRejectionIsFatalAndWireCompatible) {
    CoordinatorFixture fixture(DeviceAllReduceAlgorithm::Tree);
    const auto response = fixture.coordinator
                              .handleRegisterGroup(fixture.registration(
                                  0, GpuCollectiveBackend::Legacy))
                              .response;
    EXPECT_FALSE(response.success);
    ASSERT_TRUE(response.allow_local_fallback);
    EXPECT_FALSE(*response.allow_local_fallback);
    struct OldResponse {
        bool success;
        std::string reject_reason;
        GroupView view;
    };
    const auto old =
        struct_pack::deserialize<OldResponse>(struct_pack::serialize(response));
    ASSERT_TRUE(old.has_value());
    EXPECT_FALSE(old->success);
    EXPECT_EQ(old->reject_reason, response.reject_reason);
    const auto modern = struct_pack::deserialize<RegisterGroupResponse>(
        struct_pack::serialize(OldResponse{false, "old rejection", {}}));
    ASSERT_TRUE(modern.has_value());
    EXPECT_FALSE(modern->allow_local_fallback.has_value());
}

TEST(TreeCoordinatorTest, RejectsMissingCapabilityDuringBootstrap) {
    CoordinatorFixture fixture(DeviceAllReduceAlgorithm::Tree);
    const auto first =
        fixture.coordinator.handleRegisterGroup(fixture.registration(0))
            .response;
    ASSERT_TRUE(first.success);
    fixture.group = first.view.group_id;
    auto endpoint = fixture.endpoint(0);
    endpoint.device_collective.tree.reset();
    const auto response = fixture.publish(0, endpoint);
    EXPECT_FALSE(response.success);
    EXPECT_NE(response.reject_reason.find("Tree v1"), std::string::npos);
    EXPECT_FALSE(fixture.snapshot().members[0].endpoint);
}

TEST(TreeCoordinatorTest, RejectsIncompatibleJoinerWithoutChangingActiveTree) {
    for (const auto version : {0u, 2u}) {
        CoordinatorFixture fixture(DeviceAllReduceAlgorithm::Tree);
        fixture.ready();
        ASSERT_TRUE(
            fixture.coordinator.handleRegisterGroup(fixture.registration(2))
                .response.success);
        const auto before = fixture.snapshot();
        auto endpoint = fixture.endpoint(2);
        if (version == 0)
            endpoint.device_collective.tree.reset();
        else
            endpoint.device_collective.tree->version = version;
        EXPECT_FALSE(fixture.publish(2, endpoint).success);
        const auto after = fixture.snapshot();
        EXPECT_EQ(after, before);
        EXPECT_TRUE(after.members[0].isActive());
        EXPECT_TRUE(after.members[1].isActive());
        EXPECT_FALSE(after.members[2].isActive());
        EXPECT_EQ(after.gpu_collective_backend, GpuCollectiveBackend::New);
        EXPECT_EQ(after.all_reduce_algorithm_choices[0].algorithm,
                  DeviceAllReduceAlgorithm::Tree);
        EXPECT_FALSE(
            fixture.coordinator
                .handleConfirmReadyForActivation({fixture.group, 2, 102})
                .response.success);
    }
}

TEST(TreeCoordinatorTest, RejectsInvalidSignalRangesBeforeChangingTheView) {
    CoordinatorFixture fixture(DeviceAllReduceAlgorithm::Tree);
    fixture.ready();
    ASSERT_TRUE(fixture.coordinator.handleRegisterGroup(fixture.registration(2))
                    .response.success);
    const auto before = fixture.snapshot();
    for (uint32_t invalid = 0; invalid < 5; ++invalid) {
        auto endpoint = fixture.endpoint(2);
        auto& device = endpoint.device_collective;
        switch (invalid) {
            case 0:
                device.simple->signal_count = 1;
                break;
            case 1:
                device.simple->signal_offset = UINT64_MAX;
                break;
            case 2:
                device.simple->signal_offset = 16;
                break;
            case 3:
                device.view_epoch_signal = device.simple->signal_offset;
                break;
            case 4:
                device.view_epoch_signal_count = 1;
                break;
        }
        EXPECT_FALSE(fixture.publish(2, endpoint).success);
        EXPECT_EQ(fixture.snapshot(), before);
    }
}

TEST(TreeCoordinatorTest, AutoRingOneShotAndCpuPoliciesRemainUnchanged) {
    for (const auto algorithm :
         {std::optional<DeviceAllReduceAlgorithm>{},
          std::optional{DeviceAllReduceAlgorithm::Ring},
          std::optional{DeviceAllReduceAlgorithm::OneShot}}) {
        CoordinatorFixture fixture(algorithm);
        fixture.ready(true);
        const auto choices = fixture.snapshot().all_reduce_algorithm_choices;
        if (algorithm == DeviceAllReduceAlgorithm::Ring) {
            EXPECT_TRUE(choices.empty());
        } else {
            ASSERT_EQ(choices.size(), 1u);
            EXPECT_EQ(choices[0].algorithm, DeviceAllReduceAlgorithm::OneShot);
            EXPECT_EQ(choices[0].max_bytes,
                      algorithm ? UINT64_MAX : 512ull * 1024);
        }
    }
    CoordinatorFixture unavailable(DeviceAllReduceAlgorithm::OneShot);
    unavailable.ready();
    EXPECT_TRUE(unavailable.snapshot().all_reduce_algorithm_choices.empty());
    CoordinatorFixture cpu(DeviceAllReduceAlgorithm::Tree);
    cpu.ready(false, std::nullopt);
    EXPECT_FALSE(cpu.snapshot().gpu_collective_backend);
    EXPECT_TRUE(cpu.snapshot().all_reduce_algorithm_choices.empty());
}

TEST(TreeControlUpdateTest,
     FullRecoveryConfigurationUsesEightOperationsWithinPayload) {
    SimpleBindings simple{};
    LLBindings ll{};
    RingAllReducePlan ring{};
    OneShotAllReducePlan one_shot{};
    TreeAllReducePlan tree{};
    std::array<int32_t, kMaxNumRanks> mirror{};
    std::array<uint64_t, 5 * kMaxDeviceCollectiveChannels * kMaxNumRanks>
        signals{};
    std::array<SimplePeerProgress, kMaxDeviceCollectiveChannels * kMaxNumRanks>
        progress{};
    ControlUpdateBuilder builder;
    ASSERT_TRUE(builder.copyBytes(mirror.data(), mirror.data(), sizeof(mirror))
                    .has_value());
    ASSERT_TRUE(
        builder.copyBytes(&simple, &simple, sizeof(simple)).has_value());
    ASSERT_TRUE(builder.fillU64(signals.data(), 0, signals.size()).has_value());
    ASSERT_TRUE(
        builder.fillBytes(progress.data(), 0, sizeof(progress)).has_value());
    ASSERT_TRUE(builder.copyBytes(&ll, &ll, sizeof(ll)).has_value());
    ASSERT_TRUE(builder.copyBytes(&ring, &ring, sizeof(ring)).has_value());
    ASSERT_TRUE(
        builder.copyBytes(&one_shot, &one_shot, sizeof(one_shot)).has_value());
    ASSERT_TRUE(builder.copyBytes(&tree, &tree, sizeof(tree)).has_value());
    EXPECT_EQ(builder.controlUpdate().operation_count, 8u);
    EXPECT_LE(builder.controlUpdate().payload_size,
              kDeviceControlUpdatePayloadBytes);
    EXPECT_FALSE(builder.fillBytes(progress.data(), 0, 0).has_value());
}

}  // namespace
}  // namespace mooncake
