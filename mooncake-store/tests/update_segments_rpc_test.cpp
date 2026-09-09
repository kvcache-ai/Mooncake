#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "master_client.h"
#include "rpc_types.h"
#include "test_server_helpers.h"
#include "types.h"
#include "utils.h"

namespace mooncake::testing {

class UpdateSegmentsRpcTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("UpdateSegmentsRpcTest");
        FLAGS_logtostderr = 1;

        InProcMasterConfig config;
        ASSERT_TRUE(master_.Start(config));
    }

    static void TearDownTestSuite() {
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    static Segment MakeSegment(const std::string& name,
                               uintptr_t base = 0x300000000) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = name;
        segment.base = base;
        segment.size = 16 * 1024 * 1024;
        segment.protocol = "tcp";
        segment.te_endpoint = name;
        return segment;
    }

    static UpdateSegmentsRequest MakeRequest(
        SegmentUpdateRequestIntent request_intent,
        std::vector<SegmentUpdate> segments = {}) {
        UpdateSegmentsRequest request;
        request.request_intent = request_intent;
        request.segments = std::move(segments);
        return request;
    }

    static void Connect(MasterClient& client) {
        ASSERT_EQ(client.Connect(master_.master_address()), ErrorCode::OK);
    }

    inline static InProcMaster master_;
};

TEST_F(UpdateSegmentsRpcTest, RegisterNewMountsWithoutConfirmingSnapshot) {
    MasterClient client(generate_uuid());
    Connect(client);
    const Segment segment = MakeSegment("update_register_new");

    auto response = client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::REGISTER,
                    {{segment, SegmentRegistrationIntent::NEW}}));

    ASSERT_TRUE(response.has_value());
    EXPECT_EQ(response->client_status, ClientStatus::NEED_REMOUNT);
    ASSERT_EQ(response->results.size(), 1);
    EXPECT_EQ(response->results[0].segment_id, segment.id);
    EXPECT_EQ(response->results[0].error_code, ErrorCode::OK);

    auto status = client.QuerySegmentStatusById(segment.id);
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(*status, SegmentStatus::OK);

    auto ping = client.Ping();
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(ping->client_status, ClientStatus::NEED_REMOUNT);
}

TEST_F(UpdateSegmentsRpcTest, RegisterRejectsRemountIntentAtomically) {
    MasterClient client(generate_uuid());
    Connect(client);
    const Segment new_segment = MakeSegment("update_illegal_register_new");
    const Segment remount_segment =
        MakeSegment("update_illegal_register_remount", 0x310000000);

    auto response = client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::REGISTER,
                    {{new_segment, SegmentRegistrationIntent::NEW},
                     {remount_segment, SegmentRegistrationIntent::REMOUNT}}));

    ASSERT_FALSE(response.has_value());
    EXPECT_EQ(response.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(client.QuerySegmentStatusById(new_segment.id).has_value());
    EXPECT_FALSE(client.QuerySegmentStatusById(remount_segment.id).has_value());
}

TEST_F(UpdateSegmentsRpcTest,
       ReconcileDispatchesMixedSnapshotAndConfirmsClient) {
    MasterClient client(generate_uuid());
    Connect(client);
    const Segment existing = MakeSegment("update_mixed_existing");
    const Segment new_segment = MakeSegment("update_mixed_new", 0x310000000);

    auto registration = client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::REGISTER,
                    {{existing, SegmentRegistrationIntent::NEW}}));
    ASSERT_TRUE(registration.has_value());
    ASSERT_EQ(registration->results.size(), 1);
    ASSERT_EQ(registration->results[0].error_code, ErrorCode::OK);

    auto reconciliation = client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::RECONCILE,
                    {{existing, SegmentRegistrationIntent::REMOUNT},
                     {new_segment, SegmentRegistrationIntent::NEW}}));

    ASSERT_TRUE(reconciliation.has_value());
    EXPECT_EQ(reconciliation->client_status, ClientStatus::OK);
    ASSERT_EQ(reconciliation->results.size(), 2);
    EXPECT_EQ(reconciliation->results[0].segment_id, existing.id);
    EXPECT_EQ(reconciliation->results[0].error_code, ErrorCode::OK);
    EXPECT_EQ(reconciliation->results[1].segment_id, new_segment.id);
    EXPECT_EQ(reconciliation->results[1].error_code, ErrorCode::OK);
    auto existing_status = client.QuerySegmentStatusById(existing.id);
    ASSERT_TRUE(existing_status.has_value());
    EXPECT_EQ(*existing_status, SegmentStatus::OK);
    auto new_status = client.QuerySegmentStatusById(new_segment.id);
    ASSERT_TRUE(new_status.has_value());
    EXPECT_EQ(*new_status, SegmentStatus::OK);

    auto ping = client.Ping();
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(ping->client_status, ClientStatus::OK);
}

TEST_F(UpdateSegmentsRpcTest, EmptyReconcileConfirmsEmptyClient) {
    MasterClient client(generate_uuid());
    Connect(client);

    auto response = client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::RECONCILE));

    ASSERT_TRUE(response.has_value());
    EXPECT_EQ(response->client_status, ClientStatus::OK);
    EXPECT_TRUE(response->results.empty());
    auto ping = client.Ping();
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(ping->client_status, ClientStatus::OK);
}

TEST_F(UpdateSegmentsRpcTest, RegisterOnHealthyClientKeepsClientHealthy) {
    MasterClient client(generate_uuid());
    Connect(client);
    ASSERT_TRUE(
        client
            .UpdateSegments(MakeRequest(SegmentUpdateRequestIntent::RECONCILE))
            .has_value());
    const Segment segment = MakeSegment("update_healthy_incremental");

    auto response = client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::REGISTER,
                    {{segment, SegmentRegistrationIntent::NEW}}));

    ASSERT_TRUE(response.has_value());
    EXPECT_EQ(response->client_status, ClientStatus::OK);
    ASSERT_EQ(response->results.size(), 1);
    EXPECT_EQ(response->results[0].error_code, ErrorCode::OK);
    auto ping = client.Ping();
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(ping->client_status, ClientStatus::OK);
}

TEST_F(UpdateSegmentsRpcTest, FailedReconcileKeepsHealthyClientHealthy) {
    MasterClient healthy_client(generate_uuid());
    MasterClient owner_client(generate_uuid());
    Connect(healthy_client);
    Connect(owner_client);
    ASSERT_TRUE(
        healthy_client
            .UpdateSegments(MakeRequest(SegmentUpdateRequestIntent::RECONCILE))
            .has_value());

    const Segment foreign_segment = MakeSegment("update_foreign_owner");
    auto registration = owner_client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::REGISTER,
                    {{foreign_segment, SegmentRegistrationIntent::NEW}}));
    ASSERT_TRUE(registration.has_value());
    ASSERT_EQ(registration->results.size(), 1);
    ASSERT_EQ(registration->results[0].error_code, ErrorCode::OK);

    const Segment pending_new =
        MakeSegment("update_failed_reconcile_new", 0x310000000);
    auto reconciliation = healthy_client.UpdateSegments(
        MakeRequest(SegmentUpdateRequestIntent::RECONCILE,
                    {{pending_new, SegmentRegistrationIntent::NEW},
                     {foreign_segment, SegmentRegistrationIntent::REMOUNT}}));

    ASSERT_FALSE(reconciliation.has_value());
    EXPECT_EQ(reconciliation.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(
        healthy_client.QuerySegmentStatusById(pending_new.id).has_value());
    auto ping = healthy_client.Ping();
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(ping->client_status, ClientStatus::OK);
}

TEST_F(UpdateSegmentsRpcTest, UnknownRequestIntentIsRejected) {
    MasterClient client(generate_uuid());
    Connect(client);
    auto request = MakeRequest(SegmentUpdateRequestIntent::REGISTER);
    request.request_intent = static_cast<SegmentUpdateRequestIntent>(255);

    auto response = client.UpdateSegments(request);

    ASSERT_FALSE(response.has_value());
    EXPECT_EQ(response.error(), ErrorCode::INVALID_PARAMS);
    auto ping = client.Ping();
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(ping->client_status, ClientStatus::NEED_REMOUNT);
}

}  // namespace mooncake::testing
