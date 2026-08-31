// Integration test: prove a per-request `request_id` set on the calling
// thread is carried through the coro_rpc out-of-band request attachment all the
// way into the master GetReplicaListRpc handler. This makes the attachment
// round-trip a deterministic CI assertion rather than a VLOG-only observation.
//
// Why single-key GetReplicaList and not the batch path: MasterClient::invoke_rpc
// (single key) snapshots current_request_id_attachment() at entry and sends it
// via send_request_with_attachment; invoke_batch_rpc does not attach, so the
// batch handler would observe an empty attachment by design. We only exercise
// the single-key read route.
//
// The seam: rpc_service.cpp GetReplicaListRpc and BatchGetReplicaListRpc call
// RecordObservedRequestId(attachment) inside their attachment block. After the
// synchronous client call returns (syncAwait guarantees the handler already
// replied, hence happens-before), the test reads LastObservedRequestId(). A
// nonexistent key still records the attachment before the OBJECT_NOT_FOUND
// lookup, so the key need not exist.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "centralized_master_client.h"
#include "default_config.h"
#include "master_config.h"
#include "request_context.h"
#include "rpc_types.h"
#include "test_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {
namespace {

class RequestIdAttachmentTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder{}.build()))
            << "failed to start in-process master";
        client_ = std::make_unique<CentralizedMasterClient>(generate_uuid());
        ASSERT_EQ(client_->Connect(master_.master_address()), ErrorCode::OK);
        ClearLastObservedRequestId();
    }

    void TearDown() override {
        clear_current_request_context();
        client_.reset();
        master_.Stop();
    }

    InProcMaster master_;
    std::unique_ptr<CentralizedMasterClient> client_;
};

// A non-empty request_id set on the calling thread before a single-key read
// must arrive at the master handler intact, proving the attachment bypass works
// end to end.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnReadRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-123";
    set_current_request_context(std::move(ctx));

    auto result = client_->GetReplicaList("nonexistent_key_attachment_test");
    (void)result;  // OBJECT_NOT_FOUND is expected; the attachment is recorded
                   // before the lookup, which is all this test checks.

    EXPECT_EQ(LastObservedRequestId(), "attach-123")
        << "request_id was not propagated via the coro_rpc attachment";
}

// No per-request context on the calling thread => empty attachment => the
// handler skips the record block => last observed id stays empty.
TEST_F(RequestIdAttachmentTest, EmptyAttachmentWhenNoRequestId) {
    clear_current_request_context();

    auto result = client_->GetReplicaList("nonexistent_key_attachment_test");
    (void)result;

    EXPECT_TRUE(LastObservedRequestId().empty())
        << "expected no observed request_id when none was set on the context";
}

}  // namespace
}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    mooncake::init_ylt_log_level();
    return RUN_ALL_TESTS();
}
