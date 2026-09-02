// Integration test: prove a per-request `request_id` set on the calling
// thread is carried through the coro_rpc out-of-band request attachment all the
// way into the master GetReplicaList handler. This makes the attachment
// round-trip a deterministic CI assertion rather than a VLOG-only observation.
//
// Both single-key (invoke_rpc) and batch (invoke_batch_rpc) client templates
// snapshot current_request_id_attachment() at entry and send it via
// send_request_with_attachment. We exercise the single-key read route
// (GetReplicaList) and the batch-exist route (BatchExistKey) to prove the
// attachment bypass works for both the single and batch invocation templates.
//
// The seam: the context-handler overloads in rpc_service.cpp / centralized_rpc_service.cpp
// call RecordObservedRequestId(attachment) inside their attachment block. After
// the synchronous client call returns (syncAwait guarantees the handler already
// replied, hence happens-before), the test reads LastObservedRequestId(). A
// nonexistent key still records the attachment before the lookup, so the key
// need not exist.

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

// The batch-exist route goes through MasterClient::invoke_batch_rpc now using
// send_request_with_attachment; the BatchExistKey handler must observe the id.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnBatchExistRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-batch-exist";
    set_current_request_context(std::move(ctx));

    std::string key = "nonexistent_key_batch_exist_attachment_test";
    std::vector<std::string_view> keys{key};
    auto result = client_->BatchExistKey(keys);
    (void)result;  // existence lookup result is irrelevant; the attachment is
                   // recorded before the lookup.

    EXPECT_EQ(LastObservedRequestId(), "attach-batch-exist")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "batch-exist route (invoke_batch_rpc)";
}

// Single-key exist route: MasterClient::ExistKey goes through invoke_rpc (now
// also send_request_with_attachment); the WrappedMasterService::ExistKey
// context-handler must observe the id.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnExistKeyRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-exist-key";
    set_current_request_context(std::move(ctx));

    auto result = client_->ExistKey("nonexistent_key_exist_attachment_test");
    (void)result;  // existence lookup result is irrelevant; the attachment is
                   // recorded before the lookup.

    EXPECT_EQ(LastObservedRequestId(), "attach-exist-key")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "single-key exist route (invoke_rpc -> ExistKey)";
}

// Single-key remove route: MasterClient::Remove goes through invoke_rpc; the
// WrappedMasterService::Remove context-handler must observe the id.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnRemoveRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-remove";
    set_current_request_context(std::move(ctx));

    // Remove of a nonexistent key still records the attachment before the
    // (failing) lookup in the value-returning body.
    auto result = client_->Remove("nonexistent_key_remove_attachment_test",
                                   /*force=*/false);
    (void)result;

    EXPECT_EQ(LastObservedRequestId(), "attach-remove")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "single-key remove route (invoke_rpc -> Remove)";
}

// Single-key put-start route: CentralizedMasterClient::PutStart goes through
// invoke_rpc; the WrappedCentralizedMasterService::PutStart context-handler
// must observe the id. The put-start body may fail without a registered
// segment, but the attachment is recorded before the body runs.
TEST_F(RequestIdAttachmentTest, CarriesRequestIdOnPutStartRoute) {
    RequestContext ctx;
    ctx.request_id = "attach-put-start";
    set_current_request_context(std::move(ctx));

    ReplicateConfig config;
    auto result = client_->PutStart("nonexistent_key_put_start_attachment_test",
                                    std::vector<size_t>{1024}, config);
    (void)result;

    EXPECT_EQ(LastObservedRequestId(), "attach-put-start")
        << "request_id was not propagated via the coro_rpc attachment on the "
           "single-key put-start route (invoke_rpc -> PutStart)";
}

// Note: the dummy hop A->B bridge (DummyClient::X -> RealClient *_dummy_helper
// / *_internal_rpc V3 handler installing CurrentCtxScope so the same-thread hop
// B master RPC re-attaches the id) is exercised only end-to-end (DummyClient +
// real-client coro_rpc server + master). This in-process harness drives the
// real path (hop B only), so it does not cover the hop A bridge; that remains
// verified by reasoning (same-thread syncAwait + CurrentCtxScope, per plan).

}  // namespace
}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    mooncake::init_ylt_log_level();
    return RUN_ALL_TESTS();
}
