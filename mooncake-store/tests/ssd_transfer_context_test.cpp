#include "device/transfer_context.h"

#include <gtest/gtest.h>

#include <thread>

namespace {
thread_local aclrtContext current_context = nullptr;
thread_local bool get_fails = false;
thread_local bool set_fails = false;
thread_local int get_calls = 0;
thread_local int set_calls = 0;
int device_3_context;
int device_5_context;
int caller_context;
}  // namespace

extern "C" aclError aclrtGetCurrentContext(aclrtContext* context) {
    ++get_calls;
    if (get_fails) return 1;
    *context = current_context;
    return ACL_ERROR_NONE;
}

extern "C" aclError aclrtSetCurrentContext(aclrtContext context) {
    ++set_calls;
    if (set_fails) return 1;
    current_context = context;
    return ACL_ERROR_NONE;
}

namespace mooncake::device {
namespace {
class SsdTransferContextTest : public ::testing::Test {
   protected:
    void SetUp() override {
        current_context = nullptr;
        get_fails = false;
        set_fails = false;
        get_calls = 0;
        set_calls = 0;
    }
};

TEST_F(SsdTransferContextTest, FreshWorkerUsesRegistrationContext) {
    current_context = &device_3_context;
    TransferContext captured;
    ASSERT_TRUE(captured.Capture("ascend"));
    std::thread worker([&] {
        EXPECT_EQ(current_context, nullptr);
        {
            TransferContext::Guard guard(captured);
            EXPECT_TRUE(guard.ok());
            EXPECT_EQ(current_context, &device_3_context);
        }
        EXPECT_EQ(current_context, &device_3_context);
    });
    worker.join();
    EXPECT_EQ(current_context, &device_3_context);
}

TEST_F(SsdTransferContextTest, RestoresExistingWorkerContext) {
    current_context = &device_3_context;
    TransferContext captured;
    ASSERT_TRUE(captured.Capture("ascend"));
    current_context = &caller_context;
    {
        TransferContext::Guard guard(captured);
        EXPECT_TRUE(guard.ok());
        EXPECT_EQ(current_context, &device_3_context);
    }
    EXPECT_EQ(current_context, &caller_context);
    EXPECT_EQ(set_calls, 2);
}

TEST_F(SsdTransferContextTest, MatchingContextNeedsNoSwitch) {
    current_context = &device_3_context;
    TransferContext captured;
    ASSERT_TRUE(captured.Capture("ascend"));
    {
        TransferContext::Guard guard(captured);
        EXPECT_TRUE(guard.ok());
    }
    EXPECT_EQ(set_calls, 0);
}

TEST_F(SsdTransferContextTest, MissingRegistrationContextFailsClosed) {
    TransferContext captured;
    EXPECT_FALSE(captured.Capture("ascend"));
    TransferContext::Guard guard(captured);
    EXPECT_FALSE(guard.ok());
    EXPECT_EQ(set_calls, 0);
}

TEST_F(SsdTransferContextTest, CaptureApiFailureFailsClosed) {
    current_context = &device_3_context;
    get_fails = true;
    TransferContext captured;
    EXPECT_FALSE(captured.Capture("ascend"));
    TransferContext::Guard guard(captured);
    EXPECT_FALSE(guard.ok());
    EXPECT_EQ(set_calls, 0);
}

TEST_F(SsdTransferContextTest, BindingFailurePreservesWorkerContext) {
    current_context = &device_3_context;
    TransferContext captured;
    ASSERT_TRUE(captured.Capture("ascend"));
    current_context = &caller_context;
    set_fails = true;
    {
        TransferContext::Guard guard(captured);
        EXPECT_FALSE(guard.ok());
    }
    EXPECT_EQ(current_context, &caller_context);
    EXPECT_EQ(set_calls, 1);
}

TEST_F(SsdTransferContextTest, ReusedWorkerBindsEachArena) {
    current_context = &device_3_context;
    TransferContext first;
    ASSERT_TRUE(first.Capture("ascend"));
    current_context = &device_5_context;
    TransferContext second;
    ASSERT_TRUE(second.Capture("ascend"));
    std::thread worker([&] {
        current_context = &caller_context;
        {
            TransferContext::Guard guard(first);
            EXPECT_TRUE(guard.ok());
            EXPECT_EQ(current_context, &device_3_context);
        }
        EXPECT_EQ(current_context, &caller_context);
        {
            TransferContext::Guard guard(second);
            EXPECT_TRUE(guard.ok());
            EXPECT_EQ(current_context, &device_5_context);
        }
        EXPECT_EQ(current_context, &caller_context);
    });
    worker.join();
    EXPECT_EQ(current_context, &device_5_context);
}

TEST_F(SsdTransferContextTest, UnboundWorkerCanReportNoCurrentContext) {
    current_context = &device_3_context;
    TransferContext captured;
    ASSERT_TRUE(captured.Capture("ascend"));
    current_context = nullptr;
    get_fails = true;
    TransferContext::Guard guard(captured);
    EXPECT_TRUE(guard.ok());
    EXPECT_EQ(current_context, &device_3_context);
}

TEST_F(SsdTransferContextTest, OtherProtocolsDoNotCallAcl) {
    current_context = &caller_context;
    get_fails = true;
    set_fails = true;
    for (const auto* protocol : {"tcp", "rdma", "nvlink"}) {
        TransferContext captured;
        EXPECT_TRUE(captured.Capture(protocol));
        TransferContext::Guard guard(captured);
        EXPECT_TRUE(guard.ok());
    }
    EXPECT_EQ(current_context, &caller_context);
    EXPECT_EQ(get_calls, 0);
    EXPECT_EQ(set_calls, 0);
}
}  // namespace
}  // namespace mooncake::device
