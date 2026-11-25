#include "nof/probe_context_pool.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <set>

namespace mooncake {
namespace {

// Plays the role of SpdkInitiator::ProbeReadComplete: the completion path
// writes into the context, possibly long after the caller stopped waiting.
void Complete(NofProbeContext* ctx, bool success) {
    ctx->success.store(success, std::memory_order_release);
    ctx->done.store(true, std::memory_order_release);
}

class NofProbeContextPoolTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NofProbeContextPoolTest");
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(NofProbeContextPoolTest, TimedOutContextIsNotReissuedWhileOutstanding) {
    NofProbeContextPool pool;
    auto* ctx1 = pool.Acquire();
    // Probe timed out with the NVMe command still in flight: quarantine.
    pool.Quarantine(ctx1);
    EXPECT_EQ(pool.QuarantinedCount(), 1u);

    // The next probe must get a different context — reissuing the in-flight
    // one would let its stale completion write into the new probe.
    auto* ctx2 = pool.Acquire();
    EXPECT_NE(ctx1, ctx2);
    pool.Recycle(ctx2);
}

TEST_F(NofProbeContextPoolTest, DelayedCompletionDoesNotCorruptActiveProbe) {
    NofProbeContextPool pool;
    auto* ctx1 = pool.Acquire();
    pool.Quarantine(ctx1);

    auto* ctx2 = pool.Acquire();  // the next probe is now waiting on ctx2
    // The stale completion of the first probe arrives (processed by a later
    // poll): it must only write into its own, quarantined context.
    Complete(ctx1, true);

    EXPECT_TRUE(ctx1->done.load());
    EXPECT_TRUE(ctx1->success.load());
    EXPECT_FALSE(ctx2->done.load());
    EXPECT_FALSE(ctx2->success.load());

    pool.Quarantine(ctx2);
}

TEST_F(NofProbeContextPoolTest, QuarantinedContextReapedOnlyAfterCallbackRuns) {
    NofProbeContextPool pool;
    auto* ctx1 = pool.Acquire();
    pool.Quarantine(ctx1);

    // Callback has not run: ctx1 stays quarantined across Acquire calls.
    auto* ctx2 = pool.Acquire();
    EXPECT_NE(ctx2, ctx1);
    EXPECT_EQ(pool.QuarantinedCount(), 1u);
    pool.Recycle(ctx2);

    // The stale callback finally runs; the next Acquire reaps ctx1, so it
    // becomes reusable again (Reset before reuse).
    Complete(ctx1, true);
    bool reused = false;
    for (int i = 0; i < 3 && !reused; ++i) {
        auto* ctx = pool.Acquire();
        if (ctx == ctx1) {
            reused = true;
            EXPECT_FALSE(ctx->done.load());  // handed out clean
            EXPECT_EQ(ctx->seg, nullptr);
        }
    }
    EXPECT_TRUE(reused);
    EXPECT_EQ(pool.QuarantinedCount(), 0u);
}

TEST_F(NofProbeContextPoolTest, RecycledContextIsReusedImmediately) {
    NofProbeContextPool pool;
    auto* ctx1 = pool.Acquire();
    Complete(ctx1, true);  // normal completion before returning to the pool
    pool.Recycle(ctx1);

    auto* ctx2 = pool.Acquire();
    EXPECT_EQ(ctx1, ctx2);            // LIFO reuse preserved
    EXPECT_FALSE(ctx2->done.load());  // Reset on the way out
    EXPECT_FALSE(ctx2->success.load());
}

}  // namespace
}  // namespace mooncake
