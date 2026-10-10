// Regression test for the SpdkEnvGuard reacquire failure (PR #3424 review):
// in SPDK v23.01.1, re-initializing the environment within the same process
// after spdk_env_fini requires a NULL opts argument — passing options again
// returns -EINVAL. Finalizing the env when the last guard reference died
// therefore made every later SpdkEnvGuard::Acquire() fail, leaving NoF — and
// setup-time staging-buffer allocation through SpdkDmaAllocator — permanently
// unavailable after a client close/reopen cycle.
//
// The sequence below never calls RegisterMemory, so the NofRegistryBundle env
// pin is never constructed and cannot mask the failure; and it never destroys
// an SpdkInitiator that acquired the env, because ~Impl's UnregisterAll
// backstop would construct the bundle too (HAVE_SPDK_MEM_REGISTER builds).
// The DMA allocator exercises the guard directly without touching the page
// registry, so the test fails without the process-wide pin in SpdkEnvGuard
// and passes with it, in every build configuration.
//
// A working SPDK environment (hugepages) is required: the tests skip when the
// FIRST acquisition already fails (e.g. CI containers without hugepages), but
// any later acquisition after a full release must succeed whenever the first
// one did.

#include <gtest/gtest.h>

#include "nof/spdk_initiator.h"

namespace mooncake {
namespace {

TEST(NofEnvReacquireTest, DmaAllocatorReacquiresEnvAfterLastRelease) {
    {
        SpdkDmaAllocator first;
        void* p = first.Alloc(4096, 4096);
        if (p == nullptr) {
            GTEST_SKIP() << "SPDK env unavailable (no hugepages?); skipping";
        }
        first.Free(p);
    }  // last env reference released here, no registration pin exists

    SpdkDmaAllocator second;
    void* p = second.Alloc(4096, 4096);
    EXPECT_NE(p, nullptr)
        << "SPDK env could not be reacquired after the last reference was "
           "released";
    if (p != nullptr) {
        second.Free(p);
    }
}

TEST(NofEnvReacquireTest, ConcurrentAllocatorsShareOneEnv) {
    SpdkDmaAllocator first;
    void* p = first.Alloc(4096, 4096);
    if (p == nullptr) {
        GTEST_SKIP() << "SPDK env unavailable (no hugepages?); skipping";
    }

    // A second allocator created while the first is alive must reuse the same
    // process-global env, and freeing through one must not disturb the other.
    SpdkDmaAllocator second;
    void* q = second.Alloc(4096, 4096);
    EXPECT_NE(q, nullptr);
    first.Free(p);
    if (q != nullptr) {
        second.Free(q);
    }
}

}  // namespace
}  // namespace mooncake
