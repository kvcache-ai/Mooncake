// Regression test for the SpdkEnvGuard reacquire failure: SPDK v23.01.1
// rejects a non-NULL-opts spdk_env_init after spdk_env_fini with -EINVAL, so
// finalizing the env on last release broke every later Acquire() in the
// process. The env must stay pinned until process exit.
//
// This sequence never calls RegisterMemory, so the NofRegistryBundle env pin
// cannot mask the failure. Requires a working SPDK env (hugepages); skips
// when the first acquisition already fails (e.g. CI containers).

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
