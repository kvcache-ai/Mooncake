// real_client_hugepage_register_test.cpp
//
// Focused regression test for "[Store] Register full HugeTLB segments for
// RDMA": registering a HugeTLB ShmHelper segment by its base address widens
// the MR to the physical segment [shm->base_addr, shm->size). ShmHelper
// rounds the mapping up to hugepage granularity, so the MR bounds must
// coincide with the mapping bounds; otherwise madvise(MADV_DONTFORK) —
// active after ibv_fork_init() — fails with EINVAL when the kernel has to
// split the hugetlb VMA at a non-hugepage-aligned boundary.
//
// Sub-range registrations are deliberately NOT special-cased: they register
// the logical range exactly as passed and fail with EINVAL on RDMA when
// misaligned (documented boundary).
//
// Protocol/device are taken from the PROTOCOL / DEVICE_NAME environment
// variables (tcp|rdma, same convention as pybind_client_test). With
// PROTOCOL=rdma the segment-base test is fully discriminating against the
// unfixed code; with tcp it still verifies the widening decision path end
// to end.

#include "client_service.h"
#include "real_client.h"
#include "shm_helper.h"
#include "test_server_helpers.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <thread>

namespace mooncake {
namespace testing {

namespace {
long nrHugepages() {
    std::ifstream f("/proc/sys/vm/nr_hugepages");
    long pages = 0;
    if (f.is_open()) {
        f >> pages;
    }
    return pages;
}
}  // namespace

class RealClientHugepageRegisterTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RealClientHugepageRegisterTest");
        FLAGS_logtostderr = 1;
        // Must be set before the ShmHelper singleton is first instantiated.
        setenv("MC_STORE_USE_HUGEPAGE", "1", 1);
        if (const char* p = std::getenv("PROTOCOL")) {
            protocol_ = p;
        }
        if (const char* d = std::getenv("DEVICE_NAME")) {
            device_name_ = d;
        }
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (nrHugepages() <= 0) {
            GTEST_SKIP() << "No free hugepages on this host "
                         << "(/proc/sys/vm/nr_hugepages)";
        }

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
            << "Failed to start in-proc master";
        master_address_ = master_.master_address();

        client_ = RealClient::create();
        const std::string devices =
            (protocol_ == std::string("rdma")) ? device_name_ : std::string("");
        ASSERT_EQ(client_->setup_real("localhost:17817", "P2PHANDSHAKE",
                                      16 * 1024 * 1024, 16 * 1024 * 1024,
                                      protocol_, devices, master_address_),
                  0);
    }

    void TearDown() override {
        if (client_) {
            client_->tearDownAll();
            client_.reset();
        }
        master_.Stop();
    }

    std::shared_ptr<RealClient> client_;
    InProcMaster master_;
    std::string master_address_;

    static inline std::string protocol_ = "tcp";
    static inline std::string device_name_;
};

// Registering a HugeTLB segment by its base address with a logical size
// smaller than the physical segment must succeed: the registration is
// widened to the aligned segment size, so the MR bounds coincide with the
// mapping bounds. The unfixed code registers the logical size, whose end
// lands inside the last huge page, and fails on RDMA with EINVAL.
TEST_F(RealClientHugepageRegisterTest, SegmentBaseRegistersFullSegment) {
    // 8MB segment = 4 hugepages (2MB each).
    const size_t kSegmentSize = 8 * 1024 * 1024;
    void* base = ShmHelper::getInstance()->allocate(kSegmentSize);
    ASSERT_NE(base, nullptr);
    auto shm = ShmHelper::getInstance()->get_shm(base);
    ASSERT_NE(shm, nullptr);
    ASSERT_EQ(shm->size, kSegmentSize);
    ASSERT_TRUE(ShmHelper::getInstance()->is_hugepage());

    // Logical size whose end (3MB + 512) sits inside a 2MB huge page —
    // exactly the shape that produced EINVAL before the fix.
    const size_t kLogicalSize = 3 * 1024 * 1024 + 512;

    EXPECT_EQ(client_->register_buffer(base, kLogicalSize), 0)
        << "Segment-base registration with a non-hugepage-multiple logical "
           "size failed";
    EXPECT_EQ(client_->unregister_buffer(base), 0);

    EXPECT_EQ(ShmHelper::getInstance()->free(base), 0);
}

// The parallel preTouch path (buffers >= 4GiB) trial-registers each thread's
// block, so its block split must be kernel-page aligned. A 4GiB+1MB logical
// request aligns the segment up to 2049 hugepages (4GiB + 2MB); 2049 is not
// divisible by any thread count in {8,16}, so the legacy fixed-4KB split
// (length/num_threads) would produce misaligned blocks and the trial
// ibv_reg_mr inside preTouch would fail with EINVAL.
TEST_F(RealClientHugepageRegisterTest,
       PreTouchPageAlignedBlocksOnLargeSegment) {
    if (protocol_ != std::string("rdma")) {
        GTEST_SKIP() << "preTouch is exercised on the RDMA path only";
    }
    if (std::thread::hardware_concurrency() < 4) {
        GTEST_SKIP() << "preTouch requires hardware_concurrency >= 4";
    }

    const size_t kSegmentSize =
        (size_t)4 * 1024 * 1024 * 1024 + 1024 * 1024;  // 4GiB + 1MB
    void* base = ShmHelper::getInstance()->allocate(kSegmentSize);
    ASSERT_NE(base, nullptr);
    auto shm = ShmHelper::getInstance()->get_shm(base);
    ASSERT_NE(shm, nullptr);
    EXPECT_EQ(shm->size % (2 * 1024 * 1024), 0u);
    EXPECT_EQ(shm->size, (size_t)4 * 1024 * 1024 * 1024 + 2 * 1024 * 1024);

    // Segment-base registration widens to the aligned segment size and
    // drives the parallel preTouch over the whole segment.
    const size_t kLogicalSize = (size_t)4 * 1024 * 1024 * 1024 + 1024 * 1024;

    EXPECT_EQ(client_->register_buffer(base, kLogicalSize), 0)
        << "Whole-segment registration with parallel preTouch failed";
    EXPECT_EQ(client_->unregister_buffer(base), 0);

    EXPECT_EQ(ShmHelper::getInstance()->free(base), 0);
}

// Hot cache (SHM mode) with a cache size that is not a 2MB multiple: the fix
// registers the aligned segment size instead of the logical size, so hot
// cache creation succeeds. The unfixed code registers the logical size, whose
// end lands inside the last huge page, and InitLocalHotCache fails with
// EINVAL (IsHotCacheEnabled() == false).
TEST_F(RealClientHugepageRegisterTest, HotCacheShmRegistersAlignedSegment) {
    if (protocol_ != std::string("rdma")) {
        GTEST_SKIP() << "discriminating only with RDMA registration";
    }

    const char* orig_size = std::getenv("MC_STORE_LOCAL_HOT_CACHE_SIZE");
    const char* orig_block = std::getenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE");
    const char* orig_shm = std::getenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM");
    setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", "34865152",
           1);  // 33MB, not 2MB-multiple
    setenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "3145728", 1);  // 3MB -> 11 blocks
    setenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1", 1);

    auto client_opt = Client::Create(
        "localhost:17819", "P2PHANDSHAKE", protocol_,
        std::optional<std::string>(device_name_), master_address_);
    ASSERT_TRUE(client_opt.has_value()) << "Client::Create failed";
    EXPECT_TRUE(client_opt.value()->IsHotCacheEnabled())
        << "hot cache registration must use the aligned segment size";

    client_opt.reset();  // tear down before restoring the environment
    if (orig_size) {
        setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", orig_size, 1);
    } else {
        unsetenv("MC_STORE_LOCAL_HOT_CACHE_SIZE");
    }
    if (orig_block) {
        setenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE", orig_block, 1);
    } else {
        unsetenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE");
    }
    if (orig_shm) {
        setenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", orig_shm, 1);
    } else {
        unsetenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM");
    }
}

// Sub-range registrations are not special-cased: an interior pointer is
// registered exactly as passed. On RDMA (ibv_fork_init active) a misaligned
// interior sub-range of a hugetlb VMA fails with EINVAL; on TCP there is no
// ibv_reg_mr and the exact registration succeeds.
//
// This test is intentionally the LAST in the suite: the deliberately failed
// ibv_reg_mr leaves residue in libibverbs' process-global fork-protection
// bookkeeping (rdma-core does not roll back the DONTFORK state on failure),
// which can poison the first registration of the following test. Keeping it
// last isolates that upstream artifact from the other tests.
TEST_F(RealClientHugepageRegisterTest,
       MisalignedInteriorSubRangeRegistersExactly) {
    const size_t kSegmentSize = 8 * 1024 * 1024;
    void* base = ShmHelper::getInstance()->allocate(kSegmentSize);
    ASSERT_NE(base, nullptr);

    const size_t kOffset = 1024 * 1024;
    const size_t kLength = 3 * 1024 * 1024 + 512;
    void* sub = static_cast<char*>(base) + kOffset;

    const int rc = client_->register_buffer(sub, kLength);
    if (protocol_ == std::string("rdma")) {
        EXPECT_NE(rc, 0)
            << "misaligned interior sub-range must not be silently widened";
    } else {
        EXPECT_EQ(rc, 0);
        EXPECT_EQ(client_->unregister_buffer(sub), 0);
    }

    EXPECT_EQ(ShmHelper::getInstance()->free(base), 0);
}

}  // namespace testing
}  // namespace mooncake
