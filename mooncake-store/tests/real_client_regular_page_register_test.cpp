// real_client_regular_page_register_test.cpp
//
// Regression test for the non-HugeTLB (regular 4 KiB page) registration path.
//
// The HugeTLB fix rounds the per-MR chunk limit down to the buffer's page size
// and rejects an effective max_mr_size smaller than one page, because
// ibv_fork_init() + MADV_DONTFORK makes the kernel refuse to split a hugetlb
// VMA at a non-hugepage-aligned boundary. Regular 4 KiB pages (and THP, which
// reports the 4 KiB base page in KernelPageSize) can be split at any boundary,
// so that rounding/rejection must NOT run for them:
//   * a below-one-page max_mr_size must not be falsely rejected, and
//   * a non-page-multiple max_mr_size must not force needless extra chunking.
//
// registerLocalMemoryInternal gates the round-down/reject on
// buffer_page_size > the base page size, so ordinary buffers skip it entirely.
// This test drives register_buffer() on a plain posix_memalign buffer with
// max_mr_size set below and to a non-multiple of the base page, asserting the
// registration succeeds. On the unfixed code the below-page case returned
// ERR_INVALID_ARGUMENT (rc != 0); the fixed code registers cleanly.
//
// The false-reject lives on the RDMA registration path only (TCP performs no
// ibv_reg_mr), so the discriminating cases require PROTOCOL=rdma. Protocol and
// device come from the PROTOCOL / DEVICE_NAME environment variables, the same
// convention as pybind_client_test. No hugepages are required.

#include "config.h"
#include "real_client.h"
#include "test_server_helpers.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

namespace mooncake {
namespace testing {

class RealClientRegularPageRegisterTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RealClientRegularPageRegisterTest");
        FLAGS_logtostderr = 1;
        if (const char* p = std::getenv("PROTOCOL")) {
            protocol_ = p;
        }
        if (const char* d = std::getenv("DEVICE_NAME")) {
            device_name_ = d;
        }
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
            << "Failed to start in-proc master";
        master_address_ = master_.master_address();

        client_ = RealClient::create();
        const std::string devices =
            (protocol_ == std::string("rdma")) ? device_name_ : std::string("");
        ASSERT_EQ(client_->setup_real("localhost:17827", "P2PHANDSHAKE",
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

// An effective max_mr_size smaller than one base page must NOT be rejected for
// a regular (non-HugeTLB) buffer. The buffer is split into raw max_mr_size
// chunks and each is registered on its own; ordinary VMAs split at any
// boundary, so every chunk registers cleanly. The unfixed code rounded the
// chunk limit down to the 4 KiB page, hit aligned_limit == 0, and returned
// ERR_INVALID_ARGUMENT even though the hardware would have accepted it.
TEST_F(RealClientRegularPageRegisterTest, SubPageMaxMrSizeNotRejected) {
    if (protocol_ != std::string("rdma")) {
        GTEST_SKIP() << "the false-reject lives on the RDMA path only";
    }

    // Page-aligned regular buffer, a couple of base pages long.
    void* base = nullptr;
    ASSERT_EQ(posix_memalign(&base, 4096, 8192), 0);
    ASSERT_NE(base, nullptr);

    const uint64_t saved_max_mr_size = globalConfig().max_mr_size;
    globalConfig().max_mr_size = 1024;  // below one 4 KiB page

    const int rc = client_->register_buffer(base, 8192);

    globalConfig().max_mr_size = saved_max_mr_size;

    EXPECT_EQ(rc, 0) << "regular-page registration must not be rejected when "
                        "max_mr_size is below one base page";
    if (rc == 0) {
        EXPECT_EQ(client_->unregister_buffer(base), 0);
    }

    free(base);
}

// A non-page-multiple max_mr_size above one page must not force needless extra
// chunking or a misaligned failure on a regular buffer: the raw limit is used
// as-is and every chunk registers cleanly. This case succeeds on both the old
// and the fixed code (regular pages split at any boundary); it guards against a
// future change that would (re-)apply the HugeTLB round-down to ordinary
// memory.
TEST_F(RealClientRegularPageRegisterTest, NonPageMultipleMaxMrSizeRegisters) {
    if (protocol_ != std::string("rdma")) {
        GTEST_SKIP() << "chunk registration is exercised on the RDMA path only";
    }

    void* base = nullptr;
    const size_t kSize = 64 * 1024;
    ASSERT_EQ(posix_memalign(&base, 4096, kSize), 0);
    ASSERT_NE(base, nullptr);

    const uint64_t saved_max_mr_size = globalConfig().max_mr_size;
    globalConfig().max_mr_size = 5000;  // > one page, not a page multiple

    const int rc = client_->register_buffer(base, kSize);

    globalConfig().max_mr_size = saved_max_mr_size;

    EXPECT_EQ(rc, 0) << "regular-page chunked registration with a "
                        "non-page-multiple max_mr_size must succeed";
    if (rc == 0) {
        EXPECT_EQ(client_->unregister_buffer(base), 0);
    }

    free(base);
}

}  // namespace testing
}  // namespace mooncake
