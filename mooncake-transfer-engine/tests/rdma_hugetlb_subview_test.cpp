// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Regression test: registering a sub-view of a HugeTLB-backed mapping through
// RdmaTransport.
//
// ibv_fork_init() makes ibv_reg_mr() run MADV_DONTFORK over the range it is
// handed, and the kernel refuses to split a hugetlb VMA at a boundary that
// falls inside a huge page. A caller that registers a view of a larger
// hugepage-backed allocation (rather than the whole mapping) therefore failed
// with EINVAL below libibverbs, which surfaced as a generic registration error
// and left such callers unable to use RDMA at all.
//
// The transport now tries the caller's own range first and widens it to the
// enclosing huge pages only when that fails with EINVAL. Environments where
// ibv_reg_mr does not madvise hugetlb splits (for example
// RDMAV_HUGEPAGES_SAFE=1) therefore keep registering narrow sub-views, while a
// fork-protected stack still gets a working (widened) registration. Which of
// the two happens is detected from the actual error, never from the
// environment, so this test accepts either outcome where it cannot control it.
//
// Requires both a usable RDMA device and a reserved 2 MiB huge page, so it
// self-skips on hardware-free CI.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <sys/mman.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "config.h"
#include "error.h"
#include "transfer_engine.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace mooncake {

namespace {

constexpr size_t kHugepageSize = 2ull << 20;

// MAP_HUGE_2MB, written out so the value does not depend on <linux/mman.h>
// being pulled in ahead of <sys/mman.h>.
#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 26
#endif
constexpr int kHuge2MBFlag = 21 << MAP_HUGE_SHIFT;  // log2(2 MiB) << shift

bool rdmaDeviceUsable(ibv_device *device) {
    ibv_context *ctx = ibv_open_device(device);
    if (!ctx) return false;
    ibv_device_attr attr{};
    if (ibv_query_device(ctx, &attr) != 0) {
        ibv_close_device(ctx);
        return false;
    }
    bool ok = false;
    for (uint8_t port = 1; port <= attr.phys_port_cnt; ++port) {
        ibv_port_attr port_attr{};
        if (ibv_query_port(ctx, port, &port_attr) != 0) continue;
        if (port_attr.gid_tbl_len > 0 && port_attr.state == IBV_PORT_ACTIVE) {
            ok = true;
            break;
        }
    }
    ibv_close_device(ctx);
    return ok;
}

std::string pickRdmaDevice() {
    const char *override_name = std::getenv("MC_TEST_DEVICE_NAME");
    if (override_name && *override_name) return override_name;
    int num_devices = 0;
    ibv_device **list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) {
        if (list) ibv_free_device_list(list);
        return "";
    }
    std::string name;
    for (int i = 0; i < num_devices; ++i) {
        if (rdmaDeviceUsable(list[i])) {
            name = ibv_get_device_name(list[i]);
            break;
        }
    }
    ibv_free_device_list(list);
    return name;
}

// A hugepage-aligned mapping, or nullptr when no hugepages are reserved.
// The 2 MiB pool is requested explicitly: a bare MAP_HUGETLB takes the kernel's
// default hugepage size, which is not 2 MiB on hosts booted with
// default_hugepagesz=1G — there this test would skip instead of running.
void *mapHugepages(size_t length) {
    void *ptr =
        mmap(nullptr, length, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | kHuge2MBFlag, -1, 0);
    return ptr == MAP_FAILED ? nullptr : ptr;
}

}  // namespace

class RdmaHugetlbSubviewTest : public ::testing::Test {
   protected:
    void SetUp() override {
        device_ = pickRdmaDevice();
        if (device_.empty()) {
            GTEST_SKIP() << "no usable RDMA device available";
        }
        huge_ = mapHugepages(kHugepageSize);
        if (huge_ == nullptr) {
            GTEST_SKIP() << "cannot map a 2 MiB huge page (no hugetlb pages "
                            "reserved?)";
        }
        ASSERT_EQ(setenv("MC_TE_FILTERS", device_.c_str(), 1), 0);
        loadGlobalConfig(globalConfig());

        std::vector<std::string> filter{device_};
        engine_ = std::make_unique<TransferEngine>(true, filter);
        ASSERT_EQ(engine_->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        ASSERT_NE(engine_->getTransport("rdma"), nullptr);
    }

    void TearDown() override {
        engine_.reset();
        if (huge_ != nullptr) munmap(huge_, kHugepageSize);
        unsetenv("MC_TE_FILTERS");
        loadGlobalConfig(globalConfig());
    }

    char *base() { return static_cast<char *>(huge_); }

    std::string device_;
    void *huge_ = nullptr;
    std::unique_ptr<TransferEngine> engine_;
};

// Pre-fix the registration failed: neither end of the range was hugepage
// aligned, so ibv_reg_mr()'s MADV_DONTFORK was rejected with EINVAL.
TEST_F(RdmaHugetlbSubviewTest, MisalignedSubviewRegistersAndUnregisters) {
    char *sub = base() + 4096;
    constexpr size_t kLength = 3 * 4096;

    ASSERT_EQ(engine_->registerLocalMemory(sub, kLength, "cpu:0"), 0);
    EXPECT_EQ(engine_->unregisterLocalMemory(sub), 0);

    // The widening bookkeeping has to be released with the MR, or the same
    // sub-view can never be registered a second time.
    ASSERT_EQ(engine_->registerLocalMemory(sub, kLength, "cpu:0"), 0);
    EXPECT_EQ(engine_->unregisterLocalMemory(sub), 0);
}

// A registration that already covers whole huge pages is left alone.
TEST_F(RdmaHugetlbSubviewTest, AlignedRegistrationUnchanged) {
    ASSERT_EQ(engine_->registerLocalMemory(base(), kHugepageSize, "cpu:0"), 0);
    EXPECT_EQ(engine_->unregisterLocalMemory(base()), 0);
}

// Two views of one huge page, while fork protection is active, both widen onto
// the same pages. RdmaContext keys its MR map by base address and overwrites on
// a duplicate key, which would leak the first MR and its keys, so the second
// registration is refused instead of being silently collapsed.
//
// Without fork protection the first view registers over its own range and the
// second does too: narrow-first intentionally preserves that. The outcome is
// therefore decided by what ibv_reg_mr actually reports, not by the
// environment. Either way the claim must be released with the first
// registration.
TEST_F(RdmaHugetlbSubviewTest, SecondViewOfSameHugepageIsOverlapOrNarrow) {
    char *first = base() + 4096;
    char *second = base() + 8192;

    ASSERT_EQ(engine_->registerLocalMemory(first, 4096, "cpu:0"), 0);
    const int second_rc = engine_->registerLocalMemory(second, 4096, "cpu:0");
    if (second_rc == 0) {
        // No EINVAL: the first view registered narrow, so the two ranges are
        // disjoint registrations.
        EXPECT_EQ(engine_->unregisterLocalMemory(second), 0);
    } else {
        EXPECT_EQ(second_rc, ERR_ADDRESS_OVERLAPPED);
    }
    // The rejected registration must not have disturbed the accepted one, and
    // releasing it must release the claim so the second view can register.
    EXPECT_EQ(engine_->unregisterLocalMemory(first), 0);
    ASSERT_EQ(engine_->registerLocalMemory(second, 4096, "cpu:0"), 0);
    EXPECT_EQ(engine_->unregisterLocalMemory(second), 0);
}

// The same collision arises the other way round: an aligned registration
// already owns the hugepage, and a later view is checked against it. The
// transport overlaps the range actually being attempted — the caller's narrow
// range, which sits inside the claimed page — so the check refuses the view
// before ibv_reg_mr is called, regardless of whether widening would be needed.
// Checking only widened registrations would miss this, so every hugetlb-backed
// MR range is tracked, not just the widened ones.
//
// This one goes through the transport rather than the engine: the two ranges
// overlap as the caller states them, so TransferEngine rejects the second one
// itself (tryReserveMemoryRegions) and the transport's own guard is never
// reached.
TEST_F(RdmaHugetlbSubviewTest, ViewOfRegisteredAlignedPageRejected) {
    // Transport's register/unregister virtuals are private (TransferEngine is
    // the friend that owns them); RdmaTransport's overrides are public, so go
    // through the concrete type.
    auto *rdma = static_cast<RdmaTransport *>(engine_->getTransport("rdma"));
    ASSERT_NE(rdma, nullptr);

    ASSERT_EQ(
        rdma->registerLocalMemory(base(), kHugepageSize, "cpu:0", true, true),
        0);
    EXPECT_EQ(
        rdma->registerLocalMemory(base() + 4096, 4096, "cpu:0", true, true),
        ERR_ADDRESS_OVERLAPPED);
    EXPECT_EQ(rdma->unregisterLocalMemory(base()), 0);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    // Initialize logging once for the whole binary — calling
    // InitGoogleLogging() per-test in SetUp() aborts on the 2nd TEST_F.
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    ::testing::InitGoogleTest(&argc, argv);
    int rc = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return rc;
}
