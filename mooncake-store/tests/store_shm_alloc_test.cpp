#include "common/store_shm_alloc.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <linux/magic.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common.h"
#include "common/client_buffer_allocation.h"
#include "transfer_engine.h"

namespace mooncake {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
            original_ = value;
        }
        EXPECT_EQ(unsetenv(name), 0);
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            EXPECT_EQ(setenv(name_.c_str(), original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv(name_.c_str()), 0);
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

    void Set(const char* value) {
        ASSERT_EQ(setenv(name_.c_str(), value, 1), 0);
    }

    void Unset() { ASSERT_EQ(unsetenv(name_.c_str()), 0); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

class StoreShmAllocTest : public ::testing::Test {
   protected:
    void SetUp() override {
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = google::WARNING;
    }

    ScopedEnvVar use_shm{"MC_STORE_USE_SHM_SEGMENT"};
    ScopedEnvVar allow_tmpfs{"MC_STORE_SHM_ALLOW_TMPFS"};
    ScopedEnvVar hugetlbfs_path{"MC_HUGETLBFS_PATH"};
    ScopedEnvVar force_shm{"MC_FORCE_SHM"};
    ScopedEnvVar use_hugepage{"MC_STORE_USE_HUGEPAGE"};
    ScopedEnvVar hugepage_size{"MC_STORE_HUGEPAGE_SIZE"};
};

TEST_F(StoreShmAllocTest, UnsetFlagsAreOff) {
    EXPECT_FALSE(store_use_shm_segment_flag());
    EXPECT_FALSE(store_use_shm_segment());
    EXPECT_FALSE(store_shm_allow_tmpfs_fallback());
}

TEST_F(StoreShmAllocTest, ExplicitStoreFlagEnablesShm) {
    for (const char* value : {"1", "true", "TRUE", "yes", "on"}) {
        SCOPED_TRACE(value);
        use_shm.Set(value);
        EXPECT_TRUE(store_use_shm_segment_flag());
        EXPECT_TRUE(store_use_shm_segment());
    }
    for (const char* value : {"0", "false", "no", "off", ""}) {
        SCOPED_TRACE(value);
        use_shm.Set(value);
        EXPECT_FALSE(store_use_shm_segment_flag());
        EXPECT_FALSE(store_use_shm_segment());
    }
}

TEST_F(StoreShmAllocTest, ForceShmEnablesStoreSegmentWithoutStoreFlag) {
    force_shm.Set("1");
    EXPECT_FALSE(store_use_shm_segment_flag());
    EXPECT_TRUE(store_use_shm_segment());
}

TEST_F(StoreShmAllocTest, TmpfsFallbackFlag) {
    allow_tmpfs.Set("1");
    EXPECT_TRUE(store_shm_allow_tmpfs_fallback());
    allow_tmpfs.Set("0");
    EXPECT_FALSE(store_shm_allow_tmpfs_fallback());
}

TEST_F(StoreShmAllocTest, HostDramProtocolFilter) {
    EXPECT_TRUE(is_store_host_dram_protocol("rdma"));
    EXPECT_TRUE(is_store_host_dram_protocol("tcp"));
    EXPECT_TRUE(is_store_host_dram_protocol("efa"));
    EXPECT_FALSE(is_store_host_dram_protocol("ascend"));
    EXPECT_FALSE(is_store_host_dram_protocol("ubshmem"));
    EXPECT_FALSE(is_store_host_dram_protocol("ub"));
    EXPECT_FALSE(is_store_host_dram_protocol("sunrise_link"));
    EXPECT_FALSE(is_store_host_dram_protocol("cxl"));
    EXPECT_FALSE(is_store_host_dram_protocol("nvlink_intra"));
}

TEST_F(StoreShmAllocTest, OptionsDefaultToPosixAndDeferPopulate) {
    const SharedMemoryOptions opt = make_store_shm_options();
    EXPECT_FALSE(opt.use_hugepage);
    EXPECT_EQ(opt.hugepage_size, 0u);
    EXPECT_TRUE(opt.hugetlbfs_path.empty());
    EXPECT_FALSE(opt.populate);
}

TEST_F(StoreShmAllocTest, OptionsFollowHugepageEnvAndCustomMount) {
    use_hugepage.Set("1");
    hugepage_size.Set("2MB");
    hugetlbfs_path.Set("/tmp/mooncake_hugepages_2m");

    const SharedMemoryOptions opt = make_store_shm_options();
    EXPECT_TRUE(opt.use_hugepage);
    EXPECT_EQ(opt.hugepage_size, 2ULL * 1024 * 1024);
    EXPECT_EQ(opt.hugetlbfs_path, "/tmp/mooncake_hugepages_2m");
    EXPECT_FALSE(opt.populate);
}

TEST_F(StoreShmAllocTest, HugetlbfsPathIgnoredWithoutHugepage) {
    hugetlbfs_path.Set("/tmp/mooncake_hugepages_2m");
    const SharedMemoryOptions opt = make_store_shm_options();
    EXPECT_FALSE(opt.use_hugepage);
    EXPECT_TRUE(opt.hugetlbfs_path.empty());
}

TEST_F(StoreShmAllocTest, BindRejectsInvalidLayout) {
    EXPECT_EQ(bind_buffer_numa_segments(nullptr, 4096, {0, 1}, 4096), -1);

    const size_t page = static_cast<size_t>(getpagesize());
    void* ptr = mmap(nullptr, page * 2, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(ptr, MAP_FAILED);
    EXPECT_EQ(bind_buffer_numa_segments(ptr, page * 2 + 1, {0, 1}, page), -1);
    EXPECT_EQ(bind_buffer_numa_segments(ptr, page * 2, {}, page), -1);
    ASSERT_EQ(munmap(ptr, page * 2), 0);
}

std::unique_ptr<TransferEngine> MakeShmEngine() {
    auto engine = std::make_unique<TransferEngine>(false);
    const int rc = engine->init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0);
    if (rc != 0) {
        return nullptr;
    }
    if (!engine->installTransport("shm", nullptr)) {
        return nullptr;
    }
    return engine;
}

TEST_F(StoreShmAllocTest, AllocatesAndFreesPosixShmSegment) {
    auto engine = MakeShmEngine();
    if (!engine) {
        GTEST_SKIP() << "Failed to init TransferEngine with ShmTransport";
    }

    constexpr size_t kSize = 1 << 20;
    auto alloc = allocate_store_host_segment(*engine, kSize, "tcp", {}, true);
    ASSERT_NE(alloc.ptr, nullptr);
    EXPECT_TRUE(alloc.used_shm);
    EXPECT_FALSE(alloc.used_numa);
    EXPECT_EQ(alloc.mapped_size, kSize);
    EXPECT_EQ(alloc.location, "*");

    std::memset(alloc.ptr, 0x5a, kSize);
    EXPECT_EQ(static_cast<unsigned char*>(alloc.ptr)[0], 0x5a);
    EXPECT_EQ(static_cast<unsigned char*>(alloc.ptr)[kSize - 1], 0x5a);

    free_store_host_segment(*engine, alloc);
}

TEST_F(StoreShmAllocTest, HugepageAllocRequiresMultipleOfPageSize) {
    use_hugepage.Set("1");
    hugepage_size.Set("2MB");

    struct statfs sfs;
    const char* mount = "/dev/hugepages";
    if (statfs(mount, &sfs) != 0 || sfs.f_type != HUGETLBFS_MAGIC ||
        static_cast<size_t>(sfs.f_bsize) != SharedMemoryOptions::kHugepage2MB) {
        GTEST_SKIP() << "2MB hugetlbfs is not available at /dev/hugepages";
    }

    auto engine = MakeShmEngine();
    if (!engine) {
        GTEST_SKIP() << "Failed to init TransferEngine with ShmTransport";
    }

    const size_t hp = SharedMemoryOptions::kHugepage2MB;
    auto alloc = allocate_store_host_segment(*engine, hp / 2, "tcp", {}, true);
    if (!alloc.ptr) {
        GTEST_SKIP() << "hugetlbfs SHM allocate failed (ENOMEM or mount)";
    }
    EXPECT_TRUE(alloc.used_shm);
    EXPECT_EQ(alloc.mapped_size, hp);
    EXPECT_TRUE(make_store_shm_options().use_hugepage);
    free_store_host_segment(*engine, alloc);
}

}  // namespace
}  // namespace mooncake
