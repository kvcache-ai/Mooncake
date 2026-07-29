#include "gds/gds_context.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>

namespace mooncake {

class GdsContextTest : public ::testing::Test {
   protected:
    std::string test_dir_;

    void SetUp() override {
        test_dir_ = "/tmp/gds_test_" + std::to_string(getpid());
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }
};

// ── Test 1: Init fallback mode (no GDS hardware) ──
TEST_F(GdsContextTest, Init_FallbackOnNoGds) {
    GdsContext ctx;
    std::string data_file = test_dir_ + "/kv_cache.data";
    auto res = ctx.Init(data_file, 1024 * 1024 * 1024);  // 1GB
    // Expected: GDS_NOT_AVAILABLE (no /dev/nvidia-fs or no nvidia-fs.ko)
    if (!res) {
        EXPECT_EQ(res.error(), ErrorCode::GDS_NOT_AVAILABLE);
        EXPECT_FALSE(ctx.enabled_);
    }
}

// ── Test 2: ProbeGdsAvailable returns false in a fallback environment ──
// Only asserts false on machines without GDS hardware; on GDS machines this
// test is meaningless and skips itself.
TEST_F(GdsContextTest, ProbeGdsAvailable_NoGds) {
    if (GdsContext::IsGdsAvailable()) {
        GTEST_SKIP() << "GDS hardware present; no-GDS assertion not applicable";
    }
    GdsContext ctx;
    EXPECT_FALSE(ctx.ProbeGdsAvailable(test_dir_));
}

// ── Test 3: RecordHeader layout (4K-aligned padding + pwrite/pread) ──
// Note: without GDS hardware GdsContext cannot Init, so this test drives the
// RecordHeader-defined layout manually with pwrite/pread to verify that the
// alignment math round-trips correctly.
TEST_F(GdsContextTest, WriteRead_CpuBuffer) {
    // Open the file directly (bypassing Init's probe)
    std::string path = test_dir_ + "/manual_test.data";
    int fd = ::open(path.c_str(), O_CLOEXEC | O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    // Write test data
    std::string key = "test_key_001";
    std::string value = "hello_gds_test_data_12345678";
    const uint32_t klen = static_cast<uint32_t>(key.size());
    const uint32_t vlen = static_cast<uint32_t>(value.size());
    // Layout: header + key + zero padding + value — value starts 4K-aligned
    const uint64_t value_off = RecordHeader::ValueOffsetInRecord(klen);
    ASSERT_EQ(value_off % RecordHeader::kValueAlignment, 0u);

    RecordHeader hdr{.key_len = klen, .value_len = vlen};
    char hdr_buf[RecordHeader::SIZE];
    hdr.WriteTo(hdr_buf);
    ASSERT_EQ(::pwrite(fd, hdr_buf, RecordHeader::SIZE, 0), RecordHeader::SIZE);
    ASSERT_EQ(::pwrite(fd, key.data(), klen, RecordHeader::SIZE),
              static_cast<ssize_t>(klen));
    static const char kZeros[RecordHeader::kValueAlignment] = {};
    ASSERT_EQ(::pwrite(fd, kZeros, RecordHeader::ValuePadding(klen),
                       RecordHeader::SIZE + klen),
              static_cast<ssize_t>(RecordHeader::ValuePadding(klen)));
    ASSERT_EQ(::pwrite(fd, value.data(), vlen, value_off),
              static_cast<ssize_t>(vlen));

    // Read back
    char read_hdr_buf[RecordHeader::SIZE];
    ASSERT_EQ(::pread(fd, read_hdr_buf, RecordHeader::SIZE, 0),
              RecordHeader::SIZE);
    RecordHeader read_hdr = RecordHeader::ReadFrom(read_hdr_buf);
    EXPECT_EQ(read_hdr.key_len, key.size());
    EXPECT_EQ(read_hdr.value_len, value.size());

    std::string read_key(key.size(), '\0');
    ASSERT_EQ(::pread(fd, read_key.data(), key.size(), RecordHeader::SIZE),
              static_cast<ssize_t>(key.size()));
    EXPECT_EQ(read_key, key);

    std::string read_val(value.size(), '\0');
    ASSERT_EQ(::pread(fd, read_val.data(), value.size(), value_off),
              static_cast<ssize_t>(value.size()));
    EXPECT_EQ(read_val, value);

    ::close(fd);
}

// ── Test 4: Shutdown does not crash (even after a failed Init) ──
TEST_F(GdsContextTest, Shutdown_AfterFailedInit) {
    GdsContext ctx;
    ctx.Shutdown();  // must not crash
}

// ── Test 5: IsGdsAvailable static check ──
TEST_F(GdsContextTest, IsGdsAvailable_Static) {
    // Depends on no members; safe to call at any time
    bool avail = GdsContext::IsGdsAvailable();
    LOG(INFO) << "GDS available on this machine: " << std::boolalpha << avail;
    SUCCEED();
}

// ── Test 6: WriteRecord/ReadRecord end-to-end (requires GDS hardware) ──
// Exercise the full GdsContext record write/read path with CPU buffers and
// verify that the 4K-aligned layout round-trips under a real cuFile handle
// (including multi-slice reads).
TEST_F(GdsContextTest, WriteReadRecord_EndToEnd) {
    if (!GdsContext::IsGdsAvailable()) {
        GTEST_SKIP() << "no GDS hardware on this machine";
    }
    GdsContext ctx;
    std::string data_file = test_dir_ + "/kv_cache.data";
    auto init_res = ctx.Init(data_file, 16 * 1024 * 1024);  // 16MB
    if (!init_res) {
        GTEST_SKIP() << "GDS Init failed in this environment: "
                     << static_cast<int>(init_res.error());
    }
    ASSERT_TRUE(ctx.enabled_.load());

    // Write a two-slice record with a deliberately unaligned key length;
    // verify the value still lands on a 4K-aligned offset and reads back.
    std::string key = "e2e_key_007";
    std::string val_a = "first-fragment:";
    std::string val_b(5000, 'z');
    std::vector<Slice> slices = {Slice{val_a.data(), val_a.size()},
                                 Slice{val_b.data(), val_b.size()}};
    const uint64_t offset = 0;
    auto wr = ctx.WriteRecord(key, slices, offset);
    ASSERT_TRUE(wr.has_value());

    // The value must be written at a 4K-aligned offset — verify the layout
    // directly with pread.
    const uint64_t value_off =
        RecordHeader::ValueOffsetInRecord(static_cast<uint32_t>(key.size()));
    ASSERT_EQ(value_off % RecordHeader::kValueAlignment, 0u);
    std::string raw(value_off + val_a.size() + val_b.size(), '\0');
    ASSERT_EQ(::pread(ctx.gds_fd_, raw.data(), raw.size(), 0),
              static_cast<ssize_t>(raw.size()));
    EXPECT_EQ(raw.substr(value_off, val_a.size()), val_a);

    // Read back into multiple slices and verify.
    std::string dst_a(val_a.size(), '\0');
    std::string dst_b(val_b.size(), '\0');
    std::vector<Slice> dest = {Slice{dst_a.data(), dst_a.size()},
                               Slice{dst_b.data(), dst_b.size()}};
    auto rr = ctx.ReadRecord(
        key, dest, offset, static_cast<uint32_t>(val_a.size() + val_b.size()));
    ASSERT_TRUE(rr.has_value());
    EXPECT_EQ(dst_a, val_a);
    EXPECT_EQ(dst_b, val_b);

    // A destination total size that does not match the record must be
    // rejected (no truncated/overflowing reads).
    std::string short_buf(8, '\0');
    std::vector<Slice> bad = {Slice{short_buf.data(), short_buf.size()}};
    EXPECT_FALSE(
        ctx.ReadRecord(key, bad, offset,
                       static_cast<uint32_t>(val_a.size() + val_b.size()))
            .has_value());

    ctx.Shutdown();
    EXPECT_FALSE(ctx.enabled_.load());
}

#ifdef USE_GDS_BACKEND
// ── Registration-cache tests with an in-process fake GdsDeviceOps ──
// These exercise EnsureBufferRegistered/RegisterAndCache without GDS
// hardware: the fake records every register/deregister call and serves
// configurable GetAddressRange answers.

class FakeGdsDeviceOps final : public GdsDeviceOps {
   public:
    bool ProbeDeviceNode() override { return true; }
    GdsDeviceError DriverOpen() override { return GdsDeviceError{0, 0}; }
    GdsDeviceError FileHandleRegister(GdsDeviceFileHandle*, int) override {
        return GdsDeviceError{0, 0};
    }
    void FileHandleDeregister(GdsDeviceFileHandle) override {}

    GdsDeviceError BufRegister(void* ptr, size_t size) override {
        register_calls.emplace_back(ptr, size);
        return GdsDeviceError{0, 0};
    }
    void BufDeregister(void* ptr) override { deregister_calls.push_back(ptr); }

    // Configurable allocation query.  When `address_range_ok` is false
    // the call fails and GdsContext falls back to span registration.
    bool GetAddressRange(const void* ptr, void** base, size_t* size) override {
        if (!address_range_ok) return false;
        *base = alloc_base;
        *size = alloc_size;
        last_query_ptr = ptr;
        return true;
    }

    ssize_t Write(GdsDeviceFileHandle, void*, size_t, off_t) override {
        return -1;
    }
    ssize_t Read(GdsDeviceFileHandle, void*, size_t, off_t) override {
        return -1;
    }
    void* Malloc(size_t) override { return nullptr; }
    void Free(void*) override {}
    void Memset(void*, int, size_t) override {}
    void SetDevice(int) override {}
    void DeviceSynchronize() override {}
    int GetDevice() override { return 0; }
    void CopyDeviceToDevice(void*, const void*, size_t) override {}
    void CopyDeviceToHost(void*, const void*, size_t) override {}

    std::vector<std::pair<void*, size_t>> register_calls;
    std::vector<void*> deregister_calls;
    bool address_range_ok = true;
    void* alloc_base = nullptr;
    size_t alloc_size = 0;
    const void* last_query_ptr = nullptr;
};

class GdsRegistrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto fake = std::make_unique<FakeGdsDeviceOps>();
        fake_ = fake.get();
        ctx_.ops_ = std::move(fake);
    }

    GdsContext ctx_;
    FakeGdsDeviceOps* fake_;  // owned by ctx_.ops_
};

// Allocation snapping: the first I/O on an allocation registers the
// whole allocation once; subsequent slices inside it are pure hits.
TEST_F(GdsRegistrationTest, AllocationSnapping_SingleRegistration) {
    char* alloc = reinterpret_cast<char*>(0x100000000ULL);
    fake_->alloc_base = alloc;
    fake_->alloc_size = 1 << 20;  // 1 MiB allocation

    EXPECT_TRUE(ctx_.EnsureBufferRegistered(alloc + 4096, 4096));
    ASSERT_EQ(fake_->register_calls.size(), 1u);
    EXPECT_EQ(fake_->register_calls[0].first, alloc);
    EXPECT_EQ(fake_->register_calls[0].second, 1u << 20);
    ASSERT_EQ(ctx_.registered_buffers_.size(), 1u);
    EXPECT_EQ(ctx_.registered_buffers_.begin()->first, alloc);

    // Interior slices hit — no further driver calls.
    EXPECT_TRUE(ctx_.EnsureBufferRegistered(alloc + 8192, 4096));
    EXPECT_TRUE(ctx_.EnsureBufferRegistered(alloc, 1 << 20));
    EXPECT_EQ(fake_->register_calls.size(), 1u);
    EXPECT_TRUE(fake_->deregister_calls.empty());
}

// Same base with a different size replaces the old registration with a
// fresh (allocation-snapped) one.
TEST_F(GdsRegistrationTest, ExactBaseDifferentSize_Replaced) {
    char* alloc = reinterpret_cast<char*>(0x100000000ULL);
    fake_->address_range_ok = false;  // span mode for the first registration

    EXPECT_TRUE(ctx_.EnsureBufferRegistered(alloc, 4096));
    ASSERT_EQ(ctx_.registered_buffers_.size(), 1u);
    EXPECT_EQ(ctx_.registered_buffers_.begin()->second.size, 4096u);

    // Now the vendor reports allocation bounds: re-registration snaps.
    // (A different size is required — an identical (base, size) pair is
    // a plain cache hit and never reaches the replacement branch.)
    fake_->address_range_ok = true;
    fake_->alloc_base = alloc;
    fake_->alloc_size = 1 << 20;
    EXPECT_TRUE(ctx_.EnsureBufferRegistered(alloc, 8192));
    ASSERT_EQ(fake_->deregister_calls.size(), 1u);
    EXPECT_EQ(fake_->deregister_calls[0], alloc);
    ASSERT_EQ(ctx_.registered_buffers_.size(), 1u);
    EXPECT_EQ(ctx_.registered_buffers_.begin()->second.size, 1u << 20);
}

// Overlap cleanup must evict right-crossing extents too.  Old code kept
// them, so BufRegister overlapped a live registration and failed,
// silently degrading to the bounce buffer.
TEST_F(GdsRegistrationTest, RightCrossingOverlap_Evicted) {
    fake_->address_range_ok = false;  // span mode
    char* a = reinterpret_cast<char*>(0x1000);
    char* b = reinterpret_cast<char*>(0x1050);

    EXPECT_TRUE(ctx_.EnsureBufferRegistered(b, 0x200));  // [0x1050, 0x1250)
    ASSERT_EQ(ctx_.registered_buffers_.size(), 1u);

    // [0x1000, 0x1100) crosses the right edge of the existing extent.
    EXPECT_TRUE(ctx_.EnsureBufferRegistered(a, 0x100));
    ASSERT_EQ(fake_->deregister_calls.size(), 1u);
    EXPECT_EQ(fake_->deregister_calls[0], b);
    ASSERT_EQ(ctx_.registered_buffers_.size(), 1u);
    EXPECT_EQ(ctx_.registered_buffers_.begin()->first, a);
}

// Cap eviction picks the least-recently-used extent, not the lowest
// address.  (Old comment admitted: "begin() is lowest address, not
// oldest".)
TEST_F(GdsRegistrationTest, CapEviction_IsLruNotLowestAddress) {
    fake_->address_range_ok = false;  // span mode
    constexpr size_t kCap = 8192;     // must match kMaxRegisteredBuffers
    char* base = reinterpret_cast<char*>(0x100000000ULL);

    // Fill the cache with DECREASING addresses, so the lowest address is
    // the most recently registered extent.
    for (size_t i = 0; i < kCap; ++i) {
        char* p = base + (kCap - i) * 0x10000;
        ASSERT_TRUE(ctx_.EnsureBufferRegistered(p, 0x1000)) << "i=" << i;
    }
    ASSERT_EQ(ctx_.registered_buffers_.size(), kCap);

    // One more registration evicts exactly one extent.  The LRU victim
    // must be the first-registered (never touched since) extent, NOT
    // the lowest-address extent (which the old begin()-based eviction
    // would have picked).
    char* lowest_addr = base + 0x10000;           // registered last
    char* oldest_unused = base + kCap * 0x10000;  // registered first
    char* newcomer = base + (kCap + 1) * 0x10000;
    EXPECT_TRUE(ctx_.EnsureBufferRegistered(newcomer, 0x1000));

    ASSERT_EQ(fake_->deregister_calls.size(), 1u);
    EXPECT_EQ(fake_->deregister_calls[0], oldest_unused);
    EXPECT_EQ(ctx_.registered_buffers_.count(lowest_addr), 1u);
    EXPECT_EQ(ctx_.registered_buffers_.count(newcomer), 1u);
    EXPECT_EQ(ctx_.registered_buffers_.size(), kCap);
}
#endif  // USE_GDS_BACKEND
}  // namespace mooncake
