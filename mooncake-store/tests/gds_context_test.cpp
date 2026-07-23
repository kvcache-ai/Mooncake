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
    ASSERT_EQ(::pwrite(fd, &hdr, RecordHeader::SIZE, 0), RecordHeader::SIZE);
    ASSERT_EQ(::pwrite(fd, key.data(), klen, RecordHeader::SIZE),
              static_cast<ssize_t>(klen));
    static const char kZeros[RecordHeader::kValueAlignment] = {};
    ASSERT_EQ(::pwrite(fd, kZeros, RecordHeader::ValuePadding(klen),
                       RecordHeader::SIZE + klen),
              static_cast<ssize_t>(RecordHeader::ValuePadding(klen)));
    ASSERT_EQ(::pwrite(fd, value.data(), vlen, value_off),
              static_cast<ssize_t>(vlen));

    // Read back
    RecordHeader read_hdr;
    ASSERT_EQ(::pread(fd, &read_hdr, RecordHeader::SIZE, 0),
              RecordHeader::SIZE);
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
}  // namespace mooncake
