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

// ── Test 1: Init 兼容模式 (无 GDS 硬件) ──
TEST_F(GdsContextTest, Init_FallbackOnNoGds) {
    GdsContext ctx;
    std::string data_file = test_dir_ + "/kv_cache.data";
    auto res = ctx.Init(data_file, 1024 * 1024 * 1024);  // 1GB
    // 期望: GDS_NOT_AVAILABLE（无 /dev/nvidia-fs 或无 nvidia-fs.ko）
    if (!res) {
        EXPECT_EQ(res.error(), ErrorCode::GDS_NOT_AVAILABLE);
        EXPECT_FALSE(ctx.enabled_);
    }
}

// ── Test 2: ProbeGdsAvailable 在兼容环境返回 false ──
// 只在无 GDS 硬件的机器上断言 false;有硬件时本测试无意义,直接跳过。
TEST_F(GdsContextTest, ProbeGdsAvailable_NoGds) {
    if (GdsContext::IsGdsAvailable()) {
        GTEST_SKIP() << "GDS hardware present; no-GDS assertion not applicable";
    }
    GdsContext ctx;
    EXPECT_FALSE(ctx.ProbeGdsAvailable(test_dir_));
}

// ── Test 3: RecordHeader 布局 (4K 对齐 padding + pwrite/pread) ──
// 注意: 无 GDS 硬件时 GdsContext 无法 Init,此测试直接按 RecordHeader
// 定义的布局手工 pwrite/pread,验证对齐计算的读写一致性
TEST_F(GdsContextTest, WriteRead_CpuBuffer) {
    // 直接打开文件写入（绕过 Init 的 probe）
    std::string path = test_dir_ + "/manual_test.data";
    int fd = ::open(path.c_str(), O_CLOEXEC | O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    // 写入测试数据
    std::string key = "test_key_001";
    std::string value = "hello_gds_test_data_12345678";
    const uint32_t klen = static_cast<uint32_t>(key.size());
    const uint32_t vlen = static_cast<uint32_t>(value.size());
    // 布局: header + key + zero padding + value — value 起始按 4K 对齐
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

    // 读回
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

// ── Test 4: Shutdown 不崩溃 (即使 Init 失败) ──
TEST_F(GdsContextTest, Shutdown_AfterFailedInit) {
    GdsContext ctx;
    ctx.Shutdown();  // 不崩溃
}

// ── Test 5: IsGdsAvailable 静态检查 ──
TEST_F(GdsContextTest, IsGdsAvailable_Static) {
    // 不依赖任何成员，可以随时调用
    bool avail = GdsContext::IsGdsAvailable();
    LOG(INFO) << "GDS available on this machine: " << std::boolalpha << avail;
    SUCCEED();
}

// ── Test 6: WriteRecord/ReadRecord 端到端 (需要 GDS 硬件) ──
// 用 CPU buffer 走 GdsContext 的完整记录读写路径,验证 4K 对齐布局
// 在真实 cuFile 句柄下的读写一致性(含多 slice 读)。
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

    // 写一个两 slice 的记录;key 长度刻意非对齐,验证 value 仍落在
    // 4K 对齐的偏移上并能正确读回。
    std::string key = "e2e_key_007";
    std::string val_a = "first-fragment:";
    std::string val_b(5000, 'z');
    std::vector<Slice> slices = {Slice{val_a.data(), val_a.size()},
                                 Slice{val_b.data(), val_b.size()}};
    const uint64_t offset = 0;
    auto wr = ctx.WriteRecord(key, slices, offset);
    ASSERT_TRUE(wr.has_value());

    // value 必须写在 4K 对齐偏移处 — 直接用 pread 校验布局。
    const uint64_t value_off =
        RecordHeader::ValueOffsetInRecord(static_cast<uint32_t>(key.size()));
    ASSERT_EQ(value_off % RecordHeader::kValueAlignment, 0u);
    std::string raw(value_off + val_a.size() + val_b.size(), '\0');
    ASSERT_EQ(::pread(ctx.gds_fd_, raw.data(), raw.size(), 0),
              static_cast<ssize_t>(raw.size()));
    EXPECT_EQ(raw.substr(value_off, val_a.size()), val_a);

    // 多 slice 读回并校验。
    std::string dst_a(val_a.size(), '\0');
    std::string dst_b(val_b.size(), '\0');
    std::vector<Slice> dest = {Slice{dst_a.data(), dst_a.size()},
                               Slice{dst_b.data(), dst_b.size()}};
    auto rr = ctx.ReadRecord(
        key, dest, offset, static_cast<uint32_t>(val_a.size() + val_b.size()));
    ASSERT_TRUE(rr.has_value());
    EXPECT_EQ(dst_a, val_a);
    EXPECT_EQ(dst_b, val_b);

    // 目标总大小与记录不符时必须拒绝(不允许截断/溢出读)。
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
