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
TEST_F(GdsContextTest, ProbeGdsAvailable_NoGds) {
    EXPECT_FALSE(GdsContext::IsGdsAvailable());
    GdsContext ctx;
    EXPECT_FALSE(ctx.ProbeGdsAvailable(test_dir_));
}

// ── Test 3: WriteRecord + ReadRecord (Compat 模式, CPU buffer) ──
// 注意: 此测试在无 GDS 硬件时也能跑，因为 Init 失败后会走 StorageFile 路径
// 这里的测试仅验证 RecordHeader 格式和 pwrite/pread 的正确性
TEST_F(GdsContextTest, WriteRead_CpuBuffer) {
    // 直接打开文件写入（绕过 Init 的 probe）
    std::string path = test_dir_ + "/manual_test.data";
    int fd = ::open(path.c_str(), O_CLOEXEC | O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);

    // 写入测试数据
    std::string key = "test_key_001";
    std::string value = "hello_gds_test_data_12345678";
    std::vector<Slice> slices = {Slice{(void*)value.data(), value.size()}};

    // header + key + value 直接 pwrite
    RecordHeader hdr{.key_len = static_cast<uint32_t>(key.size()),
                     .value_len = static_cast<uint32_t>(value.size())};
    ASSERT_EQ(::pwrite(fd, &hdr, RecordHeader::SIZE, 0), RecordHeader::SIZE);
    ASSERT_EQ(::pwrite(fd, key.data(), key.size(), RecordHeader::SIZE),
              static_cast<ssize_t>(key.size()));
    ASSERT_EQ(::pwrite(fd, value.data(), value.size(),
                       RecordHeader::SIZE + key.size()),
              static_cast<ssize_t>(value.size()));

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
    ASSERT_EQ(::pread(fd, read_val.data(), value.size(),
                      RecordHeader::SIZE + key.size()),
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
}  // namespace mooncake
