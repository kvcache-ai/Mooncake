#include "device/mlu_ipc_buffer.h"

#include <gtest/gtest.h>

#ifdef USE_MLU
#include <cnrt.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <limits>
#include <vector>

#include "mlu_test_utils.h"

extern char **environ;

namespace mooncake::testing {
namespace {

bool Transfer(int fd, void *data, size_t size, bool writing) {
    auto *bytes = static_cast<char *>(data);
    while (size) {
        auto n = writing ? write(fd, bytes, size) : read(fd, bytes, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        bytes += n;
        size -= n;
    }
    return true;
}

// Always exec a fresh runtime; forking an initialized CNRT runtime is unsafe.
class Importer {
   public:
    bool Start(const MluIpcBufferHandle &handle) {
        int input[2], output[2];
        if (pipe(input) || pipe(output)) return false;
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_adddup2(&actions, input[0], STDIN_FILENO);
        posix_spawn_file_actions_adddup2(&actions, output[1], STDOUT_FILENO);
        for (int fd : {input[0], input[1], output[0], output[1]})
            posix_spawn_file_actions_addclose(&actions, fd);
        char executable[] = "/proc/self/exe";
        char mode[] = "--mlu-ipc-importer";
        char *args[] = {executable, mode, nullptr};
        int rc =
            posix_spawn(&pid_, executable, &actions, nullptr, args, environ);
        posix_spawn_file_actions_destroy(&actions);
        close(input[0]);
        close(output[1]);
        input_ = input[1];
        output_ = output[0];
        if (rc) return false;
        auto bytes = struct_pack::serialize(handle);
        size_t size = bytes.size();
        return Transfer(input_, &size, sizeof(size), true) &&
               Transfer(input_, bytes.data(), size, true);
    }
    char Receive() {
        char status = 0;
        return Transfer(output_, &status, 1, false) ? status : 0;
    }
    void Release() {
        char command = 'q';
        ASSERT_TRUE(Transfer(input_, &command, 1, true));
    }
    void Wait() {
        if (pid_ <= 0) return;
        int status = 0;
        ASSERT_EQ(waitpid(pid_, &status, 0), pid_);
        pid_ = -1;
        EXPECT_TRUE(WIFEXITED(status));
        EXPECT_EQ(WEXITSTATUS(status), 0);
    }
    ~Importer() {
        if (input_ >= 0) close(input_);
        if (output_ >= 0) close(output_);
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            waitpid(pid_, nullptr, 0);
        }
    }

   private:
    pid_t pid_ = -1;
    int input_ = -1;
    int output_ = -1;
};

int RunImporter() {
    size_t size = 0;
    if (!Transfer(0, &size, sizeof(size), false) || size > 4096) return 1;
    std::vector<char> bytes(size);
    if (!Transfer(0, bytes.data(), size, false)) return 2;
    auto handle = struct_pack::deserialize<MluIpcBufferHandle>(bytes);
    if (!handle) return 3;
    auto mapping = device::MluIpcBufferMapping::Open(*handle);
    char status = mapping ? 'y' : 'n';
    if (mapping) {
        cnrtPointerAttributes_t attr{};
        if (cnrtPointerGetAttributes(&attr, mapping->ptr()) != cnrtSuccess ||
            cnrtSetDevice(attr.device) != cnrtSuccess)
            return 6;
        std::vector<unsigned char> data(handle->size);
        if (cnrtMemcpy(data.data(), mapping->ptr(), data.size(),
                       cnrtMemcpyDevToHost) != cnrtSuccess ||
            !std::all_of(data.begin(), data.end(),
                         [](auto x) { return x == 17; }))
            status = 'e';
        std::fill(data.begin(), data.end(), 23);
        if (cnrtMemcpy(mapping->ptr(), data.data(), data.size(),
                       cnrtMemcpyHostToDev) != cnrtSuccess)
            status = 'e';
    }
    if (!Transfer(1, &status, 1, true)) return 4;
    if (!mapping) return 0;
    char command = 0;
    if (!Transfer(0, &command, 1, false) || command != 'q') return 5;
    return 0;
}

class MluIpcTest : public MluTest {
   protected:
    void SetUp() override {
        MluTest::SetUp();
        if (HasFatalFailure() || IsSkipped()) return;
        ASSERT_EQ(buffer_.allocate(4096), cnrtSuccess);
        std::vector<unsigned char> data(4096, 17);
        ASSERT_EQ(cnrtMemcpy(buffer_.data(), data.data(), data.size(),
                             cnrtMemcpyHostToDev),
                  cnrtSuccess);
    }
    MluTestBuffer buffer_;
};

TEST_F(MluIpcTest, OffsetMappingRoundTrip) {
    auto exported = device::MluIpcBufferExport::Create(buffer_.data(32), 512);
    ASSERT_TRUE(exported);
    Importer importer;
    ASSERT_TRUE(importer.Start(exported->handle()));
    ASSERT_EQ(importer.Receive(), 'y');
    importer.Release();
    importer.Wait();
    std::vector<unsigned char> data(4096);
    ASSERT_EQ(cnrtMemcpy(data.data(), buffer_.data(), data.size(),
                         cnrtMemcpyDevToHost),
              cnrtSuccess);
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(data[i], i >= 32 && i < 544 ? 23 : 17) << i;
}

TEST_F(MluIpcTest, RetirementDrainsActiveImportAndRejectsLateRequests) {
    auto exported = device::MluIpcBufferExport::Create(buffer_.data(), 4096);
    ASSERT_TRUE(exported);
    const auto handle = exported->handle();
    Importer importer;
    ASSERT_TRUE(importer.Start(handle));
    ASSERT_EQ(importer.Receive(), 'y');
    auto retired = std::async(std::launch::async,
                              [owner = std::move(*exported)]() mutable {
                                  owner = device::MluIpcBufferExport{};
                              });
    EXPECT_EQ(retired.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    importer.Release();
    importer.Wait();
    ASSERT_EQ(retired.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    retired.get();
    Importer late;
    ASSERT_TRUE(late.Start(handle));
    EXPECT_EQ(late.Receive(), 'n');
    late.Wait();
}

TEST_F(MluIpcTest, RejectsInvalidRangesAndLeaseIdentity) {
    EXPECT_FALSE(device::MluIpcBufferExport::Create(buffer_.data(4090), 32));
    EXPECT_FALSE(device::MluIpcBufferExport::Create(nullptr, 32));
    auto exported = device::MluIpcBufferExport::Create(buffer_.data(), 4096);
    ASSERT_TRUE(exported);
    for (int test = 0; test < 5; ++test) {
        auto handle = exported->handle();
        if (test == 0) handle.offset = std::numeric_limits<uint64_t>::max();
        if (test == 1) handle.lease_inode++;
        if (test == 2) handle.lease = "../bad";
        if (test == 3) handle.device_uuid = "invalid-device";
        if (test == 4) handle.size = 1ULL << 40;
        Importer importer;
        ASSERT_TRUE(importer.Start(handle));
        EXPECT_EQ(importer.Receive(), 'n') << test;
        importer.Wait();
    }
}
}  // namespace
}  // namespace mooncake::testing
#else
TEST(MluIpcBufferTest, CpuBuildRejectsDeviceHandles) {
    EXPECT_FALSE(mooncake::device::MluIpcBufferExport::Create(nullptr, 1));
    EXPECT_FALSE(mooncake::device::MluIpcBufferMapping::Open({}));
}
#endif

int main(int argc, char **argv) {
#ifdef USE_MLU
    if (argc == 2 && std::string(argv[1]) == "--mlu-ipc-importer")
        return mooncake::testing::RunImporter();
#endif
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
