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

// The fd channel that nvlink_intra uses for VMM buffers. It does not use
// CUDA, so plain fds stand in for exported cuMemCreate handles.

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <string>
#include <vector>

#include "transport/intranode_nvlink_transport/vmm_fd_channel.h"

namespace mooncake {
namespace intranode_vmm {
namespace {

VmmBufferInfo MakeInfo(size_t num_chunks) {
    VmmBufferInfo info;
    info.buffer_id = 42;
    info.endpoint = "@mooncake-nvlink-1234-0123456789abcdef";
    uint64_t offset = 0;
    for (size_t i = 0; i < num_chunks; ++i) {
        uint64_t size = (i + 1) * (2ull << 20);
        info.chunks.push_back({offset, size});
        offset += size;
    }
    return info;
}

// One memfd per chunk whose content identifies it.
int ExportMemfds(uint64_t buffer_id, size_t count, std::vector<int>& fds) {
    for (size_t i = 0; i < count; ++i) {
        int fd = memfd_create("vmm_fd_channel_test", MFD_CLOEXEC);
        if (fd < 0) return -errno;
        std::string tag = std::to_string(buffer_id) + ":" + std::to_string(i);
        if (write(fd, tag.data(), tag.size()) != (ssize_t)tag.size()) {
            close(fd);
            return -EIO;
        }
        fds.push_back(fd);
    }
    return 0;
}

std::string ReadTag(int fd) {
    char buf[64] = {};
    ssize_t n = pread(fd, buf, sizeof(buf) - 1, 0);
    return n > 0 ? std::string(buf, n) : std::string();
}

void ExpectFds(const std::vector<int>& fds, uint64_t buffer_id) {
    for (size_t i = 0; i < fds.size(); ++i) {
        EXPECT_EQ(ReadTag(fds[i]),
                  std::to_string(buffer_id) + ":" + std::to_string(i));
    }
}

void CloseAll(std::vector<int>& fds) {
    for (int fd : fds) close(fd);
    fds.clear();
}

TEST(VmmBufferInfoTest, RoundTrip) {
    VmmBufferInfo info = MakeInfo(3);
    std::vector<unsigned char> data = encodeVmmBufferInfo(info);
    ASSERT_FALSE(data.empty());
    VmmBufferInfo decoded;
    ASSERT_TRUE(decodeVmmBufferInfo(data, decoded));
    EXPECT_EQ(decoded.buffer_id, info.buffer_id);
    EXPECT_EQ(decoded.endpoint, info.endpoint);
    ASSERT_EQ(decoded.chunks.size(), 3u);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(decoded.chunks[i].offset, info.chunks[i].offset);
        EXPECT_EQ(decoded.chunks[i].size, info.chunks[i].size);
    }
}

TEST(VmmBufferInfoTest, NeverLooksLikeACudaIpcHandle) {
    // Readers that only know cudaIpc treat a 64-byte payload as a handle.
    for (size_t len = 1; len < 64; ++len) {
        VmmBufferInfo info = MakeInfo(1);
        info.endpoint = "@" + std::string(len, 'x');
        EXPECT_NE(encodeVmmBufferInfo(info).size(), 64u) << len;
    }
}

TEST(VmmBufferInfoTest, RejectsMalformedData) {
    VmmBufferInfo decoded;
    std::vector<unsigned char> data = encodeVmmBufferInfo(MakeInfo(2));
    ASSERT_FALSE(data.empty());

    std::vector<unsigned char> legacy(64, 0);
    EXPECT_FALSE(decodeVmmBufferInfo(legacy, decoded));

    auto truncated = data;
    truncated.pop_back();
    EXPECT_FALSE(decodeVmmBufferInfo(truncated, decoded));

    auto extended = data;
    extended.push_back(0);
    EXPECT_FALSE(decodeVmmBufferInfo(extended, decoded));

    auto bad_magic = data;
    bad_magic[0] ^= 0xff;
    EXPECT_FALSE(decodeVmmBufferInfo(bad_magic, decoded));

    VmmBufferInfo gap = MakeInfo(2);
    gap.chunks[1].offset += 4096;
    EXPECT_FALSE(decodeVmmBufferInfo(encodeVmmBufferInfo(gap), decoded));

    EXPECT_TRUE(encodeVmmBufferInfo(MakeInfo(0)).empty());
}

class VmmFdServerTest : public ::testing::TestWithParam<bool> {
   protected:
    void SetUp() override {
        if (GetParam()) {
            char dir_template[] = "/tmp/vmm_fd_channel_test.XXXXXX";
            ASSERT_NE(mkdtemp(dir_template), nullptr);
            dir_ = dir_template;
        }
    }
    void TearDown() override {
        if (!dir_.empty()) rmdir(dir_.c_str());
    }
    std::string dir_;
};

TEST_P(VmmFdServerTest, ServesFdsInChunkOrder) {
    VmmFdServer server;
    // More than one SCM_RIGHTS message's worth of fds.
    const size_t count = 150;
    ASSERT_EQ(server.start(dir_,
                           [&](uint64_t id, std::vector<int>& fds) {
                               if (id != 7) return -ENOENT;
                               return ExportMemfds(id, count, fds);
                           }),
              0);
    ASSERT_TRUE(server.running());
    EXPECT_EQ(server.endpoint()[0] == '@', dir_.empty());

    std::vector<int> fds;
    ASSERT_EQ(fetchVmmFds(server.endpoint(), 7, count, fds), 0);
    ASSERT_EQ(fds.size(), count);
    ExpectFds(fds, 7);
    CloseAll(fds);

    // A second importer gets its own fds.
    ASSERT_EQ(fetchVmmFds(server.endpoint(), 7, count, fds), 0);
    ExpectFds(fds, 7);
    CloseAll(fds);

    std::string endpoint = server.endpoint();
    server.stop();
    EXPECT_FALSE(server.running());
    if (!dir_.empty()) {
        struct stat st;
        EXPECT_NE(stat(endpoint.c_str(), &st), 0) << "socket file left behind";
    }
}

TEST_P(VmmFdServerTest, ReportsUnknownBufferAndCountMismatch) {
    VmmFdServer server;
    ASSERT_EQ(server.start(dir_,
                           [](uint64_t id, std::vector<int>& fds) {
                               if (id != 1) return -ENOENT;
                               return ExportMemfds(id, 2, fds);
                           }),
              0);
    std::vector<int> fds;
    EXPECT_NE(fetchVmmFds(server.endpoint(), 99, 2, fds), 0);
    EXPECT_TRUE(fds.empty());
    EXPECT_NE(fetchVmmFds(server.endpoint(), 1, 3, fds), 0);
    EXPECT_TRUE(fds.empty());
    // The server still works after failed requests.
    ASSERT_EQ(fetchVmmFds(server.endpoint(), 1, 2, fds), 0);
    ExpectFds(fds, 1);
    CloseAll(fds);
}

INSTANTIATE_TEST_SUITE_P(AbstractAndPath, VmmFdServerTest,
                         ::testing::Values(false, true),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "SocketFile" : "Abstract";
                         });

TEST(VmmFdChannelTest, FetchFromMissingServerFails) {
    std::vector<int> fds;
    EXPECT_NE(fetchVmmFds("@mooncake-nvlink-does-not-exist", 1, 1, fds, 1000),
              0);
    EXPECT_NE(fetchVmmFds("", 1, 1, fds, 1000), 0);
    EXPECT_TRUE(fds.empty());
}

}  // namespace
}  // namespace intranode_vmm
}  // namespace mooncake
