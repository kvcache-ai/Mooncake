#include "client_service.h"
#include "test_server_helpers.h"

#include <gtest/gtest.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdarg>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>

namespace {
using Key = std::array<uint8_t, 16>;
struct Target {
    std::map<Key, std::string> values;
    size_t reads = 0;
};
Target* target = nullptr;
}  // namespace

extern "C" int __real_ioctl(int fd, unsigned long request, ...);
extern "C" int __wrap_ioctl(int fd, unsigned long request, ...) {
    va_list args;
    va_start(args, request);
    void* arg = va_arg(args, void*);
    va_end(args);
    if (!target || request != NVME_IOCTL_IO_CMD) {
        return __real_ioctl(fd, request, arg);
    }

    auto* cmd = static_cast<nvme_passthru_cmd*>(arg);
    EXPECT_EQ(cmd->nsid, 7u);
    EXPECT_EQ(cmd->cdw11 & 0xff, 16u);
    EXPECT_EQ(cmd->data_len % 4096, 0u);
    EXPECT_EQ(cmd->cdw10, cmd->data_len);
    Key key{};
    const uint32_t words[] = {cmd->cdw2, cmd->cdw3, cmd->cdw14, cmd->cdw15};
    for (size_t i = 0; i < 4; ++i) {
        for (size_t b = 0; b < 4; ++b) {
            key[i * 4 + b] = static_cast<uint8_t>(words[i] >> (8 * b));
        }
    }

    auto* data = reinterpret_cast<char*>(cmd->addr);
    if (cmd->opcode == 0x01) {
        // Store-if-not-exists.
        EXPECT_EQ(cmd->cdw11 >> 8, 2u);
        return target->values.emplace(key, std::string(data, cmd->data_len))
                       .second
                   ? 0
                   : 0x89;
    }
    EXPECT_EQ(cmd->opcode, 0x02);
    ++target->reads;
    auto it = target->values.find(key);
    if (it == target->values.end()) {
        return 0x87;
    }
    std::memcpy(data, it->second.data(),
                std::min<size_t>(cmd->data_len, it->second.size()));
    cmd->result = it->second.size();
    return 0;
}

namespace mooncake::testing {
namespace {

class SharedNvmeKvLinuxTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char path[] = "/tmp/mooncake-linux-kv-XXXXXX";
        ASSERT_NE(::mkdtemp(path), nullptr);
        directory_ = path;
        std::ofstream(directory_ + "/device");
        if (const char* value = std::getenv("MOONCAKE_SHARED_NVME_KV_CONFIG")) {
            old_env_ = value;
        }
        ASSERT_EQ(::setenv("MOONCAKE_SHARED_NVME_KV_CONFIG",
                           (directory_ + "/config.json").c_str(), 1),
                  0);
        target = &target_;
        auto config = InProcMasterConfigBuilder().build();
        ASSERT_TRUE(master_.Start(config));
    }

    void TearDown() override {
        master_.Stop();
        target = nullptr;
        if (old_env_) {
            ::setenv("MOONCAKE_SHARED_NVME_KV_CONFIG", old_env_->c_str(), 1);
        } else {
            ::unsetenv("MOONCAKE_SHARED_NVME_KV_CONFIG");
        }
        std::filesystem::remove_all(directory_);
    }

    auto NewClient(const char* transport, const char* hostname) {
        {
            std::ofstream config(directory_ + "/config.json");
            config << R"({"namespaces":[{"device_path":")" << directory_
                   << "/device"
                   << R"(","nsid":7,"transport":")" << transport
                   << R"(","runtime_transfer_limit":4096}]})";
        }
        return Client::Create(hostname, "P2PHANDSHAKE", "tcp", std::nullopt,
                              master_.master_address());
    }

    std::string directory_;
    std::optional<std::string> old_env_;
    Target target_;
    InProcMaster master_;
};

TEST_F(SharedNvmeKvLinuxTest,
       ExistingExecutorHandlesPaddingAndCrossClientReads) {
    auto writer = NewClient("auto", "127.0.0.1:19011");
    auto reader = NewClient("ioctl", "127.0.0.1:19012");
    ASSERT_TRUE(writer);
    ASSERT_TRUE(reader);
    ReplicateConfig replicas;
    replicas.replica_num = 0;
    replicas.shared_nvme_kv_replica_num = 1;
    std::string payload = "unaligned inline payload";
    std::vector<Slice> input{{payload.data(), payload.size()}};
    ASSERT_TRUE((*writer)->Put("a", input, replicas));
    ASSERT_TRUE((*writer)->Put("b", input, replicas));
    ASSERT_EQ(target_.values.size(), 2);
    for (const auto& [key, value] : target_.values)
        EXPECT_EQ(value.size(), 4096);
    writer->reset();

    std::string a(payload.size(), '?'), b(payload.size(), '?');
    std::vector<Slice> output{{a.data(), a.size()}};
    ASSERT_TRUE((*reader)->Get("a", output));
    EXPECT_EQ(a, payload);
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"a", {{a.data(), a.size()}}}, {"b", {{b.data(), b.size()}}}};
    auto results = (*reader)->BatchGet({"a", "b"}, batch);
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results[0]);
    EXPECT_TRUE(results[1]);
    EXPECT_EQ(b, payload);
    EXPECT_FALSE(NewClient("io_uring", "127.0.0.1:19013"));
}
}  // namespace
}  // namespace mooncake::testing
