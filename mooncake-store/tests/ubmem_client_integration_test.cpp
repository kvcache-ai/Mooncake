// Copyright 2024 KVCache.AI
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

// Store-level ubmem integration test. Two clients each own a simulated
// shmdev (sparse backing file), so one BatchGet fans out over two
// protocol="ubmem" segments — the segment-grouped transfer-batch path in
// BatchGet — without any hardware. Data correctness and per-key result
// attribution are asserted end to end through the embedded master.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "client_service.h"
#include "test_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

class ClientIntegrationTestUbmem : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("ClientIntegrationTestUbmem");
        FLAGS_logtostderr = 1;

        for (const char* dev : {kDeviceA, kDeviceB}) {
            int fd = open(dev, O_RDWR | O_CREAT | O_TRUNC, 0666);
            ASSERT_GE(fd, 0);
            ASSERT_EQ(ftruncate(fd, kWindowBytes), 0);
            close(fd);
        }
        setenv("MC_UBMEM_DEV_SIZE", std::to_string(kWindowBytes).c_str(), 1);

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));

        // Each client installs its transport at Create time, so the device
        // env is swapped between the two creations. Client::Create mounts no
        // segment by itself; each client then mounts its device window as
        // its protocol="ubmem" segment (same shape as the tcp test).
        setenv("MC_UBMEM_DEV_PATH", kDeviceA, 1);
        client_a_ = CreateClient("localhost:17813");
        setenv("MC_UBMEM_DEV_PATH", kDeviceB, 1);
        client_b_ = CreateClient("localhost:17814");
        ASSERT_NE(client_a_, nullptr);
        ASSERT_NE(client_b_, nullptr);

        for (auto& [client, base] :
             {std::make_pair(&client_a_, &segment_base_a_),
              std::make_pair(&client_b_, &segment_base_b_)}) {
            void* window = (*client)->GetBaseAddr();
            ASSERT_NE(window, nullptr);
            auto mount_result =
                (*client)->MountSegment(window, kSegmentBytes, "ubmem");
            ASSERT_TRUE(mount_result.has_value())
                << "MountSegment failed: "
                << (mount_result.has_value() ? ""
                                             : toString(mount_result.error()));
            *base = window;
        }
    }

    static void TearDownTestSuite() {
        for (auto& [client, base] :
             {std::make_pair(&client_a_, &segment_base_a_),
              std::make_pair(&client_b_, &segment_base_b_)}) {
            if (*client && *base) {
                (*client)->UnmountSegment(*base, kSegmentBytes);
            }
        }
        client_a_.reset();
        client_b_.reset();
        master_.Stop();
        google::ShutdownGoogleLogging();
        for (const char* dev : {kDeviceA, kDeviceB}) unlink(dev);
    }

    static std::shared_ptr<Client> CreateClient(const std::string& host_name) {
        auto client_opt =
            Client::Create(host_name, master_.metadata_url(), "ubmem",
                           std::nullopt, master_.master_address());
        EXPECT_TRUE(client_opt.has_value())
            << "Failed to create ubmem client for " << host_name;
        return client_opt.has_value() ? client_opt.value() : nullptr;
    }

    static constexpr const char* kDeviceA = "tmp_ubmem_store_sim_a";
    static constexpr const char* kDeviceB = "tmp_ubmem_store_sim_b";
    static constexpr uint64_t kWindowBytes = 256ull << 20;
    static constexpr size_t kSegmentBytes = 128ull << 20;
    static constexpr int kKeysPerClient = 48;

    static InProcMaster master_;
    static std::shared_ptr<Client> client_a_;
    static std::shared_ptr<Client> client_b_;
    static void* segment_base_a_;
    static void* segment_base_b_;
};

InProcMaster ClientIntegrationTestUbmem::master_;
std::shared_ptr<Client> ClientIntegrationTestUbmem::client_a_ = nullptr;
std::shared_ptr<Client> ClientIntegrationTestUbmem::client_b_ = nullptr;
void* ClientIntegrationTestUbmem::segment_base_a_ = nullptr;
void* ClientIntegrationTestUbmem::segment_base_b_ = nullptr;

namespace {

constexpr size_t kObjectSize = 32 * 1024;

std::string keyOf(char tag, int i) {
    return std::string("ubmem_batch_key_") + tag + "_" + std::to_string(i);
}

// deterministic per-key fill: byte j = tag ^ (key index low bits) ^ j
void fillPattern(std::vector<char>& buf, char tag, int key_index) {
    for (size_t j = 0; j < buf.size(); ++j)
        buf[j] = static_cast<char>(tag ^ (key_index & 0xff) ^ (j & 0xff));
}

std::vector<std::vector<char>> putKeys(const std::shared_ptr<Client>& client,
                                       char tag, int count) {
    std::vector<std::string> keys;
    std::vector<std::vector<Slice>> batched_slices;
    std::vector<std::vector<char>> payloads;
    payloads.reserve(count);
    for (int i = 0; i < count; ++i) {
        keys.push_back(keyOf(tag, i));
        payloads.emplace_back(kObjectSize);
        fillPattern(payloads.back(), tag, i);
        batched_slices.emplace_back(
            std::vector<Slice>{{payloads.back().data(), kObjectSize}});
    }
    ReplicateConfig config;
    config.replica_num = 1;
    auto results = client->BatchPut(keys, batched_slices, config);
    for (const auto& result : results)
        EXPECT_TRUE(result.has_value())
            << "BatchPut failed: "
            << (result.has_value() ? "" : toString(result.error()));
    return payloads;
}

// allocate zeroed destination buffers and the slice map for the given keys
std::unordered_map<std::string, std::vector<Slice>> makeDestSlices(
    const std::vector<std::string>& keys,
    std::unordered_map<std::string, std::vector<char>>& buffers) {
    std::unordered_map<std::string, std::vector<Slice>> slices;
    for (const auto& key : keys) {
        auto [it, inserted] = buffers.emplace(key, kObjectSize);
        EXPECT_TRUE(inserted);
        memset(it->second.data(), 0, kObjectSize);
        slices.emplace(key,
                       std::vector<Slice>{{it->second.data(), kObjectSize}});
    }
    return slices;
}

void expectBytes(
    const std::unordered_map<std::string, std::vector<Slice>>& slices,
    const std::string& key, const std::vector<char>& expected) {
    ASSERT_EQ(expected.size(), kObjectSize);
    EXPECT_EQ(memcmp(expected.data(), slices.at(key)[0].ptr, kObjectSize), 0)
        << "data mismatch for " << key;
}

}  // namespace

// One BatchGet spans both ubmem segments: the grouped path issues one
// transfer batch per segment and must return every key's bytes intact.
TEST_F(ClientIntegrationTestUbmem, BatchGetAcrossTwoUbmemSegments) {
    auto payloads_a = putKeys(client_a_, 'a', kKeysPerClient);
    auto payloads_b = putKeys(client_b_, 'b', kKeysPerClient);

    std::vector<std::string> keys;
    std::unordered_map<std::string, const std::vector<char>*> expected;
    for (int i = 0; i < kKeysPerClient; ++i) {
        for (auto [tag, payloads] : {std::make_pair('a', &payloads_a),
                                     std::make_pair('b', &payloads_b)}) {
            keys.push_back(keyOf(tag, i));
            expected[keys.back()] = &(*payloads)[i];
        }
    }

    std::unordered_map<std::string, std::vector<char>> buffers;
    auto slices = makeDestSlices(keys, buffers);

    auto results = client_a_->BatchGet(keys, slices);
    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "BatchGet failed for " << keys[i] << ": "
            << (results[i].has_value() ? "" : toString(results[i].error()));
        expectBytes(slices, keys[i], *expected[keys[i]]);
    }
}

// A missing key keeps its slot in the result vector while the grouped
// transfers around it still succeed.
TEST_F(ClientIntegrationTestUbmem, BatchGetAttributesMissingKeyInPlace) {
    auto payloads = putKeys(client_a_, 'a', kKeysPerClient);

    std::vector<std::string> keys{keyOf('a', 0), keyOf('a', 1),
                                  "ubmem_missing_key", keyOf('a', 2)};
    std::unordered_map<std::string, std::vector<char>> buffers;
    auto slices = makeDestSlices(keys, buffers);

    auto results = client_a_->BatchGet(keys, slices);
    ASSERT_EQ(results.size(), keys.size());
    EXPECT_FALSE(results[2].has_value());  // the missing key fails alone
    EXPECT_TRUE(results[0].has_value());
    EXPECT_TRUE(results[1].has_value());
    EXPECT_TRUE(results[3].has_value());
    expectBytes(slices, keys[0], payloads[0]);
    expectBytes(slices, keys[1], payloads[1]);
    expectBytes(slices, keys[3], payloads[2]);
}

// Single-key Get keeps working alongside the grouped batch path.
TEST_F(ClientIntegrationTestUbmem, SingleGetFromPeerSegment) {
    auto payloads = putKeys(client_b_, 'b', 4);
    const auto key = keyOf('b', 2);
    std::vector<char> buffer(kObjectSize, 0);
    std::vector<Slice> slices{{buffer.data(), kObjectSize}};
    auto result = client_a_->Get(key, slices);
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_EQ(memcmp(buffer.data(), payloads[2].data(), kObjectSize), 0);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
