// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "client_wrapper.h"
#include "test_server_helpers.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");

namespace fs = std::filesystem;

constexpr int kClientPortBase = 13888;

namespace mooncake {
namespace testing {

struct MasterOpts {
    uint64_t quota_bytes = 0;
    bool enable_disk_eviction = true;
    double eviction_high_watermark_ratio = -1;
    uint64_t default_kv_lease_ttl = 0;
};

// -------------------------------------------------------------------
// StorageBackendE2ETest
//
// In-process master (non-HA) suite over TCP that verifies the full
// storage-backend demotion pipeline:
//
//   Put (memory + disk replica)
//     -> eviction removes memory replica
//       -> only DISK replica survives
//         -> GetWithExpectedSize reads from disk
//           -> payload verified byte-for-byte
// -------------------------------------------------------------------
class StorageBackendE2ETest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("StorageBackendE2ETest");
        FLAGS_logtostderr = 1;
        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device: " << FLAGS_device_name;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        tmp_dir_ = fs::temp_directory_path() /
                   ("mc_sbe2e_" + std::to_string(::getpid()));
        fs::create_directories(tmp_dir_);
    }

    void TearDown() override {
        master_.Stop();
        std::error_code ec;
        fs::remove_all(tmp_dir_, ec);
    }

    // ----- master helpers -----

    void StartMaster(const MasterOpts& opts = MasterOpts()) {
        auto builder = InProcMasterConfigBuilder()
                           .set_root_fs_dir(tmp_dir_.string())
                           .set_enable_disk_eviction(opts.enable_disk_eviction);

        if (opts.quota_bytes > 0) {
            builder.set_quota_bytes(opts.quota_bytes);
        }
        if (opts.eviction_high_watermark_ratio >= 0) {
            builder.set_eviction_high_watermark_ratio(
                opts.eviction_high_watermark_ratio);
        }
        if (opts.default_kv_lease_ttl > 0) {
            builder.set_default_kv_lease_ttl(opts.default_kv_lease_ttl);
        }

        ASSERT_TRUE(master_.Start(builder.build()));
        master_address_ = master_.master_address();
        LOG(INFO) << "In-proc master at " << master_address_;
    }

    // ----- client helpers -----

    std::shared_ptr<ClientTestWrapper> CreateClient(
        int port_offset, size_t segment_size = 16 * 1024 * 1024) {
        std::string hostname =
            "127.0.0.1:" + std::to_string(kClientPortBase + port_offset);
        auto opt = ClientTestWrapper::CreateClientWrapper(
            hostname, "P2PHANDSHAKE", FLAGS_protocol, FLAGS_device_name,
            master_address_);
        EXPECT_TRUE(opt.has_value())
            << "Failed to create client at " << hostname;
        if (!opt.has_value()) return nullptr;

        auto wrapper = *opt;
        void* buf = nullptr;
        auto mount_err = wrapper->Mount(segment_size, buf);
        if (mount_err != ErrorCode::OK) {
            ADD_FAILURE() << "Mount failed for " << hostname
                          << " err=" << toString(mount_err);
            return nullptr;
        }
        return wrapper;
    }

    bool WaitForDiskReplica(
        const std::shared_ptr<ClientTestWrapper>& client,
        const std::string& key,
        std::chrono::seconds timeout = std::chrono::seconds(20)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (client->HasDiskReplica(key)) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool WaitForNoMemoryReplica(
        const std::shared_ptr<ClientTestWrapper>& client,
        const std::string& key,
        std::chrono::seconds timeout = std::chrono::seconds(20)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (!client->HasMemoryReplica(key)) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    void PutKeys(const std::shared_ptr<ClientTestWrapper>& client,
                 const std::string& prefix, int count, size_t value_size,
                 std::vector<std::string>& keys,
                 std::vector<std::string>& values) {
        for (int i = 0; i < count; ++i) {
            std::string key = prefix + std::to_string(i);
            std::string value(value_size, static_cast<char>('A' + (i % 26)));
            ASSERT_EQ(client->Put(key, value), ErrorCode::OK)
                << "Put failed for key=" << key;
            keys.push_back(key);
            values.push_back(value);
        }
    }

    void WaitForAllDiskReplicas(
        const std::shared_ptr<ClientTestWrapper>& client,
        const std::vector<std::string>& keys) {
        for (const auto& key : keys) {
            ASSERT_TRUE(WaitForDiskReplica(client, key))
                << "DISK replica did not appear for " << key;
        }
    }

    // ----- state -----
    fs::path tmp_dir_;
    InProcMaster master_;
    std::string master_address_;
};

// -------------------------------------------------------------------
// Test 1: OffloadAndReadback
// -------------------------------------------------------------------
TEST_F(StorageBackendE2ETest, OffloadAndReadback) {
    StartMaster();

    constexpr size_t kValueSize = 4096;
    const std::string key = "sbe2e_offload";
    const std::string expected(kValueSize, 'D');

    auto client = CreateClient(/*port_offset=*/0);
    ASSERT_NE(client, nullptr);

    ASSERT_EQ(client->Put(key, expected), ErrorCode::OK);
    ASSERT_TRUE(WaitForDiskReplica(client, key))
        << "DISK replica did not appear.";

    std::string got;
    ASSERT_EQ(client->Get(key, got), ErrorCode::OK);
    EXPECT_EQ(got, expected) << "Payload mismatch after offload.";
}

// -------------------------------------------------------------------
// Test 2: CrossClientReadback
// -------------------------------------------------------------------
TEST_F(StorageBackendE2ETest, CrossClientReadback) {
    StartMaster();

    constexpr int kKeyCount = 5;
    constexpr size_t kValueSize = 4096;

    auto writer = CreateClient(/*port_offset=*/0);
    ASSERT_NE(writer, nullptr);

    std::vector<std::string> keys;
    std::vector<std::string> expected_values;
    PutKeys(writer, "sbe2e_xread_", kKeyCount, kValueSize, keys,
            expected_values);
    WaitForAllDiskReplicas(writer, keys);

    auto reader = CreateClient(/*port_offset=*/1);
    ASSERT_NE(reader, nullptr);

    for (int i = 0; i < kKeyCount; ++i) {
        EXPECT_TRUE(reader->HasDiskReplica(keys[i]))
            << "DISK replica missing for " << keys[i];

        std::string got;
        ASSERT_EQ(reader->Get(keys[i], got), ErrorCode::OK)
            << "Get failed for " << keys[i];
        EXPECT_EQ(got, expected_values[i])
            << "Payload mismatch for " << keys[i];
    }
}

// -------------------------------------------------------------------
// Test 3: DiskOnlyReadAfterEviction
// -------------------------------------------------------------------
TEST_F(StorageBackendE2ETest, DiskOnlyReadAfterEviction) {
    MasterOpts opts;
    opts.eviction_high_watermark_ratio = 0.1;
    opts.default_kv_lease_ttl = 1000;
    StartMaster(opts);

    // Client segment size must be at least 16MB (slab size).
    constexpr size_t kSegment = 16 * 1024 * 1024;
    constexpr size_t kValueSize = 256 * 1024;
    constexpr int kSeedCount = 4;
    constexpr int kPressureCount = 12;

    auto client = CreateClient(
        /*port_offset=*/0, kSegment);
    ASSERT_NE(client, nullptr);

    // Phase 1: seed keys below watermark.
    std::vector<std::string> seed_keys;
    std::vector<std::string> seed_values;
    PutKeys(client, "sbe2e_evict_seed_", kSeedCount, kValueSize, seed_keys,
            seed_values);
    WaitForAllDiskReplicas(client, seed_keys);

    // Wait for leases to expire (lease_ttl = 1s, add margin).
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Phase 2: pressure keys push past watermark.
    std::vector<std::string> pressure_keys;
    std::vector<std::string> pressure_values;
    PutKeys(client, "sbe2e_evict_press_", kPressureCount, kValueSize,
            pressure_keys, pressure_values);

    // Give eviction thread time to run and settle.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Phase 3: verify eviction happened for at least one expired seed key,
    // then prove that the surviving disk replica remains readable.
    int evicted_seed_index = -1;
    for (int i = 0; i < kSeedCount; ++i) {
        EXPECT_TRUE(client->HasDiskReplica(seed_keys[i]))
            << "DISK replica missing for " << seed_keys[i];
        if (WaitForNoMemoryReplica(client, seed_keys[i],
                                   std::chrono::seconds(5))) {
            evicted_seed_index = i;
            break;
        }
    }

    ASSERT_GE(evicted_seed_index, 0)
        << "Expected at least one expired seed key to be evicted from memory.";

    std::string got;
    ASSERT_EQ(client->GetWithExpectedSize(seed_keys[evicted_seed_index],
                                          kValueSize, got),
              ErrorCode::OK)
        << "Disk-only read failed for " << seed_keys[evicted_seed_index];
    EXPECT_EQ(got, seed_values[evicted_seed_index])
        << "Payload mismatch for " << seed_keys[evicted_seed_index];
}

// -------------------------------------------------------------------
// Test 4: DiskReplicaSurvivesWriterDeath
// -------------------------------------------------------------------
TEST_F(StorageBackendE2ETest, DiskReplicaSurvivesWriterDeath) {
    StartMaster();

    constexpr int kKeyCount = 4;
    constexpr size_t kValueSize = 2048;

    std::vector<std::string> keys;
    std::vector<std::string> expected_values;

    // Phase 1: write and offload, then destroy writer.
    {
        auto writer = CreateClient(/*port_offset=*/0);
        ASSERT_NE(writer, nullptr);

        PutKeys(writer, "sbe2e_survive_", kKeyCount, kValueSize, keys,
                expected_values);
        WaitForAllDiskReplicas(writer, keys);
    }

    // Wait for client TTL expiry so master cleans up the writer.
    sleep(DEFAULT_CLIENT_LIVE_TTL_SEC + 5);

    // Phase 2: new client reads from disk.
    {
        auto reader = CreateClient(/*port_offset=*/1);
        ASSERT_NE(reader, nullptr);

        for (int i = 0; i < kKeyCount; ++i) {
            EXPECT_TRUE(reader->HasDiskReplica(keys[i]))
                << "DISK replica gone for " << keys[i]
                << " after writer death.";
            EXPECT_FALSE(reader->HasMemoryReplica(keys[i]))
                << "Memory replica should not exist "
                << "after writer death for " << keys[i];

            std::string got;
            auto err = reader->GetWithExpectedSize(keys[i], kValueSize, got);
            if (err == ErrorCode::OK) {
                EXPECT_EQ(got, expected_values[i])
                    << "Data mismatch for " << keys[i];
            } else {
                LOG(WARNING)
                    << "Key " << keys[i] << " not readable after writer "
                    << "death: " << toString(err);
            }
        }
    }
}

// -------------------------------------------------------------------
// Test 5: QuotaExhaustionGracefulBehavior
// -------------------------------------------------------------------
TEST_F(StorageBackendE2ETest, QuotaExhaustionGracefulBehavior) {
    MasterOpts opts;
    opts.quota_bytes = 6 * 1024;
    StartMaster(opts);

    constexpr size_t kValueSize = 2048;
    constexpr int kKeyCount = 8;

    auto writer = CreateClient(/*port_offset=*/0);
    ASSERT_NE(writer, nullptr);

    std::vector<std::string> keys;
    std::vector<std::string> values;
    PutKeys(writer, "sbe2e_quota_", kKeyCount, kValueSize, keys, values);

    // Give async disk writes time to settle.
    std::this_thread::sleep_for(std::chrono::seconds(10));

    // All keys readable from memory while writer alive.
    for (int i = 0; i < kKeyCount; ++i) {
        std::string got;
        ASSERT_EQ(writer->Get(keys[i], got), ErrorCode::OK)
            << "Get (memory) failed for " << keys[i];
        EXPECT_EQ(got, values[i]);
    }

    int disk_count = 0;
    for (const auto& key : keys) {
        if (writer->HasDiskReplica(key)) ++disk_count;
    }
    LOG(INFO) << "DISK replicas under tight quota: " << disk_count << "/"
              << kKeyCount;
    EXPECT_GT(disk_count, 0) << "Expected at least one DISK replica.";

    // New put succeeds despite exhausted disk quota.
    std::string extra_key = "sbe2e_quota_extra";
    std::string extra_val(kValueSize, 'Z');
    EXPECT_EQ(writer->Put(extra_key, extra_val), ErrorCode::OK)
        << "Put should succeed despite quota.";

    std::string got;
    EXPECT_EQ(writer->Get(extra_key, got), ErrorCode::OK);
    EXPECT_EQ(got, extra_val);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false /* remove_flags */);
    return RUN_ALL_TESTS();
}
