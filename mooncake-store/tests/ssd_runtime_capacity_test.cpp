// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated public Store API regression: no allocation hooks or hardware.
#include "real_client.h"
#include "ssd_direct_runtime.h"
#include "test_server_helpers.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>
#include <sys/stat.h>

namespace mooncake::testing {
namespace {
class ScopedEnvironment {
   public:
    explicit ScopedEnvironment(std::string name) : name_(std::move(name)) {
        if (const char* previous = std::getenv(name_.c_str()))
            previous_ = previous;
    }
    ~ScopedEnvironment() {
        if (previous_)
            setenv(name_.c_str(), previous_->c_str(), 1);
        else
            unsetenv(name_.c_str());
    }
    void Set(const char* value) {
        if (value)
            setenv(name_.c_str(), value, 1);
        else
            unsetenv(name_.c_str());
    }

   private:
    std::string name_;
    std::optional<std::string> previous_;
};

std::optional<Json::Value> ParsePeerInfo(const std::string& document) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value value;
    std::string errors;
    if (!reader->parse(document.data(), document.data() + document.size(),
                       &value, &errors) ||
        !value.isObject() || value.empty())
        return {};
    return value;
}
}  // namespace

// The same fixture name as the existing isolated RealClient runtime tests.
// All caller-owned CPU storage remains alive until both clients close, even
// when an assertion aborts a test after a successful registration.
class RealClientTest : public ::testing::Test {
   protected:
    static constexpr size_t kObjects = 256;
    static constexpr size_t kLayers = 40;
    static constexpr size_t kLayerBytes = 16;
    static constexpr size_t kObjectBytes = kLayers * kLayerBytes;
    static constexpr size_t kGuardBytes = 16;
    static constexpr size_t kFlatStride = kObjectBytes + 2 * kGuardBytes;
    static constexpr size_t kLayerStride = kLayerBytes + 2 * kGuardBytes;
    static constexpr char kCanary = '?';

    static void SetUpTestSuite() {
        google::InitGoogleLogging("ssd_runtime_capacity_test");
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        // These switches are presence-based; even "0" selects TENT. This
        // fixture specifically exercises the classic CPU/TCP direct adapter.
        SetEnvironment("MC_USE_TENT", nullptr);
        SetEnvironment("MC_USE_TEV1", nullptr);
        // Bound a baseline legacy fallback failure when its listener is
        // intentionally stopped. ReadInto has its independent 10s budget.
        SetEnvironment("MC_RPC_TIMEOUT_MS", "100");
        SetEnvironment("MC_RPC_CONNECT_TIMEOUT_MS", "100");
        SetEnvironment("MOONCAKE_OFFLOAD_SSD_PUSH", "true");
        SetEnvironment("MOONCAKE_OFFLOAD_STREAM_USE_URING", "false");
        SetEnvironment("MOONCAKE_OFFLOAD_USE_URING", "false");
        SetEnvironment("MOONCAKE_USE_URING", "false");
        SetEnvironment("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "524288");
        SetEnvironment("MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES", "65536");
        SetEnvironment("MOONCAKE_OFFLOAD_STREAM_SLOTS", "2");
        SetEnvironment("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "1");
        SetEnvironment("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
                       "bucket_storage_backend");
        SetEnvironment("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "256");
        // Exercise the production default source budget, rather than masking
        // an undersized default with a test-only larger override.
        SetEnvironment("MOONCAKE_OFFLOAD_STREAM_MAX_OUTSTANDING", nullptr);
        SetEnvironment("MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY", nullptr);
    }

    void TearDown() override {
        if (requester_) {
            EXPECT_EQ(requester_->tearDownAll(), 0);
            requester_.reset();
        }
        if (owner_) {
            EXPECT_EQ(owner_->tearDownAll(), 0);
            owner_.reset();
        }
        master_.Stop();
        if (!temporary_path_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(temporary_path_, error);
            EXPECT_FALSE(error) << error.message();
        }
        environment_.clear();
    }

    void SetEnvironment(const char* name, const char* value) {
        auto [entry, inserted] = environment_.try_emplace(name);
        if (inserted) entry->second = std::make_unique<ScopedEnvironment>(name);
        entry->second->Set(value);
    }

    bool StartMaster(bool offload) {
        const auto config = InProcMasterConfigBuilder()
                                .set_http_metadata_port(0)
                                .set_enable_offload(offload)
                                .set_default_kv_lease_ttl(1000)
                                .build();
        // Retry only a listener startup port race, never a tested read/write.
        for (size_t attempt = 0; attempt < 3; ++attempt) {
            if (master_.Start(config)) return true;
            master_.Stop();
        }
        return false;
    }

    uint64_t ReadRequests() {
        const auto info = ParsePeerInfo(owner_->get_ssd_push_peer_info());
        EXPECT_TRUE(info.has_value());
        if (!info) return 0;
        EXPECT_TRUE((*info)["read_requests"].isUInt64());
        return (*info)["read_requests"].asUInt64();
    }

    void PrepareDiskOnlyBatch() {
        char path[] = "/tmp/mooncake_ssd_capacity_XXXXXX";
        ASSERT_NE(mkdtemp(path), nullptr);
        temporary_path_ = path;
        const auto peers = temporary_path_ + "/trusted-peers";
        ASSERT_EQ(mkdir(peers.c_str(), 0700), 0);
        SetEnvironment("MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY", peers.c_str());
        ASSERT_TRUE(StartMaster(true));
        owner_ = RealClient::create();
        requester_ = RealClient::create();
        // Auto-selected placement ports avoid competing with other tests.
        ASSERT_EQ(owner_->setup_real("localhost", "P2PHANDSHAKE",
                                     16 * 1024 * 1024, 16 * 1024 * 1024, "tcp",
                                     "", master_.master_address(), nullptr, "",
                                     true, temporary_path_),
                  0);
        ASSERT_EQ(requester_->setup_real("localhost", "P2PHANDSHAKE", 0,
                                         16 * 1024 * 1024, "tcp", "",
                                         master_.master_address()),
                  0);
        ASSERT_TRUE(ParsePeerInfo(owner_->get_ssd_push_peer_info()));
        ASSERT_TRUE(ParsePeerInfo(requester_->get_ssd_push_peer_info()));

        source_.resize(kObjects);
        std::vector<std::span<const char>> values;
        for (size_t object = 0; object < kObjects; ++object) {
            keys_.push_back("capacity-" + std::to_string(object));
            for (size_t byte = 0; byte < kObjectBytes; ++byte)
                source_[object][byte] = char((object * 37 + byte * 11) % 251);
            values.emplace_back(source_[object].data(), kObjectBytes);
        }
        // Write exactly once. A fallback/retry must not hide missing keys.
        ASSERT_EQ(owner_->put_batch(keys_, values), 0);
        bool persisted = false;
        const auto persist_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < persist_deadline) {
            const auto query = owner_->batch_query(keys_);
            persisted =
                query.size() == kObjects &&
                std::all_of(query.begin(), query.end(), [](const auto& q) {
                    return q && std::any_of(
                                    q->replicas.begin(), q->replicas.end(),
                                    [](const auto& replica) {
                                        return replica.is_local_disk_replica();
                                    });
                });
            if (persisted) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_TRUE(persisted) << "All 256 keys must have LOCAL_DISK replicas";

        // The final readiness query grants a lease. Poll only replica_clear
        // while it expires; another query here would continually renew it.
        auto pending = keys_;
        size_t cleared_count = 0;
        const auto clear_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!pending.empty() &&
               std::chrono::steady_clock::now() < clear_deadline) {
            const auto cleared =
                owner_->batch_replica_clear(pending, owner_->local_hostname);
            cleared_count += cleared.size();
            std::erase_if(pending, [&](const auto& key) {
                return std::find(cleared.begin(), cleared.end(), key) !=
                       cleared.end();
            });
            if (!pending.empty())
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_TRUE(pending.empty());
        ASSERT_EQ(cleared_count, kObjects);
        const auto disk_only = requester_->batch_query(keys_);
        ASSERT_EQ(disk_only.size(), kObjects);
        for (size_t object = 0; object < kObjects; ++object) {
            ASSERT_TRUE(disk_only[object]) << object;
            ASSERT_FALSE(disk_only[object]->replicas.empty()) << object;
            for (const auto& replica : disk_only[object]->replicas)
                ASSERT_TRUE(replica.is_local_disk_replica()) << object;
        }

        flat_pool_.assign(kObjects * kFlatStride, kCanary);
        std::vector<void*> pools{flat_pool_.data()};
        std::vector<size_t> pool_sizes{flat_pool_.size()};
        for (auto& layer : layer_pools_) {
            layer.assign(kObjects * kLayerStride, kCanary);
            pools.push_back(layer.data());
            pool_sizes.push_back(layer.size());
        }
        ASSERT_EQ(requester_->register_buffers(pools, pool_sizes), 0);
    }

    void CheckFlatRestore(size_t count, uint64_t expected_read_rpcs) {
        const std::vector<std::string> keys(keys_.begin(),
                                            keys_.begin() + count);
        std::vector<void*> buffers;
        for (size_t object = 0; object < count; ++object)
            buffers.push_back(flat_pool_.data() + object * kFlatStride +
                              kGuardBytes);
        std::fill(flat_pool_.begin(), flat_pool_.end(), kCanary);
        const auto before = ReadRequests();
        const auto result = requester_->batch_get_into(
            keys, buffers, std::vector<size_t>(count, kObjectBytes), 10000);
        ASSERT_EQ(result.size(), count);
        EXPECT_EQ(ReadRequests() - before, expected_read_rpcs);
        for (size_t object = 0; object < count; ++object)
            ASSERT_EQ(result[object], int64_t(kObjectBytes)) << object;
        for (size_t object = 0; object < kObjects; ++object) {
            const auto begin = flat_pool_.begin() + object * kFlatStride;
            EXPECT_TRUE(std::all_of(begin, begin + kGuardBytes,
                                    [](char byte) { return byte == kCanary; }))
                << "leading guard, object " << object;
            EXPECT_TRUE(std::all_of(begin + kGuardBytes + kObjectBytes,
                                    begin + kFlatStride,
                                    [](char byte) { return byte == kCanary; }))
                << "trailing guard, object " << object;
            if (object < count)
                EXPECT_TRUE(std::equal(source_[object].begin(),
                                       source_[object].end(),
                                       begin + kGuardBytes))
                    << "payload, object " << object;
            else
                EXPECT_TRUE(
                    std::all_of(begin, begin + kFlatStride,
                                [](char byte) { return byte == kCanary; }))
                    << "unrequested object " << object;
        }
    }

    void CheckNestedRestore(size_t count) {
        const std::vector<std::string> keys(keys_.begin(),
                                            keys_.begin() + count);
        std::vector<std::vector<void*>> buffers(count);
        std::vector<std::vector<size_t>> sizes(
            count, std::vector<size_t>(kLayers, kLayerBytes));
        for (auto& layer : layer_pools_)
            std::fill(layer.begin(), layer.end(), kCanary);
        for (size_t object = 0; object < count; ++object)
            for (auto& layer : layer_pools_)
                buffers[object].push_back(layer.data() + object * kLayerStride +
                                          kGuardBytes);
        const auto before = ReadRequests();
        const auto result = requester_->batch_get_into_multi_buffers(
            keys, buffers, sizes, false, 10000);
        ASSERT_EQ(result.size(), count);
        EXPECT_EQ(ReadRequests() - before, 1u);
        for (size_t object = 0; object < count; ++object)
            ASSERT_EQ(result[object], int(kObjectBytes)) << object;
        for (size_t layer = 0; layer < kLayers; ++layer)
            for (size_t object = 0; object < kObjects; ++object) {
                const auto begin =
                    layer_pools_[layer].begin() + object * kLayerStride;
                EXPECT_TRUE(
                    std::all_of(begin, begin + kGuardBytes,
                                [](char byte) { return byte == kCanary; }))
                    << "leading guard, layer " << layer << ", object "
                    << object;
                EXPECT_TRUE(std::all_of(
                    begin + kGuardBytes + kLayerBytes, begin + kLayerStride,
                    [](char byte) { return byte == kCanary; }))
                    << "trailing guard, layer " << layer << ", object "
                    << object;
                if (object < count)
                    EXPECT_TRUE(std::equal(
                        source_[object].begin() + layer * kLayerBytes,
                        source_[object].begin() + (layer + 1) * kLayerBytes,
                        begin + kGuardBytes))
                        << "payload, layer " << layer << ", object " << object;
                else
                    EXPECT_TRUE(
                        std::all_of(begin, begin + kLayerStride,
                                    [](char byte) { return byte == kCanary; }))
                        << "unrequested layer " << layer << ", object "
                        << object;
            }
    }

    InProcMaster master_;
    std::map<std::string, std::unique_ptr<ScopedEnvironment>> environment_;
    std::string temporary_path_;
    std::vector<std::unique_ptr<std::array<char, 64>>> independent_pools_;
    std::vector<std::array<char, kObjectBytes>> source_;
    std::vector<std::string> keys_;
    std::vector<char> flat_pool_;
    std::array<std::vector<char>, kLayers> layer_pools_;
    std::shared_ptr<RealClient> owner_, requester_;
};

TEST_F(RealClientTest, PublicBatchRegistrationSupportsMoreThan1024CpuPools) {
    ASSERT_TRUE(StartMaster(false));
    requester_ = RealClient::create();
    ASSERT_EQ(
        requester_->setup_real("localhost", "P2PHANDSHAKE", 0, 1024 * 1024,
                               "tcp", "", master_.master_address()),
        0);
    const auto before = ParsePeerInfo(requester_->get_ssd_push_peer_info());
    ASSERT_TRUE(before);
    ASSERT_TRUE((*before)["registered_l2_pools"].isUInt64());
    std::vector<void*> pointers;
    std::vector<size_t> sizes;
    for (size_t pool = 0; pool < 1025; ++pool) {
        independent_pools_.push_back(std::make_unique<std::array<char, 64>>());
        pointers.push_back(independent_pools_.back()->data());
        sizes.push_back(independent_pools_.back()->size());
    }
    ASSERT_EQ(requester_->register_buffers(pointers, sizes), 0);
    const auto after = ParsePeerInfo(requester_->get_ssd_push_peer_info());
    ASSERT_TRUE(after);
    EXPECT_EQ((*after)["registered_l2_pools"].asUInt64(),
              (*before)["registered_l2_pools"].asUInt64() + 1025);
}

TEST_F(RealClientTest,
       PublicFluentBatchShapesUseOneReadRpcFor128And256Objects) {
    PrepareDiskOnlyBatch();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(owner_->offload_rpc_server_);
    // A rejected direct preflight followed by successful legacy restore would
    // also increment ReadInto once. Disable that independent legacy listener
    // so a successful public call proves the complete owner streaming path.
    // SsdDirectRuntime owns a separate ReadInto control server.
    owner_->offload_rpc_server_->stop();
    for (const size_t count : {size_t{128}, kObjects}) {
        SCOPED_TRACE("objects=" + std::to_string(count));
        CheckFlatRestore(count, 1);
        ASSERT_FALSE(HasFatalFailure());
        CheckNestedRestore(count);
        ASSERT_FALSE(HasFatalFailure());
    }

    // Check old PeerInfo compatibility only after the four automatic-bootstrap
    // public reads, so explicit configuration cannot hide bootstrap failures.
    auto legacy = ParsePeerInfo(owner_->get_ssd_push_peer_info());
    ASSERT_TRUE(legacy);
    legacy->removeMember("max_batch_objects");
    Json::StreamWriterBuilder writer;
    writer["indentation"] = "";
    ASSERT_EQ(requester_->configure_ssd_push_peers(
                  {Json::writeString(writer, *legacy)}),
              0);
    std::fill(flat_pool_.begin(), flat_pool_.end(), kCanary);
    std::vector<ssd_offload::SsdRuntimeObject> objects;
    for (size_t object = 0; object < 128; ++object)
        objects.push_back(
            {keys_[object],
             kObjectBytes,
             0,
             {{flat_pool_.data() + object * kFlatStride + kGuardBytes,
               kObjectBytes}}});
    const auto before = ReadRequests();
    ASSERT_TRUE(requester_->ssd_direct_);
    // Missing capability means the historic 64-object limit. The adapter must
    // return nullopt before submission, permitting a caller's legacy path,
    // without entering unknown-attempt recovery or issuing ReadInto.
    EXPECT_FALSE(requester_->ssd_direct_
                     ->Read(owner_->local_rpc_addr, objects,
                            std::chrono::milliseconds(10000))
                     .has_value());
    EXPECT_EQ(ReadRequests(), before);
    EXPECT_TRUE(std::all_of(flat_pool_.begin(), flat_pool_.end(),
                            [](char byte) { return byte == kCanary; }));
    const auto info = ParsePeerInfo(requester_->get_ssd_push_peer_info());
    ASSERT_TRUE(info);
    EXPECT_EQ((*info)["pending_requests"].asUInt64(), 0u);
}
}  // namespace mooncake::testing
