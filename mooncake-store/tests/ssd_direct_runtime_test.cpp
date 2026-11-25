// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// End-to-end TCP SSD Owner-Push runtime & RealClient regression tests.
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "real_client.h"
#include "ssd_direct_runtime.h"
#include "ssd_peer_directory.h"
#include "test_server_helpers.h"

namespace mooncake {
class RealClientShutdownTestPeer {
   public:
    static RpcDrainGuard& OffloadDrain(RealClient& c) {
        return c.offload_rpc_drain_;
    }
};
}  // namespace mooncake

namespace mooncake::testing {
namespace {

class ScopedEnv {
   public:
    explicit ScopedEnv(std::string name) : name_(std::move(name)) {
        if (const char* prev = std::getenv(name_.c_str())) previous_ = prev;
    }
    ~ScopedEnv() {
        if (previous_) {
            setenv(name_.c_str(), previous_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }
    void Set(const char* value) {
        if (value) {
            setenv(name_.c_str(), value, 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> previous_;
};

std::optional<Json::Value> ParseJson(const std::string& doc) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value value;
    std::string errors;
    if (!reader->parse(doc.data(), doc.data() + doc.size(), &value, &errors) ||
        !value.isObject() || value.empty()) {
        return std::nullopt;
    }
    return value;
}

}  // namespace

// Uses RealClient's existing friend fixture name.
class RealClientTest : public ::testing::Test {
   protected:
    static constexpr size_t kObjects = 128;
    static constexpr size_t kLayers = 8;
    static constexpr size_t kLayerBytes = 128;
    static constexpr size_t kObjectBytes = kLayers * kLayerBytes;
    static constexpr size_t kGuardBytes = 16;
    static constexpr size_t kFlatStride = kObjectBytes + 2 * kGuardBytes;
    static constexpr size_t kLayerStride = kLayerBytes + 2 * kGuardBytes;
    static constexpr char kCanary = '?';

    static void SetUpTestSuite() {
        google::InitGoogleLogging("ssd_direct_runtime_test");
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        SetEnv("MC_USE_TENT", nullptr);
        SetEnv("MC_USE_TEV1", nullptr);
        SetEnv("MOONCAKE_OFFLOAD_SSD_PUSH", "true");
        SetEnv("MOONCAKE_OFFLOAD_STREAM_USE_URING", "false");
        SetEnv("MOONCAKE_OFFLOAD_USE_URING", "false");
        SetEnv("MOONCAKE_USE_URING", "false");
        SetEnv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "524288");
        SetEnv("MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES", "65536");
        SetEnv("MOONCAKE_OFFLOAD_STREAM_SLOTS", "2");
        SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "1");
        SetEnv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
               "bucket_storage_backend");
        SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "128");
        SetEnv("MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY", nullptr);
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
        if (!temp_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(temp_dir_, ec);
        }
        envs_.clear();
    }

    void SetEnv(const char* name, const char* value) {
        auto [it, inserted] = envs_.try_emplace(name);
        if (inserted) it->second = std::make_unique<ScopedEnv>(name);
        it->second->Set(value);
    }

    bool StartMaster(bool offload) {
        const auto config = InProcMasterConfigBuilder()
                                .set_http_metadata_port(0)
                                .set_enable_offload(offload)
                                .set_default_kv_lease_ttl(1000)
                                .build();
        for (size_t attempt = 0; attempt < 3; ++attempt) {
            if (master_.Start(config)) return true;
            master_.Stop();
        }
        return false;
    }

    uint64_t OwnerReadRequests() {
        auto stats = ParseJson(owner_->get_ssd_push_stats());
        EXPECT_TRUE(stats.has_value());
        return stats ? (*stats)["read_requests"].asUInt64() : 0;
    }

    static uint16_t AllocateClosedLoopbackPort() {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return 0;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            close(fd);
            return 0;
        }
        socklen_t len = sizeof(addr);
        uint16_t port = 0;
        if (getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
            port = ntohs(addr.sin_port);
        }
        close(fd);
        return port;
    }

    void PrepareDiskOnlyBatch(bool use_peer_dir) {
        char path[] = "/tmp/mooncake_ssd_runtime_XXXXXX";
        ASSERT_NE(mkdtemp(path), nullptr);
        temp_dir_ = path;
        if (use_peer_dir) {
            peer_dir_ = temp_dir_ + "/trusted-peers";
            ASSERT_EQ(mkdir(peer_dir_.c_str(), 0700), 0);
            SetEnv("MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY", peer_dir_.c_str());
        }
        ASSERT_TRUE(StartMaster(true));
        owner_ = RealClient::create();
        requester_ = RealClient::create();
        ASSERT_EQ(owner_->setup_real("127.0.0.1", "P2PHANDSHAKE",
                                     16 * 1024 * 1024, 16 * 1024 * 1024, "tcp",
                                     "", master_.master_address(), nullptr, "",
                                     true, temp_dir_),
                  0);
        ASSERT_EQ(requester_->setup_real("127.0.0.1", "P2PHANDSHAKE", 0,
                                         16 * 1024 * 1024, "tcp", "",
                                         master_.master_address()),
                  0);

        source_.resize(kObjects);
        std::vector<std::span<const char>> values;
        for (size_t obj = 0; obj < kObjects; ++obj) {
            keys_.push_back("rt-key-" + std::to_string(obj));
            for (size_t b = 0; b < kObjectBytes; ++b) {
                source_[obj][b] = static_cast<char>((obj * 29 + b * 13) % 251);
            }
            values.emplace_back(source_[obj].data(), kObjectBytes);
        }
        ASSERT_EQ(owner_->put_batch(keys_, values), 0);

        bool persisted = false;
        const auto persist_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < persist_deadline) {
            const auto query = owner_->batch_query(keys_);
            persisted =
                query.size() == kObjects &&
                std::all_of(query.begin(), query.end(), [](const auto& q) {
                    return q &&
                           std::any_of(q->replicas.begin(), q->replicas.end(),
                                       [](const auto& r) {
                                           return r.is_local_disk_replica();
                                       });
                });
            if (persisted) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_TRUE(persisted);

        auto pending = keys_;
        const auto clear_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!pending.empty() &&
               std::chrono::steady_clock::now() < clear_deadline) {
            const auto cleared =
                owner_->batch_replica_clear(pending, owner_->local_hostname);
            std::erase_if(pending, [&](const auto& key) {
                return std::find(cleared.begin(), cleared.end(), key) !=
                       cleared.end();
            });
            if (!pending.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
        ASSERT_TRUE(pending.empty());

        flat_pool_.assign(kObjects * kFlatStride, kCanary);
        ASSERT_EQ(
            requester_->register_buffer(flat_pool_.data(), flat_pool_.size()),
            0);
        for (auto& layer : layer_pools_) {
            layer.assign(kObjects * kLayerStride, kCanary);
            ASSERT_EQ(requester_->register_buffer(layer.data(), layer.size()),
                      0);
        }
    }

    auto& IncompleteRegs(RealClient& c) {
        return c.incomplete_buffer_registrations_;
    }
    auto& ActiveRegs(RealClient& c) { return c.active_buffer_registrations_; }
    auto& OffloadDrain(RealClient& c) {
        return RealClientShutdownTestPeer::OffloadDrain(c);
    }
    auto ResolveWritable(const RealClient& c, void* ptr) {
        return c.resolve_writable_buffer_region(ptr);
    }
    auto RuntimeOf(RealClient& c) { return c.snapshot_ssd_direct(); }
    tl::expected<int64_t, ErrorCode> RangedReadDirect(RealClient& c,
                                                      const std::string& key,
                                                      void* buf, size_t dst_off,
                                                      size_t src_off,
                                                      size_t len) {
        return c.get_into_range_internal(key, buf, dst_off, src_off, len, false,
                                         false);
    }
    std::string OwnerLegacyRpc() const { return owner_->local_rpc_addr; }

    InProcMaster master_;
    std::shared_ptr<RealClient> owner_;
    std::shared_ptr<RealClient> requester_;
    std::string temp_dir_;
    std::string peer_dir_;
    std::map<std::string, std::unique_ptr<ScopedEnv>> envs_;
    std::vector<std::string> keys_;
    std::vector<std::array<char, kObjectBytes>> source_;
    std::vector<char> flat_pool_;
    std::array<std::vector<char>, kLayers> layer_pools_;
};

TEST_F(RealClientTest,
       EndToEndTcpOwnerPushBatchMultiLayerRangedAndUnregisteredFallback) {
    PrepareDiskOnlyBatch(/*use_peer_dir=*/true);

    const std::string owner_json = owner_->get_ssd_push_peer_info();
    const std::string req_json = requester_->get_ssd_push_peer_info();
    auto owner_info = ParseJson(owner_json);
    auto req_info = ParseJson(req_json);
    auto owner_stats = ParseJson(owner_->get_ssd_push_stats());
    auto req_stats = ParseJson(requester_->get_ssd_push_stats());
    ASSERT_TRUE(owner_info.has_value());
    ASSERT_TRUE(req_info.has_value());
    ASSERT_TRUE(owner_stats.has_value());
    ASSERT_TRUE(req_stats.has_value());

    // Bootstrap documents carry canonical schema without metrics; stats carry
    // metrics without HMAC secrets.
    EXPECT_FALSE(owner_info->isMember("read_requests"));
    EXPECT_FALSE(owner_info->isMember("pending_requests"));
    EXPECT_FALSE(owner_stats->isMember("credential_high"));
    EXPECT_FALSE(owner_stats->isMember("credential_low"));

    // Owner binds private host and advertises configured streaming slot_bytes;
    // pure requester skips starting an RPC server (control_port == 0).
    EXPECT_TRUE((*owner_stats)["can_read_ssd"].asBool());
    EXPECT_EQ((*owner_stats)["control_host"].asString(), "127.0.0.1");
    EXPECT_GT((*owner_stats)["control_port"].asUInt64(), 0u);
    EXPECT_EQ((*owner_stats)["preferred_chunk_bytes"].asUInt64(), 65536u);
    EXPECT_FALSE((*req_stats)["can_read_ssd"].asBool());
    EXPECT_EQ((*req_stats)["control_port"].asUInt64(), 0u);

    // Negative timeout must be rejected explicitly with INVALID_PARAMS.
    auto neg = requester_->batch_get_into(
        {keys_[0]}, {flat_pool_.data() + kGuardBytes}, {kObjectBytes}, -1);
    ASSERT_EQ(neg.size(), 1u);
    EXPECT_EQ(neg[0], static_cast<int64_t>(ErrorCode::INVALID_PARAMS));

    // Non-object JSON must fail closed without throwing Json::LogicError.
    EXPECT_EQ(requester_->configure_ssd_push_peers({"[]"}), -1);
    EXPECT_EQ(requester_->configure_ssd_push_peers({"123"}), -1);

    // Cluster peer list including self must deduplicate cleanly.
    EXPECT_EQ(requester_->configure_ssd_push_peers({owner_json, req_json}), 0);
    EXPECT_EQ(owner_->configure_ssd_push_peers({owner_json, req_json}), 0);

    // 1. 128-key non-contiguous canary-guarded flat batch -> single push RPC.
    std::vector<void*> flat_ptrs;
    flat_ptrs.reserve(kObjects);
    for (size_t i = 0; i < kObjects; ++i) {
        flat_ptrs.push_back(flat_pool_.data() + i * kFlatStride + kGuardBytes);
    }
    const uint64_t before_flat = OwnerReadRequests();
    auto flat_res = requester_->batch_get_into(
        keys_, flat_ptrs, std::vector<size_t>(kObjects, kObjectBytes), 10000);
    ASSERT_EQ(flat_res.size(), kObjects);
    EXPECT_EQ(OwnerReadRequests() - before_flat, 1u);
    for (size_t i = 0; i < kObjects; ++i) {
        ASSERT_EQ(flat_res[i], static_cast<int64_t>(kObjectBytes)) << i;
        const auto* base = flat_pool_.data() + i * kFlatStride;
        EXPECT_TRUE(std::all_of(base, base + kGuardBytes,
                                [](char c) { return c == kCanary; }));
        EXPECT_TRUE(std::equal(source_[i].begin(), source_[i].end(),
                               base + kGuardBytes));
        EXPECT_TRUE(std::all_of(base + kGuardBytes + kObjectBytes,
                                base + kFlatStride,
                                [](char c) { return c == kCanary; }));
    }

    // 2. 128-key x 8-layer multi-buffer batch -> single push RPC (no broken
    // sparse-TCP fallback!).
    std::vector<std::vector<void*>> multi_ptrs(kObjects);
    std::vector<std::vector<size_t>> multi_sizes(
        kObjects, std::vector<size_t>(kLayers, kLayerBytes));
    for (size_t i = 0; i < kObjects; ++i) {
        for (size_t l = 0; l < kLayers; ++l) {
            multi_ptrs[i].push_back(layer_pools_[l].data() + i * kLayerStride +
                                    kGuardBytes);
        }
    }
    const uint64_t before_multi = OwnerReadRequests();
    auto multi_res = requester_->batch_get_into_multi_buffers(
        keys_, multi_ptrs, multi_sizes, false, 10000);
    ASSERT_EQ(multi_res.size(), kObjects);
    EXPECT_EQ(OwnerReadRequests() - before_multi, 1u);
    for (size_t i = 0; i < kObjects; ++i) {
        ASSERT_EQ(multi_res[i], static_cast<int>(kObjectBytes)) << i;
        for (size_t l = 0; l < kLayers; ++l) {
            const auto* base = layer_pools_[l].data() + i * kLayerStride;
            EXPECT_TRUE(std::equal(source_[i].begin() + l * kLayerBytes,
                                   source_[i].begin() + (l + 1) * kLayerBytes,
                                   base + kGuardBytes));
        }
    }

    // 3. Partial ranged read into a registered CPU pool uses Owner-Push.
    std::fill(flat_pool_.begin(), flat_pool_.end(), kCanary);
    const uint64_t before_ranged = OwnerReadRequests();
    auto reg_ranged =
        requester_->get_into_ranges({flat_pool_.data()}, {{keys_[0]}},
                                    {{{kGuardBytes}}}, {{{64}}}, {{{256}}});
    ASSERT_EQ(reg_ranged.size(), 1u);
    EXPECT_EQ(reg_ranged[0][0][0], 256);
    EXPECT_EQ(OwnerReadRequests() - before_ranged, 1u);
    EXPECT_TRUE(std::equal(source_[0].begin() + 64, source_[0].begin() + 320,
                           flat_pool_.data() + kGuardBytes));

    // 4. P1-7 regression: partial ranged reads into destinations outside
    // imported fixed-L2 pools (both a setup-time local client buffer via public
    // get_into_ranges and an unregistered heap buffer via execute_ranged_read)
    // must bypass Owner-Push and succeed cleanly via partial_disk_read!
    auto local_buf = requester_->allocate_client_buffer(512);
    ASSERT_TRUE(local_buf.has_value());
    std::memset(local_buf->ptr(), kCanary, 512);
    const uint64_t before_local = OwnerReadRequests();
    auto local_ranged = requester_->get_into_ranges(
        {local_buf->ptr()}, {{keys_[1]}}, {{{32}}}, {{{128}}}, {{{256}}});
    ASSERT_EQ(local_ranged.size(), 1u);
    EXPECT_EQ(local_ranged[0][0][0], 256);
    EXPECT_EQ(OwnerReadRequests() - before_local, 0u);
    EXPECT_TRUE(std::equal(source_[1].begin() + 128, source_[1].begin() + 384,
                           static_cast<char*>(local_buf->ptr()) + 32));

    std::vector<char> unreg_buf(512, kCanary);
    const uint64_t before_unreg = OwnerReadRequests();
    auto unreg_ranged =
        RangedReadDirect(*requester_, keys_[1], unreg_buf.data(), 32, 128, 256);
    ASSERT_TRUE(unreg_ranged.has_value());
    EXPECT_EQ(*unreg_ranged, 256);
    EXPECT_EQ(OwnerReadRequests() - before_unreg, 0u);
    EXPECT_TRUE(std::equal(source_[1].begin() + 128, source_[1].begin() + 384,
                           unreg_buf.data() + 32));
}

TEST_F(RealClientTest, CrashLeftoverPeerRecordDoesNotPoisonDiscovery) {
    PrepareDiskOnlyBatch(/*use_peer_dir=*/true);

    // Materialize a crash-leftover peer record (valid 0600 JSON + unlocked 0600
    // sidecar lock file) for the SAME legacy_endpoint without calling fork() in
    // a multi-threaded process.
    ssd_offload::SsdFixedL2Peer fixed{
        {0xDEAD, 0xBEEF, 0x9999}, "default", "127.0.0.1:9999", {11, 22}};
    ssd_offload::SsdPeerDirectoryEntry stale{fixed,
                                             {fixed.identity,
                                              fixed.tenant_id,
                                              OwnerLegacyRpc(),
                                              "127.0.0.1",
                                              12345,
                                              fixed.transport_endpoint,
                                              {1,
                                               fixed.identity,
                                               15,
                                               512,
                                               1024ULL * 1024 * 1024,
                                               65536,
                                               1024ULL * 1024 * 1024,
                                               65536,
                                               {"tcp"}}},
                                             "tcp",
                                             true};
    std::ostringstream stem;
    stem << "ssd-peer-" << std::hex << std::setfill('0') << std::setw(16)
         << fixed.identity.client_id_high << '-' << std::setw(16)
         << fixed.identity.client_id_low << '-' << std::setw(16)
         << fixed.identity.boot_epoch;
    const std::string stale_json = peer_dir_ + "/" + stem.str() + ".json";
    const std::string stale_lock = peer_dir_ + "/." + stem.str() + ".lock";
    {
        std::ofstream out(stale_json, std::ios::trunc);
        out << ssd_offload::EncodeSsdPeerDocument(stale);
    }
    ASSERT_EQ(chmod(stale_json.c_str(), 0600), 0);
    {
        std::ofstream out(stale_lock, std::ios::trunc);
    }
    ASSERT_EQ(chmod(stale_lock.c_str(), 0600), 0);

    // Discovery via peer directory must ignore the unlocked crashed file and
    // succeed against the live owner over TCP Owner-Push.
    std::vector<void*> ptrs{flat_pool_.data() + kGuardBytes};
    const uint64_t before = OwnerReadRequests();
    auto res =
        requester_->batch_get_into({keys_[0]}, ptrs, {kObjectBytes}, 10000);
    ASSERT_EQ(res.size(), 1u);
    EXPECT_EQ(res[0], static_cast<int64_t>(kObjectBytes));
    EXPECT_EQ(OwnerReadRequests() - before, 1u);
    EXPECT_TRUE(std::equal(source_[0].begin(), source_[0].end(),
                           flat_pool_.data() + kGuardBytes));
}

TEST_F(RealClientTest,
       TeardownObjectHasLeaseResumesRpcDrainGuardForRecoveryAndRetry) {
    ASSERT_TRUE(StartMaster(false));
    requester_ = RealClient::create();
    ASSERT_EQ(
        requester_->setup_real("127.0.0.1", "P2PHANDSHAKE", 0, 1024 * 1024,
                               "tcp", "", master_.master_address()),
        0);
    std::array<char, 4096> pool{};
    ASSERT_EQ(requester_->register_buffer(pool.data(), pool.size()), 0);

    // Simulate an in-flight / incomplete registration so
    // resolve_writable_buffer_region skips it, unregister_buffer rejects
    // active registrations, and tearDownAll() returns OBJECT_HAS_LEASE.
    ActiveRegs(*requester_).insert(pool.data());
    IncompleteRegs(*requester_).insert(pool.data());
    EXPECT_FALSE(ResolveWritable(*requester_, pool.data()).has_value());
    EXPECT_EQ(requester_->unregister_buffer(pool.data()),
              static_cast<int>(ErrorCode::OBJECT_HAS_LEASE));
    EXPECT_EQ(requester_->tearDownAll(),
              static_cast<int>(ErrorCode::OBJECT_HAS_LEASE));
    ActiveRegs(*requester_).erase(pool.data());
    // Incomplete registration still skipped by resolve_writable_buffer_region.
    EXPECT_FALSE(ResolveWritable(*requester_, pool.data()).has_value());
    EXPECT_EQ(requester_->tearDownAll(),
              static_cast<int>(ErrorCode::OBJECT_HAS_LEASE));

    // P1 #3 regression: offload_rpc_drain_ MUST remain open (never closed
    // before committing to teardown) so peer-info, recovery, offload RPCs, and
    // unregister_buffer still succeed after OBJECT_HAS_LEASE!
    {
        RpcDrainGuard::ScopedCall inflight(OffloadDrain(*requester_));
        EXPECT_TRUE(inflight.ok());
    }
    EXPECT_NE(requester_->get_ssd_push_peer_info(), "{}");
    EXPECT_EQ(requester_->recover_ssd_push(), 0);
    EXPECT_EQ(requester_->unregister_buffer(pool.data()), 0);
    EXPECT_TRUE(IncompleteRegs(*requester_).empty());

    // Retrying tearDownAll() must now complete cleanly.
    EXPECT_EQ(requester_->tearDownAll(), 0);
    requester_.reset();
}

TEST_F(RealClientTest, BoundedRecoveryReturnsPromptlyAgainstUnreachableOwner) {
    ASSERT_TRUE(StartMaster(false));
    requester_ = RealClient::create();
    ASSERT_EQ(
        requester_->setup_real("127.0.0.1", "P2PHANDSHAKE", 0, 1024 * 1024,
                               "tcp", "", master_.master_address()),
        0);
    std::array<char, 4096> pool{};
    std::array<char, 1024> unregistered{};
    ASSERT_EQ(requester_->register_buffer(pool.data(), pool.size()), 0);

    // Configure a peer pointing at a closed ephemeral loopback port; Read must
    // fail within bounded recovery time rather than hanging indefinitely.
    const uint16_t closed_port = AllocateClosedLoopbackPort();
    ASSERT_NE(closed_port, 0);
    const std::string closed_endpoint =
        "127.0.0.1:" + std::to_string(closed_port);
    ssd_offload::SsdFixedL2Peer fake_fixed{
        {99, 100, 1}, "default", closed_endpoint, {0x1111, 0x2222}};
    ssd_offload::SsdPeerDirectoryEntry fake_entry{
        fake_fixed,
        {fake_fixed.identity,
         fake_fixed.tenant_id,
         closed_endpoint,
         "127.0.0.1",
         closed_port,
         fake_fixed.transport_endpoint,
         {1,
          fake_fixed.identity,
          15,
          64,
          1024ULL * 1024 * 1024,
          65536,
          1024ULL * 1024 * 1024,
          65536,
          {"tcp"}}},
        "tcp",
        true};
    // Duplicate legacy_rpc_endpoint across distinct readable peers must fail
    // closed atomically without mutating configured peers.
    auto dup_legacy = fake_entry;
    dup_legacy.fixed.identity.client_id_low += 1;
    dup_legacy.route.owner = dup_legacy.fixed.identity;
    dup_legacy.route.capability.owner = dup_legacy.fixed.identity;
    EXPECT_EQ(requester_->configure_ssd_push_peers(
                  {ssd_offload::EncodeSsdPeerDocument(fake_entry),
                   ssd_offload::EncodeSsdPeerDocument(dup_legacy)}),
              -1);
    ASSERT_EQ(requester_->configure_ssd_push_peers(
                  {ssd_offload::EncodeSsdPeerDocument(fake_entry)}),
              0);

    auto runtime = RuntimeOf(*requester_);
    ASSERT_TRUE(runtime);
    // Unregistered buffer must fall back immediately (std::nullopt) before peer
    // resolution or submission.
    EXPECT_FALSE(
        runtime
            ->Read(closed_endpoint,
                   {{"missing", 1024, 0, {{unregistered.data(), 1024}}}},
                   std::chrono::milliseconds(300))
            .has_value());
    const auto start = std::chrono::steady_clock::now();
    auto res = runtime->Read(closed_endpoint,
                             {{"missing", 1024, 0, {{pool.data(), 1024}}}},
                             std::chrono::milliseconds(300));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    ASSERT_TRUE(res.has_value());
    EXPECT_FALSE(res->has_value());
    EXPECT_EQ(res->error(), ErrorCode::TRANSFER_FAIL);
    EXPECT_LT(elapsed.count(), 5000);

    // While pending attempt exists, HasPending blocks reuse across ALL Put/Get
    // /Upsert/Session buffer APIs and unregister_buffer returns
    // OBJECT_HAS_LEASE.
    EXPECT_TRUE(runtime->HasPending(pool.data(), 1024));
    EXPECT_EQ(requester_->get_into("blocked-get-single", pool.data(), 1024),
              static_cast<int64_t>(ErrorCode::INVALID_PARAMS));
    EXPECT_EQ(requester_->put_from("blocked-put", pool.data(), 1024),
              static_cast<int>(ErrorCode::INVALID_PARAMS));
    EXPECT_EQ(
        requester_->put_from_with_metadata("blocked-meta-put", pool.data(),
                                           pool.data() + 2048, 1024, 64),
        static_cast<int>(ErrorCode::INVALID_PARAMS));
    EXPECT_EQ(
        requester_->upsert_from("blocked-upsert-single", pool.data(), 1024),
        static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto bupsert = requester_->batch_upsert_from({"blocked-bupsert"},
                                                 {pool.data()}, {1024});
    ASSERT_EQ(bupsert.size(), 1u);
    EXPECT_EQ(bupsert[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto bput =
        requester_->batch_put_from({"blocked-put"}, {pool.data()}, {1024});
    ASSERT_EQ(bput.size(), 1u);
    EXPECT_EQ(bput[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto bmput = requester_->batch_put_from_multi_buffers(
        {"blocked-mput"}, {{pool.data()}}, {{1024}});
    ASSERT_EQ(bmput.size(), 1u);
    EXPECT_EQ(bmput[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto bup = requester_->batch_upsert_from_multi_buffers(
        {"blocked-upsert"}, {{pool.data()}}, {{1024}});
    ASSERT_EQ(bup.size(), 1u);
    EXPECT_EQ(bup[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto brget = requester_->batch_get_into_multi_buffer_ranges(
        {"blocked-rget"}, {{pool.data()}}, {{1024}}, {{0}});
    ASSERT_EQ(brget.size(), 1u);
    EXPECT_EQ(brget[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto brget_mismatch = requester_->batch_get_into_multi_buffer_ranges(
        {"mismatch-rget"}, {{pool.data(), pool.data() + 1024}}, {{1024}},
        {{0}});
    ASSERT_EQ(brget_mismatch.size(), 1u);
    EXPECT_EQ(brget_mismatch[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto brput = requester_->batch_put_from_multi_buffer_ranges(
        {"blocked-rput"}, {{pool.data()}}, {{1024}}, {{0}});
    ASSERT_EQ(brput.size(), 1u);
    EXPECT_EQ(brput[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto brput_mismatch = requester_->batch_put_from_multi_buffer_ranges(
        {"mismatch-rput"}, {{pool.data(), pool.data() + 1024}}, {{1024}},
        {{0}});
    ASSERT_EQ(brput_mismatch.size(), 1u);
    EXPECT_EQ(brput_mismatch[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto bget =
        requester_->batch_get_into({"blocked-get"}, {pool.data()}, {1024});
    ASSERT_EQ(bget.size(), 1u);
    EXPECT_EQ(bget[0], static_cast<int64_t>(ErrorCode::INVALID_PARAMS));
    auto mget = requester_->batch_get_into_multi_buffers(
        {"blocked-mget"}, {{pool.data()}}, {{1024}}, false);
    ASSERT_EQ(mget.size(), 1u);
    EXPECT_EQ(mget[0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    auto granges = requester_->get_into_ranges({pool.data()}, {{"blocked-rg"}},
                                               {{{0}}}, {{{0}}}, {{{1024}}});
    ASSERT_EQ(granges.size(), 1u);
    ASSERT_EQ(granges[0].size(), 1u);
    ASSERT_EQ(granges[0][0].size(), 1u);
    EXPECT_EQ(granges[0][0][0], static_cast<int>(ErrorCode::INVALID_PARAMS));
    EXPECT_EQ(requester_->unregister_buffer(pool.data()),
              static_cast<int>(ErrorCode::OBJECT_HAS_LEASE));
    // Retryable tearDownAll reports OBJECT_HAS_LEASE WITHOUT closing the
    // requester or offload_rpc_drain_.
    EXPECT_EQ(requester_->tearDownAll(),
              static_cast<int>(ErrorCode::OBJECT_HAS_LEASE));
    {
        RpcDrainGuard::ScopedCall inflight(OffloadDrain(*requester_));
        EXPECT_TRUE(inflight.ok());
    }
    EXPECT_EQ(requester_->recover_ssd_push(), 1);
    EXPECT_NE(requester_->get_ssd_push_peer_info(), "{}");
    // Destructor teardown is non-retryable: quarantines un-drained region while
    // completing full thread/server/segment shutdown without std::terminate().
    runtime.reset();
    requester_.reset();
}

}  // namespace mooncake::testing
