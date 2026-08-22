/**
 * @file p2p_client_integration_test.cpp
 * @brief Integration tests for P2PClientService + P2PMasterService.
 *
 * Launches an in-process P2P master, creates one or more P2PClientService
 * instances, and exercises the client→master→client round-trip for the main
 * P2P operations (Put, Get, Query, IsExist, BatchPut, BatchGet, etc.).
 *
 * Transport is "tcp" with loopback; no dedicated RDMA hardware is required.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "p2p_client_metric.h"
#include "p2p_client_service.h"
#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

// ============================================================================
// Test fixture
// ============================================================================

class P2PClientIntegrationTest : public ::testing::Test {
   protected:
    // --- Factory helpers ---

    static std::shared_ptr<P2PClientService> CreateP2PClient(
        const std::string& host_name, uint32_t rpc_port = 0,
        const std::string& local_transfer_mode = "te",
        TransferDirectionMode transfer_direction_mode =
            TransferDirectionMode::REVERSE,
        size_t te_async_poll_worker_num = 32) {
        if (rpc_port == 0) rpc_port = getFreeTcpPort();

        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port);
        if (local_transfer_mode == "te") {
            config.local_transfer_mode = LocalTransferMode::TE;
        } else {
            config.local_transfer_mode = LocalTransferMode::MEMCPY;
        }
        config.transfer_direction_mode = transfer_direction_mode;
        config.te_async_poll_worker_num = te_async_poll_worker_num;

        config.async_sender_thread_count = 0;

        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);

        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK)
            << "Init failed: " << static_cast<int>(err);

        return client;
    }

    // --- Suite-level setup / teardown ---

    static void SetUpTestSuite() {
        google::InitGoogleLogging("P2PClientIntegrationTest");
        FLAGS_logtostderr = 1;

        // 1. Start in-process P2P master
        ASSERT_TRUE(master_.Start()) << "Failed to start P2P master";
        master_address_ = master_.master_address();
        LOG(INFO) << "P2P master started at " << master_address_;

        // 2. Create two clients
        client_ = CreateP2PClient("localhost:18801");
        ASSERT_NE(client_, nullptr);
        client2_ = CreateP2PClient("localhost:18802");
        ASSERT_NE(client2_, nullptr);
        LOG(INFO) << "Two P2P clients created and registered successfully";
    }

    static void TearDownTestSuite() {
        client2_.reset();
        client_.reset();
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    // Shared across all tests in this suite
    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client_;
    static std::shared_ptr<P2PClientService> client2_;
};

// Static member definitions
InProcP2PMaster P2PClientIntegrationTest::master_;
std::string P2PClientIntegrationTest::master_address_;
std::shared_ptr<P2PClientService> P2PClientIntegrationTest::client_ = nullptr;
std::shared_ptr<P2PClientService> P2PClientIntegrationTest::client2_ = nullptr;

// ============================================================================
// Put / Get (local WRITE_LOCAL mode)
// ============================================================================

TEST_F(P2PClientIntegrationTest, PutAndGetLocal) {
    const std::string key = "p2p_local_put_get";
    const std::string data = "Hello P2P world!";

    // Get metrics baseline before operations
    auto metrics_before = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_before.has_value());

    // Put
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put_result = client_->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

    // Verify Put metrics: should have 1 local put request with correct bytes
    auto metrics_after_put = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_after_put.has_value());
    EXPECT_TRUE(metrics_after_put.value().find(
                    "mooncake_p2p_local_put_requests_total 1") !=
                std::string::npos);
    EXPECT_TRUE(metrics_after_put.value().find(
                    "mooncake_p2p_local_put_bytes_total " +
                    std::to_string(data.size())) != std::string::npos);

    // Get (local mode reads from DataManager directly)
    std::vector<char> buf(data.size(), 0);
    std::vector<Slice> get_slices;
    get_slices.emplace_back(Slice{buf.data(), buf.size()});

    auto query = client_->Query(key);
    ASSERT_TRUE(query.has_value())
        << "Query failed: " << static_cast<int>(query.error());

    auto get_result = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get_result.has_value())
        << "Get failed: " << static_cast<int>(get_result.error());

    EXPECT_EQ(std::string(buf.data(), buf.size()), data);

    // Verify Get metrics: should have 1 local get request with 1 hit
    auto metrics_after_get = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_after_get.has_value());
    EXPECT_TRUE(metrics_after_get.value().find(
                    "mooncake_p2p_local_get_requests_total 1") !=
                std::string::npos);
    EXPECT_TRUE(
        metrics_after_get.value().find("mooncake_p2p_local_get_hits_total 1") !=
        std::string::npos);
    EXPECT_TRUE(metrics_after_get.value().find(
                    "mooncake_p2p_local_get_bytes_total " +
                    std::to_string(data.size())) != std::string::npos);
}

TEST_F(P2PClientIntegrationTest, ForceLocalWriteBypass) {
    const std::string data = "force_local_payload";

    // remote_weight=0: client_ writes locally instead of asking the master
    // for a remote route.
    {
        const std::string key = "force_local_key";
        WriteRouteRequestConfig cfg;
        cfg.remote_weight = 0.0;

        std::vector<Slice> slices;
        slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
        auto put = client_->Put(key, slices, cfg);
        ASSERT_TRUE(put.has_value())
            << "Force-local Put failed: " << static_cast<int>(put.error());

        // Readable on local client.
        std::vector<char> buf(data.size(), 0);
        auto get = client_->Get(key, {(void*)buf.data()}, {buf.size()});
        ASSERT_TRUE(get.has_value());
        EXPECT_EQ(std::string(buf.data(), buf.size()), data);

        // Verify via master that the replica is on client_ (local).
        auto resp = client_->GetMasterClient().GetReplicaList(key);
        ASSERT_TRUE(resp.has_value());
        ASSERT_FALSE(resp->replicas.empty());
        const auto& desc = resp->replicas[0];
        ASSERT_TRUE(std::holds_alternative<P2PProxyDescriptor>(
            desc.descriptor_variant));
        EXPECT_EQ(
            std::get<P2PProxyDescriptor>(desc.descriptor_variant).client_id,
            client_->GetClientID());
    }

    // remote_weight=1: client_ writes to client2_ via master routing.
    // The replica should be on client2_ (remote), not on client_.
    {
        const std::string key = "force_remote_key";
        WriteRouteRequestConfig cfg;
        cfg.remote_weight = 1.0;
        cfg.local_write_waterline = 0.0;  // disable waterline for force-remote

        std::vector<Slice> slices;
        slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
        auto put = client_->Put(key, slices, cfg);
        ASSERT_TRUE(put.has_value())
            << "Force-remote Put failed: " << static_cast<int>(put.error());

        // Readable on remote client.
        std::vector<char> buf(data.size(), 0);
        auto get = client2_->Get(key, {(void*)buf.data()}, {buf.size()});
        ASSERT_TRUE(get.has_value())
            << "Force-remote data should be on client2_";
        EXPECT_EQ(std::string(buf.data(), buf.size()), data);

        // Verify via master that the replica is on client2_ (remote).
        auto resp = client_->GetMasterClient().GetReplicaList(key);
        ASSERT_TRUE(resp.has_value());
        ASSERT_FALSE(resp->replicas.empty());
        const auto& desc = resp->replicas[0];
        ASSERT_TRUE(std::holds_alternative<P2PProxyDescriptor>(
            desc.descriptor_variant));
        EXPECT_EQ(
            std::get<P2PProxyDescriptor>(desc.descriptor_variant).client_id,
            client2_->GetClientID());
    }
}

// When local utilization is below the waterline, the client writes locally
TEST_F(P2PClientIntegrationTest, WaterlineBypassWritesLocal) {
    const std::string data = "waterline_payload";

    // Local client is nearly empty (64MB DRAM, ~0% utilization).
    // With waterline=0.5 and remote_weight=0.5 (balanced), the waterline
    // triggers a local write: data stays on client_.
    const std::string key = "waterline_bypass_key";
    WriteRouteRequestConfig cfg;
    cfg.remote_weight = 0.5;          // balanced
    cfg.local_write_waterline = 0.5;  // local is < 50% full -> local write

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put(key, slices, cfg);
    ASSERT_TRUE(put.has_value())
        << "Waterline-bypass Put failed: " << static_cast<int>(put.error());

    // Readable on local client.
    std::vector<char> buf(data.size(), 0);
    auto get = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get.has_value());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);

    // Verify via master that the replica is on client_ (local).
    auto resp = client_->GetMasterClient().GetReplicaList(key);
    ASSERT_TRUE(resp.has_value());
    ASSERT_FALSE(resp->replicas.empty());
    const auto& desc = resp->replicas[0];
    ASSERT_TRUE(
        std::holds_alternative<P2PProxyDescriptor>(desc.descriptor_variant));
    EXPECT_EQ(std::get<P2PProxyDescriptor>(desc.descriptor_variant).client_id,
              client_->GetClientID());
}

// Contradictory config (waterline=0 + remote_weight=0, a dead-end combo)
// is rejected by BatchPut at the client side.
TEST_F(P2PClientIntegrationTest, ContradictoryConfigRejected) {
    const std::string data = "contradictory_payload";
    WriteRouteRequestConfig cfg;
    cfg.remote_weight = 0.0;
    cfg.local_write_waterline =
        0.0;  // dead end: forbid local write + forbid remote route

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put("contradictory_key", slices, cfg);
    ASSERT_FALSE(put.has_value());
    EXPECT_EQ(put.error(), ErrorCode::INVALID_PARAMS);
}

// ============================================================================
// IsExist
// ============================================================================

TEST_F(P2PClientIntegrationTest, IsExist) {
    const std::string key = "p2p_exist_test";
    const std::string data = "exist_data";

    // Before put: key should be reported by master as non-existent
    auto exist_before = client_->IsExist(key);
    ASSERT_TRUE(exist_before.has_value());
    EXPECT_FALSE(exist_before.value());

    // Put data
    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put_result = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value());

    // After put: should exist (via master, since AddReplica callback fired)
    auto exist_after = client_->IsExist(key);
    ASSERT_TRUE(exist_after.has_value());
    EXPECT_TRUE(exist_after.value());
}

// ============================================================================
// Get miss metrics
// ============================================================================

TEST_F(P2PClientIntegrationTest, GetMissMetrics) {
    const std::string key = "p2p_nonexistent_key_for_miss_test";

    // Get metrics baseline
    auto metrics_before = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_before.has_value());

    // Try to get a non-existent key (should be a miss)
    std::vector<char> buf(100, 0);
    auto get_result = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Verify Get miss metrics
    auto metrics_after = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_after.has_value());
    // The miss count should have increased
    EXPECT_TRUE(
        metrics_after.value().find("mooncake_p2p_total_get_misses_total") !=
        std::string::npos);
}

// ============================================================================
// Query returns replica descriptors
// ============================================================================

TEST_F(P2PClientIntegrationTest, QueryReturnsReplicas) {
    const std::string key = "p2p_query_replica";
    const std::string data = "replica_data";

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value());

    auto query = client_->Query(key);
    ASSERT_TRUE(query.has_value())
        << "Query failed: " << static_cast<int>(query.error());

    // P2P mode: replicas come from master's AddReplica record
    auto& replicas = query.value()->replicas;
    EXPECT_GE(replicas.size(), 1u)
        << "Expected at least one replica descriptor";
}

// ============================================================================
// BatchIsExist
// ============================================================================

TEST_F(P2PClientIntegrationTest, BatchIsExist) {
    // Put a few keys
    std::vector<std::string> existing_keys;
    for (int i = 0; i < 3; ++i) {
        std::string key = "p2p_batch_exist_" + std::to_string(i);
        existing_keys.push_back(key);
        std::string data = "data_" + std::to_string(i);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
        auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
        ASSERT_TRUE(put.has_value());
    }

    // Mix existing and non-existing keys
    std::vector<std::string> query_keys = existing_keys;
    query_keys.push_back("p2p_batch_exist_NOT_1");
    query_keys.push_back("p2p_batch_exist_NOT_2");

    auto results = client_->BatchIsExist(query_keys);
    ASSERT_EQ(results.size(), query_keys.size());

    for (size_t i = 0; i < existing_keys.size(); ++i) {
        EXPECT_TRUE(results[i].has_value());
        EXPECT_TRUE(results[i].value())
            << "Key " << query_keys[i] << " should exist";
    }
    for (size_t i = existing_keys.size(); i < query_keys.size(); ++i) {
        EXPECT_TRUE(results[i].has_value());
        EXPECT_FALSE(results[i].value())
            << "Key " << query_keys[i] << " should not exist";
    }
}

// ============================================================================
// BatchPut + BatchQuery
// ============================================================================

TEST_F(P2PClientIntegrationTest, BatchPutAndBatchQuery) {
    const int batch_size = 5;
    std::vector<std::string> keys;
    std::vector<std::string> payloads;
    std::vector<std::vector<Slice>> batched_slices;

    for (int i = 0; i < batch_size; ++i) {
        keys.push_back("p2p_batch_pq_" + std::to_string(i));
        payloads.push_back("payload_" + std::to_string(i));
    }
    for (int i = 0; i < batch_size; ++i) {
        std::vector<Slice> s;
        s.emplace_back(
            Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()});
        batched_slices.push_back(std::move(s));
    }

    // BatchPut
    auto put_results =
        client_->BatchPut(keys, batched_slices, WriteRouteRequestConfig{});
    ASSERT_EQ(put_results.size(), static_cast<size_t>(batch_size));
    for (auto& r : put_results) {
        EXPECT_TRUE(r.has_value())
            << "BatchPut element failed: " << static_cast<int>(r.error());
    }

    // BatchQuery
    auto query_results = client_->BatchQuery(keys);
    ASSERT_EQ(query_results.size(), static_cast<size_t>(batch_size));
    for (size_t i = 0; i < query_results.size(); ++i) {
        EXPECT_TRUE(query_results[i].has_value())
            << "BatchQuery failed for key: " << keys[i]
            << ", error: " << static_cast<int>(query_results[i].error());
    }
}

TEST_F(P2PClientIntegrationTest, RemoteBatchPutAndBatchGet) {
    // Test both local transfer modes: te and memcpy.
    const std::vector<std::string> transfer_modes = {"te", "memcpy"};

    for (const auto& mode : transfer_modes) {
        SCOPED_TRACE("local_transfer_mode=" + mode);

        std::string host = "localhost:" + std::to_string(getFreeTcpPort());
        auto remote_writer = CreateP2PClient(host, /*rpc_port=*/0, mode);
        ASSERT_NE(remote_writer, nullptr);

        const int batch_size = 6;
        std::vector<std::string> keys;
        std::vector<std::string> payloads;
        std::vector<std::vector<Slice>> batched_slices;

        keys.reserve(batch_size);
        payloads.reserve(batch_size);
        batched_slices.reserve(batch_size);
        for (int i = 0; i < batch_size; ++i) {
            std::string key_prefix = "p2p_remote_batch_" + mode + "_";
            keys.push_back(key_prefix + "key_" + std::to_string(i));
            payloads.push_back(key_prefix + "payload_" + std::to_string(i));
        }
        for (int i = 0; i < batch_size; ++i) {
            std::vector<Slice> slices;
            slices.emplace_back(Slice{const_cast<char*>(payloads[i].data()),
                                      payloads[i].size()});
            batched_slices.push_back(std::move(slices));
        }

        // Force write route to exclude local candidate so the writer must
        // execute remote Put RPCs.
        WriteRouteRequestConfig remote_put_config;
        remote_put_config.remote_weight = 1.0;  // force remote
        remote_put_config.local_write_waterline = 0.0;
        remote_put_config.max_candidates =
            WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
        auto put_results =
            remote_writer->BatchPut(keys, batched_slices, remote_put_config);
        ASSERT_EQ(put_results.size(), static_cast<size_t>(batch_size));
        for (const auto& r : put_results) {
            ASSERT_TRUE(r.has_value())
                << "Remote BatchPut failed: " << static_cast<int>(r.error());
        }

        // Validate remote BatchGet(raw buffers) path.
        std::vector<std::vector<char>> read_payloads(batch_size);
        std::vector<std::vector<void*>> all_buffers(batch_size);
        std::vector<std::vector<size_t>> all_sizes(batch_size);
        for (int i = 0; i < batch_size; ++i) {
            read_payloads[i].resize(payloads[i].size(), 0);
            all_buffers[i].push_back(read_payloads[i].data());
            all_sizes[i].push_back(read_payloads[i].size());
        }

        auto batch_get_results = remote_writer->BatchGet(
            keys, all_buffers, all_sizes, ReadRouteConfig{});
        ASSERT_EQ(batch_get_results.size(), static_cast<size_t>(batch_size));
        for (int i = 0; i < batch_size; ++i) {
            ASSERT_TRUE(batch_get_results[i].has_value())
                << "Remote BatchGet(raw) failed for key " << keys[i]
                << ", error: "
                << static_cast<int>(batch_get_results[i].error());
            EXPECT_EQ(static_cast<size_t>(batch_get_results[i].value()),
                      payloads[i].size());
            EXPECT_EQ(
                std::string(read_payloads[i].data(), read_payloads[i].size()),
                payloads[i]);
        }

        // Validate remote BatchGet(allocator) path.
        auto allocator = ClientBufferAllocator::create(8 * 1024 * 1024);
        ASSERT_NE(allocator, nullptr);
        auto batch_get_handles =
            remote_writer->BatchGet(keys, allocator, ReadRouteConfig{});
        ASSERT_EQ(batch_get_handles.size(), static_cast<size_t>(batch_size));
        for (int i = 0; i < batch_size; ++i) {
            ASSERT_TRUE(batch_get_handles[i].has_value())
                << "Remote BatchGet(allocator) failed for key " << keys[i]
                << ", error: "
                << static_cast<int>(batch_get_handles[i].error());
            auto buffer_handle = batch_get_handles[i].value();
            ASSERT_NE(buffer_handle, nullptr);
            ASSERT_EQ(buffer_handle->size(), payloads[i].size());
            EXPECT_EQ(std::string(static_cast<char*>(buffer_handle->ptr()),
                                  buffer_handle->size()),
                      payloads[i]);
        }
    }
}

// ============================================================================
// Put overwrite: writing same key twice should succeed
// ============================================================================

TEST_F(P2PClientIntegrationTest, PutOverwrite) {
    const std::string key = "p2p_overwrite";
    const std::string data1 = "version_1";
    const std::string data2 = "version_2_longer";

    GetReplicaListRequestConfig config;
    config.max_candidates = GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES;

    // First put
    {
        std::vector<Slice> s;
        s.emplace_back(Slice{const_cast<char*>(data1.data()), data1.size()});
        auto r = client_->Put(key, s, WriteRouteRequestConfig{});
        ASSERT_TRUE(r.has_value());

        auto replicas = master_.GetWrapped().GetReplicaList(key, config);
        ASSERT_TRUE(replicas.has_value());
        ASSERT_EQ(replicas.value().replicas.size(), 1);
        auto p2p_proxy_descriptor =
            replicas.value().replicas[0].get_p2p_proxy_descriptor();
        ASSERT_EQ(p2p_proxy_descriptor.client_id, client_->GetClientID());
        ASSERT_EQ(p2p_proxy_descriptor.object_size, data1.size());
    }

    // Overwrite
    {
        // Overwriting is not allowed, but the error should be ignored
        std::vector<Slice> s;
        s.emplace_back(Slice{const_cast<char*>(data2.data()), data2.size()});
        auto r = client_->Put(key, s, WriteRouteRequestConfig{});
        ASSERT_TRUE(r.has_value());

        // due to the write operation is canceled,
        // the object size of read route must not be changed
        auto replicas = master_.GetWrapped().GetReplicaList(key, config);
        ASSERT_TRUE(replicas.has_value());
        ASSERT_EQ(replicas.value().replicas.size(), 1);
        auto p2p_proxy_descriptor =
            replicas.value().replicas[0].get_p2p_proxy_descriptor();
        ASSERT_EQ(p2p_proxy_descriptor.client_id, client_->GetClientID());
        ASSERT_EQ(p2p_proxy_descriptor.object_size, data1.size());
    }

    // Read back – should see data1 (first version)
    std::vector<char> buf(data1.size(), 0);
    std::vector<Slice> get_slices;
    get_slices.emplace_back(Slice{buf.data(), buf.size()});
    auto query = client_->Query(key);
    ASSERT_TRUE(query.has_value());

    auto get = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get.has_value());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data1);
}

// ============================================================================
// RemoveAllLocal
// ============================================================================

TEST_F(P2PClientIntegrationTest, RemoveAllLocalEmpty) {
    // Establish a clean baseline first.
    auto baseline = client_->RemoveAllLocal();
    ASSERT_TRUE(baseline.has_value()) << "Baseline RemoveAllLocal failed: "
                                      << static_cast<int>(baseline.error());

    // On a clean local cache, RemoveAllLocal should succeed and return 0.
    auto result = client_->RemoveAllLocal();
    ASSERT_TRUE(result.has_value())
        << "RemoveAllLocal failed: " << static_cast<int>(result.error());
    EXPECT_EQ(result.value(), 0);
}

TEST_F(P2PClientIntegrationTest, RemoveAllLocalRemovesPutKeys) {
    // Clean baseline.
    auto baseline = client_->RemoveAllLocal();
    ASSERT_TRUE(baseline.has_value()) << "Baseline RemoveAllLocal failed: "
                                      << static_cast<int>(baseline.error());

    const int kNumKeys = 3;
    std::vector<std::string> keys;
    std::vector<std::string> payloads;
    keys.reserve(kNumKeys);
    payloads.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("p2p_remove_all_local_basic_" + std::to_string(i));
        payloads.push_back("payload_basic_" + std::to_string(i));
    }

    // Put several keys to populate local tiered cache.
    for (int i = 0; i < kNumKeys; ++i) {
        std::vector<Slice> slices;
        slices.emplace_back(
            Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()});
        auto put = client_->Put(keys[i], slices, WriteRouteRequestConfig{});
        ASSERT_TRUE(put.has_value()) << "Put failed for " << keys[i] << ": "
                                     << static_cast<int>(put.error());
    }

    // RemoveAllLocal should remove at least the kNumKeys we just put.
    auto removed = client_->RemoveAllLocal();
    ASSERT_TRUE(removed.has_value())
        << "RemoveAllLocal failed: " << static_cast<int>(removed.error());
    EXPECT_EQ(removed.value(), static_cast<long>(kNumKeys));

    // After RemoveAllLocal, local Get for each key must return
    // OBJECT_NOT_FOUND, confirming local tiered cache has been cleared.
    for (int i = 0; i < kNumKeys; ++i) {
        std::vector<char> buf(payloads[i].size(), 0);
        auto get = client_->Get(keys[i], {(void*)buf.data()}, {buf.size()});
        ASSERT_FALSE(get.has_value())
            << "Get unexpectedly succeeded after RemoveAllLocal for "
            << keys[i];
        EXPECT_EQ(get.error(), ErrorCode::OBJECT_NOT_FOUND);
    }
}

TEST_F(P2PClientIntegrationTest, RemoveAllLocalIdempotent) {
    // Clean baseline.
    auto baseline = client_->RemoveAllLocal();
    ASSERT_TRUE(baseline.has_value()) << "Baseline RemoveAllLocal failed: "
                                      << static_cast<int>(baseline.error());

    const int kNumKeys = 4;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "p2p_remove_all_local_idem_" + std::to_string(i);
        std::string data = "idem_payload_" + std::to_string(i);
        std::vector<Slice> slices;
        slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
        auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
        ASSERT_TRUE(put.has_value()) << "Put failed for " << key << ": "
                                     << static_cast<int>(put.error());
    }

    // First call should report N > 0 keys removed.
    auto first = client_->RemoveAllLocal();
    ASSERT_TRUE(first.has_value())
        << "First RemoveAllLocal failed: " << static_cast<int>(first.error());
    EXPECT_EQ(first.value(), 4);

    // Second consecutive call should succeed and report 0 (idempotent path,
    // also exercises the empty-collection branch).
    auto second = client_->RemoveAllLocal();
    ASSERT_TRUE(second.has_value())
        << "Second RemoveAllLocal failed: " << static_cast<int>(second.error());
    EXPECT_EQ(second.value(), 0);
}

// ============================================================================
// RemoveLocal (single-key)
// ============================================================================

TEST_F(P2PClientIntegrationTest, RemoveLocalAfterPut) {
    const std::string key = "p2p_remove_local_after_put";
    const std::string data = "to_be_removed";

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value())
        << "Put failed: " << static_cast<int>(put.error());

    auto removed = client_->RemoveLocal(key);
    ASSERT_TRUE(removed.has_value())
        << "RemoveLocal failed: " << static_cast<int>(removed.error());

    std::vector<char> buf(data.size(), 0);
    auto get = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_FALSE(get.has_value())
        << "Get unexpectedly succeeded after RemoveLocal";
    EXPECT_EQ(get.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(P2PClientIntegrationTest, RemoveLocalNonExistent) {
    const std::string key = "p2p_remove_local_never_put_xyz";
    auto removed = client_->RemoveLocal(key);
    ASSERT_FALSE(removed.has_value())
        << "RemoveLocal unexpectedly succeeded for non-existent key";
    EXPECT_EQ(removed.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// ============================================================================
// Remove / RemoveAll / RemoveByRegex should return NOT_IMPLEMENTED
// ============================================================================

// TEST_F(P2PClientIntegrationTest, RemoveNotImplemented) {
//     auto r = client_->Remove("any_key");
//     ASSERT_FALSE(r.has_value());
//     EXPECT_EQ(r.error(), ErrorCode::NOT_IMPLEMENTED);
// }

// TEST_F(P2PClientIntegrationTest, RemoveAllNotImplemented) {
//     auto r = client_->RemoveAll();
//     ASSERT_FALSE(r.has_value());
//     EXPECT_EQ(r.error(), ErrorCode::NOT_IMPLEMENTED);
// }

// TEST_F(P2PClientIntegrationTest, RemoveByRegexNotImplemented) {
//     auto r = client_->RemoveByRegex(".*");
//     ASSERT_FALSE(r.has_value());
//     EXPECT_EQ(r.error(), ErrorCode::NOT_IMPLEMENTED);
// }

// ============================================================================
// MountSegment / UnmountSegment should return NOT_IMPLEMENTED
// ============================================================================

// TEST_F(P2PClientIntegrationTest, MountSegmentNotImplemented) {
//     char dummy[64] = {0};
//     auto r = client_->MountSegment(dummy, sizeof(dummy));
//     ASSERT_FALSE(r.has_value());
//     EXPECT_EQ(r.error(), ErrorCode::NOT_IMPLEMENTED);
// }

// TEST_F(P2PClientIntegrationTest, UnmountSegmentNotImplemented) {
//     char dummy[64] = {0};
//     auto r = client_->UnmountSegment(dummy, sizeof(dummy));
//     ASSERT_FALSE(r.has_value());
//     EXPECT_EQ(r.error(), ErrorCode::NOT_IMPLEMENTED);
// }

// ============================================================================
// Query non-existent key should fail
// ============================================================================

TEST_F(P2PClientIntegrationTest, QueryNonExistentKey) {
    auto q = client_->Query("totally_nonexistent_key_xyz");
    ASSERT_FALSE(q.has_value()) << "Query should fail for non-existent key";
    EXPECT_EQ(q.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// ============================================================================
// Large Put + Get round-trip
// ============================================================================

TEST_F(P2PClientIntegrationTest, LargePutGet) {
    const std::string key = "p2p_large_data";
    const size_t size = 4 * 1024 * 1024;  // 4 MB
    std::vector<char> payload(size, 'X');

    // Put
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{payload.data(), payload.size()});
    auto put = client_->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value())
        << "Large Put failed: " << static_cast<int>(put.error());

    // Get
    std::vector<char> read_buf(size, 0);
    std::vector<Slice> get_slices;
    get_slices.emplace_back(Slice{read_buf.data(), read_buf.size()});

    auto query = client_->Query(key);
    ASSERT_TRUE(query.has_value());

    auto get = client_->Get(key, {(void*)read_buf.data()}, {read_buf.size()});
    ASSERT_TRUE(get.has_value())
        << "Large Get failed: " << static_cast<int>(get.error());

    EXPECT_EQ(payload, read_buf);
}

TEST_F(P2PClientIntegrationTest, LocalPutGetWithTeTransferMode) {
    auto te_client = CreateP2PClient(
        "localhost:" + std::to_string(getFreeTcpPort()), /*rpc_port=*/0, "te");
    ASSERT_NE(te_client, nullptr);

    const std::string key = "p2p_local_te_put_get";
    const size_t kHalf = 1024;
    std::vector<char> part1(kHalf, 'A');
    std::vector<char> part2(kHalf, 'B');
    std::vector<char> read_buf(kHalf * 2, 0);

    // TE local transfer path needs registered source/destination buffers.
    auto reg1 = te_client->RegisterLocalMemory(part1.data(), part1.size(), "*",
                                               false, false);
    auto reg2 = te_client->RegisterLocalMemory(part2.data(), part2.size(), "*",
                                               false, false);
    auto reg3 = te_client->RegisterLocalMemory(read_buf.data(), read_buf.size(),
                                               "*", false, false);
    ASSERT_TRUE(reg1.has_value());
    ASSERT_TRUE(reg2.has_value());
    ASSERT_TRUE(reg3.has_value());

    std::vector<Slice> put_slices = {Slice{part1.data(), part1.size()},
                                     Slice{part2.data(), part2.size()}};
    auto put_result =
        te_client->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

    auto get_result =
        te_client->Get(key, {(void*)read_buf.data()}, {read_buf.size()});
    ASSERT_TRUE(get_result.has_value())
        << "Get failed: " << static_cast<int>(get_result.error());
    ASSERT_EQ(static_cast<size_t>(get_result.value()), read_buf.size());

    EXPECT_EQ(0, std::memcmp(read_buf.data(), part1.data(), part1.size()));
    EXPECT_EQ(0, std::memcmp(read_buf.data() + part1.size(), part2.data(),
                             part2.size()));

    auto unreg1 = te_client->unregisterLocalMemory(part1.data(), false);
    auto unreg2 = te_client->unregisterLocalMemory(part2.data(), false);
    auto unreg3 = te_client->unregisterLocalMemory(read_buf.data(), false);
    EXPECT_TRUE(unreg1.has_value());
    EXPECT_TRUE(unreg2.has_value());
    EXPECT_TRUE(unreg3.has_value());
}

TEST_F(P2PClientIntegrationTest, LocalGetBufferHandleWithTeTransferMode) {
    auto te_client = CreateP2PClient(
        "localhost:" + std::to_string(getFreeTcpPort()), /*rpc_port=*/0, "te");
    ASSERT_NE(te_client, nullptr);

    const std::string key = "p2p_local_te_get_buffer";
    std::vector<char> payload(2048, 'R');

    auto reg_src = te_client->RegisterLocalMemory(
        payload.data(), payload.size(), "*", false, false);
    ASSERT_TRUE(reg_src.has_value());

    std::vector<Slice> put_slices = {{payload.data(), payload.size()}};
    auto put_result =
        te_client->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

    auto allocator = ClientBufferAllocator::create(payload.size());
    ASSERT_NE(allocator, nullptr);
    auto reg_dst = te_client->RegisterLocalMemory(
        allocator->getBase(), allocator->size(), "*", false, false);
    ASSERT_TRUE(reg_dst.has_value());

    auto get_result = te_client->Get(key, allocator, ReadRouteConfig{});
    ASSERT_TRUE(get_result.has_value())
        << "Get(buffer) failed: " << static_cast<int>(get_result.error());

    auto buffer_handle = get_result.value();
    ASSERT_NE(buffer_handle, nullptr);
    ASSERT_EQ(buffer_handle->size(), payload.size());
    EXPECT_EQ(
        0, std::memcmp(buffer_handle->ptr(), payload.data(), payload.size()));

    auto unreg_dst =
        te_client->unregisterLocalMemory(allocator->getBase(), false);
    auto unreg_src = te_client->unregisterLocalMemory(payload.data(), false);
    EXPECT_TRUE(unreg_dst.has_value());
    EXPECT_TRUE(unreg_src.has_value());
}

TEST_F(P2PClientIntegrationTest, ForwardRemotePutAndGet) {
    const std::vector<std::string> transfer_modes = {"te", "memcpy"};
    for (const auto& mode : transfer_modes) {
        SCOPED_TRACE("local_transfer_mode=" + mode);

        std::string host = "localhost:" + std::to_string(getFreeTcpPort());
        auto remote_writer = CreateP2PClient(host, /*rpc_port=*/0, mode,
                                             TransferDirectionMode::FORWARD);
        ASSERT_NE(remote_writer, nullptr);

        const std::string key = "p2p_fwd_put_get_" + mode + "_" + host;
        const std::string payload = "forward_payload_" + mode + "_data";

        WriteRouteRequestConfig route;
        route.remote_weight = 1.0;  // force remote
        route.local_write_waterline = 0.0;
        route.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;

        std::vector<Slice> slices;
        slices.emplace_back(
            Slice{const_cast<char*>(payload.data()), payload.size()});
        auto put_res = remote_writer->Put(key, slices, route);
        ASSERT_TRUE(put_res.has_value())
            << "Forward Put failed mode=" << mode
            << " err=" << static_cast<int>(put_res.error());

        // confirm the object is visible on the owner peer (suite client_).
        auto exist_on_owner = client_->IsExist(key);
        ASSERT_TRUE(exist_on_owner.has_value())
            << "Owner IsExist failed mode=" << mode
            << " err=" << static_cast<int>(exist_on_owner.error());
        EXPECT_TRUE(exist_on_owner.value())
            << "Forward Put should leave key on owner peer, mode=" << mode;

        ReadRouteConfig rcfg;
        rcfg.max_candidates =
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES;

        std::vector<char> buf(payload.size(), 0);
        auto get_res =
            remote_writer->Get(key, {(void*)buf.data()}, {buf.size()}, rcfg);
        ASSERT_TRUE(get_res.has_value())
            << "Forward Get failed mode=" << mode
            << " err=" << static_cast<int>(get_res.error());
        EXPECT_EQ(static_cast<size_t>(get_res.value()), payload.size());
        EXPECT_EQ(std::string(buf.data(), buf.size()), payload);
    }
}

// FORWARD remote BatchPut + BatchGet (raw buffers and allocator paths).
TEST_F(P2PClientIntegrationTest, ForwardRemoteBatchPutAndBatchGet) {
    const std::vector<std::string> transfer_modes = {"te", "memcpy"};
    for (const auto& mode : transfer_modes) {
        SCOPED_TRACE("local_transfer_mode=" + mode);

        std::string host = "localhost:" + std::to_string(getFreeTcpPort());
        auto remote_writer = CreateP2PClient(host, /*rpc_port=*/0, mode,
                                             TransferDirectionMode::FORWARD);
        ASSERT_NE(remote_writer, nullptr);

        const int batch_size = 6;
        std::vector<std::string> keys;
        std::vector<std::string> payloads;
        std::vector<std::vector<Slice>> batched_slices;

        keys.reserve(batch_size);
        payloads.reserve(batch_size);
        batched_slices.reserve(batch_size);
        for (int i = 0; i < batch_size; ++i) {
            std::string key_prefix = "p2p_fwd_remote_batch_" + mode + "_";
            keys.push_back(key_prefix + "key_" + std::to_string(i));
            payloads.push_back(key_prefix + "payload_" + std::to_string(i));
        }
        for (int i = 0; i < batch_size; ++i) {
            std::vector<Slice> slices;
            slices.emplace_back(Slice{const_cast<char*>(payloads[i].data()),
                                      payloads[i].size()});
            batched_slices.push_back(std::move(slices));
        }

        WriteRouteRequestConfig remote_put_config;
        remote_put_config.remote_weight = 1.0;  // force remote
        remote_put_config.local_write_waterline = 0.0;
        remote_put_config.max_candidates =
            WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;

        auto put_results =
            remote_writer->BatchPut(keys, batched_slices, remote_put_config);
        ASSERT_EQ(put_results.size(), static_cast<size_t>(batch_size));
        for (size_t i = 0; i < put_results.size(); ++i) {
            ASSERT_TRUE(put_results[i].has_value())
                << "Forward BatchPut failed mode=" << mode << " key=" << keys[i]
                << " err=" << static_cast<int>(put_results[i].error());
        }

        for (const auto& key : keys) {
            auto exist_on_owner = client_->IsExist(key);
            ASSERT_TRUE(exist_on_owner.has_value())
                << "Owner IsExist failed mode=" << mode << " key=" << key
                << " err=" << static_cast<int>(exist_on_owner.error());
            EXPECT_TRUE(exist_on_owner.value())
                << "Forward BatchPut should leave key on owner peer, key="
                << key << " mode=" << mode;
        }

        ReadRouteConfig read_config;
        read_config.max_candidates =
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES;

        std::vector<std::vector<char>> read_payloads(batch_size);
        std::vector<std::vector<void*>> all_buffers(batch_size);
        std::vector<std::vector<size_t>> all_sizes(batch_size);
        for (int i = 0; i < batch_size; ++i) {
            read_payloads[i].resize(payloads[i].size(), 0);
            all_buffers[i].push_back(read_payloads[i].data());
            all_sizes[i].push_back(read_payloads[i].size());
        }

        auto batch_get_results =
            remote_writer->BatchGet(keys, all_buffers, all_sizes, read_config);
        ASSERT_EQ(batch_get_results.size(), static_cast<size_t>(batch_size));
        for (int i = 0; i < batch_size; ++i) {
            ASSERT_TRUE(batch_get_results[i].has_value())
                << "Forward BatchGet(raw) failed mode=" << mode
                << " key=" << keys[i]
                << " err=" << static_cast<int>(batch_get_results[i].error());
            EXPECT_EQ(static_cast<size_t>(batch_get_results[i].value()),
                      payloads[i].size());
            EXPECT_EQ(
                std::string(read_payloads[i].data(), read_payloads[i].size()),
                payloads[i]);
        }

        auto allocator = ClientBufferAllocator::create(8 * 1024 * 1024);
        ASSERT_NE(allocator, nullptr);
        auto batch_get_handles =
            remote_writer->BatchGet(keys, allocator, read_config);
        ASSERT_EQ(batch_get_handles.size(), static_cast<size_t>(batch_size));
        for (int i = 0; i < batch_size; ++i) {
            ASSERT_TRUE(batch_get_handles[i].has_value())
                << "Forward BatchGet(allocator) failed mode=" << mode
                << " key=" << keys[i]
                << " err=" << static_cast<int>(batch_get_handles[i].error());
            auto buffer_handle = batch_get_handles[i].value();
            ASSERT_NE(buffer_handle, nullptr);
            ASSERT_EQ(buffer_handle->size(), payloads[i].size());
            EXPECT_EQ(std::string(static_cast<char*>(buffer_handle->ptr()),
                                  buffer_handle->size()),
                      payloads[i]);
        }
    }
}

// ============================================================================
// TE async poll offload: functional correctness (0 = sync wait, >0 = poll pool)
// Multi-key batch covers concurrent per-key chains; single-key would be redundant.
// ============================================================================

TEST_F(P2PClientIntegrationTest, TeAsyncPollForwardRemoteBatchPutAndGet) {
    const std::vector<size_t> te_async_poll_workers = {0, 4};
    constexpr int batch_size = 6;
    for (size_t te_async : te_async_poll_workers) {
        SCOPED_TRACE("te_async_poll_worker_num=" + std::to_string(te_async));

        std::string host = "localhost:" + std::to_string(getFreeTcpPort());
        auto remote_writer = CreateP2PClient(
            host, /*rpc_port=*/0, "te", TransferDirectionMode::FORWARD,
            te_async);
        ASSERT_NE(remote_writer, nullptr);

        std::vector<std::string> keys;
        std::vector<std::string> payloads;
        std::vector<std::vector<Slice>> batched_slices;
        keys.reserve(batch_size);
        payloads.reserve(batch_size);
        batched_slices.reserve(batch_size);
        for (int i = 0; i < batch_size; ++i) {
            keys.push_back("p2p_te_async_fwd_batch_" + std::to_string(te_async) +
                           "_key_" + std::to_string(i));
            payloads.push_back("payload_" + std::to_string(te_async) + "_" +
                               std::to_string(i));
        }
        for (int i = 0; i < batch_size; ++i) {
            std::vector<Slice> slices;
            slices.emplace_back(Slice{const_cast<char*>(payloads[i].data()),
                                      payloads[i].size()});
            batched_slices.push_back(std::move(slices));
        }

        WriteRouteRequestConfig remote_put_config;
        remote_put_config.remote_weight = 1.0;
        remote_put_config.local_write_waterline = 0.0;
        remote_put_config.max_candidates =
            WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
        auto put_results =
            remote_writer->BatchPut(keys, batched_slices, remote_put_config);
        ASSERT_EQ(put_results.size(), static_cast<size_t>(batch_size));
        for (size_t i = 0; i < put_results.size(); ++i) {
            ASSERT_TRUE(put_results[i].has_value())
                << "Forward BatchPut failed te_async=" << te_async
                << " key=" << keys[i]
                << " err=" << static_cast<int>(put_results[i].error());
        }

        ReadRouteConfig read_config;
        read_config.max_candidates =
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES;
        std::vector<std::vector<char>> read_payloads(batch_size);
        std::vector<std::vector<void*>> all_buffers(batch_size);
        std::vector<std::vector<size_t>> all_sizes(batch_size);
        for (int i = 0; i < batch_size; ++i) {
            read_payloads[i].resize(payloads[i].size(), 0);
            all_buffers[i].push_back(read_payloads[i].data());
            all_sizes[i].push_back(read_payloads[i].size());
        }

        auto batch_get_results =
            remote_writer->BatchGet(keys, all_buffers, all_sizes, read_config);
        ASSERT_EQ(batch_get_results.size(), static_cast<size_t>(batch_size));
        for (int i = 0; i < batch_size; ++i) {
            ASSERT_TRUE(batch_get_results[i].has_value())
                << "Forward BatchGet failed te_async=" << te_async
                << " key=" << keys[i]
                << " err=" << static_cast<int>(batch_get_results[i].error());
            EXPECT_EQ(static_cast<size_t>(batch_get_results[i].value()),
                      payloads[i].size());
            EXPECT_EQ(
                std::string(read_payloads[i].data(), read_payloads[i].size()),
                payloads[i]);
        }
    }
}

// ============================================================================
// UnregisterClient / re-register / graceful Stop
// ============================================================================

TEST_F(P2PClientIntegrationTest, UnregisterSwitchesToLocalOnly) {
    auto c = CreateP2PClient("localhost:18821");
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->GetHealthStatus(), "FULL");

    // Unregister -> stable LOCAL_ONLY (heartbeat stopped, no auto re-register).
    auto un = c->UnregisterClient();
    ASSERT_TRUE(un.has_value()) << static_cast<int>(un.error());
    EXPECT_EQ(c->GetHealthStatus(), "LOCAL_ONLY");

    // Local read/write still works in local-only mode.
    const std::string key = "p2p_local_only_rw";
    const std::string data = "local-only-data";
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    ASSERT_TRUE(c->Put(key, put_slices, WriteRouteRequestConfig{}).has_value());

    std::vector<char> buf(data.size(), 0);
    auto get_res = c->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get_res.has_value())
        << "local Get failed: " << static_cast<int>(get_res.error());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);

    // (Re-registration back to FULL is driven via the public HTTP /register
    // endpoint and is covered by
    // P2PClientHttpEndpointsTest.HttpUnregisterThenRegister.)

    c->Stop();
    c->Destroy();
}

// ============================================================================
// Metric integration tests (delta-based, direct counter assertions)
// ============================================================================

static P2PClientMetric* GetP2PMetrics(P2PClientService* c) {
    return dynamic_cast<P2PClientMetric*>(c->GetMetrics());
}

TEST_F(P2PClientIntegrationTest, MetricLocalPutGet_TE) {
    auto* m = GetP2PMetrics(client_.get());
    ASSERT_NE(m, nullptr);

    const std::string key = "metric_local_te_key";
    const std::string data = "metric_te_payload_data";

    // Snapshot before
    auto before_total_put_req = m->total_request.put_requests.value();
    auto before_total_put_bytes = m->total_request.put_bytes.value();
    auto before_local_put_req = m->local_request.put_requests.value();
    auto before_local_put_bytes = m->local_request.put_bytes.value();
    auto before_remote_put_req = m->remote_request.put_requests.value();

    // Put (default config -> local write)
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put_result = client_->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

    // Verify put metrics
    EXPECT_EQ(m->total_request.put_requests.value(), before_total_put_req + 1);
    EXPECT_EQ(m->total_request.put_bytes.value(),
              before_total_put_bytes + static_cast<int64_t>(data.size()));
    EXPECT_EQ(m->local_request.put_requests.value(), before_local_put_req + 1);
    EXPECT_EQ(m->local_request.put_bytes.value(),
              before_local_put_bytes + static_cast<int64_t>(data.size()));
    EXPECT_EQ(m->remote_request.put_requests.value(), before_remote_put_req);

    // Snapshot before get
    auto before_total_get_req = m->total_request.get_requests.value();
    auto before_total_get_hits = m->total_request.get_hits.value();
    auto before_total_get_bytes = m->total_request.get_bytes.value();
    auto before_local_get_req = m->local_request.get_requests.value();
    auto before_local_get_hits = m->local_request.get_hits.value();
    auto before_remote_get_req = m->remote_request.get_requests.value();

    // Get (local hit)
    std::vector<char> buf(data.size(), 0);
    auto get_result = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get_result.has_value())
        << "Get failed: " << static_cast<int>(get_result.error());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);

    // Verify get metrics
    EXPECT_EQ(m->total_request.get_requests.value(), before_total_get_req + 1);
    EXPECT_EQ(m->total_request.get_hits.value(), before_total_get_hits + 1);
    EXPECT_EQ(m->total_request.get_bytes.value(),
              before_total_get_bytes + static_cast<int64_t>(data.size()));
    EXPECT_EQ(m->local_request.get_requests.value(), before_local_get_req + 1);
    EXPECT_EQ(m->local_request.get_hits.value(), before_local_get_hits + 1);
    EXPECT_EQ(m->remote_request.get_requests.value(), before_remote_get_req);
}

TEST_F(P2PClientIntegrationTest, MetricLocalPutGet_Memcpy) {
    auto c = CreateP2PClient("localhost:18830", 0, "memcpy");
    ASSERT_NE(c, nullptr);

    auto* m = GetP2PMetrics(c.get());
    ASSERT_NE(m, nullptr);

    const std::string key = "metric_local_memcpy_key";
    const std::string data = "metric_memcpy_payload";

    // Put
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put_result = c->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Memcpy Put failed: " << static_cast<int>(put_result.error());

    EXPECT_EQ(m->total_request.put_requests.value(), 1);
    EXPECT_EQ(m->total_request.put_bytes.value(),
              static_cast<int64_t>(data.size()));
    EXPECT_EQ(m->local_request.put_requests.value(), 1);
    EXPECT_EQ(m->local_request.put_bytes.value(),
              static_cast<int64_t>(data.size()));
    EXPECT_EQ(m->remote_request.put_requests.value(), 0);

    // Get
    std::vector<char> buf(data.size(), 0);
    auto get_result = c->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get_result.has_value())
        << "Memcpy Get failed: " << static_cast<int>(get_result.error());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);

    EXPECT_EQ(m->total_request.get_requests.value(), 1);
    EXPECT_EQ(m->total_request.get_hits.value(), 1);
    EXPECT_EQ(m->total_request.get_bytes.value(),
              static_cast<int64_t>(data.size()));
    EXPECT_EQ(m->local_request.get_requests.value(), 1);
    EXPECT_EQ(m->local_request.get_hits.value(), 1);
    EXPECT_EQ(m->remote_request.get_requests.value(), 0);

    c->Stop();
    c->Destroy();
}

TEST_F(P2PClientIntegrationTest, MetricRemotePut) {
    auto* m_writer = GetP2PMetrics(client_.get());
    auto* m_owner = GetP2PMetrics(client2_.get());
    ASSERT_NE(m_writer, nullptr);
    ASSERT_NE(m_owner, nullptr);

    const std::string key = "metric_remote_put_key";
    const std::string data = "metric_remote_put_payload";

    // Snapshot writer metrics
    auto before_total_put = m_writer->total_request.put_requests.value();
    auto before_remote_put = m_writer->remote_request.put_requests.value();
    auto before_remote_put_bytes = m_writer->remote_request.put_bytes.value();
    auto before_local_put = m_writer->local_request.put_requests.value();
    // Snapshot owner peer metrics
    auto before_peer_write =
        m_owner->peer_request_metrics.write_remote_data.requests.value();

    // Force remote write
    WriteRouteRequestConfig cfg;
    cfg.remote_weight = 1.0;
    cfg.local_write_waterline = 0.0;
    cfg.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put_result = client_->Put(key, slices, cfg);
    ASSERT_TRUE(put_result.has_value())
        << "Remote Put failed: " << static_cast<int>(put_result.error());

    // Verify writer metrics: should go through remote path
    EXPECT_EQ(m_writer->total_request.put_requests.value(),
              before_total_put + 1);
    EXPECT_EQ(m_writer->remote_request.put_requests.value(),
              before_remote_put + 1);
    EXPECT_EQ(m_writer->remote_request.put_bytes.value(),
              before_remote_put_bytes + static_cast<int64_t>(data.size()));
    EXPECT_EQ(m_writer->local_request.put_requests.value(), before_local_put);

    // Verify owner received the write via peer RPC
    EXPECT_EQ(m_owner->peer_request_metrics.write_remote_data.requests.value(),
              before_peer_write + 1);
}

TEST_F(P2PClientIntegrationTest, MetricRemoteGet) {
    auto* m_reader = GetP2PMetrics(client2_.get());
    ASSERT_NE(m_reader, nullptr);
    auto* m_owner = GetP2PMetrics(client_.get());
    ASSERT_NE(m_owner, nullptr);

    const std::string key = "metric_remote_get_key";
    const std::string data = "metric_remote_get_payload";

    // First: client_ puts locally
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put_result = client_->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value());

    // Snapshot reader (client2_) metrics
    auto before_total_get = m_reader->total_request.get_requests.value();
    auto before_total_hits = m_reader->total_request.get_hits.value();
    auto before_local_get = m_reader->local_request.get_requests.value();
    auto before_local_misses = m_reader->local_request.get_misses.value();
    auto before_remote_get = m_reader->remote_request.get_requests.value();
    auto before_remote_hits = m_reader->remote_request.get_hits.value();
    auto before_remote_bytes = m_reader->remote_request.get_bytes.value();
    // Snapshot owner peer metrics (reverse mode: owner serves ReadRemoteData)
    auto before_peer_read_req =
        m_owner->peer_request_metrics.read_remote_data.requests.value();
    auto before_peer_read_hits =
        m_owner->peer_request_metrics.read_remote_data.hits.value();

    // client2_ reads -> local miss -> remote read from client_
    std::vector<char> buf(data.size(), 0);
    auto get_result = client2_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get_result.has_value())
        << "Remote Get failed: " << static_cast<int>(get_result.error());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);

    // Verify reader metrics
    EXPECT_EQ(m_reader->total_request.get_requests.value(),
              before_total_get + 1);
    EXPECT_EQ(m_reader->total_request.get_hits.value(), before_total_hits + 1);
    // Local attempt: miss
    EXPECT_EQ(m_reader->local_request.get_requests.value(),
              before_local_get + 1);
    EXPECT_EQ(m_reader->local_request.get_misses.value(),
              before_local_misses + 1);
    // Remote attempt: hit
    EXPECT_EQ(m_reader->remote_request.get_requests.value(),
              before_remote_get + 1);
    EXPECT_EQ(m_reader->remote_request.get_hits.value(),
              before_remote_hits + 1);
    EXPECT_EQ(m_reader->remote_request.get_bytes.value(),
              before_remote_bytes + static_cast<int64_t>(data.size()));

    // Verify owner served the read via peer RPC
    EXPECT_EQ(m_owner->peer_request_metrics.read_remote_data.requests.value(),
              before_peer_read_req + 1);
    EXPECT_EQ(m_owner->peer_request_metrics.read_remote_data.hits.value(),
              before_peer_read_hits + 1);
}

TEST_F(P2PClientIntegrationTest, MetricGetMiss) {
    auto* m = GetP2PMetrics(client_.get());
    ASSERT_NE(m, nullptr);

    const std::string key =
        "metric_nonexistent_key_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());

    auto before_total_get = m->total_request.get_requests.value();
    auto before_total_misses = m->total_request.get_misses.value();
    auto before_total_hits = m->total_request.get_hits.value();
    auto before_local_get = m->local_request.get_requests.value();
    auto before_local_misses = m->local_request.get_misses.value();

    std::vector<char> buf(64, 0);
    auto get_result = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::OBJECT_NOT_FOUND);

    EXPECT_EQ(m->total_request.get_requests.value(), before_total_get + 1);
    EXPECT_EQ(m->total_request.get_misses.value(), before_total_misses + 1);
    EXPECT_EQ(m->total_request.get_hits.value(), before_total_hits);
    EXPECT_EQ(m->local_request.get_requests.value(), before_local_get + 1);
    EXPECT_EQ(m->local_request.get_misses.value(), before_local_misses + 1);
}

TEST_F(P2PClientIntegrationTest, MetricBatchPutGet) {
    auto* m = GetP2PMetrics(client_.get());
    ASSERT_NE(m, nullptr);

    const int batch_size = 3;
    std::vector<std::string> keys;
    std::vector<std::string> payloads;
    std::vector<std::vector<Slice>> batched_slices;
    int64_t total_bytes = 0;

    for (int i = 0; i < batch_size; ++i) {
        keys.push_back("metric_batch_key_" + std::to_string(i));
        payloads.push_back("metric_batch_payload_" + std::to_string(i));
        total_bytes += static_cast<int64_t>(payloads[i].size());
    }
    for (int i = 0; i < batch_size; ++i) {
        std::vector<Slice> s;
        s.emplace_back(
            Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()});
        batched_slices.push_back(std::move(s));
    }

    // Snapshot before batch put
    auto before_total_put_req = m->total_request.put_requests.value();
    auto before_total_put_bytes = m->total_request.put_bytes.value();
    auto before_local_put_req = m->local_request.put_requests.value();

    auto put_results =
        client_->BatchPut(keys, batched_slices, WriteRouteRequestConfig{});
    ASSERT_EQ(put_results.size(), static_cast<size_t>(batch_size));
    for (auto& r : put_results) {
        ASSERT_TRUE(r.has_value())
            << "BatchPut failed: " << static_cast<int>(r.error());
    }

    EXPECT_EQ(m->total_request.put_requests.value(),
              before_total_put_req + batch_size);
    EXPECT_EQ(m->total_request.put_bytes.value(),
              before_total_put_bytes + total_bytes);
    EXPECT_EQ(m->local_request.put_requests.value(),
              before_local_put_req + batch_size);

    // Snapshot before batch get
    auto before_total_get_req = m->total_request.get_requests.value();
    auto before_total_get_hits = m->total_request.get_hits.value();
    auto before_local_get_req = m->local_request.get_requests.value();
    auto before_local_get_hits = m->local_request.get_hits.value();

    std::vector<std::vector<char>> bufs(batch_size);
    std::vector<std::vector<void*>> all_buffers(batch_size);
    std::vector<std::vector<size_t>> all_sizes(batch_size);
    for (int i = 0; i < batch_size; ++i) {
        bufs[i].resize(payloads[i].size(), 0);
        all_buffers[i].push_back(bufs[i].data());
        all_sizes[i].push_back(bufs[i].size());
    }

    auto get_results =
        client_->BatchGet(keys, all_buffers, all_sizes, ReadRouteConfig{});
    ASSERT_EQ(get_results.size(), static_cast<size_t>(batch_size));
    for (int i = 0; i < batch_size; ++i) {
        ASSERT_TRUE(get_results[i].has_value())
            << "BatchGet failed: " << static_cast<int>(get_results[i].error());
    }

    EXPECT_EQ(m->total_request.get_requests.value(),
              before_total_get_req + batch_size);
    EXPECT_EQ(m->total_request.get_hits.value(),
              before_total_get_hits + batch_size);
    EXPECT_EQ(m->local_request.get_requests.value(),
              before_local_get_req + batch_size);
    EXPECT_EQ(m->local_request.get_hits.value(),
              before_local_get_hits + batch_size);
}

TEST_F(P2PClientIntegrationTest, MetricPutAlreadyExists) {
    auto* m = GetP2PMetrics(client_.get());
    ASSERT_NE(m, nullptr);

    const std::string key = "metric_already_exists_key";
    const std::string data = "metric_already_exists_payload";

    std::vector<Slice> slices;
    slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto first_put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(first_put.has_value());

    // Snapshot before the duplicate put
    auto before_total_put_req = m->total_request.put_requests.value();
    auto before_total_put_bytes = m->total_request.put_bytes.value();
    auto before_local_put_req = m->local_request.put_requests.value();
    auto before_local_put_bytes = m->local_request.put_bytes.value();
    auto before_remote_put_req = m->remote_request.put_requests.value();

    // Duplicate put is surfaced as success (idempotent rewrite).
    auto second_put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(second_put.has_value());

    // The already-exists write is ignored in every metric layer.
    EXPECT_EQ(m->total_request.put_requests.value(), before_total_put_req);
    EXPECT_EQ(m->total_request.put_bytes.value(), before_total_put_bytes);
    EXPECT_EQ(m->local_request.put_requests.value(), before_local_put_req);
    EXPECT_EQ(m->local_request.put_bytes.value(), before_local_put_bytes);
    EXPECT_EQ(m->remote_request.put_requests.value(), before_remote_put_req);
}

TEST_F(P2PClientIntegrationTest, MetricPutFailure) {
    auto* m = GetP2PMetrics(client_.get());
    ASSERT_NE(m, nullptr);

    // Snapshot before
    auto before_total_put_req = m->total_request.put_requests.value();
    auto before_total_put_fail = m->total_request.put_failures.value();
    auto before_local_put_req = m->local_request.put_requests.value();

    // Mismatched keys/slices sizes fail validation with INVALID_PARAMS for
    // every key.
    std::vector<std::string> keys = {"metric_fail_key_0", "metric_fail_key_1"};
    std::string payload = "metric_fail_payload";
    std::vector<std::vector<Slice>> slices;
    slices.emplace_back(std::vector<Slice>{
        Slice{const_cast<char*>(payload.data()), payload.size()}});

    auto results = client_->BatchPut(keys, slices, WriteRouteRequestConfig{});
    ASSERT_EQ(results.size(), keys.size());
    for (auto& r : results) {
        EXPECT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), ErrorCode::INVALID_PARAMS);
    }

    EXPECT_EQ(m->total_request.put_requests.value(),
              before_total_put_req + static_cast<int64_t>(keys.size()));
    EXPECT_EQ(m->total_request.put_failures.value(),
              before_total_put_fail + static_cast<int64_t>(keys.size()));
    // Validation failed before any local write was attempted.
    EXPECT_EQ(m->local_request.put_requests.value(), before_local_put_req);
}

TEST_F(P2PClientIntegrationTest, KeyRetentionMetricsLifecycle) {
    auto* m = GetP2PMetrics(client_.get());
    ASSERT_NE(m, nullptr);
    ASSERT_NE(m->key_retention, nullptr);

    const std::string key = "p2p_key_retention_metric";
    const std::string data = "retention payload";
    const auto before = m->key_retention->Snapshot();

    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    ASSERT_TRUE(
        client_->Put(key, put_slices, WriteRouteRequestConfig{}).has_value());

    // Let the key age into a non-zero bucket.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    const auto mid = m->key_retention->Snapshot();
    EXPECT_EQ(mid.live_count, before.live_count + 1);
    EXPECT_EQ(mid.removed_total, before.removed_total);

    ASSERT_TRUE(client_->RemoveLocal(key).has_value());

    // RemoveLocal ends the key's lifetime and records it in the removed
    // lifetime distribution.
    const auto after = m->key_retention->Snapshot();
    EXPECT_EQ(after.live_count, before.live_count);
    EXPECT_EQ(after.removed_total, before.removed_total + 1);

    auto serialized = client_->SerializeMetrics();
    ASSERT_TRUE(serialized.has_value());
    const std::string& text = serialized.value();
    EXPECT_NE(text.find("mooncake_p2p_key_retention_live_count"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_p2p_key_retention_removed_total"),
              std::string::npos);
    // The RemoveLocal above fed the histogram, so buckets are serialized.
    EXPECT_NE(text.find("mooncake_p2p_key_retention_removed_age_"
                        "seconds_bucket"),
              std::string::npos);
    // The quantile gauges were replaced by scrape-time histograms; the
    // live-age histogram is present only while keys are alive.
    EXPECT_EQ(text.find("mooncake_p2p_key_retention_live_age_p"),
              std::string::npos);
    EXPECT_EQ(text.find("mooncake_p2p_key_retention_removed_age_p"),
              std::string::npos);
    EXPECT_EQ(text.find("mooncake_p2p_key_retention_all_lifetime_p"),
              std::string::npos);
    // The RemoveLocal above guarantees at least one removed sample, so
    // the all-lifetime histogram is present.
    EXPECT_NE(text.find("mooncake_p2p_key_retention_all_lifetime_seconds_"
                        "bucket"),
              std::string::npos);
    EXPECT_EQ(text.find("mooncake_p2p_key_retention_live_age_seconds_"
                        "bucket") != std::string::npos,
              after.live_count > 0);
}

}  // namespace testing
}  // namespace mooncake
