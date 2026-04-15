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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

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
        const std::string& local_transfer_mode = "te") {
        if (rpc_port == 0) rpc_port = getFreeTcpPort();

        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port);
        if (local_transfer_mode == "te") {
            config.local_transfer_mode =
                P2PClientConfig::LocalTransferMode::TE;
        } else {
            config.local_transfer_mode =
                P2PClientConfig::LocalTransferMode::MEMCPY;
        }

        auto client = std::make_shared<P2PClientService>(
            config.local_ip, config.te_port, config.metadata_connstring,
            config.labels);

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

        // 2. Create a client
        client_ = CreateP2PClient("localhost:18801");
        ASSERT_NE(client_, nullptr);
        LOG(INFO) << "P2P client created and registered successfully";
    }

    static void TearDownTestSuite() {
        client_.reset();
        master_.Stop();
        google::ShutdownGoogleLogging();
    }

    // Shared across all tests in this suite
    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client_;
};

// Static member definitions
InProcP2PMaster P2PClientIntegrationTest::master_;
std::string P2PClientIntegrationTest::master_address_;
std::shared_ptr<P2PClientService> P2PClientIntegrationTest::client_ = nullptr;

// ============================================================================
// Put / Get (local WRITE_LOCAL mode)
// ============================================================================

TEST_F(P2PClientIntegrationTest, PutAndGetLocal) {
    const std::string key = "p2p_local_put_get";
    const std::string data = "Hello P2P world!";

    // Put
    std::vector<Slice> put_slices;
    put_slices.emplace_back(Slice{const_cast<char*>(data.data()), data.size()});
    auto put_result = client_->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

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

        std::string host =
            "localhost:" + std::to_string(getFreeTcpPort());
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
            slices.emplace_back(
                Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()});
            batched_slices.push_back(std::move(slices));
        }

        // Force write route to exclude local candidate so the writer must
        // execute remote Put RPCs.
        WriteRouteRequestConfig remote_put_config;
        remote_put_config.allow_local = false;
        remote_put_config.prefer_local = false;
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
                << ", error: " << static_cast<int>(batch_get_results[i].error());
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
                << ", error: " << static_cast<int>(batch_get_handles[i].error());
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
    auto reg1 =
        te_client->RegisterLocalMemory(part1.data(), part1.size(), "*", false,
                                       false);
    auto reg2 =
        te_client->RegisterLocalMemory(part2.data(), part2.size(), "*", false,
                                       false);
    auto reg3 = te_client->RegisterLocalMemory(read_buf.data(), read_buf.size(),
                                               "*", false, false);
    ASSERT_TRUE(reg1.has_value());
    ASSERT_TRUE(reg2.has_value());
    ASSERT_TRUE(reg3.has_value());

    std::vector<Slice> put_slices = {
        Slice{part1.data(), part1.size()}, Slice{part2.data(), part2.size()}};
    auto put_result = te_client->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

    auto get_result = te_client->Get(key, {(void*)read_buf.data()},
                                     {read_buf.size()});
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

    auto reg_src = te_client->RegisterLocalMemory(payload.data(), payload.size(),
                                                  "*", false, false);
    ASSERT_TRUE(reg_src.has_value());

    std::vector<Slice> put_slices = {{payload.data(), payload.size()}};
    auto put_result = te_client->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

    auto allocator = ClientBufferAllocator::create(payload.size());
    ASSERT_NE(allocator, nullptr);
    auto reg_dst = te_client->RegisterLocalMemory(allocator->getBase(),
                                                  allocator->size(), "*",
                                                  false, false);
    ASSERT_TRUE(reg_dst.has_value());

    auto get_result = te_client->Get(key, allocator, ReadRouteConfig{});
    ASSERT_TRUE(get_result.has_value())
        << "Get(buffer) failed: " << static_cast<int>(get_result.error());

    auto buffer_handle = get_result.value();
    ASSERT_NE(buffer_handle, nullptr);
    ASSERT_EQ(buffer_handle->size(), payload.size());
    EXPECT_EQ(0, std::memcmp(buffer_handle->ptr(), payload.data(), payload.size()));

    auto unreg_dst = te_client->unregisterLocalMemory(allocator->getBase(), false);
    auto unreg_src = te_client->unregisterLocalMemory(payload.data(), false);
    EXPECT_TRUE(unreg_dst.has_value());
    EXPECT_TRUE(unreg_src.has_value());
}

}  // namespace testing
}  // namespace mooncake
