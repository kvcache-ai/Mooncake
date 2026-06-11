/**
 * @file p2p_client_integration_test.cpp
 * @brief Cross-process integration tests for P2PClientService.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "p2p_client_process_test_helper.h"
#include "p2p_client_service.h"
#include "types.h"

namespace mooncake {
namespace testing {

class P2PClientIntegrationTest : public ::testing::Test {
   protected:
    static std::shared_ptr<P2PClientService> CreateP2PClient(
        const std::string& host_name, uint32_t rpc_port = 0,
        const std::string& local_transfer_mode = "te",
        TransferDirectionMode transfer_direction_mode =
            TransferDirectionMode::REVERSE) {
        if (rpc_port == 0) {
            rpc_port = getFreeTcpPort();
        }

        auto config = ClientConfigBuilder::build_p2p_real_client(
            host_name, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers": [{"type": "DRAM", "capacity": 67108864, "priority": 100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port);
        config.local_transfer_mode =
            local_transfer_mode == "te" ? LocalTransferMode::TE
                                        : LocalTransferMode::MEMCPY;
        config.transfer_direction_mode = transfer_direction_mode;
        config.enable_http_server = false;
        config.http_port = 0;

        auto client = std::make_shared<P2PClientService>(
            config.metadata_connstring, config.http_port,
            config.enable_http_server, config.labels);
        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK)
            << "Init failed: " << static_cast<int>(err);
        if (err != ErrorCode::OK) {
            return nullptr;
        }
        return client;
    }

    static void SetUpTestSuite() {
        google::InitGoogleLogging("P2PClientIntegrationTest");
        FLAGS_logtostderr = 1;

        ASSERT_TRUE(master_process_.Start()) << "Failed to start P2P master";
        master_address_ = master_process_.master_address();
        LOG(INFO) << "P2P master started at " << master_address_;

        client_ = CreateP2PClient("localhost:18801");
        ASSERT_NE(client_, nullptr);
        LOG(INFO) << "P2P client created and registered successfully";
    }

    static void TearDownTestSuite() {
        client_.reset();
        master_process_.Stop();
        google::ShutdownGoogleLogging();
    }

    static ScopedP2PMasterProcess master_process_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client_;
};

ScopedP2PMasterProcess P2PClientIntegrationTest::master_process_;
std::string P2PClientIntegrationTest::master_address_;
std::shared_ptr<P2PClientService> P2PClientIntegrationTest::client_ = nullptr;

TEST_F(P2PClientIntegrationTest, PutAndGetLocal) {
    const std::string key = "p2p_local_put_get";
    const std::string data = "Hello P2P world!";

    auto metrics_before = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_before.has_value());

    std::vector<Slice> put_slices{
        Slice{const_cast<char*>(data.data()), data.size()}};
    auto put_result = client_->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value())
        << "Put failed: " << static_cast<int>(put_result.error());

    auto metrics_after_put = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_after_put.has_value());
    EXPECT_TRUE(metrics_after_put.value().find(
                    "mooncake_p2p_local_put_requests_total 1") !=
                std::string::npos);
    EXPECT_TRUE(metrics_after_put.value().find(
                    "mooncake_p2p_local_put_bytes_total " +
                    std::to_string(data.size())) != std::string::npos);

    std::vector<char> buf(data.size(), 0);
    auto query = client_->Query(key);
    ASSERT_TRUE(query.has_value())
        << "Query failed: " << static_cast<int>(query.error());

    auto get_result = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get_result.has_value())
        << "Get failed: " << static_cast<int>(get_result.error());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data);

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

TEST_F(P2PClientIntegrationTest, IsExist) {
    const std::string key = "p2p_exist_test";
    const std::string data = "exist_data";

    auto exist_before = client_->IsExist(key);
    ASSERT_TRUE(exist_before.has_value());
    EXPECT_FALSE(exist_before.value());

    std::vector<Slice> slices{
        Slice{const_cast<char*>(data.data()), data.size()}};
    auto put_result = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put_result.has_value());

    auto exist_after = client_->IsExist(key);
    ASSERT_TRUE(exist_after.has_value());
    EXPECT_TRUE(exist_after.value());
}

TEST_F(P2PClientIntegrationTest, GetMissMetrics) {
    const std::string key = "p2p_nonexistent_key_for_miss_test";

    auto metrics_before = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_before.has_value());

    std::vector<char> buf(100, 0);
    auto get_result = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::OBJECT_NOT_FOUND);

    auto metrics_after = client_->SerializeMetrics();
    ASSERT_TRUE(metrics_after.has_value());
    EXPECT_TRUE(
        metrics_after.value().find("mooncake_p2p_local_get_misses_total") !=
        std::string::npos);
}

TEST_F(P2PClientIntegrationTest, QueryReturnsReplicas) {
    const std::string key = "p2p_query_replica";
    const std::string data = "replica_data";

    std::vector<Slice> slices{
        Slice{const_cast<char*>(data.data()), data.size()}};
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value());

    auto query = client_->Query(key);
    ASSERT_TRUE(query.has_value())
        << "Query failed: " << static_cast<int>(query.error());
    auto& replicas = query.value()->replicas;
    EXPECT_GE(replicas.size(), 1u);
}

TEST_F(P2PClientIntegrationTest, BatchIsExist) {
    std::vector<std::string> existing_keys;
    for (int i = 0; i < 3; ++i) {
        std::string key = "p2p_batch_exist_" + std::to_string(i);
        existing_keys.push_back(key);
        std::string data = "data_" + std::to_string(i);
        std::vector<Slice> slices{
            Slice{const_cast<char*>(data.data()), data.size()}};
        auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
        ASSERT_TRUE(put.has_value());
    }

    std::vector<std::string> query_keys = existing_keys;
    query_keys.push_back("p2p_batch_exist_NOT_1");
    query_keys.push_back("p2p_batch_exist_NOT_2");

    auto results = client_->BatchIsExist(query_keys);
    ASSERT_EQ(results.size(), query_keys.size());

    for (size_t i = 0; i < existing_keys.size(); ++i) {
        EXPECT_TRUE(results[i].has_value());
        EXPECT_TRUE(results[i].value());
    }
    for (size_t i = existing_keys.size(); i < query_keys.size(); ++i) {
        EXPECT_TRUE(results[i].has_value());
        EXPECT_FALSE(results[i].value());
    }
}

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
        batched_slices.push_back(
            {Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()}});
    }

    auto put_results =
        client_->BatchPut(keys, batched_slices, WriteRouteRequestConfig{});
    ASSERT_EQ(put_results.size(), static_cast<size_t>(batch_size));
    for (auto& r : put_results) {
        EXPECT_TRUE(r.has_value())
            << "BatchPut element failed: " << static_cast<int>(r.error());
    }

    auto query_results = client_->BatchQuery(keys);
    ASSERT_EQ(query_results.size(), static_cast<size_t>(batch_size));
    for (size_t i = 0; i < query_results.size(); ++i) {
        EXPECT_TRUE(query_results[i].has_value())
            << "BatchQuery failed for key: " << keys[i]
            << ", error: " << static_cast<int>(query_results[i].error());
    }
}

TEST_F(P2PClientIntegrationTest, RemoteBatchPutAndBatchGet) {
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

        for (int i = 0; i < batch_size; ++i) {
            std::string key_prefix = "p2p_remote_batch_" + mode + "_";
            keys.push_back(key_prefix + "key_" + std::to_string(i));
            payloads.push_back(key_prefix + "payload_" + std::to_string(i));
        }
        for (int i = 0; i < batch_size; ++i) {
            batched_slices.push_back(
                {Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()}});
        }

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

        auto allocator = ClientBufferAllocator::create(8 * 1024 * 1024);
        ASSERT_NE(allocator, nullptr);
        auto batch_get_handles =
            remote_writer->BatchGet(keys, allocator, ReadRouteConfig{});
        ASSERT_EQ(batch_get_handles.size(), static_cast<size_t>(batch_size));
        for (int i = 0; i < batch_get_handles.size(); ++i) {
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

TEST_F(P2PClientIntegrationTest, PutOverwrite) {
    const std::string key = "p2p_overwrite";
    const std::string data1 = "version_1";
    const std::string data2 = "version_2_longer";

    {
        std::vector<Slice> s{
            Slice{const_cast<char*>(data1.data()), data1.size()}};
        auto r = client_->Put(key, s, WriteRouteRequestConfig{});
        ASSERT_TRUE(r.has_value());
    }

    {
        std::vector<Slice> s{
            Slice{const_cast<char*>(data2.data()), data2.size()}};
        auto r = client_->Put(key, s, WriteRouteRequestConfig{});
        ASSERT_TRUE(r.has_value());
    }

    std::vector<char> buf(data1.size(), 0);
    auto query = client_->Query(key);
    ASSERT_TRUE(query.has_value());

    auto get = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_TRUE(get.has_value());
    EXPECT_EQ(std::string(buf.data(), buf.size()), data1);
}

TEST_F(P2PClientIntegrationTest, RemoveAllLocalEmpty) {
    auto baseline = client_->RemoveAllLocal();
    ASSERT_TRUE(baseline.has_value());

    auto result = client_->RemoveAllLocal();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 0);
}

TEST_F(P2PClientIntegrationTest, RemoveAllLocalRemovesPutKeys) {
    auto baseline = client_->RemoveAllLocal();
    ASSERT_TRUE(baseline.has_value());

    const int kNumKeys = 3;
    std::vector<std::string> keys;
    std::vector<std::string> payloads;
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("p2p_remove_all_local_basic_" + std::to_string(i));
        payloads.push_back("payload_basic_" + std::to_string(i));
    }

    for (int i = 0; i < kNumKeys; ++i) {
        std::vector<Slice> slices{
            Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()}};
        auto put = client_->Put(keys[i], slices, WriteRouteRequestConfig{});
        ASSERT_TRUE(put.has_value());
    }

    auto removed = client_->RemoveAllLocal();
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed.value(), static_cast<long>(kNumKeys));

    for (int i = 0; i < kNumKeys; ++i) {
        std::vector<char> buf(payloads[i].size(), 0);
        auto get = client_->Get(keys[i], {(void*)buf.data()}, {buf.size()});
        ASSERT_FALSE(get.has_value());
        EXPECT_EQ(get.error(), ErrorCode::OBJECT_NOT_FOUND);
    }
}

TEST_F(P2PClientIntegrationTest, RemoveAllLocalIdempotent) {
    auto baseline = client_->RemoveAllLocal();
    ASSERT_TRUE(baseline.has_value());

    const int kNumKeys = 4;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "p2p_remove_all_local_idem_" + std::to_string(i);
        std::string data = "idem_payload_" + std::to_string(i);
        std::vector<Slice> slices{
            Slice{const_cast<char*>(data.data()), data.size()}};
        auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
        ASSERT_TRUE(put.has_value());
    }

    auto first = client_->RemoveAllLocal();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first.value(), 4);

    auto second = client_->RemoveAllLocal();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second.value(), 0);
}

TEST_F(P2PClientIntegrationTest, RemoveLocalAfterPut) {
    const std::string key = "p2p_remove_local_after_put";
    const std::string data = "to_be_removed";

    std::vector<Slice> slices{
        Slice{const_cast<char*>(data.data()), data.size()}};
    auto put = client_->Put(key, slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value());

    auto removed = client_->RemoveLocal(key);
    ASSERT_TRUE(removed.has_value());

    std::vector<char> buf(data.size(), 0);
    auto get = client_->Get(key, {(void*)buf.data()}, {buf.size()});
    ASSERT_FALSE(get.has_value());
    EXPECT_EQ(get.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(P2PClientIntegrationTest, RemoveLocalNonExistent) {
    const std::string key = "p2p_remove_local_never_put_xyz";
    auto removed = client_->RemoveLocal(key);
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(removed.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(P2PClientIntegrationTest, QueryNonExistentKey) {
    auto q = client_->Query("totally_nonexistent_key_xyz");
    ASSERT_FALSE(q.has_value());
    EXPECT_EQ(q.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(P2PClientIntegrationTest, LargePutGet) {
    const std::string key = "p2p_large_data";
    const size_t size = 4 * 1024 * 1024;
    std::vector<char> payload(size, 'X');

    std::vector<Slice> put_slices{{payload.data(), payload.size()}};
    auto put = client_->Put(key, put_slices, WriteRouteRequestConfig{});
    ASSERT_TRUE(put.has_value())
        << "Large Put failed: " << static_cast<int>(put.error());

    std::vector<char> read_buf(size, 0);
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

    EXPECT_TRUE(te_client->unregisterLocalMemory(part1.data(), false).has_value());
    EXPECT_TRUE(te_client->unregisterLocalMemory(part2.data(), false).has_value());
    EXPECT_TRUE(
        te_client->unregisterLocalMemory(read_buf.data(), false).has_value());
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

    EXPECT_TRUE(
        te_client->unregisterLocalMemory(allocator->getBase(), false).has_value());
    EXPECT_TRUE(te_client->unregisterLocalMemory(payload.data(), false).has_value());
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
        route.allow_local = false;
        route.prefer_local = false;
        route.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;

        std::vector<Slice> slices{
            Slice{const_cast<char*>(payload.data()), payload.size()}};
        auto put_res = remote_writer->Put(key, slices, route);
        ASSERT_TRUE(put_res.has_value())
            << "Forward Put failed mode=" << mode
            << " err=" << static_cast<int>(put_res.error());

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

        for (int i = 0; i < batch_size; ++i) {
            std::string key_prefix = "p2p_fwd_remote_batch_" + mode + "_";
            keys.push_back(key_prefix + "key_" + std::to_string(i));
            payloads.push_back(key_prefix + "payload_" + std::to_string(i));
        }
        for (int i = 0; i < batch_size; ++i) {
            batched_slices.push_back(
                {Slice{const_cast<char*>(payloads[i].data()), payloads[i].size()}});
        }

        WriteRouteRequestConfig remote_put_config;
        remote_put_config.allow_local = false;
        remote_put_config.prefer_local = false;
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

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    mooncake::testing::SetP2PClientIntegrationTestBinaryPath(argv[0]);
    if (auto child_exit =
            mooncake::testing::MaybeRunP2PClientIntegrationTestChildProcess(
                argc, argv);
        child_exit.has_value()) {
        return *child_exit;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
