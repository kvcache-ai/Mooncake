#include <glog/logging.h>
#include <gtest/gtest.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <sys/stat.h>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "client_resources.h"
#include "p2p/client/p2p_client_config_builder.h"
#include "p2p/client/peer_client.h"
#include "config.h"
#include "../real_client_test_helper.h"
#include "test_p2p_server_helpers.h"

namespace mooncake {
namespace testing {

namespace {

class ScopedMaxMrSize {
   public:
    explicit ScopedMaxMrSize(size_t value)
        : original_(globalConfig().max_mr_size) {
        globalConfig().max_mr_size = value;
    }
    ~ScopedMaxMrSize() { globalConfig().max_mr_size = original_; }

   private:
    size_t original_;
};

}  // namespace

class P2PRealClientTest : public ::testing::Test {
   protected:
    P2PClientConfig DeploymentConfig() {
        auto config = P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1", "P2PHANDSHAKE", "tcp", std::nullopt,
            master_.master_address(),
            R"({"tiers":[{"type":"DRAM","capacity":67108864,"priority":100}]})",
            4 * 1024 * 1024, nullptr, "", 0);
        config.enable_http_server = false;
        config.enable_metric_collection = false;
        config.async_sender_thread_count = 0;
        config.te_async_poll_worker_num = 2;
        return config;
    }
    static void SetUpTestSuite() {
        google::InitGoogleLogging("P2PRealClientTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void StartClients(
        size_t count = 2, const std::string& host = "127.0.0.1",
        int default_read_priority = 0,
        TransferDirectionMode direction = TransferDirectionMode::REVERSE,
        LocalTransferMode local_mode = LocalTransferMode::TE) {
        ASSERT_TRUE(master_.Start());
        P2PMasterClient observer(generate_uuid());
        ASSERT_EQ(observer.Connect(master_.master_address()), ErrorCode::OK);

        for (size_t i = 0; i < count; ++i) {
            auto config = P2PClientConfigBuilder::build_p2p_real_client(
                host, "P2PHANDSHAKE", "tcp", std::nullopt,
                master_.master_address(),
                R"({"tiers":[{"type":"DRAM","capacity":67108864,"priority":100}]})",
                /*local_buffer_size=*/16 * 1024 * 1024, nullptr, "",
                /*client_rpc_port=*/0);
            config.enable_http_server = false;
            config.enable_metric_collection = false;
            config.async_sender_thread_count = 0;
            config.te_async_poll_worker_num = 2;
            config.transfer_direction_mode = direction;
            config.local_transfer_mode = local_mode;
            // A non-default runtime policy makes dropping nullopt observable.
            config.runtime_config_json = Json::Value(Json::objectValue);
            config.runtime_config_json["write"]["remote_weight"] = 1.0;
            config.runtime_config_json["write"]["local_write_waterline"] = 0.0;
            config.runtime_config_json["read"]["p2p_config"]["priority_limit"] =
                default_read_priority;

            auto client = RealClient::create();
            clients_.push_back(client);
            ASSERT_EQ(client->setup(config), 0);

            // Discover identity through the public RealClient descriptor API,
            // then wait for the initial heartbeat to make this peer routable.
            const auto key = "ready_" + std::to_string(i);
            const std::string value = "ready";
            P2PWriteRouteConfig local;
            local.remote_weight = 0.0;
            ASSERT_EQ(client->put(key, value, local), 0);
            auto descriptors = client->get_replica_desc(key);
            ASSERT_EQ(descriptors.size(), 1);
            client_ids_.push_back(descriptors.front().client_id);
            ASSERT_TRUE(WaitForRoutableClient(observer, client_ids_.back()));
        }
    }

    void TearDown() override {
        for (auto& client : clients_) {
            EXPECT_EQ(client->tearDownAll(), 0);
        }
        clients_.clear();
        master_.Stop();
    }

    void ExpectValue(const std::shared_ptr<BufferHandle>& buffer,
                     const std::string& value) {
        ASSERT_NE(buffer, nullptr);
        EXPECT_EQ(buffer->size(), value.size());
        EXPECT_EQ(std::string(static_cast<const char*>(buffer->ptr()),
                              buffer->size()),
                  value);
    }

    void ExpectOwner(const std::string& key, const UUID& client_id,
                     size_t object_size) {
        const auto descriptors = clients_[0]->get_replica_desc(key);
        ASSERT_EQ(descriptors.size(), 1);
        EXPECT_EQ(descriptors.front().client_id, client_id);
        EXPECT_EQ(descriptors.front().object_size, object_size);
        EXPECT_NE(descriptors.front().rpc_port, 0);
    }

    static void ExpectSentinel(const BufferHandle& buffer, size_t offset = 0) {
        const auto* begin = static_cast<const unsigned char*>(buffer.ptr());
        EXPECT_TRUE(std::all_of(begin + offset, begin + buffer.size(),
                                [](unsigned char c) { return c == 0x5a; }));
    }

    void CheckReadIntoCapacities() {
        constexpr size_t capacity = 1024 * 1024;
        const std::vector<std::string> keys = {"capacity_a", "capacity_b"};
        const std::vector<std::string> values = {std::string(3000, 'a'),
                                                 std::string(5100, 'b')};
        P2PWriteRouteConfig local;
        local.remote_weight = 0.0;
        ASSERT_EQ(clients_[0]->put_batch(keys, {values[0], values[1]}, local),
                  0);
        for (size_t reader = 0; reader < clients_.size(); ++reader) {
            SCOPED_TRACE(reader == 0 ? "local" : "remote");
            auto allocator = clients_[reader]->GetBufferAllocator();
            auto first = allocator->allocate(capacity);
            auto second = allocator->allocate(capacity);
            ASSERT_TRUE(first.has_value());
            ASSERT_TRUE(second.has_value());
            std::memset(first->ptr(), 0x5a, capacity);
            ASSERT_EQ(
                clients_[reader]->get_into(keys[0], first->ptr(), capacity),
                values[0].size());
            EXPECT_EQ(
                std::memcmp(first->ptr(), values[0].data(), values[0].size()),
                0);
            ExpectSentinel(*first, values[0].size());

            std::memset(first->ptr(), 0x5a, capacity);
            std::memset(second->ptr(), 0x5a, capacity);
            EXPECT_EQ(
                clients_[reader]->batch_get_into(
                    keys, {first->ptr(), second->ptr()}, {capacity, capacity}),
                (std::vector<int64_t>{3000, 5100}));
            EXPECT_EQ(
                std::memcmp(first->ptr(), values[0].data(), values[0].size()),
                0);
            EXPECT_EQ(
                std::memcmp(second->ptr(), values[1].data(), values[1].size()),
                0);
            ExpectSentinel(*first, values[0].size());
            ExpectSentinel(*second, values[1].size());

            std::memset(first->ptr(), 0x5a, capacity);
            EXPECT_EQ(clients_[reader]->get_into(keys[0], first->ptr(),
                                                 values[0].size() - 1),
                      toInt(ErrorCode::INVALID_PARAMS));
            ExpectSentinel(*first);
            std::memset(second->ptr(), 0x5a, capacity);
            EXPECT_EQ(
                clients_[reader]->batch_get_into(
                    keys, {first->ptr(), second->ptr()},
                    {values[0].size() - 1, capacity}),
                (std::vector<int64_t>{toInt(ErrorCode::INVALID_PARAMS), 5100}));
            ExpectSentinel(*first);
            EXPECT_EQ(
                std::memcmp(second->ptr(), values[1].data(), values[1].size()),
                0);
            ExpectSentinel(*second, values[1].size());
        }
    }

    InProcP2PMaster master_;
    std::vector<std::shared_ptr<RealClient>> clients_;
    std::vector<UUID> client_ids_;
};

TEST_F(P2PRealClientTest, InvalidShmRegistrationClosesFdAndLeavesNoContext) {
    ASSERT_TRUE(master_.Start());
    auto client = RealClient::create();
    auto config = DeploymentConfig();
    ASSERT_EQ(client->setup(config), 0);
    std::unique_ptr<std::FILE, decltype(&std::fclose)> file(std::tmpfile(),
                                                            std::fclose);
    ASSERT_NE(file, nullptr);
    const int fd = fileno(file.get());
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, 4096), 0);
    struct stat original{};
    ASSERT_EQ(fstat(fd, &original), 0);
    const int passed_fd = dup(fd);
    ASSERT_GE(passed_fd, 0);
    {
        ScopedMaxMrSize limit(1);
        auto result =
            RealClientTestHelper::MapShm(*client, passed_fd, 0x100000, 4096,
                                         false, {1, 2});
        EXPECT_FALSE(result);
    }
    struct stat after{};
    const bool duplicate_still_open = fstat(passed_fd, &after) == 0 &&
                                      after.st_dev == original.st_dev &&
                                      after.st_ino == original.st_ino;
    EXPECT_FALSE(duplicate_still_open);
    if (duplicate_still_open) close(passed_fd);
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*client, {1, 2}), nullptr);
    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST_F(P2PRealClientTest, FailedLocalPoolRegistrationRejectsSetup) {
    ASSERT_TRUE(master_.Start());
    auto config = DeploymentConfig();
    auto client = RealClient::create();
    {
        ScopedMaxMrSize limit(1);
        EXPECT_NE(client->setup(config), 0);
        EXPECT_FALSE(client->is_initialized());
    }
    EXPECT_EQ(client->tearDownAll(), 0);
    client = RealClient::create();
    ASSERT_EQ(client->setup(config), 0);
    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST_F(P2PRealClientTest, SetupAndIdempotentClose) {
    ASSERT_NO_FATAL_FAILURE(StartClients(1));
    auto& client = clients_[0];
    EXPECT_TRUE(client->is_initialized());
    EXPECT_EQ(client->deployment_mode(), DeploymentMode::P2P);
    EXPECT_FALSE(client->get_hostname().empty());
    ASSERT_NE(client->GetBufferAllocator(), nullptr);
    ASSERT_TRUE(client->service_ready_internal());

    EXPECT_EQ(client->tearDownAll(), 0);
    EXPECT_FALSE(client->is_initialized());
    EXPECT_EQ(client->deployment_mode(), DeploymentMode::P2P);
    EXPECT_TRUE(client->get_hostname().empty());
    EXPECT_EQ(client->tearDownAll(), 0);
    auto readiness = client->service_ready_internal();
    ASSERT_FALSE(readiness);
    EXPECT_EQ(readiness.error(), ErrorCode::SHUTTING_DOWN);
    const std::string value = "closed";
    EXPECT_LT(client->put("after_close", value), 0);
}

TEST_F(P2PRealClientTest, LegacyInitAllReportsNotImplemented) {
    RealClient client;
    auto readiness = client.service_ready_internal();
    ASSERT_FALSE(readiness);
    EXPECT_EQ(readiness.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(client.initAll("tcp", "", 16 * 1024 * 1024),
              toInt(ErrorCode::NOT_IMPLEMENTED));
    EXPECT_FALSE(client.is_initialized());
}

TEST_F(P2PRealClientTest, OmittedConfigsUseRuntimeDefaultsForSingleAndBatch) {
    ASSERT_NO_FATAL_FAILURE(StartClients());
    const std::string value("single\0payload", 14);
    ASSERT_EQ(clients_[0]->put("default_single", value), 0);
    ExpectOwner("default_single", client_ids_[1], value.size());
    ExpectValue(clients_[0]->get_buffer("default_single"), value);
    EXPECT_EQ(clients_[0]->isExist("default_single"), 1);
    EXPECT_EQ(clients_[0]->getSize("default_single"), value.size());

    const std::vector<std::string> keys = {"default_batch_a",
                                           "default_batch_b"};
    const std::vector<std::string> values = {"first", "second batch value"};
    const std::vector<std::span<const char>> spans = {values[0], values[1]};
    ASSERT_EQ(clients_[0]->put_batch(keys, spans), 0);
    const auto buffers = clients_[0]->batch_get_buffer(keys);
    ASSERT_EQ(buffers.size(), keys.size());
    const auto descriptors = clients_[0]->batch_get_replica_desc(keys);
    ASSERT_EQ(descriptors.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        ExpectValue(buffers[i], values[i]);
        ASSERT_TRUE(descriptors.contains(keys[i]));
        ASSERT_EQ(descriptors.at(keys[i]).size(), 1);
        EXPECT_EQ(descriptors.at(keys[i]).front().client_id, client_ids_[1]);
        EXPECT_EQ(descriptors.at(keys[i]).front().object_size,
                  values[i].size());
    }
    EXPECT_EQ(clients_[0]->batchIsExist(keys), (std::vector<int>{1, 1}));
}

TEST_F(P2PRealClientTest, ExplicitConfigsOverrideRuntimeDefaults) {
    ASSERT_NO_FATAL_FAILURE(StartClients());
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    P2PReadRouteConfig read;
    read.max_candidates = 1;

    const std::string value = "explicit configuration";
    ASSERT_EQ(clients_[0]->put("explicit_single", value, local), 0);
    ExpectOwner("explicit_single", client_ids_[0], value.size());
    ExpectValue(clients_[1]->get_buffer("explicit_single", read), value);

    const std::vector<std::string> keys = {"explicit_batch_a",
                                           "explicit_batch_b"};
    const std::vector<std::string> values = {"explicit first",
                                             "explicit second"};
    const std::vector<std::span<const char>> spans = {values[0], values[1]};
    ASSERT_EQ(clients_[0]->put_batch(keys, spans, local), 0);
    const auto buffers = clients_[1]->batch_get_buffer(keys, read);
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        ExpectOwner(keys[i], client_ids_[0], values[i].size());
        ExpectValue(buffers[i], values[i]);
    }
}

TEST_F(P2PRealClientTest,
       ReadConfigsPreserveRuntimeDefaultsAndExplicitOverrides) {
    ASSERT_NO_FATAL_FAILURE(StartClients(2, "127.0.0.1", 101));
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    P2PReadRouteConfig allowed;
    allowed.priority_limit = 0;

    // Store on the other client and read each fresh key with restrictive
    // defaults first. A local hit or an already cached route bypasses the
    // master's route filter, so neither can validate read config forwarding.
    const std::string single_key = "read_config_single";
    const std::string single_value = "filtered remote single";
    ASSERT_EQ(clients_[0]->put(single_key, single_value, local), 0);
    ExpectOwner(single_key, client_ids_[0], single_value.size());
    EXPECT_EQ(clients_[1]->get_buffer(single_key), nullptr);
    ExpectValue(clients_[1]->get_buffer(single_key, allowed), single_value);

    const std::vector<std::string> keys = {"read_config_batch_a",
                                           "read_config_batch_b"};
    const std::vector<std::string> values = {"filtered first",
                                             "filtered second"};
    const std::vector<std::span<const char>> spans = {values[0], values[1]};
    ASSERT_EQ(clients_[0]->put_batch(keys, spans, local), 0);
    const auto filtered = clients_[1]->batch_get_buffer(keys);
    ASSERT_EQ(filtered.size(), keys.size());
    for (const auto& buffer : filtered) EXPECT_EQ(buffer, nullptr);

    const auto buffers = clients_[1]->batch_get_buffer(keys, allowed);
    ASSERT_EQ(buffers.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        SCOPED_TRACE(keys[i]);
        ExpectValue(buffers[i], values[i]);
    }
}

TEST_F(P2PRealClientTest, RegisteredBufferSingleAndBatchRoundTrips) {
    ASSERT_NO_FATAL_FAILURE(StartClients());
    auto allocator = clients_[0]->GetBufferAllocator();
    ASSERT_NE(allocator, nullptr);
    auto source = allocator->allocate(32);
    auto destination = allocator->allocate(32);
    ASSERT_TRUE(source.has_value());
    ASSERT_TRUE(destination.has_value());
    const std::string value = "registered buffer payload";
    std::memcpy(source->ptr(), value.data(), value.size());
    ASSERT_EQ(
        clients_[0]->put_from("buffer_single", source->ptr(), value.size()), 0);
    ASSERT_EQ(clients_[0]->get_into("buffer_single", destination->ptr(),
                                    value.size()),
              value.size());
    EXPECT_EQ(std::memcmp(destination->ptr(), value.data(), value.size()), 0);

    const std::vector<std::string> keys = {"buffer_batch"};
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    P2PReadRouteConfig read;
    read.max_candidates = 1;
    EXPECT_EQ(clients_[0]->batch_put_from(keys, {source->ptr()}, {value.size()},
                                          local),
              (std::vector<int>{0}));
    std::memset(destination->ptr(), 0, value.size());
    EXPECT_EQ(clients_[0]->batch_get_into(keys, {destination->ptr()},
                                          {value.size()}, read),
              (std::vector<int64_t>{static_cast<int64_t>(value.size())}));
    EXPECT_EQ(std::memcmp(destination->ptr(), value.data(), value.size()), 0);
}

TEST_F(P2PRealClientTest, ReadIntoAcceptsCapacityAndRejectsShortBuffers) {
    ASSERT_NO_FATAL_FAILURE(StartClients());
    CheckReadIntoCapacities();
}

TEST_F(P2PRealClientTest,
       ForwardReadIntoAcceptsCapacityAndRejectsShortBuffers) {
    ASSERT_NO_FATAL_FAILURE(
        StartClients(2, "127.0.0.1", 0, TransferDirectionMode::FORWARD));
    CheckReadIntoCapacities();
}

TEST_F(P2PRealClientTest, MemcpyReadIntoAcceptsCapacityAndRejectsShortBuffers) {
    ASSERT_NO_FATAL_FAILURE(StartClients(2, "127.0.0.1", 0,
                                         TransferDirectionMode::REVERSE,
                                         LocalTransferMode::MEMCPY));
    CheckReadIntoCapacities();
}

TEST_F(P2PRealClientTest, ScatterReadPreservesUnusedDestinationBytes) {
    ASSERT_NO_FATAL_FAILURE(StartClients());
    const std::string key = "scatter_capacity";
    const std::string value = std::string(1024, 'a') + std::string(1976, 'b');
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    ASSERT_EQ(clients_[0]->put(key, value, local), 0);
    for (size_t reader = 0; reader < clients_.size(); ++reader) {
        SCOPED_TRACE(reader == 0 ? "local" : "remote");
        auto allocator = clients_[reader]->GetBufferAllocator();
        auto first = allocator->allocate(1024);
        auto second = allocator->allocate(8192);
        auto unused = allocator->allocate(256);
        ASSERT_TRUE(first.has_value());
        ASSERT_TRUE(second.has_value());
        ASSERT_TRUE(unused.has_value());
        for (const auto* buffer : {&*first, &*second, &*unused}) {
            std::memset(buffer->ptr(), 0x5a, buffer->size());
        }
        EXPECT_EQ(clients_[reader]->batch_get_into_multi_buffers(
                      {key}, {{first->ptr(), second->ptr(), unused->ptr()}},
                      {{1024, 8192, 256}}, false),
                  (std::vector<int>{3000}));
        EXPECT_EQ(std::memcmp(first->ptr(), value.data(), 1024), 0);
        EXPECT_EQ(std::memcmp(second->ptr(), value.data() + 1024, 1976), 0);
        ExpectSentinel(*second, 1976);
        ExpectSentinel(*unused);

        std::memset(first->ptr(), 0x5a, first->size());
        std::memset(second->ptr(), 0x5a, second->size());
        EXPECT_EQ(
            clients_[reader]->batch_get_into_multi_buffers(
                {key}, {{first->ptr(), second->ptr()}}, {{1024, 1975}}, false),
            (std::vector<int>{toInt(ErrorCode::INVALID_PARAMS)}));
        ExpectSentinel(*first);
        ExpectSentinel(*second);
    }
}

TEST_F(P2PRealClientTest, ScatterReadRejectsMismatchedInnerSizes) {
    ASSERT_NO_FATAL_FAILURE(StartClients(1));
    const std::string key = "scatter_shape";
    const std::string value = "shape validation";
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    ASSERT_EQ(clients_[0]->put(key, value, local), 0);
    auto buffer = clients_[0]->GetBufferAllocator()->allocate(4096);
    ASSERT_TRUE(buffer.has_value());
    std::memset(buffer->ptr(), 0x5a, buffer->size());

    for (const auto& sizes :
         std::vector<std::vector<size_t>>{{}, {4096, 4096}}) {
        SCOPED_TRACE(sizes.size());
        EXPECT_EQ(clients_[0]->batch_get_into_multi_buffers(
                      {key}, {{buffer->ptr()}}, {sizes}, false),
                  (std::vector<int>{toInt(ErrorCode::INVALID_PARAMS)}));
        ExpectSentinel(*buffer);
    }

    // Reject the entire batch before writing even the valid first entry.
    auto native = RealClientTestHelper::NativeClient(*clients_[0]);
    auto results = native->BatchGet(
        {key, key}, {{buffer->ptr()}, {buffer->ptr()}}, {{4096}, {}});
    ASSERT_EQ(results.size(), 2);
    for (const auto& result : results) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
    ExpectSentinel(*buffer);
}

TEST_F(P2PRealClientTest, PeerReadRequiresExactTransferLength) {
    ASSERT_NO_FATAL_FAILURE(StartClients(1));
    const std::string key = "peer_capacity";
    const std::string value(3000, 'p');
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    ASSERT_EQ(clients_[0]->put(key, value, local), 0);
    const auto descriptors = clients_[0]->get_replica_desc(key);
    ASSERT_EQ(descriptors.size(), 1);
    PeerClient peer;
    ASSERT_TRUE(peer.Connect(descriptors.front().ip_address + ":" +
                             std::to_string(descriptors.front().rpc_port)));
    ClientResources receiver;
    ASSERT_EQ(receiver.InitTransferEngine("127.0.0.1", "P2PHANDSHAKE", "tcp",
                                          std::nullopt),
              ErrorCode::OK);
    ASSERT_EQ(receiver.InitLocalBufferAllocator(2 * 1024 * 1024, "tcp", false),
              ErrorCode::OK);
    ASSERT_NE(receiver.GetBufferAllocator(), nullptr);
    auto destination = receiver.GetBufferAllocator()->allocate(1024 * 1024);
    ASSERT_TRUE(destination.has_value());
    RemoteReadRequest request;
    request.key = key;
    request.dest_buffers = {{receiver.GetTransferEngine()->getLocalIpAndPort(),
                             reinterpret_cast<uint64_t>(destination->ptr()),
                             value.size()}};
    // Public Get clips capacity using metadata. The peer RPC retains exact
    // lengths so a stale route cannot report bytes that the owner never wrote.
    std::memset(destination->ptr(), 0x5a, destination->size());
    ASSERT_TRUE(peer.ReadRemoteData(request));
    EXPECT_EQ(std::memcmp(destination->ptr(), value.data(), value.size()), 0);
    ExpectSentinel(*destination, value.size());

    for (const auto size : {value.size() - 1, destination->size()}) {
        SCOPED_TRACE(size);
        std::memset(destination->ptr(), 0x5a, destination->size());
        request.dest_buffers.front().size = size;
        auto result = peer.ReadRemoteData(request);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        ExpectSentinel(*destination);
    }
}

TEST_F(P2PRealClientTest, StaleReadRouteCannotReportUnwrittenBytes) {
    ASSERT_NO_FATAL_FAILURE(StartClients());
    const std::string key = "replaced_object_size";
    const std::string original(3000, 'a');
    const std::string replacement(1000, 'b');
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    ASSERT_EQ(clients_[0]->put(key, original, local), 0);
    auto destination = clients_[1]->GetBufferAllocator()->allocate(4096);
    ASSERT_TRUE(destination.has_value());
    // Populate the reader's route cache before replacing the owner's object.
    ASSERT_EQ(
        clients_[1]->get_into(key, destination->ptr(), destination->size()),
        original.size());
    ASSERT_EQ(clients_[0]->removeLocal(key), 0);
    ASSERT_EQ(clients_[0]->put(key, replacement, local), 0);
    std::memset(destination->ptr(), 0x5a, destination->size());
    const int64_t size =
        clients_[1]->get_into(key, destination->ptr(), destination->size());
    EXPECT_TRUE(size < 0 || size == static_cast<int64_t>(replacement.size()));
    if (size < 0) {
        ExpectSentinel(*destination);
    } else if (size == static_cast<int64_t>(replacement.size())) {
        EXPECT_EQ(std::memcmp(destination->ptr(), replacement.data(),
                              replacement.size()),
                  0);
        ExpectSentinel(*destination, replacement.size());
    }

    auto buffer = clients_[1]->get_buffer(key);
    if (buffer) {
        ExpectValue(buffer, replacement);
    }
}

TEST_F(P2PRealClientTest, IPv6LoopbackPutGet) {
    // TE reads its address-family setting once per process. CTest runs this
    // case separately with MC_USE_IPV6=1, without IPv4 fixtures in that
    // process.
    const char* use_ipv6 = std::getenv("MC_USE_IPV6");
    if (!use_ipv6 || std::string(use_ipv6) != "1") {
        GTEST_SKIP()
            << "Run this case in a separate process with MC_USE_IPV6=1";
    }
    const int fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (fd < 0) {
        const int error = errno;
        LOG(WARNING) << "IPv6 socket unavailable: " << std::strerror(error);
        GTEST_SKIP() << "IPv6 socket unavailable: " << std::strerror(error);
    }
    sockaddr_in6 address{};
    address.sin6_family = AF_INET6;
    address.sin6_addr = in6addr_loopback;
    const int bind_result =
        bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    const int error = errno;
    close(fd);
    if (bind_result != 0) {
        LOG(WARNING) << "IPv6 loopback unavailable: " << std::strerror(error);
        GTEST_SKIP() << "IPv6 loopback unavailable: " << std::strerror(error);
    }

    ASSERT_NO_FATAL_FAILURE(StartClients(1, "[::1]"));
    P2PWriteRouteConfig local;
    local.remote_weight = 0.0;
    const std::string value = "P2P RealClient IPv6 loopback";
    ASSERT_EQ(clients_[0]->put("ipv6_loopback", value, local), 0);
    ExpectValue(clients_[0]->get_buffer("ipv6_loopback"), value);
    const auto descriptors = clients_[0]->get_replica_desc("ipv6_loopback");
    ASSERT_EQ(descriptors.size(), 1);
    EXPECT_EQ(descriptors.front().ip_address, "::1");
    EXPECT_EQ(descriptors.front().object_size, value.size());
}

}  // namespace testing
}  // namespace mooncake
