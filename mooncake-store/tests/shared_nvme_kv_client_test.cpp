#include "client_service.h"
#include "shared_nvme_kv_backend.h"
#include "test_server_helpers.h"

#include <gtest/gtest.h>
#include <array>
#include <functional>
#include <map>
#include <mutex>

#include "nvme_kv/connector_test_peer.h"

namespace mooncake::testing {
namespace {

// Independent executor instances model clients connecting to one external NS.
struct KvTarget {
    std::mutex mutex;
    std::map<NvmeKvCommandExecutor::PhysicalKey, std::string> values;
    ErrorCode store_error = ErrorCode::OK;
    ErrorCode read_error = ErrorCode::OK;
    std::function<void()> before_store;
    std::function<void()> before_read;
    size_t reads = 0;
};

class TestExecutor : public NvmeKvCommandExecutor {
   public:
    explicit TestExecutor(std::shared_ptr<KvTarget> target)
        : target_(std::move(target)) {}

    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value) override {
        if (target_->before_store) {
            target_->before_store();
        }

        std::lock_guard lock(target_->mutex);
        if (target_->store_error != ErrorCode::OK) {
            return tl::make_unexpected(target_->store_error);
        }
        if (!target_->values.emplace(key, std::move(value)).second) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        return {};
    }

    tl::expected<std::string, ErrorCode> Retrieve(const PhysicalKey& key,
                                                  uint32_t) const override {
        if (target_->before_read) {
            target_->before_read();
        }

        std::lock_guard lock(target_->mutex);
        ++target_->reads;
        if (target_->read_error != ErrorCode::OK) {
            return tl::make_unexpected(target_->read_error);
        }
        auto it = target_->values.find(key);
        if (it == target_->values.end()) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        return it->second;
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey&) override {
        ADD_FAILURE()
            << "Shared KV reads and writes must not issue physical Delete";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }

    const Capabilities& GetCapabilities() const override { return caps_; }

   private:
    std::shared_ptr<KvTarget> target_;
    Capabilities caps_;
};

std::shared_ptr<NvmeKvObjectStore> MakeObjectStore(
    std::unique_ptr<NvmeKvCommandExecutor> executor) {
    auto connector = std::make_unique<NvmeKvConnector>();
    test::NvmeKvConnectorTestPeer::SetExecutor(*connector, std::move(executor));
    return SharedNvmeKvBackend::CreateObjectStore(std::move(connector));
}

class SharedNvmeKvClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto config = InProcMasterConfigBuilder().build();
        ASSERT_TRUE(master_.Start(config));
        writer_ = NewClient("127.0.0.1:19001");
        reader_ = NewClient("127.0.0.1:19002");
        ASSERT_NE(writer_, nullptr);
        ASSERT_NE(reader_, nullptr);
        shared_.replica_num = 0;
        shared_.shared_nvme_kv_replica_num = 1;
    }

    void TearDown() override {
        writer_.reset();
        reader_.reset();
        master_.Stop();
    }

    std::shared_ptr<Client> NewClient(const std::string& hostname) {
        auto client = Client::Create(hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_.master_address());
        if (!client) {
            return nullptr;
        }
        (*client)->SetSharedKvBackends(
            {MakeObjectStore(std::make_unique<TestExecutor>(target_))});
        return *client;
    }

    auto Put(const std::string& key, const ReplicateConfig& config) {
        std::vector<Slice> slices{{value_.data(), value_.size()}};
        return writer_->Put(key, slices, config);
    }

    InProcMaster master_;
    std::string value_ = "shared-value";
    std::shared_ptr<KvTarget> target_ = std::make_shared<KvTarget>();
    std::shared_ptr<Client> writer_, reader_;
    ReplicateConfig shared_;
};

TEST_F(SharedNvmeKvClientTest, ReadsAfterWriterExitThroughSingleAndBatchApis) {
    ASSERT_TRUE(Put("a", shared_));
    ASSERT_TRUE(Put("b", shared_));
    writer_.reset();
    std::string a(value_.size(), '\0'), b(value_.size(), '\0');
    std::vector<Slice> single{{a.data(), 1}, {a.data() + 1, a.size() - 1}};
    ASSERT_TRUE(reader_->Get("a", single));
    EXPECT_EQ(a, value_);
    std::unordered_map<std::string, std::vector<Slice>> slices{
        {"a", {{a.data(), a.size()}}},
        {"b", {{b.data(), 1}, {b.data() + 1, b.size() - 1}}}};
    auto result = reader_->BatchGet({"a", "b"}, slices);
    ASSERT_EQ(result.size(), 2);
    ASSERT_TRUE(result[0]);
    ASSERT_TRUE(result[1]);
    EXPECT_EQ(a, value_);
    EXPECT_EQ(b, value_);
}

TEST_F(SharedNvmeKvClientTest, ReadsAcceptExcessContiguousCapacity) {
    ASSERT_TRUE(Put("a", shared_));
    ASSERT_TRUE(Put("b", shared_));
    std::string output(value_.size() + 16, 'x');
    std::vector<Slice> slices{{output.data(), 4},
                              {output.data() + 4, output.size() - 4}};

    ASSERT_TRUE(reader_->Get("a", slices));
    EXPECT_EQ(output.substr(0, value_.size()), value_);
    EXPECT_EQ(output.substr(value_.size()), std::string(16, 'x'));

    output.assign(output.size(), 'x');
    std::string too_small(value_.size() - 1, 'x');
    std::unordered_map<std::string, std::vector<Slice>> outputs{
        {"a", {{output.data(), output.size()}}},
        {"b", {{too_small.data(), too_small.size()}}}};
    auto results = reader_->BatchGet({"a", "b"}, outputs);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results[0]);
    ASSERT_FALSE(results[1]);
    EXPECT_EQ(results[1].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(output.substr(0, value_.size()), value_);
    EXPECT_EQ(output.substr(value_.size()), std::string(16, 'x'));
    EXPECT_EQ(too_small, std::string(value_.size() - 1, 'x'));
    EXPECT_TRUE(reader_->Query("b"));
}

TEST_F(SharedNvmeKvClientTest, SetupRejectsInvalidSharedPoolConfiguration) {
    const char* previous = ::getenv("MOONCAKE_SHARED_NVME_KV_CONFIG");
    const auto saved =
        previous ? std::optional<std::string>(previous) : std::nullopt;
    ::setenv("MOONCAKE_SHARED_NVME_KV_CONFIG",
             "/nonexistent/shared-kv-config.json", 1);
    auto client = Client::Create("127.0.0.1:19003", "P2PHANDSHAKE", "tcp",
                                 std::nullopt, master_.master_address());
    if (saved)
        ::setenv("MOONCAKE_SHARED_NVME_KV_CONFIG", saved->c_str(), 1);
    else
        ::unsetenv("MOONCAKE_SHARED_NVME_KV_CONFIG");
    EXPECT_FALSE(client);
}

TEST_F(SharedNvmeKvClientTest, BatchUsesCommonStagingAndFinalizesEachResult) {
    auto mixed = shared_;
    mixed.replica_num = 1;  // No memory segments: shared KV can still succeed.
    std::vector<std::vector<Slice>> inputs(3, {{value_.data(), value_.size()}});
    size_t stages = 0, stores = 0;
    target_->before_store = [&] {
        target_->store_error =
            ++stores == 2 ? ErrorCode::TRANSFER_FAIL : ErrorCode::OK;
    };
    auto results = writer_->BatchPut(
        {"ok", "stage-failure", "store-failure"}, inputs, mixed,
        [&](const std::vector<Slice>& slices)
            -> tl::expected<std::vector<Slice>, ErrorCode> {
            if (++stages == 2) {
                return tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
            return slices;
        });
    ASSERT_EQ(results.size(), 3);
    EXPECT_TRUE(results[0]);
    EXPECT_FALSE(results[1]);
    EXPECT_FALSE(results[2]);
    EXPECT_EQ(stages, 3);
    EXPECT_EQ(stores, 2);
    EXPECT_TRUE(reader_->Query("ok"));
    EXPECT_FALSE(reader_->Query("stage-failure"));
    EXPECT_FALSE(reader_->Query("store-failure"));
    target_->before_store = {};
    target_->store_error = ErrorCode::OK;
    // Failed starts were revoked with their allocation IDs; retry gets new IDs.
    results = writer_->BatchPut({"ok", "stage-failure", "store-failure"},
                                inputs, mixed);
    for (const auto& result : results) {
        EXPECT_TRUE(result);
    }
}

TEST_F(SharedNvmeKvClientTest, PutRejectsConflictingPhysicalValue) {
    target_->values.emplace(EncodeSharedNvmeKvPhysicalKey(1),
                            "conflicting-physical-value");
    auto result = Put("conflict", shared_);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_WRITE);
    EXPECT_FALSE(reader_->Query("conflict"));
    EXPECT_EQ(target_->values.at(EncodeSharedNvmeKvPhysicalKey(1)),
              "conflicting-physical-value");
}

TEST_F(SharedNvmeKvClientTest, BatchPreservesPhysicalConflictErrors) {
    target_->values.emplace(EncodeSharedNvmeKvPhysicalKey(1),
                            "conflicting-physical-value");
    std::vector<std::vector<Slice>> inputs(2, {{value_.data(), value_.size()}});
    auto results = writer_->BatchPut({"conflict", "ok"}, inputs, shared_);
    ASSERT_EQ(results.size(), 2);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::INVALID_WRITE);
    ASSERT_TRUE(results[1]);
    EXPECT_FALSE(reader_->Query("conflict"));
    EXPECT_TRUE(reader_->Query("ok"));

    // An existing logical object remains an idempotent success without a write.
    std::string replacement = "replacement";
    std::vector<std::vector<Slice>> repeated_input{
        {{replacement.data(), replacement.size()}}};
    auto repeated = writer_->BatchPut({"ok"}, repeated_input, shared_);
    ASSERT_EQ(repeated.size(), 1);
    ASSERT_TRUE(repeated[0]);
    std::string output(value_.size(), '\0');
    std::vector<Slice> slices{{output.data(), output.size()}};
    ASSERT_TRUE(reader_->Get("ok", slices));
    EXPECT_EQ(output, value_);
    EXPECT_EQ(target_->values.size(), 2);
}

TEST_F(SharedNvmeKvClientTest, BatchPreservesGatherAcrossNamespaces) {
    auto second = std::make_shared<KvTarget>();
    auto connect = [&](const char* hostname) {
        auto client = Client::Create(hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_.master_address());
        if (!client) {
            return std::shared_ptr<Client>{};
        }
        (*client)->SetSharedKvBackends(
            {MakeObjectStore(std::make_unique<TestExecutor>(target_)),
             MakeObjectStore(std::make_unique<TestExecutor>(second))});
        return *client;
    };
    writer_ = connect("127.0.0.1:19003");
    reader_ = connect("127.0.0.1:19004");
    ASSERT_NE(writer_, nullptr);
    ASSERT_NE(reader_, nullptr);
    std::array<char, 32> input{};
    input[0] = 'a';
    input[16] = 'b';
    std::vector<std::string> keys;
    std::vector<std::vector<Slice>> inputs;
    // Use a large batch that populates both namespaces.
    for (size_t i = 0; i < 600; ++i) {
        keys.push_back(std::to_string(i));
        inputs.push_back({{input.data(), 1}, {input.data() + 16, 1}});
    }
    auto results = writer_->BatchPut(keys, inputs, shared_);
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        ASSERT_TRUE(result);
    }
    EXPECT_GT(target_->values.size(), 256);
    EXPECT_GT(second->values.size(), 256);
    for (int i = 0; i < 16; ++i) {
        ASSERT_TRUE(writer_->Put("single-" + std::to_string(i), inputs.front(),
                                 shared_));
    }
    writer_.reset();

    for (int i = 0; i < 16; ++i) {
        std::string value(2, '?');
        std::vector<Slice> slices{{value.data(), value.size()}};
        ASSERT_TRUE(reader_->Get("single-" + std::to_string(i), slices));
        EXPECT_EQ(value, "ab");
    }

    std::vector<std::array<char, 32>> output(keys.size());
    std::unordered_map<std::string, std::vector<Slice>> outputs;
    for (size_t i = 0; i < keys.size(); ++i) {
        outputs[keys[i]] = {{output[i].data(), 2}};
    }
    results = reader_->BatchGet(keys, outputs);
    ASSERT_EQ(results.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(results[i]);
        EXPECT_EQ(output[i][0], 'a');
        EXPECT_EQ(output[i][1], 'b');
        EXPECT_EQ(output[i][16], 0);
    }

    outputs[keys[0]] = {{output[0].data(), 1}, {output[0].data() + 16, 1}};
    results = reader_->BatchGet({keys[0], keys[1]}, outputs);
    ASSERT_EQ(results.size(), 2u);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::NOT_SUPPORTED);
    EXPECT_TRUE(results[1]);
    EXPECT_TRUE(reader_->Query(keys[0]));
}

TEST_F(SharedNvmeKvClientTest, SharedBatchRejectsMemoryOnlySameNodeMode) {
    auto config = shared_;
    config.replica_num = 1;
    config.prefer_alloc_in_same_node = true;
    std::vector<std::vector<Slice>> inputs{{{value_.data(), value_.size()}}};
    auto results = writer_->BatchPut({"same-node"}, inputs, config);
    ASSERT_EQ(results.size(), 1);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(reader_->Query("same-node"));
}

TEST_F(SharedNvmeKvClientTest,
       SingleAndBatchRejectLeaseExpiryDuringDeviceRead) {
    ASSERT_TRUE(Put("a", shared_));
    for (bool batch : {false, true}) {
        auto query = reader_->Query("a");
        ASSERT_TRUE(query);
        auto replicas = query->replicas;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
        QueryResult short_lease(std::move(replicas), deadline);
        target_->before_read = [deadline] {
            std::this_thread::sleep_until(deadline +
                                          std::chrono::milliseconds(1));
        };
        std::string output(value_.size(), '\0');
        std::vector<Slice> slices{{output.data(), output.size()}};
        if (batch) {
            std::unordered_map<std::string, std::vector<Slice>> outputs{
                {"a", slices}};
            auto result = reader_->BatchGet({"a"}, {short_lease}, outputs);
            ASSERT_EQ(result.size(), 1);
            ASSERT_FALSE(result[0]);
            EXPECT_EQ(result[0].error(), ErrorCode::LEASE_EXPIRED);
        } else {
            auto result = reader_->Get("a", short_lease, slices);
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::LEASE_EXPIRED);
        }
        target_->before_read = {};
    }
}

TEST_F(SharedNvmeKvClientTest,
       ExistenceFollowsMasterUntilReadReportsDefiniteMiss) {
    ASSERT_TRUE(Put("a", shared_));
    ASSERT_TRUE(Put("b", shared_));
    target_->values.clear();

    auto exists = reader_->IsExist("a");
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
    auto batch = reader_->BatchIsExist({"a", "missing", "b"});
    ASSERT_EQ(batch.size(), 3);
    for (const auto& result : batch) ASSERT_TRUE(result);
    EXPECT_TRUE(*batch[0]);
    EXPECT_FALSE(*batch[1]);
    EXPECT_TRUE(*batch[2]);
    EXPECT_EQ(target_->reads, 0);

    std::string output(value_.size(), 'x');
    std::vector<Slice> slices{{output.data(), output.size()}};
    target_->read_error = ErrorCode::TRANSFER_FAIL;
    auto read = reader_->Get("a", slices);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error(), ErrorCode::TRANSFER_FAIL);
    EXPECT_TRUE(reader_->Query("a"));

    target_->read_error = ErrorCode::OK;
    read = reader_->Get("a", slices);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error(), ErrorCode::OBJECT_NOT_FOUND);
    exists = reader_->IsExist("a");
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
    batch = reader_->BatchIsExist({"a", "b"});
    ASSERT_EQ(batch.size(), 2);
    ASSERT_TRUE(batch[0]);
    ASSERT_TRUE(batch[1]);
    EXPECT_FALSE(*batch[0]);
    EXPECT_TRUE(*batch[1]);
    EXPECT_EQ(target_->reads, 2);
}

TEST_F(SharedNvmeKvClientTest, ExistenceRejectsUnpublishedWrites) {
    bool checked = false;
    target_->before_store = [&] {
        checked = true;
        auto exists = reader_->IsExist("pending");
        ASSERT_TRUE(exists);
        EXPECT_FALSE(*exists);
        auto batch = reader_->BatchIsExist({"pending", "missing"});
        ASSERT_EQ(batch.size(), 2);
        for (const auto& result : batch) {
            ASSERT_TRUE(result);
            EXPECT_FALSE(*result);
        }
    };
    ASSERT_TRUE(Put("pending", shared_));
    EXPECT_TRUE(checked);
    target_->before_store = {};
    auto exists = reader_->IsExist("pending");
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
}

TEST_F(SharedNvmeKvClientTest,
       FlexibleWriteFallsBackToSharedAndFailedStoreStaysInvisible) {
    auto mixed = shared_;
    mixed.replica_num = 1;  // No memory segments are mounted.
    ASSERT_TRUE(Put("a", mixed));
    auto query = reader_->Query("a");
    ASSERT_TRUE(query);
    ASSERT_EQ(query->replicas.size(), 1);
    EXPECT_TRUE(query->replicas[0].is_shared_kv_replica());
    target_->store_error = ErrorCode::TRANSFER_FAIL;
    EXPECT_FALSE(Put("b", mixed));
    EXPECT_FALSE(reader_->Query("b"));
    EXPECT_TRUE(reader_->Query("a"));
}
}  // namespace
}  // namespace mooncake::testing
