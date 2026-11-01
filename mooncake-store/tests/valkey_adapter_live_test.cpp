// Live tests for ValkeyObjectStorageAdapter against a Valkey server with the
// valkey-large-object module loaded. Skipped unless MOONCAKE_VALKEY_ADDR
// (host:port) is set, e.g.:
//
//   valkey-server --port 6390 --loadmodule libvalkey_large_object.so
//   MOONCAKE_VALKEY_ADDR=127.0.0.1:6390 ./valkey_adapter_live_test
//
// The RDMA cases also need the module's Transfer Engine backend on the
// server (fabric-provider MooncakeTe) and, for this process, the Transfer
// Engine transport and an address the server can reach:
//
//   MOONCAKE_VALKEY_RDMA_PROTOCOL=efa MOONCAKE_VALKEY_RDMA_LOCAL_IP=10.0.0.5

#include "storage/distributed/valkey_adapter.h"

#include <gtest/gtest.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/distributed/distributed_storage_backend.h"
#include "transfer_engine.h"

namespace mooncake {
namespace {

bool LiveEnabled() {
    const char* addr = std::getenv("MOONCAKE_VALKEY_ADDR");
    return addr && *addr;
}

std::string Pattern(size_t size, char seed) {
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>(seed + i % 97);
    return data;
}

class ValkeyAdapterLiveTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (!LiveEnabled()) GTEST_SKIP() << "Set MOONCAKE_VALKEY_ADDR";
        // A per-run namespace, so runs do not see each other's keys.
        prefix_ =
            "/mooncake-valkey-test-" + std::to_string(getpid()) + "-" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            "/";
        adapter_ = std::make_unique<ValkeyObjectStorageAdapter>(prefix_);
        ASSERT_TRUE(adapter_->Init());
    }

    void TearDown() override {
        if (!adapter_) return;
        auto keys = adapter_->ListKeys();
        if (keys) {
            for (const auto& key : *keys)
                (void)adapter_->Delete(key.logical_key);
        }
    }

    std::string prefix_;
    std::unique_ptr<ValkeyObjectStorageAdapter> adapter_;
};

TEST_F(ValkeyAdapterLiveTest, PutGetRoundTrip) {
    const std::string data = Pattern(3 << 20, 'a');
    ASSERT_TRUE(adapter_->Put("obj", {data.data(), data.size()}));
    std::string out(data.size(), '\0');
    auto got = adapter_->Get("obj", out.data(), out.size());
    ASSERT_TRUE(got);
    EXPECT_EQ(*got, data.size());
    EXPECT_EQ(out, data);
}

TEST_F(ValkeyAdapterLiveTest, PutVConcatenatesRegions) {
    const std::string a = Pattern(1 << 20, 'a'), b = Pattern(12345, 'k');
    iovec iov[2] = {{const_cast<char*>(a.data()), a.size()},
                    {const_cast<char*>(b.data()), b.size()}};
    ASSERT_TRUE(adapter_->PutV("multi", iov, 2));
    std::string out(a.size() + b.size(), '\0');
    ASSERT_TRUE(adapter_->Get("multi", out.data(), out.size()));
    EXPECT_EQ(out, a + b);
}

TEST_F(ValkeyAdapterLiveTest, GetReadsAPrefixAndRejectsOverlongReads) {
    const std::string data = Pattern(4096, 'p');
    ASSERT_TRUE(adapter_->Put("short", {data.data(), data.size()}));
    std::string head(100, '\0');
    auto got = adapter_->Get("short", head.data(), head.size());
    ASSERT_TRUE(got);
    EXPECT_EQ(head, data.substr(0, 100));
    std::string too_long(data.size() + 1, '\0');
    auto bad = adapter_->Get("short", too_long.data(), too_long.size());
    ASSERT_FALSE(bad);
    EXPECT_EQ(bad.error(), ErrorCode::FILE_READ_FAIL);
}

TEST_F(ValkeyAdapterLiveTest, MissingKeyIsFileNotFound) {
    std::string out(16, '\0');
    auto got = adapter_->Get("missing", out.data(), out.size());
    ASSERT_FALSE(got);
    EXPECT_EQ(got.error(), ErrorCode::FILE_NOT_FOUND);
    auto exists = adapter_->Exists("missing");
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
}

TEST_F(ValkeyAdapterLiveTest, OverwriteReplacesObject) {
    const std::string first = Pattern(8192, 'x'), second = Pattern(4096, 'y');
    ASSERT_TRUE(adapter_->Put("k", {first.data(), first.size()}));
    ASSERT_TRUE(adapter_->Put("k", {second.data(), second.size()}));
    std::string out(second.size(), '\0');
    ASSERT_TRUE(adapter_->Get("k", out.data(), out.size()));
    EXPECT_EQ(out, second);
    auto keys = adapter_->ListKeys();
    ASSERT_TRUE(keys);
    ASSERT_EQ(keys->size(), 1u);
    EXPECT_EQ(keys->front().size, second.size());
}

TEST_F(ValkeyAdapterLiveTest, ExistsDeleteAndBinaryKeys) {
    std::string key = "tenant";
    key.push_back('\0');
    key += "a/b*[x]?\\";
    const std::string data = "payload";
    ASSERT_TRUE(adapter_->Put(key, {data.data(), data.size()}));
    auto exists = adapter_->Exists(key);
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
    auto keys = adapter_->ListKeys();
    ASSERT_TRUE(keys);
    ASSERT_EQ(keys->size(), 1u);
    EXPECT_EQ(keys->front().logical_key, key);
    EXPECT_EQ(keys->front().size, data.size());
    ASSERT_TRUE(adapter_->Delete(key));
    exists = adapter_->Exists(key);
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
}

TEST_F(ValkeyAdapterLiveTest, EmptyObjectIsRejected) {
    auto put = adapter_->Put("empty", {});
    ASSERT_FALSE(put);
    EXPECT_EQ(put.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(ValkeyAdapterLiveTest, BatchPutAndGet) {
    constexpr int kCount = 64;
    std::vector<std::string> payloads;
    std::vector<iovec> iovs(kCount);
    std::vector<ObjectPutRequest> puts;
    for (int i = 0; i < kCount; ++i) {
        payloads.push_back(Pattern(256 * 1024 + i, 'a' + i % 26));
    }
    for (int i = 0; i < kCount; ++i) {
        iovs[i] = {payloads[i].data(), payloads[i].size()};
        puts.push_back({"batch/" + std::to_string(i), &iovs[i], 1});
    }
    for (const auto& result : adapter_->PutBatch(puts)) ASSERT_TRUE(result);

    std::vector<std::string> outs(kCount);
    std::vector<ObjectGetRequest> gets;
    for (int i = 0; i < kCount; ++i) {
        outs[i].assign(payloads[i].size(), '\0');
        gets.push_back(
            {"batch/" + std::to_string(i), outs[i].data(), outs[i].size()});
    }
    gets.push_back({"batch/missing", outs[0].data(), 16});
    auto results = adapter_->GetBatch(gets);
    ASSERT_EQ(results.size(), gets.size());
    for (int i = 0; i < kCount; ++i) {
        ASSERT_TRUE(results[i]) << i;
        EXPECT_EQ(outs[i], payloads[i]) << i;
    }
    ASSERT_FALSE(results.back());
    EXPECT_EQ(results.back().error(), ErrorCode::FILE_NOT_FOUND);
}

TEST_F(ValkeyAdapterLiveTest, HealthCheck) {
    EXPECT_TRUE(adapter_->CheckHealth());
}

// The Store's DistributedStorageBackend driving the real adapter: offload,
// existence, load, and metadata scan.
TEST_F(ValkeyAdapterLiveTest, DistributedBackendOffloadAndLoad) {
    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = prefix_ + "backend/";
    distributed_config.fs_adapter_type = "valkey";
    distributed_config.enable_health_check = true;
    DistributedStorageBackend backend(
        FileStorageConfig{}, distributed_config, nullptr,
        std::make_unique<ValkeyObjectStorageAdapter>(distributed_config.fsdir));
    ASSERT_TRUE(backend.Init());

    const std::string a = Pattern(2 << 20, 'a'), b = Pattern(777, 'b');
    const std::string c = Pattern(1 << 20, 'c');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"kv/first",
         {{const_cast<char*>(a.data()), a.size()},
          {const_cast<char*>(b.data()), b.size()}}},
        {"kv/second", {{const_cast<char*>(c.data()), c.size()}}},
    };
    std::map<std::string, int64_t> offloaded;
    auto offload = backend.BatchOffload(
        batch, [&](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>& metadata) {
            for (size_t i = 0; i < keys.size(); ++i) {
                offloaded[keys[i]] = metadata[i].data_size;
            }
            return ErrorCode::OK;
        });
    ASSERT_TRUE(offload);
    EXPECT_EQ(*offload, 2);
    EXPECT_EQ(offloaded.at("kv/first"),
              static_cast<int64_t>(a.size() + b.size()));
    EXPECT_EQ(offloaded.at("kv/second"), static_cast<int64_t>(c.size()));

    auto exists = backend.IsExist("kv/first");
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);

    std::string first(a.size() + b.size(), '\0'), second(c.size(), '\0');
    std::unordered_map<std::string, Slice> load{
        {"kv/first", {first.data(), first.size()}},
        {"kv/second", {second.data(), second.size()}},
    };
    ASSERT_TRUE(backend.BatchLoad(load));
    EXPECT_EQ(first, a + b);
    EXPECT_EQ(second, c);

    std::map<std::string, int64_t> scanned;
    ASSERT_TRUE(
        backend.ScanMeta([&](const std::vector<std::string>& keys,
                             std::vector<StorageObjectMetadata>& metadata) {
            for (size_t i = 0; i < keys.size(); ++i) {
                scanned[keys[i]] = metadata[i].data_size;
            }
            return ErrorCode::OK;
        }));
    EXPECT_EQ(scanned, offloaded);

    // Clean up the backend's namespace.
    ValkeyObjectStorageAdapter cleaner(distributed_config.fsdir);
    ASSERT_TRUE(cleaner.Init());
    for (const auto& key : {"kv/first", "kv/second"}) (void)cleaner.Delete(key);
}

// ─── RDMA transport ──────────────────────────────────────────────────────────

const char* RdmaProtocol() {
    return std::getenv("MOONCAKE_VALKEY_RDMA_PROTOCOL");
}
const char* RdmaLocalIp() {
    return std::getenv("MOONCAKE_VALKEY_RDMA_LOCAL_IP");
}

class ValkeyAdapterRdmaLiveTest : public ::testing::Test {
   protected:
    static constexpr size_t kBufferSize = 256ull << 20;

    void SetUp() override {
        if (!LiveEnabled() || !RdmaProtocol() || !RdmaLocalIp()) {
            GTEST_SKIP() << "Set MOONCAKE_VALKEY_ADDR, "
                            "MOONCAKE_VALKEY_RDMA_PROTOCOL and "
                            "MOONCAKE_VALKEY_RDMA_LOCAL_IP";
        }
        engine_ = std::make_unique<TransferEngine>(false);
        engine_->getLocalTopology()->discover({});
        const std::string ip = RdmaLocalIp();
        ASSERT_EQ(
            engine_->init("P2PHANDSHAKE", ip + ":13360", ip.c_str(), 13360), 0);
        ASSERT_NE(engine_->installTransport(RdmaProtocol(), nullptr), nullptr);
        buffer_ = static_cast<char*>(std::aligned_alloc(4096, kBufferSize));
        ASSERT_NE(buffer_, nullptr);
        ASSERT_EQ(engine_->registerLocalMemory(buffer_, kBufferSize, "cpu:0"),
                  0);

        setenv("MOONCAKE_VALKEY_TRANSPORT", "rdma", 1);
        prefix_ =
            "/mooncake-valkey-rdma-test-" + std::to_string(getpid()) + "-" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name();
        adapter_ = std::make_unique<ValkeyObjectStorageAdapter>(prefix_);
        adapter_->BindTransferEndpoint(engine_->getLocalIpAndPort());
        ASSERT_TRUE(adapter_->Init());
    }

    void TearDown() override {
        unsetenv("MOONCAKE_VALKEY_TRANSPORT");
        if (adapter_) {
            auto keys = adapter_->ListKeys();
            if (keys) {
                for (const auto& key : *keys) {
                    (void)adapter_->Delete(key.logical_key);
                }
            }
            adapter_.reset();
        }
        if (engine_ && buffer_) engine_->unregisterLocalMemory(buffer_);
        std::free(buffer_);
    }

    char* Region(size_t offset) { return buffer_ + offset; }

    std::unique_ptr<TransferEngine> engine_;
    char* buffer_ = nullptr;
    std::string prefix_;
    std::unique_ptr<ValkeyObjectStorageAdapter> adapter_;
};

TEST_F(ValkeyAdapterRdmaLiveTest, MultiRegionPutAndGetUseRdma) {
    const std::string a = Pattern(8 << 20, 'a'), b = Pattern(12345, 'k');
    std::memcpy(Region(0), a.data(), a.size());
    std::memcpy(Region(64 << 20), b.data(), b.size());
    iovec iov[2] = {{Region(0), a.size()}, {Region(64 << 20), b.size()}};
    ASSERT_TRUE(adapter_->PutV("multi", iov, 2));

    char* out = Region(128 << 20);
    std::memset(out, 0, a.size() + b.size());
    auto got = adapter_->Get("multi", out, a.size() + b.size());
    ASSERT_TRUE(got);
    EXPECT_EQ(std::string(out, a.size() + b.size()), a + b);

    const auto stats = adapter_->GetTransportStats();
    EXPECT_EQ(stats.rdma_requests, 2u);
    EXPECT_EQ(stats.rdma_fallbacks, 0u);
    EXPECT_EQ(stats.tcp_requests, 0u);
}

TEST_F(ValkeyAdapterRdmaLiveTest, BatchesUseRdma) {
    constexpr int kCount = 32;
    constexpr size_t kSize = 1 << 20;
    std::vector<iovec> iovs(kCount);
    std::vector<ObjectPutRequest> puts;
    std::vector<ObjectGetRequest> gets;
    for (int i = 0; i < kCount; ++i) {
        const std::string data = Pattern(kSize, 'a' + i % 26);
        std::memcpy(Region(i * kSize), data.data(), kSize);
        iovs[i] = {Region(i * kSize), kSize};
        puts.push_back({"batch/" + std::to_string(i), &iovs[i], 1});
        gets.push_back({"batch/" + std::to_string(i),
                        Region((64 << 20) + i * kSize), kSize});
    }
    for (const auto& result : adapter_->PutBatch(puts)) ASSERT_TRUE(result);
    gets.push_back({"batch/missing", Region(200 << 20), 16});
    auto results = adapter_->GetBatch(gets);
    for (int i = 0; i < kCount; ++i) {
        ASSERT_TRUE(results[i]) << i;
        EXPECT_EQ(0, std::memcmp(Region(i * kSize),
                                 Region((64 << 20) + i * kSize), kSize))
            << i;
    }
    ASSERT_FALSE(results.back());
    EXPECT_EQ(results.back().error(), ErrorCode::FILE_NOT_FOUND);
    const auto stats = adapter_->GetTransportStats();
    EXPECT_EQ(stats.rdma_requests, 2u * kCount + 1);
    EXPECT_EQ(stats.rdma_fallbacks, 0u);
}

// A buffer outside the registered memory is not reachable by the server, so
// the request is redone over TCP and still succeeds.
TEST_F(ValkeyAdapterRdmaLiveTest, UnregisteredBuffersFallBackToTcp) {
    const std::string data = Pattern(2 << 20, 'u');
    ASSERT_TRUE(adapter_->Put("heap", {data.data(), data.size()}));
    std::string out(data.size(), '\0');
    ASSERT_TRUE(adapter_->Get("heap", out.data(), out.size()));
    EXPECT_EQ(out, data);
    const auto stats = adapter_->GetTransportStats();
    EXPECT_EQ(stats.rdma_requests, 0u);
    EXPECT_EQ(stats.rdma_fallbacks, 2u);
    EXPECT_EQ(stats.tcp_requests, 2u);
}

// A read shorter than the object cannot be served by BLOB.RDMA_GET, which
// writes the whole object; it is redone over TCP.
TEST_F(ValkeyAdapterRdmaLiveTest, PrefixReadFallsBackToTcp) {
    const std::string data = Pattern(1 << 20, 'p');
    std::memcpy(Region(0), data.data(), data.size());
    ASSERT_TRUE(adapter_->Put("prefix", {Region(0), data.size()}));
    auto got = adapter_->Get("prefix", Region(64 << 20), 4096);
    ASSERT_TRUE(got);
    EXPECT_EQ(std::string(Region(64 << 20), 4096), data.substr(0, 4096));
    EXPECT_EQ(adapter_->GetTransportStats().rdma_fallbacks, 1u);
}

}  // namespace
}  // namespace mooncake
