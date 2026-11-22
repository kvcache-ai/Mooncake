#include <gtest/gtest.h>

#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/s3_helper.h"
#include "crc32c.h"
#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_codec.h"
#include "ha/oplog/oplog_batch_types.h"
#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/snapshot/object/backends/s3/s3_snapshot_object_store.h"
#include "oss_fault_test_helpers.h"

namespace mooncake::test {
namespace {

using Server = OssFaultHttpServer;
using Routes = std::map<std::string, Server::Response>;
constexpr char kBucket[] = "snapshot-test";
constexpr char kCluster[] = "s3-fallback-test";

class MemoryBackend final : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        auto it = values.find(std::string(key));
        if (it == values.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        values[std::string(key)] = value;
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& output) override {
        output.clear();
        for (const auto& [key, value] : values) {
            if (key >= begin && key < end &&
                (limit == 0 || output.size() < limit))
                output.push_back({key, value});
        }
        return ErrorCode::OK;
    }
    ErrorCode DeleteRange(std::string_view, std::string_view) override {
        return ErrorCode::INVALID_PARAMS;
    }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::INVALID_PARAMS; }
    bool SupportsTxn() const override { return false; }
    std::map<std::string, std::string> values;
};

class ScopedEnvironment {
   public:
    void Set(const char* name, const std::string& value) {
        const char* old = std::getenv(name);
        original_.try_emplace(
            name, old ? std::optional<std::string>(old) : std::nullopt);
        setenv(name, value.c_str(), 1);
    }
    ~ScopedEnvironment() {
        for (const auto& [name, value] : original_) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
    }

   private:
    std::map<std::string, std::optional<std::string>> original_;
};

struct Snapshot {
    std::string pointer;
    std::vector<std::string> keys;  // descriptor, manifest, segments, chunk
    std::map<std::string, std::string> objects;
};

Snapshot MakeSnapshot(const std::string& root, uint64_t batch) {
    const std::string id = std::to_string(batch) + "-7";
    Snapshot result;
    result.keys = {ha::BuildBatchOpLogSnapshotDescriptorKey(root, id),
                   ha::BuildBatchOpLogSnapshotManifestKey(root, id),
                   ha::BuildBatchOpLogSnapshotSegmentsKey(root, id),
                   ha::BuildBatchOpLogSnapshotObjectChunkKey(root, id, 0)};
    StandbyObjectMetadata metadata;
    metadata.client_id = {1, 2};
    metadata.size = 4096;
    std::vector<StandbyObjectEntry> objects{
        {.tenant_id = "tenant", .key = "surviving", .metadata = metadata}};
    if (batch == 1)
        objects.push_back({.tenant_id = "tenant",
                           .key = "removed-by-suffix",
                           .metadata = metadata});
    const auto chunk = EncodeBatchOpLogSnapshotObjectChunk(0, objects);
    const auto segments = EncodeBatchOpLogSnapshotSegments({});
    result.objects[result.keys[2]] = {segments.begin(), segments.end()};
    result.objects[result.keys[3]] = {chunk.begin(), chunk.end()};
    ha::BatchOpLogSnapshotManifest manifest;
    manifest.snapshot_id = id;
    manifest.last_included_seq = manifest.last_included_batch_id = batch;
    manifest.segments = {
        .key = result.keys[2],
        .stored_size = segments.size(),
        .crc32c = Crc32cValue(segments.data(), segments.size())};
    manifest.object_chunks = {
        {.chunk_index = 0,
         .key = result.keys[3],
         .object_count = objects.size(),
         .stored_size = chunk.size(),
         .crc32c = Crc32cValue(chunk.data(), chunk.size())}};
    const auto bytes = ha::EncodeBatchOpLogSnapshotManifest(manifest);
    result.objects[result.keys[1]] = bytes;
    ha::BatchOpLogSnapshotDescriptor descriptor;
    descriptor.snapshot_id = id;
    descriptor.last_included_seq = descriptor.last_included_batch_id = batch;
    descriptor.manifest_key = result.keys[1];
    descriptor.manifest_size = bytes.size();
    descriptor.manifest_crc32c = Crc32cValue(bytes.data(), bytes.size());
    result.pointer = ha::EncodeBatchOpLogSnapshotDescriptor(descriptor);
    result.objects[result.keys[0]] = result.pointer;
    return result;
}

std::string Target(const std::string& key) {
    return "/" + std::string(kBucket) + "/" + key;
}

std::string SnapshotRoot(int artifact, bool missing_on_get, int status) {
    return "snapshots/" + std::to_string(artifact) + "-" +
           (missing_on_get ? "get-" : "head-") + std::to_string(status);
}

void AddSnapshots(Routes& routes, const std::string& root) {
    for (uint64_t batch : {1, 2}) {
        for (const auto& [key, bytes] : MakeSnapshot(root, batch).objects)
            routes[Target(key)] = Server::Reply(200, bytes);
    }
}

class S3SnapshotObjectStoreTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        Routes routes;
        for (int artifact = 0; artifact < 4; ++artifact) {
            for (bool missing_on_get : {false, true}) {
                for (int status : {403, 404, 503}) {
                    if (status == 503 && artifact != 3) continue;
                    const auto root =
                        SnapshotRoot(artifact, missing_on_get, status);
                    AddSnapshots(routes, root);
                    const auto latest = MakeSnapshot(root, 2);
                    // HEAD errors intentionally have no XML response body.
                    routes[(missing_on_get ? "GET " : "") +
                           Target(latest.keys[artifact])] =
                        Server::Reply(status, "");
                }
            }
        }
        for (int status : {403, 404, 503}) {
            const auto root = "snapshots/misleading-" + std::to_string(status);
            AddSnapshots(routes, root);
            const auto latest = MakeSnapshot(root, 2);
            routes["GET " + Target(latest.keys[3])] =
                Server::Reply(status,
                              "<Error><Code>NoSuchKey</Code>"
                              "<Message>missing</Message></Error>");
        }
        const std::string absent_root = "snapshots/absent-bucket";
        AddSnapshots(routes, absent_root);
        for (uint64_t batch : {1, 2}) {
            for (const auto& key : MakeSnapshot(absent_root, batch).keys) {
                routes["HEAD " + Target(key)] = Server::Reply(404, "");
                routes["GET " + Target(key)] = Server::Reply(
                    404,
                    "<Error><Code>NoSuchBucket</Code>"
                    "<Message>The specified bucket does not exist</Message>"
                    "</Error>");
            }
        }
        routes["missing"] = Server::Reply(404, "");
        routes["host-not-found"] = Server::Reply(403, "host not found 404");
        routes["HEAD " + Target("large-missing")] = Server::Raw(
            "HTTP/1.1 200 OK\r\nContent-Length: 104857601\r\n"
            "Connection: close\r\n\r\n");
        routes["GET " + Target("large-missing")] = Server::Reply(404, "");
        routes["HEAD " + Target("bucket-missing")] = Server::Reply(200, "x");
        routes["GET " + Target("bucket-missing")] = Server::Reply(
            404,
            "<Error><Code>NoSuchBucket</Code>"
            "<Message>The specified bucket does not exist</Message>"
            "</Error>");
        server_ = std::make_unique<Server>(std::move(routes), true);
        environment_ = std::make_unique<ScopedEnvironment>();
        environment_->Set("MOONCAKE_AWS_S3_ENDPOINT", Endpoint());
        environment_->Set("MOONCAKE_AWS_BUCKET_NAME", kBucket);
        environment_->Set("MOONCAKE_AWS_REGION", "us-east-1");
        environment_->Set("MOONCAKE_AWS_ACCESS_KEY_ID", "test-key");
        environment_->Set("MOONCAKE_AWS_SECRET_ACCESS_KEY", "test-secret");
        environment_->Set("MOONCAKE_AWS_USE_HTTPS", "false");
        environment_->Set("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", "false");
        environment_->Set("MOONCAKE_AWS_CONNECT_TIMEOUT_MS", "100");
        environment_->Set("MOONCAKE_AWS_REQUEST_TIMEOUT_MS", "1000");
        environment_->Set("AWS_EC2_METADATA_DISABLED", "true");
        environment_->Set("AWS_DEFAULT_REGION", "us-east-1");
        environment_->Set("AWS_ACCESS_KEY_ID", "test-key");
        environment_->Set("AWS_SECRET_ACCESS_KEY", "test-secret");
        environment_->Set("AWS_RETRY_MODE", "standard");
        environment_->Set("AWS_MAX_ATTEMPTS", "1");
        environment_->Set("NO_PROXY", "127.0.0.1");
        environment_->Set("no_proxy", "127.0.0.1");
        store_ = std::make_unique<S3SnapshotObjectStore>();
    }

    static void TearDownTestSuite() {
        store_.reset();
        server_->Stop();
        EXPECT_TRUE(server_->error().empty()) << server_->error();
        server_.reset();
        environment_.reset();
    }

    static std::string Endpoint() {
        return "127.0.0.1:" + std::to_string(server_->port());
    }

    static void SetBucketResponse(int status) {
        const auto target = "HEAD /" + std::string(kBucket);
        server_->SetResponse(target, Server::Reply(status, ""));
        server_->SetResponse(target + "/", Server::Reply(status, ""));
    }

    void SetUp() override { SetBucketResponse(200); }

    static void PrepareBackend(MemoryBackend& backend, const std::string& root,
                               bool suffix_present = true) {
        backend.Put(ha::BuildBatchOpLogSnapshotLatestKey(kCluster),
                    MakeSnapshot(root, 2).pointer);
        backend.Put(ha::BuildBatchOpLogSnapshotFallbackKey(kCluster),
                    MakeSnapshot(root, 1).pointer);
        // From-zero replay must not hide a failure to restore fallback.
        backend.Put(ha::BuildBatchOpLogSnapshotCompactionFloorKey(kCluster),
                    "1");
        backend.Put(BuildDurablePrefixKey(kCluster),
                    EncodeDurablePrefix({.batch_id = 2, .last_seq = 2}));
        if (suffix_present) {
            OpLogEntry entry;
            entry.sequence_id = 2;
            entry.op_type = OpType::REMOVE;
            entry.tenant_id = "tenant";
            entry.object_key = "removed-by-suffix";
            OpLogBatchRecord batch{.batch_id = 2,
                                   .first_seq = 2,
                                   .last_seq = 2,
                                   .entries = {entry}};
            backend.Put(BuildBatchRecordKey(kCluster, 2),
                        EncodeOpLogBatchRecord(batch));
        }
    }

    inline static std::unique_ptr<Server> server_;
    inline static std::unique_ptr<ScopedEnvironment> environment_;
    inline static std::unique_ptr<S3SnapshotObjectStore> store_;
};

TEST_F(S3SnapshotObjectStoreTest, ClassifiesBodylessHead404) {
    auto result = store_->InspectObject("missing");
    ASSERT_FALSE(result);
    EXPECT_TRUE(store_->IsNotFoundError(result.error())) << result.error();
    EXPECT_NE(std::string::npos, result.error().find("404"));
}

TEST_F(S3SnapshotObjectStoreTest, ClassifiesDownloadHead404) {
    std::vector<uint8_t> bytes;
    auto result = store_->DownloadBuffer("missing", bytes);
    ASSERT_FALSE(result);
    EXPECT_TRUE(store_->IsNotFoundError(result.error())) << result.error();
    std::string text;
    auto get_result = store_->DownloadString("missing", text);
    ASSERT_FALSE(get_result);
    EXPECT_TRUE(store_->IsNotFoundError(get_result.error()))
        << get_result.error();
}

TEST_F(S3SnapshotObjectStoreTest, ClassifiesRangedGet404) {
    std::vector<uint8_t> bytes;
    auto result = store_->DownloadBuffer("large-missing", bytes);
    ASSERT_FALSE(result);
    EXPECT_TRUE(store_->IsNotFoundError(result.error())) << result.error();
    EXPECT_TRUE(bytes.empty());
}

TEST_F(S3SnapshotObjectStoreTest, KeepsMissingBucketAsInfrastructure) {
    std::vector<uint8_t> bytes;
    auto result = store_->DownloadBuffer("bucket-missing", bytes);
    ASSERT_FALSE(result);
    EXPECT_FALSE(store_->IsNotFoundError(result.error())) << result.error();
}

TEST_F(S3SnapshotObjectStoreTest, KeepsMissingBucketHead404AsInfrastructure) {
    SetBucketResponse(404);
    const auto key = MakeSnapshot("snapshots/absent-bucket", 2).keys[0];
    auto inspection = store_->InspectObject(key);
    ASSERT_FALSE(inspection);
    EXPECT_FALSE(store_->IsNotFoundError(inspection.error()))
        << inspection.error();

    std::vector<uint8_t> bytes;
    auto download = store_->DownloadBuffer(key, bytes);
    ASSERT_FALSE(download);
    EXPECT_FALSE(store_->IsNotFoundError(download.error())) << download.error();

    S3Helper helper(Endpoint(), kBucket);
    auto simple_download = helper.DownloadBuffer(key, bytes);
    ASSERT_FALSE(simple_download);
    EXPECT_FALSE(store_->IsNotFoundError(simple_download.error()))
        << simple_download.error();
}

TEST_F(S3SnapshotObjectStoreTest, RechecksBucketAfterItDisappears) {
    auto missing_object = store_->InspectObject("missing");
    ASSERT_FALSE(missing_object);
    EXPECT_TRUE(store_->IsNotFoundError(missing_object.error()));
    SetBucketResponse(404);
    auto missing_bucket = store_->InspectObject("missing");
    ASSERT_FALSE(missing_bucket);
    EXPECT_FALSE(store_->IsNotFoundError(missing_bucket.error()))
        << missing_bucket.error();
}

TEST_F(S3SnapshotObjectStoreTest, KeepsBucketCheck503AsInfrastructure) {
    SetBucketResponse(503);
    auto result = store_->InspectObject("missing");
    ASSERT_FALSE(result);
    EXPECT_FALSE(store_->IsNotFoundError(result.error())) << result.error();
}

TEST_F(S3SnapshotObjectStoreTest, BucketHeadFailuresDoNotFallBack) {
    const std::string root = "snapshots/absent-bucket";
    for (int status : {403, 404}) {
        SetBucketResponse(status);
        for (const std::string floor : {"0", "1"}) {
            SCOPED_TRACE("bucket status=" + std::to_string(status) +
                         "; compaction floor=" + floor);
            MemoryBackend backend;
            PrepareBackend(backend, root);
            backend.Put(ha::BuildBatchOpLogSnapshotCompactionFloorKey(kCluster),
                        floor);
            BatchOpLogSnapshotProvider provider(kCluster, backend, *store_,
                                                root);
            StandbyMetadataStore metadata;
            StandbySegmentRegistry registry;
            auto result = provider.RestoreBaseline(metadata, registry);
            ASSERT_FALSE(result);
            EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error());
            EXPECT_EQ(0u, metadata.GetKeyCount());
        }
    }
    const auto fallback = MakeSnapshot(root, 1);
    for (const auto& request : server_->requests()) {
        EXPECT_EQ(std::string::npos,
                  request.find("HEAD " + Target(fallback.keys[0]) + " "));
    }
}

TEST_F(S3SnapshotObjectStoreTest, ExplicitMissingKeyDoesNotRequireBucketCheck) {
    SetBucketResponse(403);
    const std::string root = "snapshots/misleading-404";
    MemoryBackend backend;
    PrepareBackend(backend, root);
    BatchOpLogSnapshotProvider provider(kCluster, backend, *store_, root);
    StandbyMetadataStore metadata;
    StandbySegmentRegistry registry;
    auto result = provider.RestoreBaseline(metadata, registry);
    ASSERT_TRUE(result) << toString(result.error());
    EXPECT_EQ(1u, result->last_included_batch_id);
    EXPECT_EQ(2u, result->last_applied_batch_id);
    EXPECT_TRUE(metadata.Exists("tenant", "surviving"));
    EXPECT_FALSE(metadata.Exists("tenant", "removed-by-suffix"));
}

TEST_F(S3SnapshotObjectStoreTest, DoesNotClassifyFreeTextAsNotFound) {
    S3Helper helper(Endpoint(), kBucket);
    std::vector<uint8_t> bytes;
    auto result = helper.DownloadBuffer("host-not-found", bytes);
    ASSERT_FALSE(result);
    EXPECT_FALSE(store_->IsNotFoundError(result.error())) << result.error();
    EXPECT_FALSE(
        store_->IsNotFoundError("HeadObject failed: No response body."));
    EXPECT_FALSE(store_->IsNotFoundError("host not found"));
}

TEST_F(S3SnapshotObjectStoreTest, KeepsUnreachableEndpointAsInfrastructure) {
    Server unreachable({});
    const auto endpoint = "127.0.0.1:" + std::to_string(unreachable.port());
    unreachable.Stop();
    S3Helper helper(endpoint, kBucket);
    uint64_t size = 0;
    std::optional<uint32_t> crc;
    auto result = helper.InspectObject("missing", size, crc);
    ASSERT_FALSE(result);
    EXPECT_FALSE(store_->IsNotFoundError(result.error())) << result.error();
}

TEST_F(S3SnapshotObjectStoreTest, RejectsFallbackWithoutRequiredSuffix) {
    const auto root = SnapshotRoot(3, false, 404);
    MemoryBackend backend;
    PrepareBackend(backend, root, false);
    BatchOpLogSnapshotProvider provider(kCluster, backend, *store_, root);
    StandbyMetadataStore metadata;
    StandbySegmentRegistry registry;
    auto result = provider.RestoreBaseline(metadata, registry);
    ASSERT_FALSE(result);
    EXPECT_EQ(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP, result.error());
    EXPECT_EQ(0u, metadata.GetKeyCount());
}

TEST_F(S3SnapshotObjectStoreTest,
       MissingSnapshotArtifactsFallBackAndReplaySuffix) {
    for (int artifact = 0; artifact < 4; ++artifact) {
        for (bool missing_on_get : {false, true}) {
            const auto root = SnapshotRoot(artifact, missing_on_get, 404);
            SCOPED_TRACE(root);
            MemoryBackend backend;
            PrepareBackend(backend, root);
            BatchOpLogSnapshotProvider provider(kCluster, backend, *store_,
                                                root);
            StandbyMetadataStore metadata;
            StandbySegmentRegistry registry;
            OpLogApplier applier(&metadata, kCluster);
            auto result =
                provider.RestoreBaseline(metadata, registry, &applier);
            ASSERT_TRUE(result) << toString(result.error());
            EXPECT_EQ(1u, result->last_included_batch_id);
            EXPECT_EQ(2u, result->last_applied_batch_id);
            EXPECT_EQ(2u, result->last_applied_seq);
            EXPECT_TRUE(metadata.Exists("tenant", "surviving"));
            EXPECT_FALSE(metadata.Exists("tenant", "removed-by-suffix"));
        }
    }
}

TEST_F(S3SnapshotObjectStoreTest, InfrastructureErrorsDoNotFallBack) {
    for (int artifact = 0; artifact < 4; ++artifact) {
        for (bool missing_on_get : {false, true}) {
            for (int status : {403, 503}) {
                if (status == 503 && artifact != 3) continue;
                const auto root =
                    SnapshotRoot(artifact, missing_on_get, status);
                SCOPED_TRACE(root);
                MemoryBackend backend;
                PrepareBackend(backend, root);
                BatchOpLogSnapshotProvider provider(kCluster, backend, *store_,
                                                    root);
                StandbyMetadataStore metadata;
                StandbySegmentRegistry registry;
                auto result = provider.RestoreBaseline(metadata, registry);
                ASSERT_FALSE(result);
                EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error());
                EXPECT_EQ(0u, metadata.GetKeyCount());
            }
        }
    }
}

TEST_F(S3SnapshotObjectStoreTest, HttpFailureOverridesMissingKeyCode) {
    for (int status : {403, 503}) {
        const auto root = "snapshots/misleading-" + std::to_string(status);
        SCOPED_TRACE(root);
        const auto latest = MakeSnapshot(root, 2);
        std::vector<uint8_t> bytes;
        auto download = store_->DownloadBuffer(latest.keys[3], bytes);
        ASSERT_FALSE(download);
        EXPECT_FALSE(store_->IsNotFoundError(download.error()))
            << download.error();
        MemoryBackend backend;
        PrepareBackend(backend, root);
        BatchOpLogSnapshotProvider provider(kCluster, backend, *store_, root);
        StandbyMetadataStore metadata;
        StandbySegmentRegistry registry;
        auto result = provider.RestoreBaseline(metadata, registry);
        EXPECT_FALSE(result);
        if (!result) EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR, result.error());
        EXPECT_EQ(0u, metadata.GetKeyCount());
    }
}

}  // namespace
}  // namespace mooncake::test
