#include "ha/snapshot/batch_oplog_snapshot_types.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <sstream>
#include <string>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

namespace mooncake::ha::test {

namespace {

BatchOpLogSnapshotDescriptor MakeDescriptor() {
    return {
        .snapshot_id = "9-12345",
        .last_included_seq = 42,
        .last_included_batch_id = 9,
        .producer_view_version = 7,
        .manifest_key = "snapshots/batch-oplog/9-12345/manifest.json",
        .manifest_size = 128,
        .manifest_crc32c = 17,
        .created_at_ms = 1700000000000,
    };
}

BatchOpLogSnapshotManifest MakeManifest() {
    return {
        .snapshot_id = "9-12345",
        .last_included_seq = 42,
        .last_included_batch_id = 9,
        .producer_view_version = 7,
        .segments = {.key = "snapshots/batch-oplog/9-12345/segments.bin",
                     .stored_size = 64,
                     .crc32c = 18},
        .object_chunks =
            {
                {.chunk_index = 0,
                 .key = "snapshots/batch-oplog/9-12345/objects/0.bin",
                 .object_count = 100,
                 .stored_size = 1024,
                 .crc32c = 19},
                {.chunk_index = 1,
                 .key = "snapshots/batch-oplog/9-12345/objects/1.bin",
                 .object_count = 50,
                 .stored_size = 512,
                 .crc32c = 20},
            },
    };
}

Json::Value ParseJson(const std::string& value) {
    Json::CharReaderBuilder builder;
    Json::Value root;
    std::string errors;
    std::istringstream stream(value);
    EXPECT_TRUE(Json::parseFromStream(builder, stream, &root, &errors))
        << errors;
    return root;
}

std::string WriteJson(const Json::Value& root) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

void ExpectDescriptorRejected(const std::string& json) {
    BatchOpLogSnapshotDescriptor descriptor;
    std::string reason;
    EXPECT_FALSE(
        DecodeBatchOpLogSnapshotDescriptor(json, &descriptor, &reason));
    EXPECT_FALSE(reason.empty());
}

void ExpectManifestRejected(const std::string& json) {
    SCOPED_TRACE(json);
    BatchOpLogSnapshotManifest manifest;
    std::string reason;
    EXPECT_FALSE(DecodeBatchOpLogSnapshotManifest(json, &manifest, &reason));
    EXPECT_FALSE(reason.empty());
}

}  // namespace

TEST(BatchOpLogSnapshotTypesTest, DescriptorRoundTripsCompactJson) {
    const auto encoded = EncodeBatchOpLogSnapshotDescriptor(MakeDescriptor());
    EXPECT_EQ(encoded.find('\n'), std::string::npos);

    auto json = ParseJson(encoded);
    json["future_optional_field"] = true;

    BatchOpLogSnapshotDescriptor decoded;
    std::string reason;
    ASSERT_TRUE(
        DecodeBatchOpLogSnapshotDescriptor(WriteJson(json), &decoded, &reason))
        << reason;
    EXPECT_EQ(decoded.schema_version, kBatchOpLogSnapshotSchemaVersion);
    EXPECT_EQ(decoded.snapshot_format, kBatchOpLogSnapshotFormat);
    EXPECT_EQ(decoded.snapshot_id, "9-12345");
    EXPECT_EQ(decoded.last_included_seq, 42u);
    EXPECT_EQ(decoded.last_included_batch_id, 9u);
    EXPECT_EQ(decoded.producer_view_version, 7u);
    EXPECT_EQ(decoded.manifest_key,
              "snapshots/batch-oplog/9-12345/manifest.json");
    EXPECT_EQ(decoded.manifest_size, 128u);
    EXPECT_EQ(decoded.manifest_crc32c, 17u);
    EXPECT_EQ(decoded.created_at_ms, 1700000000000);
}

TEST(BatchOpLogSnapshotTypesTest, ManifestRoundTripsChunksAndAllowsEmptySet) {
    BatchOpLogSnapshotManifest decoded;
    std::string reason;
    auto encoded = EncodeBatchOpLogSnapshotManifest(MakeManifest());
    EXPECT_EQ(encoded.find('\n'), std::string::npos);
    auto json = ParseJson(encoded);
    json["future_optional_field"] = true;
    ASSERT_TRUE(
        DecodeBatchOpLogSnapshotManifest(WriteJson(json), &decoded, &reason))
        << reason;
    ASSERT_EQ(decoded.object_chunks.size(), 2u);
    EXPECT_EQ(decoded.segments.stored_size, 64u);
    EXPECT_EQ(decoded.object_chunks[1].chunk_index, 1u);
    EXPECT_EQ(decoded.object_chunks[1].object_count, 50u);

    auto empty = MakeManifest();
    empty.object_chunks.clear();
    ASSERT_TRUE(DecodeBatchOpLogSnapshotManifest(
        EncodeBatchOpLogSnapshotManifest(empty), &decoded, &reason));
    EXPECT_TRUE(decoded.object_chunks.empty());
}

TEST(BatchOpLogSnapshotTypesTest, JsonRoundTripPreservesEscapedKeys) {
    auto descriptor = MakeDescriptor();
    descriptor.manifest_key = R"(snapshots/"quoted"/manifest\key.json)";
    BatchOpLogSnapshotDescriptor decoded_descriptor;
    ASSERT_TRUE(DecodeBatchOpLogSnapshotDescriptor(
        EncodeBatchOpLogSnapshotDescriptor(descriptor), &decoded_descriptor));
    EXPECT_EQ(decoded_descriptor.manifest_key, descriptor.manifest_key);

    auto manifest = MakeManifest();
    manifest.segments.key = R"(snapshots/"quoted"/segments\key.bin)";
    manifest.object_chunks[0].key = R"(snapshots/"quoted"/objects\0.bin)";
    BatchOpLogSnapshotManifest decoded_manifest;
    ASSERT_TRUE(DecodeBatchOpLogSnapshotManifest(
        EncodeBatchOpLogSnapshotManifest(manifest), &decoded_manifest));
    EXPECT_EQ(decoded_manifest.segments.key, manifest.segments.key);
    EXPECT_EQ(decoded_manifest.object_chunks[0].key,
              manifest.object_chunks[0].key);
}

TEST(BatchOpLogSnapshotTypesTest, RejectsInvalidDescriptorJson) {
    const auto encoded = EncodeBatchOpLogSnapshotDescriptor(MakeDescriptor());
    auto json = ParseJson(encoded);

    ExpectDescriptorRejected("{");
    ExpectDescriptorRejected("[]");
    ExpectDescriptorRejected(encoded.substr(0, encoded.size() - 1) +
                             ",\"schema_version\":1}");

    auto invalid = json;
    invalid.removeMember("manifest_key");
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["manifest_size"] = "128";
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["schema_version"] = 2;
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["snapshot_format"] = "standby-oplog-materialized/v2";
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["last_included_batch_id"] = Json::UInt64(0);
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["snapshot_id"] = "10-12345";
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["manifest_key"] = "";
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["manifest_size"] = Json::UInt64(0);
    ExpectDescriptorRejected(WriteJson(invalid));
    invalid = json;
    invalid["created_at_ms"] = Json::Int64(-1);
    ExpectDescriptorRejected(WriteJson(invalid));

    auto overflow = encoded;
    const std::string old_value = "\"last_included_seq\":42";
    const auto position = overflow.find(old_value);
    ASSERT_NE(position, std::string::npos);
    overflow.replace(position, old_value.size(),
                     "\"last_included_seq\":18446744073709551616");
    ExpectDescriptorRejected(overflow);
}

TEST(BatchOpLogSnapshotTypesTest, AcceptsAnEmptyCursor) {
    auto descriptor = MakeDescriptor();
    descriptor.snapshot_id = "0-12345";
    descriptor.last_included_seq = 0;
    descriptor.last_included_batch_id = 0;

    BatchOpLogSnapshotDescriptor decoded;
    std::string reason;
    EXPECT_TRUE(DecodeBatchOpLogSnapshotDescriptor(
        EncodeBatchOpLogSnapshotDescriptor(descriptor), &decoded, &reason))
        << reason;
}

TEST(BatchOpLogSnapshotTypesTest, RejectsInvalidManifestJson) {
    const auto encoded = EncodeBatchOpLogSnapshotManifest(MakeManifest());
    auto json = ParseJson(encoded);

    ExpectManifestRejected(encoded.substr(0, encoded.size() - 1) +
                           ",\"schema_version\":1}");

    auto invalid = json;
    invalid.removeMember("segments");
    ExpectManifestRejected(WriteJson(invalid));
    invalid = json;
    invalid["segments"]["key"] = "";
    ExpectManifestRejected(WriteJson(invalid));
    invalid = json;
    invalid["segments"]["crc32c"] =
        Json::UInt64(static_cast<uint64_t>(UINT32_MAX) + 1);
    ExpectManifestRejected(WriteJson(invalid));
    invalid = json;
    invalid["object_chunks"][1]["chunk_index"] = Json::UInt64(2);
    ExpectManifestRejected(WriteJson(invalid));
    invalid = json;
    invalid["object_chunks"][0]["object_count"] = Json::UInt64(0);
    ExpectManifestRejected(WriteJson(invalid));
    invalid = json;
    invalid["snapshot_id"] = "8-12345";
    ExpectManifestRejected(WriteJson(invalid));
}

TEST(BatchOpLogSnapshotTypesTest, BuildsControlAndArtifactKeys) {
    EXPECT_EQ(BuildBatchOpLogSnapshotMaintenanceKey("cluster-a/"),
              "/oplog/cluster-a/snapshot/maintenance");
    EXPECT_EQ(BuildBatchOpLogSnapshotLatestKey("cluster-a"),
              "/oplog/cluster-a/snapshot/latest");
    EXPECT_EQ(BuildBatchOpLogSnapshotFallbackKey("cluster-a"),
              "/oplog/cluster-a/snapshot/fallback");
    EXPECT_EQ(BuildBatchOpLogSnapshotCompactionFloorKey("cluster-a"),
              "/oplog/cluster-a/snapshot/compaction_floor");
    EXPECT_TRUE(BuildBatchOpLogSnapshotLatestKey("../cluster").empty());

    const auto snapshot_id = BuildBatchOpLogSnapshotId(9, 12345);
    ASSERT_EQ(snapshot_id, "9-12345");
    EXPECT_EQ(BuildBatchOpLogSnapshotDescriptorKey("snapshots", snapshot_id),
              "snapshots/batch-oplog/9-12345/descriptor.json");
    EXPECT_EQ(BuildBatchOpLogSnapshotManifestKey("snapshots/", snapshot_id),
              "snapshots/batch-oplog/9-12345/manifest.json");
    EXPECT_EQ(BuildBatchOpLogSnapshotSegmentsKey("snapshots", snapshot_id),
              "snapshots/batch-oplog/9-12345/segments.bin");
    EXPECT_EQ(
        BuildBatchOpLogSnapshotObjectChunkKey("snapshots", snapshot_id, 7),
        "snapshots/batch-oplog/9-12345/objects/7.bin");
}

TEST(BatchOpLogSnapshotTypesTest, RejectsUnsafeSnapshotIdInArtifactKeys) {
    EXPECT_TRUE(BuildBatchOpLogSnapshotId(9, 0).empty());
    EXPECT_TRUE(
        BuildBatchOpLogSnapshotDescriptorKey("snapshots", "9-x").empty());
    EXPECT_TRUE(BuildBatchOpLogSnapshotDescriptorKey("snapshots",
                                                     "9-12345/../../latest")
                    .empty());
}

}  // namespace mooncake::ha::test
