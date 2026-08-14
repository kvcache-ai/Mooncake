#include "ha/snapshot/batch_oplog/metadata.h"

#include <charconv>
#include <memory>
#include <string>
#include <utility>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "ha/oplog/oplog_types.h"

namespace mooncake::ha {

namespace {

void SetReason(std::string* reason, const std::string& value) {
    if (reason != nullptr) {
        *reason = value;
    }
}

std::string WriteJson(const Json::Value& root) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

bool ParseJson(std::string_view value, Json::Value& root, std::string* reason) {
    Json::CharReaderBuilder builder;
    builder["allowComments"] = false;
    builder["collectComments"] = false;
    builder["failIfExtra"] = true;
    builder["rejectDupKeys"] = true;
    builder["strictRoot"] = true;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;
    if (!reader->parse(value.data(), value.data() + value.size(), &root,
                       &errors)) {
        SetReason(reason, errors.empty() ? "malformed json" : errors);
        return false;
    }
    if (!root.isObject()) {
        SetReason(reason, "snapshot metadata must be a JSON object");
        return false;
    }
    return true;
}

bool GetString(const Json::Value& root, const char* field, std::string& value,
               std::string* reason) {
    if (!root.isMember(field) || !root[field].isString()) {
        SetReason(reason, std::string("field must be a string: ") + field);
        return false;
    }
    value = root[field].asString();
    return true;
}

bool GetUInt32(const Json::Value& root, const char* field, uint32_t& value,
               std::string* reason) {
    if (!root.isMember(field) || !root[field].isUInt()) {
        SetReason(reason, std::string("field must be uint32: ") + field);
        return false;
    }
    value = root[field].asUInt();
    return true;
}

bool GetUInt64(const Json::Value& root, const char* field, uint64_t& value,
               std::string* reason) {
    if (!root.isMember(field) || !root[field].isUInt64()) {
        SetReason(reason, std::string("field must be uint64: ") + field);
        return false;
    }
    value = root[field].asUInt64();
    return true;
}

template <typename Integer>
bool GetInt64(const Json::Value& root, const char* field, Integer& value,
              std::string* reason) {
    if (!root.isMember(field) || !root[field].isInt64()) {
        SetReason(reason, std::string("field must be int64: ") + field);
        return false;
    }
    value = static_cast<Integer>(root[field].asInt64());
    return true;
}

template <typename Integer>
bool ParseDecimal(std::string_view value, Integer& result) {
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto parsed = std::from_chars(begin, end, result);
    return begin != end && parsed.ec == std::errc() && parsed.ptr == end;
}

bool ParseSnapshotId(std::string_view snapshot_id, uint64_t& batch_id) {
    const size_t separator = snapshot_id.find('-');
    if (separator == std::string_view::npos || separator == 0 ||
        separator + 1 == snapshot_id.size() ||
        snapshot_id.find('-', separator + 1) != std::string_view::npos) {
        return false;
    }

    uint64_t parsed_batch_id = 0;
    int64_t lease_id = 0;
    if (!ParseDecimal(snapshot_id.substr(0, separator), parsed_batch_id) ||
        !ParseDecimal(snapshot_id.substr(separator + 1), lease_id) ||
        lease_id <= 0 ||
        snapshot_id !=
            std::to_string(parsed_batch_id) + "-" + std::to_string(lease_id)) {
        return false;
    }
    batch_id = parsed_batch_id;
    return true;
}

template <typename SnapshotMetadata>
bool ValidateIdentity(const SnapshotMetadata& metadata, std::string* reason) {
    if (metadata.schema_version != kBatchOpLogSnapshotSchemaVersion) {
        SetReason(reason, "unsupported snapshot schema_version");
        return false;
    }
    if (metadata.snapshot_format != kBatchOpLogSnapshotFormat) {
        SetReason(reason, "unsupported snapshot_format");
        return false;
    }
    uint64_t snapshot_batch_id = 0;
    if (!ParseSnapshotId(metadata.snapshot_id, snapshot_batch_id) ||
        snapshot_batch_id != metadata.last_included_batch_id) {
        SetReason(reason, "snapshot_id does not match the batch cursor");
        return false;
    }
    if ((metadata.last_included_seq == 0) !=
        (metadata.last_included_batch_id == 0)) {
        SetReason(reason,
                  "sequence and batch cursors must both be zero or non-zero");
        return false;
    }
    if (metadata.producer_view_version < 0) {
        SetReason(reason, "producer_view_version must be non-negative");
        return false;
    }
    return true;
}

template <typename SnapshotMetadata>
void EncodeIdentity(const SnapshotMetadata& metadata, Json::Value& root) {
    root["schema_version"] = static_cast<Json::UInt>(metadata.schema_version);
    root["snapshot_format"] = metadata.snapshot_format;
    root["snapshot_id"] = metadata.snapshot_id;
    root["last_included_seq"] =
        static_cast<Json::UInt64>(metadata.last_included_seq);
    root["last_included_batch_id"] =
        static_cast<Json::UInt64>(metadata.last_included_batch_id);
    root["producer_view_version"] =
        static_cast<Json::Int64>(metadata.producer_view_version);
}

template <typename SnapshotMetadata>
bool DecodeIdentity(const Json::Value& root, SnapshotMetadata& metadata,
                    std::string* reason) {
    return GetUInt32(root, "schema_version", metadata.schema_version, reason) &&
           GetString(root, "snapshot_format", metadata.snapshot_format,
                     reason) &&
           GetString(root, "snapshot_id", metadata.snapshot_id, reason) &&
           GetUInt64(root, "last_included_seq", metadata.last_included_seq,
                     reason) &&
           GetUInt64(root, "last_included_batch_id",
                     metadata.last_included_batch_id, reason) &&
           GetInt64(root, "producer_view_version",
                    metadata.producer_view_version, reason) &&
           ValidateIdentity(metadata, reason);
}

Json::Value EncodeObjectDescriptor(
    const BatchOpLogSnapshotObjectDescriptor& descriptor) {
    Json::Value root(Json::objectValue);
    root["key"] = descriptor.key;
    root["stored_size"] = static_cast<Json::UInt64>(descriptor.stored_size);
    root["crc32c"] = static_cast<Json::UInt>(descriptor.crc32c);
    return root;
}

bool DecodeObjectDescriptor(const Json::Value& root,
                            BatchOpLogSnapshotObjectDescriptor& descriptor,
                            std::string* reason) {
    if (!root.isObject()) {
        SetReason(reason, "segments must be a JSON object");
        return false;
    }
    if (!GetString(root, "key", descriptor.key, reason) ||
        !GetUInt64(root, "stored_size", descriptor.stored_size, reason) ||
        !GetUInt32(root, "crc32c", descriptor.crc32c, reason)) {
        return false;
    }
    if (descriptor.key.empty() || descriptor.stored_size == 0) {
        SetReason(reason, "segments key and stored_size must be non-zero");
        return false;
    }
    return true;
}

Json::Value EncodeChunkDescriptor(
    const BatchOpLogSnapshotChunkDescriptor& descriptor) {
    Json::Value root(Json::objectValue);
    root["chunk_index"] = static_cast<Json::UInt64>(descriptor.chunk_index);
    root["key"] = descriptor.key;
    root["object_count"] = static_cast<Json::UInt64>(descriptor.object_count);
    root["stored_size"] = static_cast<Json::UInt64>(descriptor.stored_size);
    root["crc32c"] = static_cast<Json::UInt>(descriptor.crc32c);
    return root;
}

bool DecodeChunkDescriptor(const Json::Value& root, uint64_t expected_index,
                           BatchOpLogSnapshotChunkDescriptor& descriptor,
                           std::string* reason) {
    if (!root.isObject()) {
        SetReason(reason, "object chunk must be a JSON object");
        return false;
    }
    if (!GetUInt64(root, "chunk_index", descriptor.chunk_index, reason) ||
        !GetString(root, "key", descriptor.key, reason) ||
        !GetUInt64(root, "object_count", descriptor.object_count, reason) ||
        !GetUInt64(root, "stored_size", descriptor.stored_size, reason) ||
        !GetUInt32(root, "crc32c", descriptor.crc32c, reason)) {
        return false;
    }
    if (descriptor.chunk_index != expected_index) {
        SetReason(reason, "object chunk indices must be contiguous from zero");
        return false;
    }
    if (descriptor.key.empty() || descriptor.object_count == 0 ||
        descriptor.stored_size == 0) {
        SetReason(
            reason,
            "object chunk key, object_count, and stored_size must be non-zero");
        return false;
    }
    return true;
}

std::string BuildControlKey(const std::string& cluster_id,
                            std::string_view name) {
    std::string normalized = cluster_id;
    if (!NormalizeAndValidateClusterId(normalized) || normalized.empty()) {
        return {};
    }
    return "/oplog/" + normalized + "/snapshot/" + std::string(name);
}

std::string BuildArtifactPrefix(const std::string& snapshot_root,
                                std::string_view snapshot_id) {
    uint64_t ignored_batch_id = 0;
    if (!ParseSnapshotId(snapshot_id, ignored_batch_id)) {
        return {};
    }
    std::string normalized = snapshot_root;
    while (!normalized.empty() && normalized.back() == '/') {
        normalized.pop_back();
    }
    if (normalized.empty()) {
        return {};
    }
    return normalized + "/batch-oplog/" + std::string(snapshot_id) + "/";
}

}  // namespace

std::string EncodeBatchOpLogSnapshotDescriptor(
    const BatchOpLogSnapshotDescriptor& descriptor) {
    Json::Value root(Json::objectValue);
    EncodeIdentity(descriptor, root);
    root["manifest_key"] = descriptor.manifest_key;
    root["manifest_size"] = static_cast<Json::UInt64>(descriptor.manifest_size);
    root["manifest_crc32c"] =
        static_cast<Json::UInt>(descriptor.manifest_crc32c);
    root["created_at_ms"] = static_cast<Json::Int64>(descriptor.created_at_ms);
    return WriteJson(root);
}

tl::expected<BatchOpLogSnapshotDescriptor, std::string>
DecodeBatchOpLogSnapshotDescriptor(std::string_view value) {
    std::string reason;
    Json::Value root;
    BatchOpLogSnapshotDescriptor decoded;
    if (!ParseJson(value, root, &reason) ||
        !DecodeIdentity(root, decoded, &reason) ||
        !GetString(root, "manifest_key", decoded.manifest_key, &reason) ||
        !GetUInt64(root, "manifest_size", decoded.manifest_size, &reason) ||
        !GetUInt32(root, "manifest_crc32c", decoded.manifest_crc32c, &reason) ||
        !GetInt64(root, "created_at_ms", decoded.created_at_ms, &reason)) {
        return tl::make_unexpected(std::move(reason));
    }
    if (decoded.manifest_key.empty() || decoded.manifest_size == 0 ||
        decoded.created_at_ms < 0) {
        SetReason(&reason,
                  "manifest key and size must be non-zero and created_at_ms "
                  "non-negative");
        return tl::make_unexpected(std::move(reason));
    }
    return decoded;
}

std::string EncodeBatchOpLogSnapshotManifest(
    const BatchOpLogSnapshotManifest& manifest) {
    Json::Value root(Json::objectValue);
    EncodeIdentity(manifest, root);
    root["segments"] = EncodeObjectDescriptor(manifest.segments);
    Json::Value chunks(Json::arrayValue);
    for (const auto& descriptor : manifest.object_chunks) {
        chunks.append(EncodeChunkDescriptor(descriptor));
    }
    root["object_chunks"] = std::move(chunks);
    return WriteJson(root);
}

tl::expected<BatchOpLogSnapshotManifest, std::string>
DecodeBatchOpLogSnapshotManifest(std::string_view value) {
    std::string reason;
    Json::Value root;
    BatchOpLogSnapshotManifest decoded;
    if (!ParseJson(value, root, &reason) ||
        !DecodeIdentity(root, decoded, &reason)) {
        return tl::make_unexpected(std::move(reason));
    }
    if (!root.isMember("segments")) {
        SetReason(&reason, "missing field: segments");
        return tl::make_unexpected(std::move(reason));
    }
    if (!DecodeObjectDescriptor(root["segments"], decoded.segments, &reason)) {
        return tl::make_unexpected(std::move(reason));
    }
    if (!root.isMember("object_chunks") || !root["object_chunks"].isArray()) {
        SetReason(&reason, "object_chunks must be a JSON array");
        return tl::make_unexpected(std::move(reason));
    }
    const auto& chunks = root["object_chunks"];
    decoded.object_chunks.reserve(chunks.size());
    for (Json::ArrayIndex index = 0; index < chunks.size(); ++index) {
        BatchOpLogSnapshotChunkDescriptor descriptor;
        if (!DecodeChunkDescriptor(chunks[index], index, descriptor, &reason)) {
            return tl::make_unexpected(std::move(reason));
        }
        decoded.object_chunks.push_back(std::move(descriptor));
    }
    return decoded;
}

std::string BuildBatchOpLogSnapshotId(uint64_t last_included_batch_id,
                                      int64_t maintenance_lease_id) {
    if (maintenance_lease_id <= 0) {
        return {};
    }
    return std::to_string(last_included_batch_id) + "-" +
           std::to_string(maintenance_lease_id);
}

std::string BuildBatchOpLogSnapshotMaintenanceKey(
    const std::string& cluster_id) {
    return BuildControlKey(cluster_id, "maintenance");
}

std::string BuildBatchOpLogSnapshotLatestKey(const std::string& cluster_id) {
    return BuildControlKey(cluster_id, "latest");
}

std::string BuildBatchOpLogSnapshotFallbackKey(const std::string& cluster_id) {
    return BuildControlKey(cluster_id, "fallback");
}

std::string BuildBatchOpLogSnapshotCompactionFloorKey(
    const std::string& cluster_id) {
    return BuildControlKey(cluster_id, "compaction_floor");
}

std::string BuildBatchOpLogSnapshotArtifactPrefix(
    const std::string& snapshot_root, std::string_view snapshot_id) {
    return BuildArtifactPrefix(snapshot_root, snapshot_id);
}

std::string BuildBatchOpLogSnapshotDescriptorKey(
    const std::string& snapshot_root, std::string_view snapshot_id) {
    const std::string prefix = BuildArtifactPrefix(snapshot_root, snapshot_id);
    return prefix.empty() ? std::string() : prefix + "descriptor.json";
}

std::string BuildBatchOpLogSnapshotManifestKey(const std::string& snapshot_root,
                                               std::string_view snapshot_id) {
    const std::string prefix = BuildArtifactPrefix(snapshot_root, snapshot_id);
    return prefix.empty() ? std::string() : prefix + "manifest.json";
}

std::string BuildBatchOpLogSnapshotSegmentsKey(const std::string& snapshot_root,
                                               std::string_view snapshot_id) {
    const std::string prefix = BuildArtifactPrefix(snapshot_root, snapshot_id);
    return prefix.empty() ? std::string() : prefix + "segments.bin";
}

std::string BuildBatchOpLogSnapshotObjectChunkKey(
    const std::string& snapshot_root, std::string_view snapshot_id,
    uint64_t chunk_index) {
    const std::string prefix = BuildArtifactPrefix(snapshot_root, snapshot_id);
    return prefix.empty()
               ? std::string()
               : prefix + "objects/" + std::to_string(chunk_index) + ".bin";
}

}  // namespace mooncake::ha
