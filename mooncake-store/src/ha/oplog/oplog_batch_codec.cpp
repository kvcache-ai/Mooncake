#include "ha/oplog/oplog_batch_codec.h"

#include <memory>
#include <sstream>

#include <glog/logging.h>
#include <xxhash.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "ha/oplog/oplog_serializer.h"

namespace mooncake {

namespace {

void SetReason(std::string* reason, const std::string& value) {
    if (reason != nullptr) {
        *reason = value;
    }
}

std::string WriteJson(const Json::Value& root) {
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    std::ostringstream oss;
    writer->write(root, &oss);
    return oss.str();
}

bool ParseJson(const std::string& value, Json::Value* root,
               std::string* reason) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;
    if (!reader->parse(value.data(), value.data() + value.size(), root,
                       &errors)) {
        SetReason(reason, errors.empty() ? "malformed json" : errors);
        return false;
    }
    if (!root->isObject()) {
        SetReason(reason, "payload must be a JSON object");
        return false;
    }
    return true;
}

bool GetUIntField(const Json::Value& root, const char* field, uint32_t* out,
                  std::string* reason) {
    if (!root.isMember(field)) {
        SetReason(reason, std::string("missing field: ") + field);
        return false;
    }
    if (!root[field].isUInt()) {
        SetReason(reason, std::string("field must be uint32: ") + field);
        return false;
    }
    *out = root[field].asUInt();
    return true;
}

bool GetUInt64Field(const Json::Value& root, const char* field, uint64_t* out,
                    std::string* reason) {
    if (!root.isMember(field)) {
        SetReason(reason, std::string("missing field: ") + field);
        return false;
    }
    if (!root[field].isUInt64()) {
        SetReason(reason, std::string("field must be uint64: ") + field);
        return false;
    }
    *out = root[field].asUInt64();
    return true;
}

Json::Value BatchRecordToJson(const OpLogBatchRecord& batch,
                              bool include_checksum) {
    Json::Value root;
    root["schema_version"] = static_cast<Json::UInt>(batch.schema_version);
    root["batch_id"] = static_cast<Json::UInt64>(batch.batch_id);
    root["first_seq"] = static_cast<Json::UInt64>(batch.first_seq);
    root["last_seq"] = static_cast<Json::UInt64>(batch.last_seq);
    Json::Value entries(Json::arrayValue);
    for (const auto& entry : batch.entries) {
        Json::Value entry_root;
        std::string errors;
        Json::CharReaderBuilder builder;
        std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        const std::string serialized = SerializeOpLogEntry(entry);
        if (!reader->parse(serialized.data(),
                           serialized.data() + serialized.size(), &entry_root,
                           &errors)) {
            LOG(ERROR) << "Failed to reparse serialized OpLogEntry: " << errors;
            continue;
        }
        entries.append(entry_root);
    }
    root["entries"] = std::move(entries);
    if (include_checksum) {
        root["checksum"] = static_cast<Json::UInt>(batch.checksum);
    }
    return root;
}

uint32_t ComputeBatchChecksum(const OpLogBatchRecord& batch) {
    const std::string stable_payload =
        WriteJson(BatchRecordToJson(batch, /*include_checksum=*/false));
    return static_cast<uint32_t>(
        XXH32(stable_payload.data(), stable_payload.size(), 0));
}

bool JsonEntryToOpLogEntry(const Json::Value& root, OpLogEntry* entry,
                           std::string* reason) {
    const std::string serialized = WriteJson(root);
    if (!DeserializeOpLogEntry(serialized, *entry)) {
        SetReason(reason, "invalid oplog entry");
        return false;
    }
    if (!OpLogManager::VerifyChecksum(*entry)) {
        SetReason(reason, "oplog entry checksum mismatch");
        return false;
    }
    return true;
}

}  // namespace

std::string EncodeDurablePrefix(const DurablePrefix& prefix) {
    Json::Value root;
    root["schema_version"] =
        static_cast<Json::UInt>(kDurablePrefixSchemaVersion);
    root["batch_id"] = static_cast<Json::UInt64>(prefix.batch_id);
    root["last_seq"] = static_cast<Json::UInt64>(prefix.last_seq);
    return WriteJson(root);
}

bool DecodeDurablePrefix(const std::string& value, DurablePrefix* prefix,
                         std::string* reason) {
    if (reason != nullptr) {
        reason->clear();
    }
    if (prefix == nullptr) {
        SetReason(reason, "prefix output is null");
        return false;
    }

    Json::Value root;
    if (!ParseJson(value, &root, reason)) {
        return false;
    }
    uint32_t schema_version = 0;
    if (!GetUIntField(root, "schema_version", &schema_version, reason)) {
        return false;
    }
    if (schema_version != kDurablePrefixSchemaVersion) {
        SetReason(reason, "unsupported durable prefix schema_version");
        return false;
    }
    if (!GetUInt64Field(root, "batch_id", &prefix->batch_id, reason)) {
        return false;
    }
    if (!GetUInt64Field(root, "last_seq", &prefix->last_seq, reason)) {
        return false;
    }
    return true;
}

std::string EncodeOpLogBatchRecord(const OpLogBatchRecord& batch) {
    OpLogBatchRecord encoded = batch;
    encoded.schema_version = kOpLogBatchRecordSchemaVersion;
    encoded.checksum = ComputeBatchChecksum(encoded);
    return WriteJson(BatchRecordToJson(encoded, /*include_checksum=*/true));
}

bool DecodeOpLogBatchRecord(const std::string& value, OpLogBatchRecord* batch,
                            std::string* reason) {
    if (reason != nullptr) {
        reason->clear();
    }
    if (batch == nullptr) {
        SetReason(reason, "batch output is null");
        return false;
    }

    Json::Value root;
    if (!ParseJson(value, &root, reason)) {
        return false;
    }
    uint32_t schema_version = 0;
    if (!GetUIntField(root, "schema_version", &schema_version, reason)) {
        return false;
    }
    if (schema_version != kOpLogBatchRecordSchemaVersion) {
        SetReason(reason, "unsupported batch record schema_version");
        return false;
    }
    if (!root.isMember("entries")) {
        SetReason(reason, "missing field: entries");
        return false;
    }
    if (!root["entries"].isArray()) {
        SetReason(reason, "field must be array: entries");
        return false;
    }

    OpLogBatchRecord decoded;
    decoded.schema_version = schema_version;
    if (!GetUInt64Field(root, "batch_id", &decoded.batch_id, reason)) {
        return false;
    }
    if (!GetUInt64Field(root, "first_seq", &decoded.first_seq, reason)) {
        return false;
    }
    if (!GetUInt64Field(root, "last_seq", &decoded.last_seq, reason)) {
        return false;
    }
    if (!GetUIntField(root, "checksum", &decoded.checksum, reason)) {
        return false;
    }
    decoded.entries.reserve(root["entries"].size());
    for (const auto& entry_root : root["entries"]) {
        OpLogEntry entry;
        if (!JsonEntryToOpLogEntry(entry_root, &entry, reason)) {
            return false;
        }
        decoded.entries.push_back(std::move(entry));
    }

    const uint32_t expected = ComputeBatchChecksum(decoded);
    if (decoded.checksum != expected) {
        SetReason(reason, "batch record checksum mismatch");
        return false;
    }
    if (!ValidateOpLogBatchRecordShape(decoded, reason)) {
        return false;
    }
    *batch = std::move(decoded);
    return true;
}

}  // namespace mooncake
