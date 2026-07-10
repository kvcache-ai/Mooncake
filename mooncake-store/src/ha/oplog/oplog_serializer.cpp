#include "ha/oplog/oplog_serializer.h"

#include <glog/logging.h>
#include <sstream>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "utils/base64.h"

namespace mooncake {

std::string SerializeOpLogEntry(const OpLogEntry& entry) {
    Json::Value root;
    root["sequence_id"] = static_cast<Json::UInt64>(entry.sequence_id);
    root["timestamp_ms"] = static_cast<Json::UInt64>(entry.timestamp_ms);
    root["op_type"] = static_cast<int>(entry.op_type);
    root["object_key"] = entry.object_key;
    // CRITICAL: Base64 encode binary payload to prevent UTF-8 corruption in
    // JSON
    root["payload"] = base64::Encode(entry.payload);
    root["checksum"] = static_cast<Json::UInt>(entry.checksum);
    root["prefix_hash"] = static_cast<Json::UInt>(entry.prefix_hash);

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";  // Compact format
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    std::ostringstream oss;
    writer->write(root, &oss);
    return oss.str();
}

bool DeserializeOpLogEntry(const std::string& json_str, OpLogEntry& entry) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;

    if (!reader->parse(json_str.data(), json_str.data() + json_str.size(),
                       &root, &errors)) {
        LOG(ERROR) << "Failed to parse JSON: " << errors;
        return false;
    }

    try {
        entry.sequence_id = root["sequence_id"].asUInt64();
        entry.timestamp_ms = root["timestamp_ms"].asUInt64();
        entry.op_type = static_cast<OpType>(root["op_type"].asInt());
        entry.object_key = root["object_key"].asString();
        // CRITICAL: Base64 decode payload to restore binary data
        entry.payload = base64::Decode(root["payload"].asString());
        entry.checksum = root["checksum"].asUInt();
        entry.prefix_hash = root["prefix_hash"].asUInt();
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to deserialize OpLogEntry: " << e.what();
        return false;
    }

    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "OpLogSerializer: entry size rejected, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return false;
    }

    return true;
}

}  // namespace mooncake
