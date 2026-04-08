#include "ha/oplog/oplog_codec.h"

#include <memory>
#include <sstream>
#include <exception>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "utils/base64.h"

namespace mooncake {
namespace ha {
namespace oplog {

std::string SerializeEntryPayload(const OpLogEntry& entry) {
    Json::Value root;
    root["sequence_id"] = static_cast<Json::UInt64>(entry.sequence_id);
    root["timestamp_ms"] = static_cast<Json::UInt64>(entry.timestamp_ms);
    root["op_type"] = static_cast<int>(entry.op_type);
    root["object_key"] = entry.object_key;
    root["payload"] = base64::Encode(entry.payload);
    root["checksum"] = static_cast<Json::UInt>(entry.checksum);
    root["prefix_hash"] = static_cast<Json::UInt>(entry.prefix_hash);

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    std::ostringstream oss;
    writer->write(root, &oss);
    return oss.str();
}

tl::expected<OpLogEntry, ErrorCode> DeserializeEntryPayload(
    std::string_view payload) {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;
    OpLogEntry entry;

    if (!reader->parse(payload.data(), payload.data() + payload.size(), &root,
                       &errors)) {
        LOG(ERROR) << "Failed to parse OpLog payload JSON: " << errors;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    try {
        entry.sequence_id = root["sequence_id"].asUInt64();
        entry.timestamp_ms = root["timestamp_ms"].asUInt64();
        entry.op_type = static_cast<OpType>(root["op_type"].asInt());
        entry.object_key = root["object_key"].asString();
        entry.payload = base64::Decode(root["payload"].asString());
        entry.checksum = root["checksum"].asUInt();
        entry.prefix_hash = root["prefix_hash"].asUInt();
    } catch (const std::exception& exception) {
        LOG(ERROR) << "Failed to deserialize OpLog entry: " << exception.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::string size_reason;
    if (!OpLogManager::ValidateEntrySize(entry, &size_reason)) {
        LOG(ERROR) << "Rejected OpLog entry payload, sequence_id="
                   << entry.sequence_id << ", key=" << entry.object_key
                   << ", reason=" << size_reason;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return entry;
}

OpLogAppendRequest BuildAppendRequest(const OpLogEntry& entry) {
    return OpLogAppendRequest{
        .expected_next_seq = entry.sequence_id,
        .producer_view_version = 0,
        .payload = SerializeEntryPayload(entry),
    };
}

}  // namespace oplog
}  // namespace ha
}  // namespace mooncake
