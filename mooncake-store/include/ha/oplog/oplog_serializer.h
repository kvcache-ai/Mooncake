// mooncake-store/include/oplog_serializer.h
#pragma once

#include <string>

#include "ha/oplog/oplog_manager.h"

namespace mooncake {

// Serialize an OpLogEntry to JSON string (with base64-encoded payload).
// Format is backend-agnostic; all storage backends should use this.
std::string SerializeOpLogEntry(const OpLogEntry& entry);

// Deserialize a JSON string to OpLogEntry.
// Returns true on success, false on parse error or size validation failure.
bool DeserializeOpLogEntry(const std::string& json_str, OpLogEntry& entry);

}  // namespace mooncake
