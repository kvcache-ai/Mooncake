#include "ha/oplog/oplog_types.h"

#include <xxhash.h>

namespace mooncake {

namespace {

uint32_t ComputePrefixHash(std::string_view key) {
    if (key.empty()) {
        return 0;
    }
    return static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
}

}  // namespace

bool NormalizeAndValidateClusterId(std::string& cluster_id) {
    while (!cluster_id.empty() && cluster_id.back() == '/') {
        cluster_id.pop_back();
    }
    return cluster_id.empty() || IsValidClusterIdComponent(cluster_id);
}

uint32_t ComputeOpLogChecksum(std::string_view payload) {
    return static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
}

bool VerifyOpLogChecksum(const OpLogEntry& entry) {
    return ComputeOpLogChecksum(entry.payload) == entry.checksum &&
           (entry.prefix_hash == 0 ||
            ComputePrefixHash(entry.object_key) == entry.prefix_hash);
}

bool ValidateOpLogEntrySize(const OpLogEntry& entry, std::string* reason) {
    if (entry.object_key.size() > kMaxOpLogObjectKeySize) {
        if (reason != nullptr) {
            *reason = "object_key too large: size=" +
                      std::to_string(entry.object_key.size());
        }
        return false;
    }
    if (entry.payload.size() > kMaxOpLogPayloadSize) {
        if (reason != nullptr) {
            *reason = "payload too large: size=" +
                      std::to_string(entry.payload.size());
        }
        return false;
    }
    return true;
}

}  // namespace mooncake
