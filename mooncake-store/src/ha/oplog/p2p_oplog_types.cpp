#include "ha/oplog/p2p_oplog_types.h"

#include <string>
#include <vector>

#include <ylt/struct_pack.hpp>

namespace mooncake {

// ============================================================================
// SerializeP2PPayload implementations
// ============================================================================
// struct_pack::serialize() returns std::vector<char>, so we convert to
// std::string for storage in OpLogEntry::payload.

std::string SerializeP2PPayload(const RegisterClientPayload& payload) {
    auto buf = struct_pack::serialize(payload);
    return std::string(buf.begin(), buf.end());
}

std::string SerializeP2PPayload(const AddReplicaPayload& payload) {
    auto buf = struct_pack::serialize(payload);
    return std::string(buf.begin(), buf.end());
}

std::string SerializeP2PPayload(const RemoveReplicaPayload& payload) {
    auto buf = struct_pack::serialize(payload);
    return std::string(buf.begin(), buf.end());
}

std::string SerializeP2PPayload(const MountSegmentPayload& payload) {
    auto buf = struct_pack::serialize(payload);
    return std::string(buf.begin(), buf.end());
}

std::string SerializeP2PPayload(const UnmountSegmentPayload& payload) {
    auto buf = struct_pack::serialize(payload);
    return std::string(buf.begin(), buf.end());
}

// ============================================================================
// DeserializeP2PPayload implementations
// ============================================================================

bool DeserializeP2PPayload(const std::string& data,
                           RegisterClientPayload& payload) {
    auto result =
        struct_pack::deserialize_to(payload, data.data(), data.size());
    return result == struct_pack::errc::ok;
}

bool DeserializeP2PPayload(const std::string& data,
                           AddReplicaPayload& payload) {
    auto result =
        struct_pack::deserialize_to(payload, data.data(), data.size());
    return result == struct_pack::errc::ok;
}

bool DeserializeP2PPayload(const std::string& data,
                           RemoveReplicaPayload& payload) {
    auto result =
        struct_pack::deserialize_to(payload, data.data(), data.size());
    return result == struct_pack::errc::ok;
}

bool DeserializeP2PPayload(const std::string& data,
                           MountSegmentPayload& payload) {
    auto result =
        struct_pack::deserialize_to(payload, data.data(), data.size());
    return result == struct_pack::errc::ok;
}

bool DeserializeP2PPayload(const std::string& data,
                           UnmountSegmentPayload& payload) {
    auto result =
        struct_pack::deserialize_to(payload, data.data(), data.size());
    return result == struct_pack::errc::ok;
}

}  // namespace mooncake