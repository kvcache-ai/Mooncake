#include "ha/oplog/p2p_oplog_types.h"

#include <string>

#include <ylt/struct_pack.hpp>

namespace mooncake {

std::string SerializeP2PPayload(const RegisterClientPayload& payload) {
    return struct_pack::serialize<std::string>(payload);
}

std::string SerializeP2PPayload(const AddReplicaPayload& payload) {
    return struct_pack::serialize<std::string>(payload);
}

std::string SerializeP2PPayload(const RemoveReplicaPayload& payload) {
    return struct_pack::serialize<std::string>(payload);
}

std::string SerializeP2PPayload(const MountSegmentPayload& payload) {
    return struct_pack::serialize<std::string>(payload);
}

std::string SerializeP2PPayload(const UnmountSegmentPayload& payload) {
    return struct_pack::serialize<std::string>(payload);
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