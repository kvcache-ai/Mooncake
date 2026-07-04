#include "mooncake_pg_device_bootstrap.h"

#include <cstring>

#include <torch/torch.h>

namespace mooncake {

std::string serverHostOnly(const std::string& server_name) {
    const auto pos = server_name.rfind(':');
    if (pos == std::string::npos) return server_name;
    return server_name.substr(0, pos);
}

std::string deviceCollectiveP2pKey(int backendIndex, int rank) {
    return "mooncake_pg_device_collective_p2p/" + std::to_string(backendIndex) +
           "/" + std::to_string(rank);
}

std::string deviceCollectiveRdmaKey(int backendIndex, int rank) {
    return "mooncake_pg_device_collective_rdma/" +
           std::to_string(backendIndex) + "/" + std::to_string(rank);
}

template <typename T>
static void appendRdmaBytes(std::vector<uint8_t>& out, const T& value) {
    const auto* ptr = reinterpret_cast<const uint8_t*>(&value);
    out.insert(out.end(), ptr, ptr + sizeof(T));
}

template <typename T>
static T readRdmaBytes(const std::vector<uint8_t>& in, size_t& offset) {
    TORCH_CHECK(offset + sizeof(T) <= in.size(),
                "Invalid Mooncake PG Device API RDMA metadata.");
    T value;
    std::memcpy(&value, in.data() + offset, sizeof(T));
    offset += sizeof(T);
    return value;
}

std::vector<uint8_t> serializeRdmaMetadata(
    const device::RdmaLocalMetadata& meta) {
    std::vector<uint8_t> out;
    appendRdmaBytes(out, meta.raddr);
    appendRdmaBytes(out, meta.rkey);
    appendRdmaBytes(out, meta.subnet_prefix);
    appendRdmaBytes(out, meta.interface_id);
    const uint32_t qp_count = static_cast<uint32_t>(meta.qpns.size());
    const uint32_t lid_count = static_cast<uint32_t>(meta.lids.size());
    appendRdmaBytes(out, qp_count);
    appendRdmaBytes(out, lid_count);
    for (int32_t qpn : meta.qpns) appendRdmaBytes(out, qpn);
    for (int32_t lid : meta.lids) appendRdmaBytes(out, lid);
    return out;
}

device::RdmaLocalMetadata deserializeRdmaMetadata(
    const std::vector<uint8_t>& bytes) {
    size_t offset = 0;
    device::RdmaLocalMetadata meta;
    meta.raddr = readRdmaBytes<int64_t>(bytes, offset);
    meta.rkey = readRdmaBytes<int32_t>(bytes, offset);
    meta.subnet_prefix = readRdmaBytes<int64_t>(bytes, offset);
    meta.interface_id = readRdmaBytes<int64_t>(bytes, offset);
    const uint32_t qp_count = readRdmaBytes<uint32_t>(bytes, offset);
    const uint32_t lid_count = readRdmaBytes<uint32_t>(bytes, offset);
    meta.qpns.reserve(qp_count);
    meta.lids.reserve(lid_count);
    for (uint32_t i = 0; i < qp_count; ++i) {
        meta.qpns.push_back(readRdmaBytes<int32_t>(bytes, offset));
    }
    for (uint32_t i = 0; i < lid_count; ++i) {
        meta.lids.push_back(readRdmaBytes<int32_t>(bytes, offset));
    }
    TORCH_CHECK(offset == bytes.size(),
                "Unexpected trailing Mooncake PG Device API RDMA metadata.");
    return meta;
}

}  // namespace mooncake
