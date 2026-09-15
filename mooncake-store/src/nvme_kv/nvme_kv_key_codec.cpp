#include "nvme_kv_key_codec.h"

#include <cstring>

#include <xxhash.h>

namespace mooncake {
namespace {

constexpr uint8_t kIdentityEncodingVersion = 1;
constexpr uint8_t kRootKeyRole = 1;
constexpr uint8_t kChunkKeyRole = 2;

void AppendU32(std::string& encoded, uint32_t value) {
    const size_t offset = encoded.size();
    encoded.resize(offset + sizeof(value));
    std::memcpy(encoded.data() + offset, &value, sizeof(value));
}

bool ReadU32(std::string_view encoded, size_t& offset, uint32_t& value) {
    if (offset + sizeof(value) > encoded.size()) {
        return false;
    }
    std::memcpy(&value, encoded.data() + offset, sizeof(value));
    offset += sizeof(value);
    return true;
}

std::string BuildDerivationInput(const NvmeKvObjectIdentity& identity,
                                 uint8_t role, uint32_t slot,
                                 std::optional<uint32_t> chunk_index) {
    std::string encoded = SerializeNvmeKvCanonicalIdentity(identity);
    encoded.push_back(static_cast<char>(role));
    AppendU32(encoded, slot);
    encoded.push_back(chunk_index.has_value() ? 1 : 0);
    if (chunk_index) {
        AppendU32(encoded, *chunk_index);
    }
    return encoded;
}

NvmeKvPhysicalKey DerivePhysicalKey(std::string_view encoded) {
    NvmeKvPhysicalKey physical_key{};
    const uint64_t low = XXH64(encoded.data(), encoded.size(), 0);
    const uint64_t high = XXH64(encoded.data(), encoded.size(), 1);
    std::memcpy(physical_key.data(), &low, sizeof(low));
    std::memcpy(physical_key.data() + sizeof(low), &high, sizeof(high));
    return physical_key;
}

}  // namespace

uint32_t ComputeNvmeKvChecksum(std::span<const uint8_t> data) {
    return static_cast<uint32_t>(XXH32(data.data(), data.size(), 0));
}

std::string SerializeNvmeKvCanonicalIdentity(
    const NvmeKvObjectIdentity& identity) {
    std::string encoded(1, static_cast<char>(kIdentityEncodingVersion));
    AppendU32(encoded, static_cast<uint32_t>(identity.logical_key.size()));
    encoded.append(identity.logical_key);
    return encoded;
}

bool ParseNvmeKvCanonicalIdentity(std::string_view encoded,
                                  NvmeKvObjectIdentity& identity) {
    identity = {};
    if (encoded.empty() ||
        static_cast<uint8_t>(encoded.front()) != kIdentityEncodingVersion) {
        return false;
    }
    size_t offset = 1;
    uint32_t key_size = 0;
    if (!ReadU32(encoded, offset, key_size) ||
        key_size != encoded.size() - offset) {
        return false;
    }
    identity.logical_key = std::string(encoded.substr(offset));
    return true;
}

std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(
    const NvmeKvObjectIdentity& identity) {
    const std::string encoded = SerializeNvmeKvCanonicalIdentity(identity);
    std::array<uint8_t, 32> hash{};
    for (uint64_t seed = 0; seed < 4; ++seed) {
        const uint64_t value = XXH64(encoded.data(), encoded.size(), seed);
        std::memcpy(hash.data() + seed * sizeof(value), &value, sizeof(value));
    }
    return hash;
}

NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const NvmeKvObjectIdentity& identity,
                                          uint32_t slot) {
    return DerivePhysicalKey(
        BuildDerivationInput(identity, kRootKeyRole, slot, std::nullopt));
}

NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(
    const NvmeKvObjectIdentity& identity, uint32_t chunk_index, uint32_t slot) {
    return DerivePhysicalKey(
        BuildDerivationInput(identity, kChunkKeyRole, slot, chunk_index));
}

}  // namespace mooncake
