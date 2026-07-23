#include "nvme_kv_key_codec.h"

#include <cstring>

#include <xxhash.h>

namespace mooncake {
namespace {

enum class NvmeKvCanonicalFieldTag : uint8_t {
    kLogicalKey = 1,
    kTenantId = 2,
    kDomainId = 3,
    kNamespaceId = 4,
    kPurpose = 10,
    kChunkIndex = 11,
    kSlotIndex = 12,
};

constexpr uint8_t kNvmeKvCanonicalIdentityEncodingVersion = 1;

void AppendU8(std::string& out, uint8_t value) { out.push_back(value); }

void AppendU32(std::string& out, uint32_t value) {
    const size_t offset = out.size();
    out.resize(offset + sizeof(value));
    std::memcpy(out.data() + offset, &value, sizeof(value));
}

void AppendTaggedBytes(std::string& out, NvmeKvCanonicalFieldTag tag,
                       std::string_view value) {
    AppendU8(out, static_cast<uint8_t>(tag));
    AppendU32(out, static_cast<uint32_t>(value.size()));
    out.append(value);
}

void AppendTaggedU32(std::string& out, NvmeKvCanonicalFieldTag tag,
                     uint32_t value) {
    AppendU8(out, static_cast<uint8_t>(tag));
    AppendU32(out, sizeof(value));
    AppendU32(out, value);
}

bool ReadTaggedField(std::string_view encoded, size_t& offset,
                     NvmeKvCanonicalFieldTag expected_tag,
                     std::string_view& value) {
    if (offset + 1 + sizeof(uint32_t) > encoded.size()) {
        return false;
    }
    const auto tag = static_cast<NvmeKvCanonicalFieldTag>(encoded[offset]);
    offset += 1;
    uint32_t size = 0;
    std::memcpy(&size, encoded.data() + offset, sizeof(size));
    offset += sizeof(size);
    if (tag != expected_tag || offset + size > encoded.size()) {
        return false;
    }
    value = encoded.substr(offset, size);
    offset += size;
    return true;
}

bool ReadTaggedU32(std::string_view encoded, size_t& offset,
                   NvmeKvCanonicalFieldTag expected_tag, uint32_t& value) {
    std::string_view bytes;
    if (!ReadTaggedField(encoded, offset, expected_tag, bytes) ||
        bytes.size() != sizeof(value)) {
        return false;
    }
    std::memcpy(&value, bytes.data(), sizeof(value));
    return true;
}

std::string SerializeNvmeKvDerivationInput(
    const NvmeKvObjectIdentity& identity, std::string_view purpose,
    uint32_t slot, std::optional<uint32_t> chunk_index) {
    std::string encoded = SerializeNvmeKvCanonicalIdentity(identity);
    AppendTaggedBytes(encoded, NvmeKvCanonicalFieldTag::kPurpose, purpose);
    if (chunk_index.has_value()) {
        AppendTaggedU32(encoded, NvmeKvCanonicalFieldTag::kChunkIndex,
                        *chunk_index);
    }
    AppendTaggedU32(encoded, NvmeKvCanonicalFieldTag::kSlotIndex, slot);
    return encoded;
}

NvmeKvPhysicalKey HashIdentitySeed128(std::string_view seed0,
                                      std::string_view seed1) {
    NvmeKvPhysicalKey physical_key{};
    const uint64_t h0 = XXH64(seed0.data(), seed0.size(), 0);
    const uint64_t h1 = XXH64(seed1.data(), seed1.size(), 0);
    std::memcpy(physical_key.data(), &h0, sizeof(h0));
    std::memcpy(physical_key.data() + 8, &h1, sizeof(h1));
    return physical_key;
}

}  // namespace

uint32_t ComputeNvmeKvChecksum(std::span<const uint8_t> data) {
    return static_cast<uint32_t>(XXH32(data.data(), data.size(), 0));
}

std::string SerializeNvmeKvCanonicalIdentity(
    const NvmeKvObjectIdentity& identity) {
    std::string encoded;
    AppendU8(encoded, kNvmeKvCanonicalIdentityEncodingVersion);
    AppendTaggedBytes(encoded, NvmeKvCanonicalFieldTag::kLogicalKey,
                      identity.logical_key);
    AppendTaggedBytes(encoded, NvmeKvCanonicalFieldTag::kTenantId,
                      identity.isolation.tenant_id);
    AppendTaggedBytes(encoded, NvmeKvCanonicalFieldTag::kDomainId,
                      identity.isolation.domain_id);
    AppendTaggedBytes(encoded, NvmeKvCanonicalFieldTag::kNamespaceId,
                      identity.isolation.namespace_id);
    return encoded;
}

bool ParseNvmeKvCanonicalIdentity(std::string_view encoded_identity,
                                  NvmeKvObjectIdentity& identity) {
    identity = {};
    if (encoded_identity.empty() ||
        static_cast<uint8_t>(encoded_identity.front()) !=
            kNvmeKvCanonicalIdentityEncodingVersion) {
        return false;
    }

    size_t offset = 1;
    std::string_view logical_key;
    std::string_view tenant_id;
    std::string_view domain_id;
    std::string_view namespace_id;
    if (!ReadTaggedField(encoded_identity, offset,
                         NvmeKvCanonicalFieldTag::kLogicalKey, logical_key) ||
        !ReadTaggedField(encoded_identity, offset,
                         NvmeKvCanonicalFieldTag::kTenantId, tenant_id) ||
        !ReadTaggedField(encoded_identity, offset,
                         NvmeKvCanonicalFieldTag::kDomainId, domain_id) ||
        !ReadTaggedField(encoded_identity, offset,
                         NvmeKvCanonicalFieldTag::kNamespaceId, namespace_id) ||
        offset != encoded_identity.size()) {
        return false;
    }

    identity.logical_key = std::string(logical_key);
    identity.isolation.tenant_id = std::string(tenant_id);
    identity.isolation.domain_id = std::string(domain_id);
    identity.isolation.namespace_id = std::string(namespace_id);
    return true;
}

std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(
    const NvmeKvObjectIdentity& identity) {
    std::array<uint8_t, 32> hash{};
    const std::string encoded_identity =
        SerializeNvmeKvCanonicalIdentity(identity);
    const std::string seed0 = encoded_identity + std::string("\0", 1);
    const std::string seed1 = encoded_identity + std::string("\1", 1);
    const std::string seed2 = encoded_identity + std::string("\2", 1);
    const std::string seed3 = encoded_identity + std::string("\3", 1);
    const uint64_t h0 = XXH64(seed0.data(), seed0.size(), 0);
    const uint64_t h1 = XXH64(seed1.data(), seed1.size(), 0);
    const uint64_t h2 = XXH64(seed2.data(), seed2.size(), 0);
    const uint64_t h3 = XXH64(seed3.data(), seed3.size(), 0);
    std::memcpy(hash.data(), &h0, sizeof(h0));
    std::memcpy(hash.data() + 8, &h1, sizeof(h1));
    std::memcpy(hash.data() + 16, &h2, sizeof(h2));
    std::memcpy(hash.data() + 24, &h3, sizeof(h3));
    return hash;
}

std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(const std::string& key) {
    return ComputeNvmeKvVerifyHash(
        NvmeKvObjectIdentity{.logical_key = key, .isolation = {}});
}

void NormalizeNvmeKvPhysicalKey(NvmeKvPhysicalKey& physical_key) {
    static_cast<void>(physical_key);
}

NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const NvmeKvObjectIdentity& identity,
                                          uint32_t slot) {
    return HashIdentitySeed128(
        SerializeNvmeKvDerivationInput(identity, "pk:0", slot, std::nullopt),
        SerializeNvmeKvDerivationInput(identity, "pk:1", slot, std::nullopt));
}

NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const std::string& key,
                                          uint32_t slot) {
    return EncodeNvmeKvPhysicalKey(
        NvmeKvObjectIdentity{.logical_key = key, .isolation = {}}, slot);
}

NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(
    const NvmeKvObjectIdentity& identity, uint32_t chunk_index, uint32_t slot) {
    return HashIdentitySeed128(
        SerializeNvmeKvDerivationInput(identity, "chunk:0", slot, chunk_index),
        SerializeNvmeKvDerivationInput(identity, "chunk:1", slot, chunk_index));
}

NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(const std::string& key,
                                               uint32_t chunk_index,
                                               uint32_t slot) {
    return EncodeNvmeKvChunkPhysicalKey(
        NvmeKvObjectIdentity{.logical_key = key, .isolation = {}}, chunk_index,
        slot);
}

std::optional<uint32_t> ResolveNvmeKvPhysicalKeySlot(
    const NvmeKvObjectIdentity& identity, const NvmeKvPhysicalKey& physical_key,
    uint32_t max_slots) {
    for (uint32_t slot = 0; slot < max_slots; ++slot) {
        if (EncodeNvmeKvPhysicalKey(identity, slot) == physical_key) {
            return slot;
        }
    }
    return std::nullopt;
}

std::optional<uint32_t> ResolveNvmeKvChunkPhysicalKeySlot(
    const NvmeKvObjectIdentity& identity, uint32_t chunk_index,
    const NvmeKvPhysicalKey& physical_key, uint32_t max_slots) {
    for (uint32_t slot = 0; slot < max_slots; ++slot) {
        if (EncodeNvmeKvChunkPhysicalKey(identity, chunk_index, slot) ==
            physical_key) {
            return slot;
        }
    }
    return std::nullopt;
}

}  // namespace mooncake
