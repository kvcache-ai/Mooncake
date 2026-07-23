#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace mooncake {

using NvmeKvPhysicalKey = std::array<uint8_t, 16>;

struct NvmeKvIsolationContext {
    std::string tenant_id;
    std::string domain_id;
    std::string namespace_id;

    [[nodiscard]] bool Empty() const {
        return tenant_id.empty() && domain_id.empty() && namespace_id.empty();
    }
};

enum class NvmeKvObjectRole : uint32_t {
    kUnknown = 0,
    kRootInline = 1,
    kRootManifest = 2,
    kChunk = 3,
    kShard = 4,
};

struct NvmeKvObjectIdentity {
    std::string logical_key;
    NvmeKvIsolationContext isolation;
    [[nodiscard]] bool HasIsolationContext() const {
        return !isolation.Empty();
    }
};

constexpr uint32_t kNvmeKvMaxPhysicalKeySlots = 64;

uint32_t ComputeNvmeKvChecksum(std::span<const uint8_t> data);
std::string SerializeNvmeKvCanonicalIdentity(
    const NvmeKvObjectIdentity& identity);
bool ParseNvmeKvCanonicalIdentity(std::string_view encoded_identity,
                                  NvmeKvObjectIdentity& identity);
std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(
    const NvmeKvObjectIdentity& identity);
std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(const std::string& key);
void NormalizeNvmeKvPhysicalKey(NvmeKvPhysicalKey& physical_key);
NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const NvmeKvObjectIdentity& identity,
                                          uint32_t slot = 0);
NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const std::string& key,
                                          uint32_t slot = 0);
NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(
    const NvmeKvObjectIdentity& identity, uint32_t chunk_index,
    uint32_t slot = 0);
NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(const std::string& key,
                                               uint32_t chunk_index,
                                               uint32_t slot = 0);
std::optional<uint32_t> ResolveNvmeKvPhysicalKeySlot(
    const NvmeKvObjectIdentity& identity, const NvmeKvPhysicalKey& physical_key,
    uint32_t max_slots = kNvmeKvMaxPhysicalKeySlots);
std::optional<uint32_t> ResolveNvmeKvChunkPhysicalKeySlot(
    const NvmeKvObjectIdentity& identity, uint32_t chunk_index,
    const NvmeKvPhysicalKey& physical_key,
    uint32_t max_slots = kNvmeKvMaxPhysicalKeySlots);

}  // namespace mooncake
