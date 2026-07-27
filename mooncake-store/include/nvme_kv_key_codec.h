#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace mooncake {

using NvmeKvPhysicalKey = std::array<uint8_t, 16>;

struct NvmeKvObjectIdentity {
    std::string logical_key;
};

constexpr uint32_t kNvmeKvMaxPhysicalKeySlots = 64;

uint32_t ComputeNvmeKvChecksum(std::span<const uint8_t> data);
std::string SerializeNvmeKvCanonicalIdentity(
    const NvmeKvObjectIdentity& identity);
bool ParseNvmeKvCanonicalIdentity(std::string_view encoded_identity,
                                  NvmeKvObjectIdentity& identity);
std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(
    const NvmeKvObjectIdentity& identity);
NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const NvmeKvObjectIdentity& identity,
                                          uint32_t slot = 0);
NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(
    const NvmeKvObjectIdentity& identity, uint32_t chunk_index,
    uint32_t slot = 0);

}  // namespace mooncake
