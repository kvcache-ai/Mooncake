#pragma once

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace mooncake {

// NVMe KV commands address values by a binary key. Mooncake derives a
// fixed-width 16-byte physical key from the logical object key so every ioctl
// command can use the same compact key format, independent of logical key
// length. Header verification keeps the original logical key binding separate
// from the device key and protects against hash collisions or stale catalog
// entries.
using NvmeKvPhysicalKey = std::array<uint8_t, 16>;

uint32_t ComputeNvmeKvChecksum(std::span<const uint8_t> data);
std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(const std::string& key);
std::string NvmeKvPhysicalKeyToHex(const NvmeKvPhysicalKey& physical_key);
bool ParseNvmeKvPhysicalKeyHex(std::string_view physical_key_hex,
                               NvmeKvPhysicalKey& physical_key);

// Root objects use a hash of the logical key. Large-object chunks derive
// independent physical keys from the logical key plus chunk index so chunks do
// not collide with the root manifest or with each other. All 16 bytes are used;
// device-specific interpretation is left to the NVMe KV device implementation.
NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const std::string& key);
NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(const std::string& key,
                                               uint32_t chunk_index);

}  // namespace mooncake
