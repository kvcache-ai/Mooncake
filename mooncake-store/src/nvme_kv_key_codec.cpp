#include "nvme_kv_key_codec.h"

#include <cstring>
#include <iomanip>
#include <sstream>

#include <xxhash.h>

namespace mooncake {

uint32_t ComputeNvmeKvChecksum(std::span<const uint8_t> data) {
    return static_cast<uint32_t>(XXH32(data.data(), data.size(), 0));
}

std::array<uint8_t, 32> ComputeNvmeKvVerifyHash(const std::string& key) {
    std::array<uint8_t, 32> hash{};
    const std::string seed0 = "verify:0:" + key;
    const std::string seed1 = "verify:1:" + key;
    const std::string seed2 = "verify:2:" + key;
    const std::string seed3 = "verify:3:" + key;
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

std::string NvmeKvPhysicalKeyToHex(const NvmeKvPhysicalKey& physical_key) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (uint8_t b : physical_key) {
        oss << std::setw(2) << static_cast<int>(b);
    }
    return oss.str();
}

bool ParseNvmeKvPhysicalKeyHex(std::string_view physical_key_hex,
                               NvmeKvPhysicalKey& physical_key) {
    if (physical_key_hex.size() != physical_key.size() * 2) {
        return false;
    }
    for (size_t i = 0; i < physical_key.size(); ++i) {
        const auto hi = physical_key_hex[i * 2];
        const auto lo = physical_key_hex[i * 2 + 1];
        const auto hex_value = [](char ch) -> int {
            if (ch >= '0' && ch <= '9') return ch - '0';
            if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
            if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
            return -1;
        };
        const int high = hex_value(hi);
        const int low = hex_value(lo);
        if (high < 0 || low < 0) {
            return false;
        }
        physical_key[i] = static_cast<uint8_t>((high << 4) | low);
    }
    return true;
}

NvmeKvPhysicalKey EncodeNvmeKvPhysicalKey(const std::string& key) {
    NvmeKvPhysicalKey physical_key{};
    const std::string seed0 = "pk:0:" + key;
    const std::string seed1 = "pk:1:" + key;
    const uint64_t h0 = XXH64(seed0.data(), seed0.size(), 0);
    const uint64_t h1 = XXH64(seed1.data(), seed1.size(), 0);
    std::memcpy(physical_key.data(), &h0, sizeof(h0));
    std::memcpy(physical_key.data() + 8, &h1, sizeof(h1));
    return physical_key;
}

NvmeKvPhysicalKey EncodeNvmeKvChunkPhysicalKey(const std::string& key,
                                               uint32_t chunk_index) {
    NvmeKvPhysicalKey physical_key{};
    const std::string chunk_suffix = key + ":" + std::to_string(chunk_index);
    const std::string seed0 = "chunk:0:" + chunk_suffix;
    const std::string seed1 = "chunk:1:" + chunk_suffix;
    const uint64_t h0 = XXH64(seed0.data(), seed0.size(), 0);
    const uint64_t h1 = XXH64(seed1.data(), seed1.size(), 0);
    std::memcpy(physical_key.data(), &h0, sizeof(h0));
    std::memcpy(physical_key.data() + 8, &h1, sizeof(h1));
    return physical_key;
}

}  // namespace mooncake
